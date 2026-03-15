package com.uplus.batch.domain.extraction.service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.domain.extraction.dto.AiExtractionResponse;
import com.uplus.batch.domain.extraction.dto.QualityScoringResponse;
import com.uplus.batch.domain.extraction.entity.*;
import com.uplus.batch.domain.extraction.repository.*;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class ConsultationAnalysisManager {

    private final GeminiExtractor geminiExtractor;
    private final GeminiQualityScorer geminiQualityScorer;
    private final ConsultationRawTextRepository rawTextRepository;
    private final ConsultationExtractionRepository extractionRepository;
    private final ConsultationEvaluationRepository evaluationRepository;
    private final ResultEventStatusRepository resultEventRepository;
    private final ExcellentEventStatusRepository excellentEventRepository;
    private final ManualRepository manualRepository;
    private final AnalysisCodeRepository analysisCodeRepository;
    private final ObjectMapper objectMapper;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processIntegratedBundledTasks(List<TaskPair> taskPairs) {
        if (taskPairs == null || taskPairs.isEmpty()) return;

        /* [STEP 1] 모든 태스크 상태 선점 (DB 락 방지 및 상태 처리) */
        taskPairs.forEach(pair -> { pair.summaryTask().start(); pair.scoringTask().start(); });
        resultEventRepository.saveAll(taskPairs.stream().map(TaskPair::summaryTask).toList());
        excellentEventRepository.saveAll(taskPairs.stream().map(TaskPair::scoringTask).toList());

        /* [STEP 2] 성격별 그룹화 및 프롬프트 코드 구성 */
        Map<String, List<TaskPair>> groups = taskPairs.stream()
                .collect(Collectors.groupingBy(pair -> getGroupType(pair.summaryTask().getCategoryCode())));
        Map<String, String> validCodes = getAnalysisCodesFromDb();

        groups.forEach((groupType, pairs) -> {
            try {
                log.info("[Batch] {} 그룹 분석 시작 ({}건)", groupType, pairs.size());

                List<ConsultationRawText> rawDataList = pairs.stream()
                        .map(p -> rawTextRepository.findByConsultId(p.summaryTask().getConsultId())
                                .orElseThrow(() -> new RuntimeException("원문 없음")))
                        .toList();

                String manualContent = manualRepository.findByCategoryCodeAndIsActiveTrue(pairs.get(0).summaryTask().getCategoryCode())
                        .map(Manual::getContent).orElse("기본 매뉴얼");

                /* [STEP 3] AI 비동기 실행: 요약은 번들(1회), 채점은 병렬(N회) */
                CompletableFuture<List<AiExtractionResponse>> extractionFuture = CompletableFuture.supplyAsync(() ->
                        geminiExtractor.extractBatch(rawDataList.stream().map(ConsultationRawText::getRawTextJson).toList(), groupType, validCodes)
                );

                List<CompletableFuture<QualityScoringResponse>> scoringFutures = rawDataList.stream()
                        .map(raw -> CompletableFuture.supplyAsync(() -> geminiQualityScorer.evaluate(raw.getRawTextJson(), manualContent))
                                .handle((res, ex) -> {
                                    if (ex != null) { return null; } // 개별 채점 실패 시 null 반환 (격리)
                                    return res;
                                })
                        ).toList();

                /* [STEP 4] 결과 수집: 번들 요약 결과 대기 및 개수 검증 */
                List<AiExtractionResponse> extractionResults = extractionFuture.join();
                if (extractionResults.size() != pairs.size()) {
                    throw new RuntimeException("AI 응답 개수 불일치");
                }

                /* [STEP 5] 개별 저장 루프: 채점 낙오자가 있어도 나머지는 정상 처리 */
                for (int i = 0; i < pairs.size(); i++) {
                    TaskPair pair = pairs.get(i);
                    Long cId = pair.summaryTask().getConsultId();

                    try {
                        QualityScoringResponse scoreRes = scoringFutures.get(i).join();

                        if (scoreRes == null) { // AI 채점만 실패한 경우 처리
                            pair.summaryTask().fail("채점 API 호출 실패");
                            pair.scoringTask().fail("채점 API 호출 실패");
                            continue; 
                        }

                        saveExtraction(cId, extractionResults.get(i));
                        saveEvaluation(pair.scoringTask().getConsultId(), scoreRes);

                        pair.summaryTask().complete();
                        pair.scoringTask().complete();

                    } catch (Exception e) {
                        pair.summaryTask().fail("저장 오류: " + truncate(e.getMessage()));
                        pair.scoringTask().fail("저장 오류: " + truncate(e.getMessage()));
                    }
                }

            } catch (Exception e) {
                /* [STEP 6] 그룹 치명적 에러: 번들 요약 실패 시 해당 그룹 전원 실패 처리 */
                log.error("[Group Error] {} 실패: {}", groupType, e.getMessage());
                pairs.forEach(p -> {
                    p.summaryTask().fail("번들링 실패: " + truncate(e.getMessage()));
                    p.scoringTask().fail("번들링 실패: " + truncate(e.getMessage()));
                });
            }
        });

        /* [STEP 7] 최종 상태 DB 동기화 */
        resultEventRepository.saveAll(taskPairs.stream().map(TaskPair::summaryTask).toList());
        excellentEventRepository.saveAll(taskPairs.stream().map(TaskPair::scoringTask).toList());
    }

    private String getGroupType(String code) {
        if (code.contains("OTB")) return "OUTBOUND";
        if (code.contains("CHN")) return "INBOUND_CHN";
        return "INBOUND_NORMAL";
    }

    private Map<String, String> getAnalysisCodesFromDb() {
        List<String> targets = List.of("complaint_category", "defense_category", "outbound_category");
        
        return analysisCodeRepository.findAllByClassificationIn(targets).stream()
                .collect(Collectors.groupingBy(
                        AnalysisCode::getClassification,
                        Collectors.mapping(
                            code -> code.getCodeName() + "(" + code.getDescription() + ")", 
                            Collectors.joining(", ")
                        )
                ));
    }

    private void saveExtraction(Long id, AiExtractionResponse res) throws Exception {
        ConsultationExtraction entity = ConsultationExtraction.builder()
                .consultId(id)
                .res(res)
                .actionsJson(objectMapper.writeValueAsString(res.defense_actions()))
                .build();
        extractionRepository.save(entity);
    }

    private void saveEvaluation(Long id, QualityScoringResponse res) {
        evaluationRepository.save(ConsultationEvaluation.builder()
                .consultId(id)
                .score(res.score())
                .evaluationReason(res.evaluation_reason())
                .isCandidate(res.is_candidate())
                .build());
    }

    private String truncate(String msg) {
        return (msg != null && msg.length() > 200) ? msg.substring(0, 200) + "..." : msg;
    }

    public record TaskPair(ResultEventStatus summaryTask, ExcellentEventStatus scoringTask) {}
}