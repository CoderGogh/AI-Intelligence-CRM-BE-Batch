package com.uplus.batch.domain.extraction.service;

import java.util.concurrent.CompletableFuture;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation; 
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.domain.extraction.dto.AiExtractionResponse;
import com.uplus.batch.domain.extraction.dto.QualityScoringResponse;
import com.uplus.batch.domain.extraction.entity.ConsultationEvaluation;
import com.uplus.batch.domain.extraction.entity.ConsultationExtraction;
import com.uplus.batch.domain.extraction.entity.ConsultationRawText;
import com.uplus.batch.domain.extraction.entity.ExcellentEventStatus;
import com.uplus.batch.domain.extraction.entity.Manual; 
import com.uplus.batch.domain.extraction.entity.ResultEventStatus;
import com.uplus.batch.domain.extraction.repository.ConsultationEvaluationRepository;
import com.uplus.batch.domain.extraction.repository.ConsultationExtractionRepository;
import com.uplus.batch.domain.extraction.repository.ConsultationRawTextRepository;
import com.uplus.batch.domain.extraction.repository.ExcellentEventStatusRepository; 
import com.uplus.batch.domain.extraction.repository.ResultEventStatusRepository;    
import com.uplus.batch.domain.extraction.repository.ManualRepository;
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
    private final ManualRepository manualRepository; 
    private final ObjectMapper objectMapper;
    private final ResultEventStatusRepository resultEventRepository;
    private final ExcellentEventStatusRepository excellentEventRepository;

    /**
     * 요약 추출과 우수 채점을 한 번의 원문 조회로 병렬 처리하는 통합 프로세서
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processIntegratedTask(ResultEventStatus summaryTask, ExcellentEventStatus scoringTask) {
        try {
            // 1. [선점] 두 이벤트 상태 모두 PROCESSING으로 변경 및 DB 즉시 반영
            summaryTask.start();
            scoringTask.start();
            resultEventRepository.save(summaryTask);     
            excellentEventRepository.save(scoringTask);   

            // 2. [조회] 원문 로드
            ConsultationRawText rawData = rawTextRepository.findByConsultId(summaryTask.getConsultId())
                    .orElseThrow(() -> new RuntimeException("원본 데이터를 찾을 수 없습니다."));

            log.info("[Batch] AI 통합 분석 시작 - ConsultID: {}", summaryTask.getConsultId());

            // 3. [비동기 병렬 호출]
            CompletableFuture<AiExtractionResponse> extractionFuture = CompletableFuture.supplyAsync(() -> 
                geminiExtractor.extract(rawData.getRawTextJson(), summaryTask.getCategoryCode().contains("CHN")));

            CompletableFuture<QualityScoringResponse> scoringFuture = CompletableFuture.supplyAsync(() -> {
                String manualContent = manualRepository.findByCategoryCodeAndIsActiveTrue(summaryTask.getCategoryCode())
                        .map(Manual::getContent)
                        .orElseThrow(() -> new RuntimeException("활성화된 매뉴얼을 찾을 수 없습니다: " + summaryTask.getCategoryCode()));
                
                return geminiQualityScorer.evaluate(rawData.getRawTextJson(), manualContent);
            });

            // 4. 대기
            CompletableFuture.allOf(extractionFuture, scoringFuture).join();

            // 5. [저장 및 완료] 요약 결과 저장 및 상태 COMPLETED 변경
            saveExtraction(summaryTask.getConsultId(), extractionFuture.get());
            summaryTask.complete();
            resultEventRepository.save(summaryTask);      

            // 채점 결과 저장 및 상태 COMPLETED 변경
            saveEvaluation(scoringTask.getConsultId(), scoringFuture.get());
            scoringTask.complete();
            excellentEventRepository.save(scoringTask);    

            log.info("[Success] 상담 ID {} 통합 분석 완료", summaryTask.getConsultId());

        } catch (Exception e) {
            log.error("[Critical Error] 통합 분석 실패 - ID {}: {}", summaryTask.getConsultId(), e.getMessage());
            String errorMsg = truncate(e.getMessage());
            
            // 6. [실패] 에러 발생 시 FAILED 마킹 및 DB 반영
            summaryTask.fail(errorMsg);
            scoringTask.fail(errorMsg);
            resultEventRepository.save(summaryTask);     
            excellentEventRepository.save(scoringTask);    
        }
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
        ConsultationEvaluation entity = ConsultationEvaluation.builder()
                .consultId(id)
                .score(res.score())
                .evaluationReason(res.evaluation_reason())
                .isCandidate(res.is_candidate())
                .build();
        evaluationRepository.save(entity);
    }

    private String truncate(String msg) { 
        return (msg != null && msg.length() > 200) ? msg.substring(0, 200) + "..." : msg; 
    }
}