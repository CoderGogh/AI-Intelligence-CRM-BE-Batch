package com.uplus.batch.domain.extraction.scheduler;

import java.util.List;

import org.springframework.data.domain.PageRequest; // 추가
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.uplus.batch.domain.extraction.entity.ResultEventStatus;
import com.uplus.batch.domain.extraction.entity.ExcellentEventStatus;
import com.uplus.batch.domain.extraction.repository.ExcellentEventStatusRepository;
import com.uplus.batch.domain.extraction.repository.ResultEventStatusRepository;
import com.uplus.batch.domain.extraction.service.ConsultationAnalysisManager;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class IntegratedAnalysisScheduler {

    private final ResultEventStatusRepository resultEventRepository;
    private final ExcellentEventStatusRepository excellentEventRepository;
    private final ConsultationAnalysisManager analysisManager;

    @Scheduled(fixedDelay = 60000)
    public void executeIntegratedAnalysis() {
        log.info("[Batch] 통합 분석 엔진 가동 - 분석 대기 데이터 확인 중...");

        // 1. JOIN 쿼리를 통해 추출/채점 모두 REQUESTED인 건만 50개 가져오기
        List<ResultEventStatus> pendingResults = 
            resultEventRepository.findReadyToProcessPairs(PageRequest.of(0, 50));

        if (pendingResults.isEmpty()) {
            return;
        }

        log.info("[Batch] 통합 분석 시작 - 준비된 쌍(Pair): {}건", pendingResults.size());

        for (ResultEventStatus resultTask : pendingResults) {
            try {
                excellentEventRepository.findByConsultId(resultTask.getConsultId())
                    .ifPresentOrElse(
                        excellentTask -> analysisManager.processIntegratedTask(resultTask, excellentTask),
                        () -> log.error("[Data Error] 상담 ID {} : JOIN 검색에는 잡혔으나 상세 조회에 실패했습니다.", resultTask.getConsultId())
                    );
                
            } catch (Exception e) {
                log.error("[Critical Error] 상담 ID {} 통합 처리 중 오류: {}", 
                    resultTask.getConsultId(), e.getMessage());
            }
        }
        log.info("[Batch] 통합 분석 프로세스 종료");
    }
}