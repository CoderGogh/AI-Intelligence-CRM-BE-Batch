package com.uplus.batch.domain.extraction.scheduler;

import java.util.List;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.uplus.batch.domain.extraction.entity.EventStatus;
import com.uplus.batch.domain.extraction.entity.ExcellentEventStatus;
import com.uplus.batch.domain.extraction.entity.ResultEventStatus;
import com.uplus.batch.domain.extraction.repository.ExcellentEventStatusRepository;
import com.uplus.batch.domain.extraction.repository.ResultEventStatusRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class IntegratedRetryScheduler {

    private final ResultEventStatusRepository resultEventRepository;
    private final ExcellentEventStatusRepository scoringEventRepository;

    /**
     * 1시간마다 실패한(FAILED) 태스크들을 다시 REQUESTED로 변경
     */
    @Scheduled(fixedDelay = 3600000) 
    @Transactional
    public void retryAllFailedTasks() {
        log.info("[Retry] 통합 재배치 엔진 가동...");

        // 1. 요약 실패 복구
        List<ResultEventStatus> failedSummaries = 
        		resultEventRepository.findByStatusAndRetryCountLessThan(EventStatus.FAILED, 3);
        failedSummaries.forEach(ResultEventStatus::retry);

        // 2. 채점 실패 복구
        List<ExcellentEventStatus> failedScoring = 
            scoringEventRepository.findByStatusAndRetryCountLessThan(EventStatus.FAILED, 3);
        failedScoring.forEach(ExcellentEventStatus::retry);

        log.info("[Retry] 복구 완료 - 요약: {}건, 채점: {}건", 
            failedSummaries.size(), failedScoring.size());
    }
}