package com.uplus.batch.domain.extraction.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;

import com.uplus.batch.domain.extraction.entity.ExcellentEventStatus;
import com.uplus.batch.domain.extraction.entity.ResultEventStatus;
import com.uplus.batch.domain.extraction.repository.ExcellentEventStatusRepository;
import com.uplus.batch.domain.extraction.repository.ResultEventStatusRepository;
import com.uplus.batch.domain.extraction.scheduler.IntegratedAnalysisScheduler;

@ExtendWith(MockitoExtension.class)
class ExtractionBusinessTest {

    // 🚀 스케줄러가 사용하는 필드 3개를 모두 Mock으로 선언해야 합니다.
    @Mock private ResultEventStatusRepository resultEventRepository;
    @Mock private ExcellentEventStatusRepository excellentEventRepository;
    @Mock private ConsultationAnalysisManager analysisManager;
    
    @InjectMocks
    private IntegratedAnalysisScheduler scheduler;

    private ResultEventStatus testTask;
    private ExcellentEventStatus testScoringTask;

    @BeforeEach
    void setUp() {
        testTask = ResultEventStatus.builder()
                .consultId(300L)
                .categoryCode("M_OTB_001")
                .build();

        testScoringTask = new ExcellentEventStatus(300L);
    }

    @Test
    @DisplayName("성공: 대기 중인 태스크 쌍을 찾아 매니저에게 번들 분석을 요청한다")
    void executeIntegratedAnalysis_Success() {
        // Given: 50개를 가져왔을 때 1건의 대기 데이터가 있다고 가정
        given(resultEventRepository.findReadyToProcessPairs(any(Pageable.class)))
                .willReturn(List.of(testTask));
        
        // 짝이 맞는 채점 태스크도 존재한다고 가정
        given(excellentEventRepository.findByConsultId(300L))
                .willReturn(Optional.of(testScoringTask));

        // When
        scheduler.executeIntegratedAnalysis();

        // Then: 매니저의 번들 처리 메서드가 최소 1번 호출되었는지 검증
        verify(analysisManager, times(1)).processIntegratedBundledTasks(anyList());
    }

    @Test
    @DisplayName("실패: 처리할 대기 데이터가 없으면 즉시 종료한다")
    void executeIntegratedAnalysis_NoData_Return() {
        // Given: 대기 데이터가 빈 리스트일 때
        given(resultEventRepository.findReadyToProcessPairs(any(Pageable.class)))
                .willReturn(Collections.emptyList());

        // When
        scheduler.executeIntegratedAnalysis();

        // Then: 매니저가 호출되지 않아야 함
        verify(analysisManager, never()).processIntegratedBundledTasks(anyList());
    }

    @Test
    @DisplayName("실패: 요약 태스크는 있으나 짝궁 채점 태스크가 없으면 제외하고 진행한다")
    void executeIntegratedAnalysis_NoPair_Skip() {
        // Given: 요약 태스크는 있지만
        given(resultEventRepository.findReadyToProcessPairs(any(Pageable.class)))
                .willReturn(List.of(testTask));
        
        // 짝꿍 채점 태스크가 존재하지 않을 때
        given(excellentEventRepository.findByConsultId(300L))
                .willReturn(Optional.empty());

        // When
        scheduler.executeIntegratedAnalysis();

        // Then: 유효한 짝(TaskPair)이 없으므로 매니저는 호출되지 않음
        verify(analysisManager, never()).processIntegratedBundledTasks(anyList());
    }
}