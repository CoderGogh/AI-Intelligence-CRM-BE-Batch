package com.uplus.batch.domain.extraction.service;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;
import static org.assertj.core.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.domain.extraction.dto.*;
import com.uplus.batch.domain.extraction.entity.*;
import com.uplus.batch.domain.extraction.repository.*;
import com.uplus.batch.domain.extraction.service.ConsultationAnalysisManager.TaskPair;

@ExtendWith(MockitoExtension.class)
class ConsultationAnalysisManagerTest {

    @InjectMocks
    private ConsultationAnalysisManager manager;

    @Mock private GeminiExtractor geminiExtractor;
    @Mock private GeminiQualityScorer geminiQualityScorer;
    @Mock private ConsultationRawTextRepository rawTextRepository;
    @Mock private ManualRepository manualRepository;
    @Mock private ConsultationExtractionRepository extractionRepository;
    @Mock private ConsultationEvaluationRepository evaluationRepository;
    @Mock private ResultEventStatusRepository resultEventRepository;
    @Mock private ExcellentEventStatusRepository excellentEventRepository;
    @Mock private AnalysisCodeRepository analysisCodeRepository;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("성공: 50개 번들 태스크를 카테고리별로 그룹화하여 통합 분석 및 저장을 완료한다")
    void processIntegratedBundledTasks_Success() throws Exception {
        // Given: 짝이 맞는 태스크 쌍 2개 준비 (하나는 CHN, 하나는 OTB 가정)
        ResultEventStatus summaryTask = new ResultEventStatus(100L, "M_CHN_001");
        ExcellentEventStatus scoringTask = new ExcellentEventStatus(100L);
        TaskPair pair = new TaskPair(summaryTask, scoringTask);
        List<TaskPair> taskPairs = List.of(pair);

        // 1. 원문 조회 모킹
        ConsultationRawText rawData = mock(ConsultationRawText.class);
        given(rawData.getRawTextJson()).willReturn("{\"chat\": \"상담내용\"}");
        given(rawTextRepository.findByConsultId(anyLong())).willReturn(Optional.of(rawData));

        // 2. 매뉴얼 및 분석코드 모킹
        Manual manual = mock(Manual.class);
        given(manual.getContent()).willReturn("매뉴얼 내용");
        given(manualRepository.findByCategoryCodeAndIsActiveTrue(anyString())).willReturn(Optional.of(manual));
        given(analysisCodeRepository.findAllByClassificationIn(anyList())).willReturn(Collections.emptyList());

        // 3. AI 번들 추출기 응답 준비 (11개 필드 규격)
        AiExtractionResponse mockAiRes = new AiExtractionResponse(
            "상황 → 조치 → 결과", // raw_summary
            true, "요금사유", "COST", // CHN 필드
            true, true, List.of("할인"), "BNFT", // CHN 필드
            null, null, null // OTB 필드 (null)
        );
        given(geminiExtractor.extractBatch(anyList(), anyString(), anyMap()))
            .willReturn(List.of(mockAiRes));

        // 4. 품질 채점기 응답 준비
        given(geminiQualityScorer.evaluate(anyString(), anyString()))
            .willReturn(new QualityScoringResponse(95, "우수함", true));

        // When
        manager.processIntegratedBundledTasks(taskPairs);

        // Then
        assertThat(summaryTask.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(scoringTask.getStatus()).isEqualTo(EventStatus.COMPLETED);
        
        // 저장이 각각 1번씩 호출되었는지 검증
        verify(extractionRepository, times(1)).save(any(ConsultationExtraction.class));
        verify(evaluationRepository, times(1)).save(any(ConsultationEvaluation.class));
    }

    @Test
    @DisplayName("실패: AI 분석 중 에러 발생 시 관련 태스크들이 모두 FAILED 상태가 된다")
    void processIntegratedBundledTasks_AiError_Fail() {
        // Given
        TaskPair pair = new TaskPair(new ResultEventStatus(200L, "CHN"), new ExcellentEventStatus(200L));
        List<TaskPair> taskPairs = List.of(pair);

        given(rawTextRepository.findByConsultId(anyLong())).willReturn(Optional.of(mock(ConsultationRawText.class)));
        given(analysisCodeRepository.findAllByClassificationIn(anyList())).willReturn(Collections.emptyList());
        given(manualRepository.findByCategoryCodeAndIsActiveTrue(anyString())).willReturn(Optional.of(mock(Manual.class)));

        // AI 호출 시 에러 발생 시뮬레이션
        given(geminiExtractor.extractBatch(anyList(), anyString(), anyMap()))
            .willThrow(new RuntimeException("API Connection Timeout"));

        // When
        manager.processIntegratedBundledTasks(taskPairs);

        // Then
        assertThat(pair.summaryTask().getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(pair.scoringTask().getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(pair.summaryTask().getFailReason()).contains("API Connection Timeout");
    }
}