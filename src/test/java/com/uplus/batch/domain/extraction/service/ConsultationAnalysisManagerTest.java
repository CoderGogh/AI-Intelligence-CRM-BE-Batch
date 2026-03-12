package com.uplus.batch.domain.extraction.service;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;
import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.Optional;

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

@ExtendWith(MockitoExtension.class)
class ConsultationAnalysisManagerTest {

    @InjectMocks
    private ConsultationAnalysisManager manager;

    @Mock private GeminiExtractor extractor;
    @Mock private GeminiQualityScorer scorer;
    @Mock private ConsultationRawTextRepository rawTextRepository;
    @Mock private ManualRepository manualRepository;
    @Mock private ConsultationExtractionRepository extractionRepository;
    @Mock private ConsultationEvaluationRepository evaluationRepository;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    private final Long TEST_ID = 100L;
    private final String TEST_CATEGORY = "CHN_001";

    @Test
    @DisplayName("성공: 모든 데이터가 존재할 때 분석 및 저장이 완료된다")
    void processIntegratedTask_Success() throws Exception {
        ResultEventStatus summaryTask = new ResultEventStatus(TEST_ID, TEST_CATEGORY);
        ExcellentEventStatus scoringTask = new ExcellentEventStatus(TEST_ID);

        // 1. 원문 준비
        ConsultationRawText rawData = mock(ConsultationRawText.class);
        given(rawData.getRawTextJson()).willReturn("{\"chat\": \"test\"}");
        given(rawTextRepository.findByConsultId(anyLong())).willReturn(Optional.of(rawData));

        // 2. 매뉴얼 준비 (anyString으로 유연하게)
        Manual manual = mock(Manual.class);
        given(manual.getContent()).willReturn("매뉴얼 가이드라인");
        given(manualRepository.findByCategoryCodeAndIsActiveTrue(anyString())).willReturn(Optional.of(manual));

        // 3. AI 준비
        given(extractor.extract(anyString(), anyBoolean()))
            .willReturn(new AiExtractionResponse(true, "A", true, false, List.of("할인"), "요약"));
        given(scorer.evaluate(anyString(), anyString()))
            .willReturn(new QualityScoringResponse(90, "Good", true));

        manager.processIntegratedTask(summaryTask, scoringTask);

        assertThat(summaryTask.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(scoringTask.getStatus()).isEqualTo(EventStatus.COMPLETED);
    }

    @Test
    @DisplayName("실패: DB 저장 중 에러가 발생해도 상태가 FAILED로 기록되어야 한다")
    void processIntegratedTask_DatabaseSaveError_Fail() throws Exception {
        // Given: 로직이 저장 단계까지 가도록 모든 '가짜 재료'를 완벽히 준비합니다.
        ResultEventStatus summaryTask = new ResultEventStatus(TEST_ID, TEST_CATEGORY);
        ExcellentEventStatus scoringTask = new ExcellentEventStatus(TEST_ID);

        ConsultationRawText rawData = mock(ConsultationRawText.class);
        given(rawData.getRawTextJson()).willReturn("test");
        given(rawTextRepository.findByConsultId(anyLong())).willReturn(Optional.of(rawData));

        Manual manual = mock(Manual.class);
        given(manual.getContent()).willReturn("가짜 매뉴얼");
        given(manualRepository.findByCategoryCodeAndIsActiveTrue(anyString())).willReturn(Optional.of(manual));

        given(extractor.extract(any(), anyBoolean())).willReturn(new AiExtractionResponse(true, "A", true, false, List.of(), "S"));
        given(scorer.evaluate(any(), any())).willReturn(new QualityScoringResponse(80, "R", false));

        // ★ 여기서 '저장'할 때만 에러가 나도록 설정
        doThrow(new RuntimeException("DB Connection Error")).when(extractionRepository).save(any());

        // When
        manager.processIntegratedTask(summaryTask, scoringTask);

        // Then
        assertThat(summaryTask.getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(summaryTask.getFailReason()).contains("DB Connection Error");
    }

    @Test
    @DisplayName("실패: 매뉴얼이 없을 때 상태가 FAILED로 기록된다")
    void processIntegratedTask_ManualNotFound_Fail() {
        ResultEventStatus summaryTask = new ResultEventStatus(TEST_ID, TEST_CATEGORY);
        ExcellentEventStatus scoringTask = new ExcellentEventStatus(TEST_ID);

        ConsultationRawText rawData = mock(ConsultationRawText.class);
        given(rawData.getRawTextJson()).willReturn("test");
        given(rawTextRepository.findByConsultId(anyLong())).willReturn(Optional.of(rawData));
        
        // ★ 매뉴얼 레포지토리가 '데이터 없음'을 반환하도록 설정
        given(manualRepository.findByCategoryCodeAndIsActiveTrue(anyString())).willReturn(Optional.empty());

        manager.processIntegratedTask(summaryTask, scoringTask);

        assertThat(summaryTask.getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(summaryTask.getFailReason()).contains("활성화된 매뉴얼을 찾을 수 없습니다");
    }

    @Test
    @DisplayName("실패: 원본 데이터가 없을 때 상태가 FAILED로 기록된다")
    void processIntegratedTask_RawTextNotFound_Fail() {
        ResultEventStatus summaryTask = new ResultEventStatus(TEST_ID, TEST_CATEGORY);
        ExcellentEventStatus scoringTask = new ExcellentEventStatus(TEST_ID);

        given(rawTextRepository.findByConsultId(anyLong())).willReturn(Optional.empty());

        manager.processIntegratedTask(summaryTask, scoringTask);

        assertThat(summaryTask.getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(summaryTask.getFailReason()).contains("원본 데이터를 찾을 수 없습니다");
    }
}