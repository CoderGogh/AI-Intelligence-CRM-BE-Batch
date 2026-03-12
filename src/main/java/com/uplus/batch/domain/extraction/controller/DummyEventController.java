package com.uplus.batch.domain.extraction.controller;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/dummy")
@RequiredArgsConstructor
@Slf4j
public class DummyEventController {

    private final JdbcTemplate jdbcTemplate;

    @PostMapping("/setup-manuals")
    @Transactional
    public String setupManuals() {
        log.info("[Dummy] 지시형 매뉴얼 더미 데이터 생성 시작...");
        
        try {
            jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 0");
            jdbcTemplate.execute("TRUNCATE TABLE manuals");
            jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 1");

            List<Object[]> batchArgs = new ArrayList<>();
            
            // --- 1. 부가서비스 (ADD) ---
            addManual(batchArgs, "M_ADD_01", "부가서비스 가입 (스마트폰즈)", "스마트폰즈 가입 혜택 및 월 이용료를 반드시 고지해야 함.");
            addManual(batchArgs, "M_ADD_02", "멤버십 혜택 안내", "등급별 멤버십 포인트 활용처 및 제휴사 혜택을 상세히 안내해야 함.");
            addManual(batchArgs, "M_ADD_03", "로밍 서비스 가입", "국가별 로밍 요금제 및 데이터 차단 설정 방법을 안내해야 함.");
            addManual(batchArgs, "M_ADD_04", "부가서비스 해지", "해지 시 소멸되는 혜택 및 재가입 제한 사항을 반드시 확인시켜야 함.");
            addManual(batchArgs, "M_ADD_05", "OTT 서비스 변경", "결합 상품 변경 시 계정 연동 및 유료 결제 전환 주의사항을 안내해야 함.");
            addManual(batchArgs, "M_ADD_06", "U+ 폰케어 가입/관리", "보험 가입 조건 및 사고 발생 시 보상 청구 절차를 설명해야 함.");
            
            // --- 2. 해지/재약정 (CHN) ---
            addManual(batchArgs, "M_CHN_01", "약정 만료 해지", "약정 만료 시점을 고지하고 재약정 혜택을 제안하여 해지를 방어해야 함.");
            addManual(batchArgs, "M_CHN_02", "위약금 문의", "중도 해지 시 발생하는 위약금 및 할인 반환금을 정확히 산출하여 안내해야 함.");
            addManual(batchArgs, "M_CHN_03", "번호이동 해지", "타사 이동 시 기존 결합 할인 및 멤버십 혜택이 중단됨을 경고해야 함.");
            addManual(batchArgs, "M_CHN_04", "인터넷 해지 제공", "인터넷 해지 절차 및 임대 장비 반납 방법을 상세히 안내해야 함.");
            addManual(batchArgs, "M_CHN_05", "상품 업그레이드 제안", "해지 방어를 위해 기기 변경 지원금 또는 요금 할인 혜택을 우선 제안해야 함.");
            addManual(batchArgs, "M_CHN_06", "재약정 혜택 안내", "재약정 시 제공되는 사은품 또는 요금 할인 혜택을 강조하여 설명해야 함.");
            addManual(batchArgs, "M_CHN_07", "약정 기간 안내", "현재 가입된 상품의 약정 종료일 및 위약금 면제 시점을 안내해야 함.");

            // --- 3. 기기변경 (DEV) ---
            addManual(batchArgs, "M_DEV_01", "기기 혜택 소개", "최신 단말기 구매 시 제공되는 LG U+ 전용 사은품을 안내해야 함.");
            addManual(batchArgs, "M_DEV_02", "공시지원금 안내", "공시지원금과 선택약정 할인 중 고객에게 유리한 방식을 비교 설명해야 함.");
            addManual(batchArgs, "M_DEV_03", "기기변경 할부", "기존 기기의 잔여 할부금 처리 방안 및 중고폰 보상 프로그램을 안내해야 함.");
            addManual(batchArgs, "M_DEV_04", "번호이동 접수", "타사 번호이동 시 가입 혜택 및 유심 발급 비용을 고지해야 함.");
            addManual(batchArgs, "M_DEV_05", "기기 A/S 접수", "제조사 서비스센터 위치 및 파손 보험 수리비 지원 절차를 안내해야 함.");
            addManual(batchArgs, "M_DEV_06", "유심/eSIM 발급", "유심 교체 비용을 고지하고 비대면 본인 확인 절차를 준수해야 함.");

            // --- 4. 요금/납부 (FEE) ---
            addManual(batchArgs, "M_FEE_01", "5G 요금제 안내", "5G 요금제별 데이터 용량 및 속도 제어(QoS) 조건을 설명해야 함.");
            addManual(batchArgs, "M_FEE_02", "LTE 요금제 안내", "LTE 요금제의 가성비를 강조하고 데이터 쉐어링 혜택을 안내해야 함.");
            addManual(batchArgs, "M_FEE_03", "데이터 쉐어링 변경", "보조 기기 데이터 공유 신청 및 해지 시 유의사항을 안내해야 함.");
            addManual(batchArgs, "M_FEE_04", "미납 요금 안내", "미납금 납부 방법 및 서비스 정지 예정 시점을 정확히 고지해야 함.");
            addManual(batchArgs, "M_FEE_05", "분할 납부 신청", "고액 미납 시 분할 납부 가능 조건 및 이자 발생 여부를 안내해야 함.");
            addManual(batchArgs, "M_FEE_06", "카드/계좌 변경", "결제 수단 변경 시 이번 달 청구분 반영 시점을 명확히 안내해야 함.");
            addManual(batchArgs, "M_FEE_07", "청구서 항목 문의", "청구 내역을 항목별로 상세 설명하고 소액결제 차단 방법을 안내해야 함.");
            addManual(batchArgs, "M_FEE_08", "요금 감면 안내", "복지 할인 신청을 위한 구비 서류 및 접수 방법을 안내해야 함.");
            addManual(batchArgs, "M_FEE_09", "결합 할인 적용", "가족 결합 및 인터넷 결합 시 최대 할인 금액을 계산하여 안내해야 함.");
            addManual(batchArgs, "M_FEE_10", "장기고객 할인", "가입 기간에 따른 할인 혜택 및 장기 고객 쿠폰 제공을 안내해야 함.");

            // --- 5. 장애/AS (TRB) ---
            addManual(batchArgs, "M_TRB_01", "인터넷 속도 저하", "속도 측정 결과에 따라 공유기 리셋 또는 기사 방문을 안내해야 함.");
            addManual(batchArgs, "M_TRB_02", "인터넷 연결 불가", "단말 신호 상태를 점검하고 지역 장애 여부를 즉시 확인해야 함.");
            addManual(batchArgs, "M_TRB_03", "Wi-Fi 불안정", "무선 신호 간섭 최소화를 위한 채널 변경 방법을 안내해야 함.");
            addManual(batchArgs, "M_TRB_04", "TV 화면 결함", "셋톱박스 HDMI 케이블 연결 상태 및 해상도 설정을 확인해야 함.");
            addManual(batchArgs, "M_TRB_05", "리모컨 오작동", "배터리 교체 유도 및 셋톱박스와의 페어링 재설정을 안내해야 함.");
            addManual(batchArgs, "M_TRB_06", "셋톱박스 오류", "에러 코드별 자가 조치 방법을 안내하고 장비 교체를 검토해야 함.");
            addManual(batchArgs, "M_TRB_07", "통화 품질 불량", "현 위치 음영 지역 여부를 조회하고 가정용 중계기 설치를 제안해야 함.");
            addManual(batchArgs, "M_TRB_08", "데이터 연결 불가", "단말기 APN 설정 상태 및 데이터 로밍 차단 여부를 체크해야 함.");
            addManual(batchArgs, "M_TRB_09", "스마트홈 장애", "IoT 허브 연결 상태 확인 및 전용 앱 재로그인을 유도해야 함.");
            addManual(batchArgs, "M_TRB_10", "신규 설치 접수", "설치 희망지 개통 가능 여부 및 설치비 발생을 사전 고지해야 함.");
            addManual(batchArgs, "M_TRB_11", "이사 이전 설치", "이전 설치 비용을 안내하고 이사 날짜에 맞춘 방문 예약을 확정해야 함.");

            // --- 6. 기타 (ETC) - 새로 추가된 부분 ---
            addManual(batchArgs, "M_ETC_01", "매장 위치 안내", "고객의 현재 위치에서 가장 가까운 직영점 및 대리점의 위치와 영업시간을 안내해야 함.");
            addManual(batchArgs, "M_ETC_02", "영수증 발급 문의", "결제 영수증의 재발급 방법(이메일, 문자, 홈페이지)을 고객이 선택할 수 있도록 안내해야 함.");
            addManual(batchArgs, "M_ETC_03", "서비스 개선 제안", "고객의 제안 사항을 경청하고 감사 인사를 전한 뒤, 관련 부서에 정식으로 접수해야 함.");
            addManual(batchArgs, "M_ETC_04", "불만 접수 (민원)", "고객의 불편 사항에 대해 정중히 사과하고, 가이드에 따라 상급자 또는 관련 부서로 신속히 이관해야 함.");
            String sql = "INSERT INTO manuals (category_code, title, content, is_active, emp_id, created_by, created_at, updated_at) " +
                         "VALUES (?, ?, ?, 1, 1, 2, NOW(), NOW())";

            jdbcTemplate.batchUpdate(sql, batchArgs);

            log.info("[Dummy] 지시형 매뉴얼 {}건 생성 완료", batchArgs.size());
            return "Successfully created " + batchArgs.size() + " strict manuals.";
        } catch (Exception e) {
            log.error("[Dummy] 오류: {}", e.getMessage());
            return "Error: " + e.getMessage();
        }
    }

    private void addManual(List<Object[]> args, String code, String title, String checkPoint) {
        String content = String.format("""
            # [%s] 채점 및 상담 가이드
            
            ## [필수 수행 지침]
            - 상담원은 고객에게 %s
            - 상담 시작 시 소속과 이름을 밝히고 밝게 인사해야 함.
            - 고객의 문의 사항을 끝까지 경청하고 공감을 표현해야 함.
            - 전문 용어를 지양하고 고객이 이해하기 쉬운 언어로 설명해야 함.
            - 상담 종료 전 추가 문의 사항이 있는지 확인해야 함.
            
            ## [금지 사항]
            - 고객의 말을 중간에 끊거나 부정적인 단어를 사용해서는 안 됨.
            - 확인되지 않은 정보를 확정적으로 안내해서는 안 됨.
            """, title, checkPoint);
        args.add(new Object[]{code, title, content});
    }

    /**
     * 2. 분석 관련 모든 데이터 초기화 (분석 결과 + 상태 테이블)
     */
    @PostMapping("/clear-all")
    @Transactional
    public String clearAnalysisData() {
        log.info("[Dummy] 분석 및 채점 테이블 초기화 시작 (retention_analysis, consultation_evaluations)");
        try {
            jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 0");

            jdbcTemplate.execute("TRUNCATE TABLE retention_analysis");      // 요약 및 해지방어 추출 결과
            jdbcTemplate.execute("TRUNCATE TABLE consultation_evaluations"); // AI 점수 및 채점 결과
            
            // 상태 관리 테이블들
            jdbcTemplate.execute("TRUNCATE TABLE result_event_status");
            jdbcTemplate.execute("TRUNCATE TABLE excellent_event_status");

            jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 1");

            log.info("[Dummy] 모든 관련 테이블 초기화 완료");
            return "Successfully cleared: retention_analysis, consultation_evaluations, and status tables.";
        } catch (Exception e) {
            log.error("[Dummy] 초기화 중 오류 발생: {}", e.getMessage());
            return "Error: " + e.getMessage();
        }
    }

    /**
     * 3. 통합 분석 대기열 생성
     */
    @PostMapping("/event-status")
    @Transactional
    public String generateEventStatus(@RequestParam Long startId, @RequestParam Long endId) {
        log.info("[Dummy] 대기열 생성: consultId {} ~ {}", startId, endId);
        String sqlR = "INSERT INTO result_event_status (consult_id, category_code, status, retry_count, created_at, updated_at) " +
                      "SELECT consult_id, category_code, 'REQUESTED', 0, NOW(), NOW() FROM consultation_results WHERE consult_id BETWEEN ? AND ?";
        String sqlE = "INSERT INTO excellent_event_status (consult_id, status, retry_count, created_at, updated_at) " +
                      "SELECT consult_id, 'REQUESTED', 0, NOW(), NOW() FROM consultation_results WHERE consult_id BETWEEN ? AND ?";
        try {
            int r = jdbcTemplate.update(sqlR, startId, endId);
            int e = jdbcTemplate.update(sqlE, startId, endId);
            return String.format("Created %d pairs (REQUESTED)", r);
        } catch (Exception e) { return "Error: " + e.getMessage(); }
    }
}