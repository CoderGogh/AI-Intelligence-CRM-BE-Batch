package com.uplus.batch.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 분석 배치 Job 수동 트리거 컨트롤러
 */
@Slf4j
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class AnalysisJobController {

    private final JobLauncher jobLauncher;
    private final Job customerRiskJob;
    private final Job weeklyPerformanceJob;
    private final Job monthlyPerformanceJob;

    /**
     * 고객 특이사항 집계 Job 실행
     *
     * @param targetDate 집계 대상 날짜 (yyyy-MM-dd). 생략 시 어제.
     *
     * curl -X POST "http://localhost:8081/api/jobs/customer-risk?targetDate=2025-01-15"
     */
    @PostMapping("/customer-risk")
    public ResponseEntity<String> runCustomerRisk(
            @RequestParam(required = false) String targetDate
    ) throws Exception {

        JobParametersBuilder builder = new JobParametersBuilder()
                .addLong("runId", System.currentTimeMillis());

        if (targetDate != null && !targetDate.isBlank()) {
            builder.addString("targetDate", targetDate);
        }

        JobParameters params = builder.toJobParameters();

        log.info("[AnalysisJob] customerRiskJob 수동 실행 요청 — targetDate={}", targetDate);
        jobLauncher.run(customerRiskJob, params);

        return ResponseEntity.ok("CustomerRisk job started (targetDate=" + targetDate + ")");
    }

    /**
     * 주간 전체 상담 성과 집계 Job 실행
     *
     * @param startDate 집계 시작 날짜 (yyyy-MM-dd, 월요일)
     * @param endDate   집계 종료 날짜 (yyyy-MM-dd, 일요일)
     *
     * curl -X POST "http://localhost:8081/api/jobs/weekly-performance?startDate=2025-01-13&endDate=2025-01-19"
     */
    @PostMapping("/weekly-performance")
    public ResponseEntity<String> runWeeklyPerformance(
            @RequestParam String startDate,
            @RequestParam String endDate
    ) throws Exception {

        JobParameters params = new JobParametersBuilder()
                .addLong("runId", System.currentTimeMillis())
                .addString("startDate", startDate)
                .addString("endDate", endDate)
                .addString("targetCollection", "weekly_report_snapshot")
                .toJobParameters();

        log.info("[AnalysisJob] weeklyPerformanceJob 수동 실행 요청 — {} ~ {}", startDate, endDate);
        jobLauncher.run(weeklyPerformanceJob, params);

        return ResponseEntity.ok("WeeklyPerformance job started (" + startDate + " ~ " + endDate + ")");
    }

    /**
     * 월간 전체 상담 성과 집계 Job 실행
     *
     * @param startDate 집계 시작 날짜 (yyyy-MM-dd, 1일)
     * @param endDate   집계 종료 날짜 (yyyy-MM-dd, 말일)
     *
     * curl -X POST "http://localhost:8081/api/jobs/monthly-performance?startDate=2025-01-01&endDate=2025-01-31"
     */
    @PostMapping("/monthly-performance")
    public ResponseEntity<String> runMonthlyPerformance(
            @RequestParam String startDate,
            @RequestParam String endDate
    ) throws Exception {

        JobParameters params = new JobParametersBuilder()
                .addLong("runId", System.currentTimeMillis())
                .addString("startDate", startDate)
                .addString("endDate", endDate)
                .addString("targetCollection", "monthly_report_snapshot")
                .toJobParameters();

        log.info("[AnalysisJob] monthlyPerformanceJob 수동 실행 요청 — {} ~ {}", startDate, endDate);
        jobLauncher.run(monthlyPerformanceJob, params);

        return ResponseEntity.ok("MonthlyPerformance job started (" + startDate + " ~ " + endDate + ")");
    }
}