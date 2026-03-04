package com.uplus.batch.jobs.raw_text_dummy.config;

import com.uplus.batch.jobs.raw_text_dummy.tasklet.RawTextDummyTasklet;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * 상담 원문 더미 데이터 생성 배치 Job
 *
 * <p>대상 테이블: consultation_raw_texts (41번)
 *
 * <p>실행 조건: consultation_results 데이터가 먼저 존재해야 함
 *
 * <pre>
 * curl -X POST "http://localhost:8081/dummy/raw-texts?startId=1&endId=100"
 * </pre>
 */
@Configuration
@RequiredArgsConstructor
public class RawTextDummyJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final RawTextDummyTasklet rawTextDummyTasklet;

    @Bean
    public Job rawTextDummyJob() {
        return new JobBuilder("rawTextDummyJob", jobRepository)
                .start(rawTextDummyStep())
                .build();
    }

    @Bean
    public Step rawTextDummyStep() {
        return new StepBuilder("rawTextDummyStep", jobRepository)
                .tasklet(rawTextDummyTasklet, transactionManager)
                .build();
    }
}
