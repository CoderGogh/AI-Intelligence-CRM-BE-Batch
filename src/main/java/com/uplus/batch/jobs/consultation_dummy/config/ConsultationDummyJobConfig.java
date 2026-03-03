package com.uplus.batch.jobs.consultation_dummy.config;

import com.uplus.batch.domain.consultation.dto.ConsultationRow;
import com.uplus.batch.jobs.consultation_dummy.processor.ConsultationProcessor;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * 상담 결과 더미데이터 생성 배치 Job
 *
 * ─── 대상 테이블 ───
 * 30. consultation_results
 *
 * ─── 실행 (컨트롤러 트리거) ───
 * curl -X POST "http://localhost:8090/dummy/consultations?count=50"
 */
@Configuration
@RequiredArgsConstructor
public class ConsultationDummyJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;

    @Bean
    public Job consultationDummyJob(Step consultationStep) {
        return new JobBuilder("consultationDummyJob", jobRepository)
                .start(consultationStep)
                .build();
    }

    @Bean
    public Step consultationStep(
            ItemReader<Integer> consultationReader,
            ConsultationProcessor consultationProcessor,
            JdbcBatchItemWriter<ConsultationRow> consultationWriter) {

        return new StepBuilder("consultationStep", jobRepository)
                .<Integer, ConsultationRow>chunk(500, transactionManager)
                .reader(consultationReader)
                .processor(consultationProcessor)
                .writer(consultationWriter)
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<Integer> consultationReader(
            @Value("#{jobParameters['count']}") Long count) {

        return new ListItemReader<>(
                IntStream.rangeClosed(1, count.intValue())
                        .boxed()
                        .toList()
        );
    }

    @Bean
    public JdbcBatchItemWriter<ConsultationRow> consultationWriter() {
        JdbcBatchItemWriter<ConsultationRow> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("""
                INSERT INTO consultation_results
                (emp_id, customer_id, channel, category_code, duration_sec,
                 iam_issue, iam_action, iam_memo, created_at)
                VALUES
                (:empId, :customerId, :channel, :categoryCode, :durationSec,
                 :iamIssue, :iamAction, :iamMemo, :createdAt)
                """);
        writer.setItemSqlParameterSourceProvider(
                new BeanPropertyItemSqlParameterSourceProvider<>());
        return writer;
    }
}
