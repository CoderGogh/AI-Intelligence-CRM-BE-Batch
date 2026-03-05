package com.uplus.batch.jobs.weekly_report.config;

import com.uplus.batch.jobs.common.step.keyword.KeywordStatsTasklet;
import com.uplus.batch.jobs.weekly_report.step.admin.WeeklySubscriptionStatsTasklet;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class WeeklyReportJobConfig {

  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;
  private final WeeklySubscriptionStatsTasklet weeklySubscriptionStatsTasklet;
  private final MongoTemplate mongoTemplate;

  @Bean
  public Job weeklyAdminReportJob() {
    return new JobBuilder("weeklyAdminReportJob", jobRepository) //
        .start(weeklySubscriptionStatsStep())
        .next(weeklyKeywordStatsStep())
        .build();
  }

  @Bean
  public Step weeklySubscriptionStatsStep() {
    return new StepBuilder("weeklySubscriptionStatsStep", jobRepository) //
        .tasklet(weeklySubscriptionStatsTasklet, transactionManager)
        .build();
  }

  @Bean
  public Step weeklyKeywordStatsStep() {
    return new StepBuilder("weeklyKeywordStatsStep", jobRepository)
        .tasklet(new KeywordStatsTasklet(mongoTemplate, "weekly_report_snapshot"), transactionManager)
        .build();
  }
}
