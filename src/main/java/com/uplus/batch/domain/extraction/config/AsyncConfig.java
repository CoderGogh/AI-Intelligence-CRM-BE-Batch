package com.uplus.batch.domain.extraction.config;

import java.util.concurrent.Executor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableAsync
public class AsyncConfig {

    @Bean(name = "geminiTaskExecutor")
    public Executor geminiTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        
        // 1. 기본 일꾼 수: 평소에는 15명이 대기
        executor.setCorePoolSize(15);
        
        // 2. 최대 일꾼 수: 일이 몰리면 30명까지 투입
        executor.setMaxPoolSize(30);
        
        // 3. 대기소: 30명도 꽉 차면 100건까지 줄 세워둠
        executor.setQueueCapacity(100);
        
        executor.setThreadNamePrefix("Gemini-API-");
        
        // 서버 종료 요청이 와도 진행 중인 AI 분석이 끝날 때까지 최대 60초 기다려줌
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        
        // 호출한 스레드(메인 스레드)가 직접 일을 하게 해서 자연스럽게 유입 속도를 늦춤
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        
        executor.initialize();
        return executor;
    }
}