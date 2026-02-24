package com.uplus.batch.jobs.subscription_dummy.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DB에서 읽은 고객 정보 (구독 생성 시 참조용)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CustomerInfo {

    private Long customerId;	
    private LocalDate birthDate;
    private String gradeCode;       // VVIP, VIP, DIAMOND
    private LocalDateTime createdAt; // 고객 등록일
}