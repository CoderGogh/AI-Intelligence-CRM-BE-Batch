package com.uplus.batch.jobs.subscription_dummy.dto;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 37. customer_subscription_additional (고객 구독중인 부가서비스) DTO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AdditionalSubscriptionRow {
 
	private Long customerId;		// 고객 식별자
    private Long contractId;        // 통합 계약 ID (FK → customer_contracts)
    private String serviceCode;     // 서비스 식별자 (FK → product_additional)
    private LocalDateTime joinDate;        // 가입일시
    private LocalDateTime extinguishDate;  // 해지일시 (nullable)
}