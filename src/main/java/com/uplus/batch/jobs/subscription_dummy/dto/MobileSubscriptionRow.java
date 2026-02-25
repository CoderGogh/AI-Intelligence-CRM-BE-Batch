package com.uplus.batch.jobs.subscription_dummy.dto;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 38. customer_subscription_mobile (고객이 구독중인 모바일 상품) DTO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MobileSubscriptionRow {

	private Long customerId;		// 고객 식별자
    private Long contractId;       // 통합 계약 ID (FK → customer_contracts)
    private String mobileCode;     // 모바일 상품 코드 (FK → product_mobile)
    private LocalDateTime joinedAt;       // 가입/개통일
    private LocalDateTime extinguishAt;   // 해지일 (nullable)
}