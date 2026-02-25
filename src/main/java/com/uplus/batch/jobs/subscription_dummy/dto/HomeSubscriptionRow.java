package com.uplus.batch.jobs.subscription_dummy.dto;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 39. customer_subscription_home (고객이 구독중인 인터넷/TV 상품) DTO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HomeSubscriptionRow {

	private Long customerId;		// 고객 식별자
    private Long contractId;       // 통합 계약 ID (FK → customer_contracts)
    private String homeCode;       // 홈 상품 코드 (FK → product_home)
    private LocalDateTime joinedAt;       // 가입/개통일
    private LocalDateTime extinguishAt;   // 해지일 (nullable)
}