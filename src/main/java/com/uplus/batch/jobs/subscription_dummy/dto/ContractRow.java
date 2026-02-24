package com.uplus.batch.jobs.subscription_dummy.dto;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 45. customer_contracts (고객 계약 결합상품 정의) DTO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ContractRow {

    private Long contractId;       // 통합 계약 ID (Auto Increment)
    private String comboCode;      // 적용 결합 코드 (FK → combination_discount, nullable)
    private LocalDateTime createdAt;      // 계약 생성일
    private LocalDateTime updatedAt;      // 변경 일시
    private LocalDateTime extinguishAt;   // 해지일 (nullable)
}