package com.uplus.batch.jobs.subscription_dummy.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 고객 1명에 대한 전체 구독 플랜 (계약 + 모바일 + 홈 + 부가서비스)
 * Processor → Writer 간 전달 단위
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CustomerSubscriptionPlan {

    private ContractRow contract;
    private List<MobileSubscriptionRow> mobileSubscriptions;
    private List<HomeSubscriptionRow> homeSubscriptions;
    private List<AdditionalSubscriptionRow> additionalSubscriptions;
}