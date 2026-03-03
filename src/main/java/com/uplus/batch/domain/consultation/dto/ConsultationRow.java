package com.uplus.batch.domain.consultation.dto;

import java.time.LocalDateTime;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ConsultationRow {

    private Integer empId;
    private Long customerId;
    private String channel;
    private String categoryCode;
    private Integer durationSec;
    private String iamIssue;
    private String iamAction;
    private String iamMemo;
    private LocalDateTime createdAt;
}
