package com.uplus.batch.domain.summary.repository;

import com.uplus.batch.domain.extraction.entity.EventStatus;
import com.uplus.batch.domain.summary.entity.SummaryEventStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface SummaryEventStatusRepository extends JpaRepository<SummaryEventStatus, Long> {

    List<SummaryEventStatus> findTop100ByStatusOrderByCreatedAtAsc(EventStatus status);

    List<SummaryEventStatus> findByStatusAndRetryCountLessThan(EventStatus status, int retryLimit);
}
