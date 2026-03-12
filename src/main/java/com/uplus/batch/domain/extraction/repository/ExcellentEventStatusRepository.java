package com.uplus.batch.domain.extraction.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.uplus.batch.domain.extraction.entity.EventStatus;
import com.uplus.batch.domain.extraction.entity.ExcellentEventStatus;

public interface ExcellentEventStatusRepository extends JpaRepository<ExcellentEventStatus, Long> {
    Optional<ExcellentEventStatus> findByConsultId(Long consultId);
    List<ExcellentEventStatus> findByStatusAndRetryCountLessThan(EventStatus status, int retryLimit);
}
