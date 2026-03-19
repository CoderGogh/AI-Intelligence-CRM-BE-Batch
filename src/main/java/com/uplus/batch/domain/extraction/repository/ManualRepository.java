package com.uplus.batch.domain.extraction.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.uplus.batch.domain.extraction.entity.Manual;

public interface ManualRepository extends JpaRepository<Manual, Integer> {
    Optional<Manual> findByCategoryCodeAndIsActiveTrue(String categoryCode);
}