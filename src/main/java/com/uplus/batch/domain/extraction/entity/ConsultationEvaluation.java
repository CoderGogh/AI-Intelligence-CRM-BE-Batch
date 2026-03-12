package com.uplus.batch.domain.extraction.entity;

import java.time.LocalDateTime;

import org.hibernate.annotations.CreationTimestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "consultation_evaluations")
@Getter @NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ConsultationEvaluation {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long evaluationId;

    @Column(nullable = false)
    private Long consultId;

    private int score;
    @Column(columnDefinition = "TEXT")
    private String evaluationReason;
    private boolean isCandidate;

    @CreationTimestamp
    private LocalDateTime createdAt;

    @Builder
    public ConsultationEvaluation(Long consultId, int score, String evaluationReason, boolean isCandidate) {
        this.consultId = consultId;
        this.score = score;
        this.evaluationReason = evaluationReason;
        this.isCandidate = isCandidate;
    }
}