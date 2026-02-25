package com.uplus.batch.jobs.subscription_dummy.tasklet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Step 2: 가족 결합 처리
 *
 * Step 1에서 고객 1명 = contract 1개로 INSERT 완료된 상태에서,
 * 전체 contract의 20%를 2~4명씩 묶어 같은 contract_id를 공유하게 UPDATE
 *
 * 결합 코드: BND-TOGETHER, BND-FMLY-01, BND-FMLY-02, BND-FMLY-SR 중 랜덤
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FamilyCombineTasklet implements Tasklet {

    private final JdbcTemplate jdbcTemplate;

    private static final double FAMILY_RATIO = 0.20; // 20%
    private static final int MIN_FAMILY_SIZE = 2;
    private static final int MAX_FAMILY_SIZE = 4;

    private static final List<String> FAMILY_COMBOS = List.of(
            "BND-TOGETHER", "BND-FMLY-01", "BND-FMLY-02", "BND-FMLY-SR"
    );

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {

        // ── 1) 전체 contract_id 조회 ──
        List<Long> allContractIds = jdbcTemplate.queryForList(
                "SELECT contract_id FROM customer_contracts ORDER BY contract_id",
                Long.class
        );

        log.info("전체 계약 수: {}", allContractIds.size());
        if (allContractIds.size() < MIN_FAMILY_SIZE * 2) {
            log.warn("계약 수가 너무 적어 가족 결합 스킵");
            return RepeatStatus.FINISHED;
        }

        // ── 2) 셔플 후 20% 추출 ──
        Collections.shuffle(allContractIds, new Random(42));
        int familyPoolSize = (int) (allContractIds.size() * FAMILY_RATIO);
        List<Long> familyPool = new ArrayList<>(allContractIds.subList(0, familyPoolSize));

        // ── 3) 2~4명씩 가족 그룹 생성 ──
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int idx = 0;
        int familyCount = 0;
        int mergedContracts = 0;

        while (idx < familyPool.size()) {
            int remaining = familyPool.size() - idx;
            if (remaining < MIN_FAMILY_SIZE) break;

            int familySize = remaining <= MAX_FAMILY_SIZE
                    ? remaining
                    : rng.nextInt(MIN_FAMILY_SIZE, MAX_FAMILY_SIZE + 1);

            // 가족 구성원들의 contract_id
            List<Long> familyContractIds = familyPool.subList(idx, idx + familySize);

            // 대표 contract_id = 첫 번째 (나머지가 여기로 합류)
            Long mainContractId = familyContractIds.get(0);

            // 가족 결합 코드 랜덤 배정
            String familyCombo = FAMILY_COMBOS.get(rng.nextInt(FAMILY_COMBOS.size()));

            // ── 4) 대표 계약에 결합 코드 UPDATE ──
            jdbcTemplate.update(
                    "UPDATE customer_contracts SET combo_code = ? WHERE contract_id = ?",
                    familyCombo, mainContractId
            );

            // ── 5) 나머지 구성원의 구독을 대표 contract_id로 UPDATE ──
            for (int i = 1; i < familyContractIds.size(); i++) {
                Long oldContractId = familyContractIds.get(i);

                // 모바일 구독 → 대표 contract_id로
                jdbcTemplate.update(
                        "UPDATE customer_subscription_mobile SET contract_id = ? WHERE contract_id = ?",
                        mainContractId, oldContractId
                );

                // 홈 구독 → 대표 contract_id로
                jdbcTemplate.update(
                        "UPDATE customer_subscription_home SET contract_id = ? WHERE contract_id = ?",
                        mainContractId, oldContractId
                );

                // 부가서비스 → 대표 contract_id로
                jdbcTemplate.update(
                        "UPDATE customer_subscription_additional SET contract_id = ? WHERE contract_id = ?",
                        mainContractId, oldContractId
                );

                // 빈 계약 삭제
                jdbcTemplate.update(
                        "DELETE FROM customer_contracts WHERE contract_id = ?",
                        oldContractId
                );

                mergedContracts++;
            }

            familyCount++;
            idx += familySize;
        }

        log.info("가족 결합 완료 - 가족 그룹: {}건, 병합된 계약: {}건", familyCount, mergedContracts);
        return RepeatStatus.FINISHED;
    }
}