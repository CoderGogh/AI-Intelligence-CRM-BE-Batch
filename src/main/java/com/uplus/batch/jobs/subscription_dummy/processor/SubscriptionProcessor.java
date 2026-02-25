package com.uplus.batch.jobs.subscription_dummy.processor;

import com.uplus.batch.jobs.subscription_dummy.dto.AdditionalSubscriptionRow;
import com.uplus.batch.jobs.subscription_dummy.dto.ContractRow;
import com.uplus.batch.jobs.subscription_dummy.dto.CustomerInfo;
import com.uplus.batch.jobs.subscription_dummy.dto.CustomerSubscriptionPlan;
import com.uplus.batch.jobs.subscription_dummy.dto.HomeSubscriptionRow;
import com.uplus.batch.jobs.subscription_dummy.dto.MobileSubscriptionRow;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

/**
 * 고객 정보를 읽어 구독 플랜(계약+모바일+홈+부가서비스)을 생성하는 Processor
 *
 * ─── 비즈니스 규칙 ───
 * 1) 고객 1명당 최소 홈 1건 or 모바일 1건, 최대 홈+모바일 합계 4건
 * 2) 부가서비스 0~10건
 * 3) 홈/인터넷 결합 고객 비율 40%
 * 4) 모바일 결합 고객 비율 40%
 * 5) 가입일: 고객 birth_date 이후, 2020-01-01 이후
 * 6) 해지일: 10% 중도해지(약정 이전), 30% 만기해지(약정~현재), 60% NULL
 * 7) 시니어 상품: 가입 시점 만65세 이상만
 */
@Slf4j
@Component
public class SubscriptionProcessor implements ItemProcessor<CustomerInfo, CustomerSubscriptionPlan> {

    private static final LocalDate MIN_JOIN_DATE = LocalDate.of(2020, 1, 1);
    private static final LocalDate NOW = LocalDate.now();

    // ──────────────────────────────────────────────
    // 결합상품 코드 (combination_discount 테이블 기준)
    // ──────────────────────────────────────────────
    private static final String COMBO_TOGETHER = "BND-TOGETHER";  // 무제한 모바일+인터넷
    private static final String COMBO_FMLY_01 = "BND-FMLY-01";   // 가족결합 모바일+인터넷
    private static final String COMBO_FMLY_02 = "BND-FMLY-02";   // 가족 모바일만
    private static final String COMBO_FMLY_SR = "BND-FMLY-SR";   // 시니어 가족결합
    private static final String COMBO_INET2 = "BND-INET2";        // 인터넷 끼리

    // ──────────────────────────────────────────────
    // 모바일 상품 코드 (product_mobile 테이블 기준 29건)
    // ──────────────────────────────────────────────
    // 무제한 요금제 (데이터 '완전 무제한')
    private static final List<String> MOBILE_UNLIMITED = List.of(
            "MOB-5G-SIG", "MOB-5G-PS", "MOB-5G-PP", "MOB-5G-PR", "MOB-5G-PE",
            "MOB-LTE-PM"
    );

    // 일반 요금제 (target_group = '일반')
    private static final List<String> MOBILE_GENERAL = List.of(
            "MOB-5G-SIG", "MOB-5G-PS", "MOB-5G-PP", "MOB-5G-PR", "MOB-5G-PE",
            "MOB-5G-ST", "MOB-5G-SM", "MOB-5G-LT", "MOB-5G-SL", "MOB-5G-MN",
            "MOB-NGT-01", "MOB-NGT-03", "MOB-NGT-05", "MOB-NGT-07", "MOB-NGT-11", "MOB-NGT-UNL",
            "MOB-LTE-PM", "MOB-LTE-UNL", "MOB-LTE-D33", "MOB-LTE-STD"
    );

    // 유쓰 (만34세이하)
    private static final List<String> MOBILE_YOUTH = List.of("MOB-5G-YST", "MOB-5G-YSM");

    // 청소년 (만4~18세)
    private static final List<String> MOBILE_TEEN = List.of("MOB-5G-TN", "MOB-LTE-TN19");

    // 키즈 (만4~12세)
    private static final List<String> MOBILE_KIDS = List.of("MOB-5G-KD", "MOB-LTE-KD22");

    // 시니어 (만65세이상)
    private static final List<String> MOBILE_SENIOR = List.of("MOB-5G-SR", "MOB-LTE-SR33");

    // ──────────────────────────────────────────────
    // 홈 상품 코드 (product_home 테이블 기준 21건)
    // ──────────────────────────────────────────────
    private static final List<String> HOME_INTERNET = List.of(
            "NET-100M", "NET-500M", "NET-GIG-500", "NET-GIG-1G", "NET-GIG-1G-R", "NET-GIG-10G"
    );
    private static final List<String> HOME_IPTV = List.of(
            "TV-BASIC", "TV-STD", "TV-PREM", "TV-PREM-VOD", "TV-ADD"
    );
    private static final List<String> HOME_BUNDLE = List.of(
            "BND-NET-TV-500", "BND-NET-TV-1G", "BND-NET-TV-SH"
    );
    private static final List<String> HOME_PHONE = List.of("TEL-HOME", "TEL-HOME-UNL");
    private static final List<String> HOME_SMARTHOME = List.of(
            "SH-PET", "SH-GOOGLE", "SH-GUARD", "SH-KIDS", "SH-FREE"
    );
    // 인터넷 단독 가입용 (결합/스마트홈 제외)
    private static final List<String> HOME_INTERNET_ALL = List.of(
            "NET-100M", "NET-500M", "NET-GIG-500", "NET-GIG-1G", "NET-GIG-1G-R", "NET-GIG-10G",
            "TV-BASIC", "TV-STD", "TV-PREM", "TV-PREM-VOD"
    );

    // ──────────────────────────────────────────────
    // 부가서비스 코드 (product_additional 테이블 기준 22건)
    // ──────────────────────────────────────────────
    private static final List<String> ADDITIONAL_ALL = List.of(
            "ADD-MTVI", "ADD-TVING", "ADD-NETFLIX", "ADD-DISNEY", "ADD-WAVVE",
            "ADD-DATA-SH", "ADD-DATA-GFT", "ADD-DATA-ADD1", "ADD-DATA-ADD5",
            "ADD-ROAM-3G", "ADD-ROAM-5G", "ADD-ROAM-10G", "ADD-ROAM-UNL",
            "ADD-SAFE-CALL", "ADD-SAFE-PHISH",
            "ADD-INS-PHONE", "ADD-INS-PHONE2",
            "ADD-MUSIC", "ADD-GAME", "ADD-KIDS", "ADD-NAVI", "ADD-MEMBERSHIP"
    );

    // 약정 기간 (3년 = 36개월, 인터넷전화는 약정 없음)
    private static final int CONTRACT_MONTHS = 36;

    @Override
    public CustomerSubscriptionPlan process(CustomerInfo customer) {

        ThreadLocalRandom rng = ThreadLocalRandom.current();

        // ── 1) 가입일 결정: max(birth_date, 2020-01-01) ~ 현재 ──
        LocalDate earliestJoin = customer.getBirthDate();
        if (earliestJoin.isBefore(MIN_JOIN_DATE)) {
            earliestJoin = MIN_JOIN_DATE;
        }
        LocalDate latestJoin = NOW;
        if (earliestJoin.isAfter(latestJoin)) {
            earliestJoin = MIN_JOIN_DATE;
        }

        long joinRange = ChronoUnit.DAYS.between(earliestJoin, latestJoin);
        if (joinRange <= 0) joinRange = 1;
        LocalDate joinDate = earliestJoin.plusDays(rng.nextLong(joinRange));
        LocalDateTime joinedAt = joinDate.atTime(rng.nextInt(9, 19), rng.nextInt(60), 0);

        // ── 2) 고객 나이 계산 (가입 시점 기준) ──
        int ageAtJoin = (int) ChronoUnit.YEARS.between(customer.getBirthDate(), joinDate);
        boolean isSenior = ageAtJoin >= 65;
        boolean isYouth = ageAtJoin >= 19 && ageAtJoin <= 34;
        boolean isTeen = ageAtJoin >= 4 && ageAtJoin <= 18;
        boolean isKid = ageAtJoin >= 4 && ageAtJoin <= 12;

        // ── 3) 결합상품 결정 (40% 홈결합, 40% 모바일결합, 20% 결합없음) ──
        double comboRoll = rng.nextDouble();
        String comboCode = null;
        boolean hasHomeCombo = false;
        boolean hasMobileCombo = false;

        if (comboRoll < 0.20) {
            // 홈+모바일 결합
            if (isSenior) {
                comboCode = COMBO_FMLY_SR;
            } else {
                comboCode = rng.nextBoolean() ? COMBO_TOGETHER : COMBO_FMLY_01;
            }
            hasHomeCombo = true;
            hasMobileCombo = true;
        } else if (comboRoll < 0.40) {
            // 홈 결합만 (인터넷 끼리)
            comboCode = COMBO_INET2;
            hasHomeCombo = true;
        } else if (comboRoll < 0.60) {
            // 모바일 결합만 (가족)
            if (isSenior) {
                comboCode = COMBO_FMLY_SR;
                hasHomeCombo = true;  // 시니어는 인터넷 포함
                hasMobileCombo = true;
            } else {
                comboCode = COMBO_FMLY_02;
                hasMobileCombo = true;
            }
        }
        // else: 40% → 결합 없음 (comboCode = null)

        // ── 4) 모바일 상품 결정 ──
        List<MobileSubscriptionRow> mobileList = new ArrayList<>();
        int mobileCount = decideMobileCount(comboCode, rng);

        for (int i = 0; i < mobileCount; i++) {
            String mobileCode = pickMobileCode(comboCode, isSenior, isYouth, isTeen, isKid, i, rng);
            LocalDateTime mobileJoinedAt = adjustJoinTime(joinedAt, i, rng);
            LocalDateTime mobileExtinguish = decideExtinguishDate(mobileJoinedAt, CONTRACT_MONTHS, rng);

            mobileList.add(MobileSubscriptionRow.builder()
            		.customerId(customer.getCustomerId())
                    .mobileCode(mobileCode)
                    .joinedAt(mobileJoinedAt)
                    .extinguishAt(mobileExtinguish)
                    .build());
        }

        // ── 5) 홈 상품 결정 ──
        List<HomeSubscriptionRow> homeList = new ArrayList<>();
        int homeCount = decideHomeCount(comboCode, mobileCount, rng);

        for (int i = 0; i < homeCount; i++) {
            String homeCode = pickHomeCode(comboCode, i, rng);
            LocalDateTime homeJoinedAt = adjustJoinTime(joinedAt, i, rng);
            // 인터넷전화는 약정 없음 → 해지 NULL 많음
            int contractMonths = homeCode.startsWith("TEL-") ? 0 : CONTRACT_MONTHS;
            LocalDateTime homeExtinguish = decideExtinguishDate(homeJoinedAt, contractMonths, rng);

            homeList.add(HomeSubscriptionRow.builder()
            		.customerId(customer.getCustomerId())
                    .homeCode(homeCode)
                    .joinedAt(homeJoinedAt)
                    .extinguishAt(homeExtinguish)
                    .build());
        }

        // 최소 1건 보장 (모바일 0 + 홈 0 방지)
        if (mobileCount == 0 && homeCount == 0) {
            String fallbackMobile = pickMobileForAge(isSenior, isYouth, isTeen, isKid, rng);
            mobileList.add(MobileSubscriptionRow.builder()
            		.customerId(customer.getCustomerId())
                    .mobileCode(fallbackMobile)
                    .joinedAt(joinedAt)
                    .extinguishAt(decideExtinguishDate(joinedAt, CONTRACT_MONTHS, rng))
                    .build());
        }

        // ── 6) 부가서비스 결정 (0~10건) ──
        List<AdditionalSubscriptionRow> additionalList = new ArrayList<>();
        int additionalCount = 0;

        if (rng.nextDouble() < 0.40) {  // 40%만 구독
            double roll = rng.nextDouble();
            if (roll < 0.6) additionalCount = rng.nextInt(1, 3);   // 1~2건
            else if (roll < 0.9) additionalCount = rng.nextInt(3, 6);  // 3~5건
            else additionalCount = rng.nextInt(6, 11);                  // 6~10건
        }

        List<String> shuffled = new ArrayList<>(ADDITIONAL_ALL);
        Collections.shuffle(shuffled, rng);
        for (int i = 0; i < Math.min(additionalCount, shuffled.size()); i++) {
            LocalDateTime addJoinDate = adjustJoinTime(joinedAt, i, rng);
            // 부가서비스는 약정 없음 → 해지 비율 다르게 적용 (70% NULL, 20% 해지, 10% 최근해지)
            LocalDateTime addExtinguish = decideAdditionalExtinguish(addJoinDate, rng);

            additionalList.add(AdditionalSubscriptionRow.builder()
            		.customerId(customer.getCustomerId())
                    .serviceCode(shuffled.get(i))
                    .joinDate(addJoinDate)
                    .extinguishDate(addExtinguish)
                    .build());
        }

        // ── 7) 계약(Contract) 생성 ──
        // 해지일: 모든 하위 상품이 해지되었으면 계약도 해지
        LocalDateTime contractExtinguish = resolveContractExtinguish(mobileList, homeList);

        ContractRow contract = ContractRow.builder()
                .comboCode(comboCode)
                .createdAt(joinedAt)
                .updatedAt(joinedAt)
                .extinguishAt(contractExtinguish)
                .build();

        return CustomerSubscriptionPlan.builder()
                .contract(contract)
                .mobileSubscriptions(mobileList)
                .homeSubscriptions(homeList)
                .additionalSubscriptions(additionalList)
                .build();
    }

    // ════════════════════════════════════════════════
    // 모바일 상품 수 결정
    // ════════════════════════════════════════════════
    private int decideMobileCount(String comboCode, ThreadLocalRandom rng) {
        if (comboCode == null) {
            // 결합 없음: 1~2건
            return rng.nextInt(1, 3);
        }
        return switch (comboCode) {
            case COMBO_TOGETHER -> rng.nextInt(1, 4);  // 1~3건 (최대5지만 현실적으로)
            case COMBO_FMLY_01 -> rng.nextInt(1, 4);   // 1~3건
            case COMBO_FMLY_02 -> rng.nextInt(1, 4);   // 1~3건
            case COMBO_FMLY_SR -> 1;                    // 모바일1
            case COMBO_INET2 -> rng.nextInt(0, 2);      // 0~1건 (인터넷 전용 결합)
            default -> 1;
        };
    }

    // ════════════════════════════════════════════════
    // 홈 상품 수 결정 (최대 합계 4건 제한)
    // ════════════════════════════════════════════════
    private int decideHomeCount(String comboCode, int mobileCount, ThreadLocalRandom rng) {
        int maxHome = Math.max(0, 4 - mobileCount);

        if (comboCode == null) {
            // 결합 없음: 0~1건
            return Math.min(rng.nextInt(0, 2), maxHome);
        }
        int base = switch (comboCode) {
            case COMBO_TOGETHER -> rng.nextInt(1, 3);  // 1~2건
            case COMBO_FMLY_01 -> rng.nextInt(1, 3);   // 1~2건
            case COMBO_FMLY_02 -> 0;                    // 모바일만
            case COMBO_FMLY_SR -> 1;                    // 인터넷1
            case COMBO_INET2 -> 2;                      // 인터넷 2건
            default -> rng.nextInt(0, 2);
        };
        return Math.min(base, maxHome);
    }

    // ════════════════════════════════════════════════
    // 모바일 상품 코드 선택
    // ════════════════════════════════════════════════
    private String pickMobileCode(String comboCode, boolean isSenior, boolean isYouth,
                                  boolean isTeen, boolean isKid, int index, ThreadLocalRandom rng) {

        // 결합별 제약
        if (COMBO_TOGETHER.equals(comboCode)) {
            // 무제한 요금제만 가능
            return pickRandom(MOBILE_UNLIMITED, rng);
        }
        if (COMBO_FMLY_SR.equals(comboCode)) {
            // 시니어 상품
            return pickRandom(MOBILE_SENIOR, rng);
        }

        // 나이별 분기
        return pickMobileForAge(isSenior, isYouth, isTeen, isKid, rng);
    }

    private String pickMobileForAge(boolean isSenior, boolean isYouth,
                                    boolean isTeen, boolean isKid, ThreadLocalRandom rng) {
        if (isSenior) {
            // 80% 시니어 전용, 20% 일반
            return rng.nextDouble() < 0.8 ? pickRandom(MOBILE_SENIOR, rng)
                    : pickRandom(MOBILE_GENERAL, rng);
        }
        if (isKid) {
            return pickRandom(MOBILE_KIDS, rng);
        }
        if (isTeen) {
            return pickRandom(MOBILE_TEEN, rng);
        }
        if (isYouth) {
            // 40% 유쓰 전용, 60% 일반
            return rng.nextDouble() < 0.4 ? pickRandom(MOBILE_YOUTH, rng)
                    : pickRandom(MOBILE_GENERAL, rng);
        }
        return pickRandom(MOBILE_GENERAL, rng);
    }

    // ════════════════════════════════════════════════
    // 홈 상품 코드 선택
    // ════════════════════════════════════════════════
    private String pickHomeCode(String comboCode, int index, ThreadLocalRandom rng) {
        if (COMBO_INET2.equals(comboCode)) {
            // 인터넷 끼리 결합: 인터넷 상품만
            return pickRandom(HOME_INTERNET, rng);
        }

        // 첫 번째 상품은 인터넷 or 결합세트
        if (index == 0) {
            // 60% 인터넷 단독, 30% 결합세트, 10% IPTV
            double roll = rng.nextDouble();
            if (roll < 0.6) return pickRandom(HOME_INTERNET, rng);
            if (roll < 0.9) return pickRandom(HOME_BUNDLE, rng);
            return pickRandom(HOME_IPTV, rng);
        }

        // 두 번째 이후: IPTV, 스마트홈, 인터넷전화 등
        double roll = rng.nextDouble();
        if (roll < 0.35) return pickRandom(HOME_IPTV, rng);
        if (roll < 0.55) return pickRandom(HOME_SMARTHOME, rng);
        if (roll < 0.70) return pickRandom(HOME_PHONE, rng);
        return pickRandom(HOME_INTERNET_ALL, rng);
    }

    // ════════════════════════════════════════════════
    // 해지일 결정 (약정 기반)
    // 10% 중도해지(약정 전), 30% 만기해지(약정~현재), 60% NULL
    // ════════════════════════════════════════════════
    private LocalDateTime decideExtinguishDate(LocalDateTime joinedAt, int contractMonths,
                                                ThreadLocalRandom rng) {
        double roll = rng.nextDouble();

        if (roll < 0.60) {
            // 60% → 유지 중 (NULL)
            return null;
        }

        LocalDateTime contractEnd = joinedAt.plusMonths(contractMonths);

        if (roll < 0.70) {
            // 10% → 중도해지 (가입일 ~ 약정만료 사이)
            if (contractMonths <= 0) return null;
            long daysBetween = ChronoUnit.DAYS.between(joinedAt, contractEnd);
            if (daysBetween <= 1) return null;
            long cancelDays = rng.nextLong(30, Math.max(31, daysBetween));
            LocalDateTime cancelDate = joinedAt.plusDays(cancelDays);
            // 미래 날짜 방지
            return cancelDate.isAfter(LocalDateTime.now()) ? null : cancelDate;
        }

        // 30% → 만기 이후 해지 (약정종료 ~ 현재)
        if (contractEnd.isAfter(LocalDateTime.now())) {
            // 아직 약정 중이면 NULL
            return null;
        }
        long daysAfterContract = ChronoUnit.DAYS.between(contractEnd, LocalDateTime.now());
        if (daysAfterContract <= 1) return null;
        long extinguishDays = rng.nextLong(1, daysAfterContract);
        return contractEnd.plusDays(extinguishDays);
    }

    // 부가서비스 해지일 (약정 없음)
    private LocalDateTime decideAdditionalExtinguish(LocalDateTime joinDate, ThreadLocalRandom rng) {
        double roll = rng.nextDouble();
        if (roll < 0.70) return null; // 70% 유지

        // 30% 해지: 가입일 + 1개월 ~ 현재
        long daysSince = ChronoUnit.DAYS.between(joinDate, LocalDateTime.now());
        if (daysSince <= 30) return null;
        long cancelDays = rng.nextLong(30, daysSince);
        LocalDateTime cancelDate = joinDate.plusDays(cancelDays);
        return cancelDate.isAfter(LocalDateTime.now()) ? null : cancelDate;
    }

    // ════════════════════════════════════════════════
    // 계약 해지일 결정: 모든 하위 상품이 해지된 경우에만 계약도 해지
    // ════════════════════════════════════════════════
    private LocalDateTime resolveContractExtinguish(
            List<MobileSubscriptionRow> mobileList, List<HomeSubscriptionRow> homeList) {

        boolean allCanceled = true;
        LocalDateTime latestCancel = null;

        for (MobileSubscriptionRow m : mobileList) {
            if (m.getExtinguishAt() == null) {
                allCanceled = false;
                break;
            }
            if (latestCancel == null || m.getExtinguishAt().isAfter(latestCancel)) {
                latestCancel = m.getExtinguishAt();
            }
        }
        if (allCanceled) {
            for (HomeSubscriptionRow h : homeList) {
                if (h.getExtinguishAt() == null) {
                    allCanceled = false;
                    break;
                }
                if (latestCancel == null || h.getExtinguishAt().isAfter(latestCancel)) {
                    latestCancel = h.getExtinguishAt();
                }
            }
        }

        return allCanceled ? latestCancel : null;
    }

    // ════════════════════════════════════════════════
    // 유틸
    // ════════════════════════════════════════════════
    private String pickRandom(List<String> list, ThreadLocalRandom rng) {
        return list.get(rng.nextInt(list.size()));
    }

    // 각 상품의 가입일을 살짝 다르게 (같은 날 or 수일 차이)
    private LocalDateTime adjustJoinTime(LocalDateTime base, int index, ThreadLocalRandom rng) {
        if (index == 0) return base;
        int offsetDays = rng.nextInt(0, 31); // 0~30일 차이
        LocalDateTime adjusted = base.plusDays(offsetDays);
        return adjusted.isAfter(LocalDateTime.now()) ? base : adjusted;
    }
}