package com.uplus.batch.jobs.subscription_dummy.writer;

import com.uplus.batch.jobs.subscription_dummy.dto.AdditionalSubscriptionRow;
import com.uplus.batch.jobs.subscription_dummy.dto.ContractRow;
import com.uplus.batch.jobs.subscription_dummy.dto.CustomerSubscriptionPlan;
import com.uplus.batch.jobs.subscription_dummy.dto.HomeSubscriptionRow;
import com.uplus.batch.jobs.subscription_dummy.dto.MobileSubscriptionRow;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

/**
 * 고객 구독 플랜을 4개 테이블에 순차 INSERT하는 Writer
 *
 * 순서: customer_contracts → customer_subscription_mobile
 *       → customer_subscription_home → customer_subscription_additional
 *
 * contract_id가 Auto Increment이므로 계약 INSERT 후 생성된 ID를
 * 하위 구독 테이블의 FK로 사용
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SubscriptionCompositeWriter implements ItemWriter<CustomerSubscriptionPlan> {

    private final JdbcTemplate jdbcTemplate;

    private static final String INSERT_CONTRACT = """
            INSERT INTO customer_contracts
            (combo_code, created_at, extinguish_at)
            VALUES (?, ?, ?)
            """;

    private static final String INSERT_MOBILE = """
            INSERT INTO customer_subscription_mobile
            (customer_id, contract_id, mobile_code, joined_at, extinguish_at)
            VALUES (?, ?, ?, ?, ?)
            """;

    private static final String INSERT_HOME = """
            INSERT INTO customer_subscription_home
            (customer_id, contract_id, home_code, joined_at, extinguish_at)
            VALUES (?, ?, ?, ?, ?)
            """;

    private static final String INSERT_ADDITIONAL = """
            INSERT INTO customer_subscription_additional
            (customer_id, contract_id, service_code, join_date, extinguish_date)
            VALUES (?, ?, ?, ?, ?)
            """;

    @Override
    public void write(Chunk<? extends CustomerSubscriptionPlan> chunk) throws Exception {

        for (CustomerSubscriptionPlan plan : chunk) {

            // ── 1) customer_contracts INSERT → contract_id 획득 ──
            Long contractId = insertContract(plan.getContract());

            if (contractId == null) {
                log.warn("Failed to insert contract for combo_code={}",
                        plan.getContract().getComboCode());
                continue;
            }

            // ── 2) customer_subscription_mobile INSERT ──
            for (MobileSubscriptionRow mobile : plan.getMobileSubscriptions()) {
                insertMobile(contractId, mobile);
            }

            // ── 3) customer_subscription_home INSERT ──
            for (HomeSubscriptionRow home : plan.getHomeSubscriptions()) {
                insertHome(contractId, home);
            }

            // ── 4) customer_subscription_additional INSERT ──
            for (AdditionalSubscriptionRow additional : plan.getAdditionalSubscriptions()) {
                insertAdditional(contractId, additional);
            }
        }
    }

    private Long insertContract(ContractRow contract) {
        KeyHolder keyHolder = new GeneratedKeyHolder();

        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(
                    INSERT_CONTRACT, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, contract.getComboCode());                             // nullable
            ps.setTimestamp(2, Timestamp.valueOf(contract.getCreatedAt()));
            ps.setTimestamp(3, contract.getExtinguishAt() != null
                    ? Timestamp.valueOf(contract.getExtinguishAt()) : null);
            return ps;
        }, keyHolder);

        Number key = keyHolder.getKey();
        return key != null ? key.longValue() : null;
    }

    private void insertMobile(Long contractId, MobileSubscriptionRow row) {
        jdbcTemplate.update(INSERT_MOBILE,
        		row.getCustomerId(),
                contractId,
                row.getMobileCode(),
                Timestamp.valueOf(row.getJoinedAt()),
                row.getExtinguishAt() != null
                        ? Timestamp.valueOf(row.getExtinguishAt()) : null
        );
    }

    private void insertHome(Long contractId, HomeSubscriptionRow row) {
        jdbcTemplate.update(INSERT_HOME,
        		row.getCustomerId(),
                contractId,
                row.getHomeCode(),
                Timestamp.valueOf(row.getJoinedAt()),
                row.getExtinguishAt() != null
                        ? Timestamp.valueOf(row.getExtinguishAt()) : null
        );
    }

    private void insertAdditional(Long contractId, AdditionalSubscriptionRow row) {
        jdbcTemplate.update(INSERT_ADDITIONAL,
        		row.getCustomerId(),
                contractId,
                row.getServiceCode(),
                Timestamp.valueOf(row.getJoinDate()),
                row.getExtinguishDate() != null
                        ? Timestamp.valueOf(row.getExtinguishDate()) : null
        );
    }
}