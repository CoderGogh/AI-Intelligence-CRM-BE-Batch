package com.uplus.batch.jobs.raw_text_dummy.tasklet;

import com.uplus.batch.jobs.raw_text_dummy.generator.RawTextGenerator;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * consultation_results 를 조회해 consultation_raw_texts 에 상담 원문 더미 데이터를 삽입한다.
 *
 * <p>실행 예시:
 * <pre>
 * curl -X POST "http://localhost:8081/dummy/raw-texts?startId=1&endId=100"
 * </pre>
 *
 * <p>CHURN 비율: 약 30% (M_CHN_* 코드 + 기타 카테고리 15% 오버라이드)
 */
@Component
@RequiredArgsConstructor
public class RawTextDummyTasklet implements Tasklet {

    private final JdbcTemplate jdbcTemplate;
    private final RawTextGenerator generator;
    private final Random random = new Random();

    private static final int CHUNK_SIZE = 100;

    @Override
    public RepeatStatus execute(StepContribution contribution,
            ChunkContext chunkContext) {

        long startId = Long.parseLong(
                chunkContext.getStepContext().getJobParameters()
                        .get("startId").toString());
        long endId = Long.parseLong(
                chunkContext.getStepContext().getJobParameters()
                        .get("endId").toString());

        if (startId > endId) {
            throw new IllegalArgumentException("startId must be <= endId");
        }

        long current = startId;
        while (current <= endId) {
            long rangeEnd = Math.min(current + CHUNK_SIZE - 1, endId);
            processChunk(current, rangeEnd);
            current = rangeEnd + 1;
        }

        return RepeatStatus.FINISHED;
    }

    private void processChunk(long from, long to) {
        String selectSql = """
                SELECT consult_id, category_code
                FROM consultation_results
                WHERE consult_id BETWEEN ? AND ?
                ORDER BY consult_id
                """;

        // (consultId, categoryCode) 쌍 조회
        List<long[]> rows = new ArrayList<>();
        List<String> categories = new ArrayList<>();

        jdbcTemplate.query(selectSql, rs -> {
            rows.add(new long[]{rs.getLong("consult_id")});
            categories.add(rs.getString("category_code"));
        }, from, to);

        if (rows.isEmpty()) return;

        // 배치 INSERT
        String insertSql = """
                INSERT INTO consultation_raw_texts (consult_id, raw_text_json)
                VALUES (?, ?)
                """;

        List<Object[]> batchArgs = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            long consultId    = rows.get(i)[0];
            String category   = categories.get(i);
            String rawTextJson = generator.generate(category, random);
            batchArgs.add(new Object[]{consultId, rawTextJson});
        }

        jdbcTemplate.batchUpdate(insertSql, batchArgs);
    }
}
