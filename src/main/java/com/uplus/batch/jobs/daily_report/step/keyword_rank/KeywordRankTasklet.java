package com.uplus.batch.jobs.daily_report.step.keyword_rank;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import com.uplus.batch.common.elasticsearch.ElasticsearchAnalyzeService;
import com.uplus.batch.jobs.daily_report.dto.DailyReportSnapshot;
import com.uplus.batch.jobs.daily_report.dto.GradeSnapshot;
import com.uplus.batch.jobs.daily_report.step.keyword_rank.Consultation;
import com.uplus.batch.jobs.daily_report.step.keyword_rank.ConsultationReaderConfig;
import com.uplus.batch.jobs.daily_report.step.keyword_rank.KeywordAggregator;
import com.uplus.batch.jobs.daily_report.step.keyword_rank.KeywordCount;
import java.time.LocalTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class KeywordRankTasklet implements Tasklet {

  private final ConsultationReaderConfig readerConfig;
  private final ElasticsearchAnalyzeService analyzeService;
  private final MongoTemplate mongoTemplate;

  private final ElasticsearchClient elasticsearchClient; // 직접 주입

  @Override
  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
      throws Exception {

    // 어제 날짜만 조회
    LocalDate targetDate = LocalDate.now().minusDays(1);
    String startOfDay = targetDate.atStartOfDay().toString();
    String endOfDay = targetDate.atTime(LocalTime.MAX).toString();

    // 1. 쿼리 객체 생성 (가장 안전한 'of' 문법)
    Query rangeQuery = Query.of(q -> q
        .range(r -> r
            .date(d -> d
                .field("created_at")
                .gte(startOfDay)
                .lte(endOfDay)
            )
        )
    );

    // 2. ES 집계 실행
    SearchResponse<Void> response = elasticsearchClient.search(s -> s
            .index("consult-index")
            .size(0)
            .query(rangeQuery) // 생성한 쿼리 주입
            .aggregations("total_keywords", a -> a
                .terms(t -> t.field("content").size(20)) // "content.field"
            )
            .aggregations("by_grade", a -> a
                .terms(t -> t.field("grade_code"))
                .aggregations("grade_keywords", subA -> subA
                    .terms(t -> t.field("content").size(6)) // "content.field"
                )
            ),
        Void.class
    );

    // 2. 결과 파싱 및 Snapshot 생성
    List<KeywordCount> topKeywords = parseTotalKeywords(response);
    List<GradeSnapshot> gradeSnapshots = parseGradeSnapshots(response);

    DailyReportSnapshot snapshot = DailyReportSnapshot.builder()
        .date(targetDate)
        .topKeywords(topKeywords)
        .byGradeCode(gradeSnapshots)
        .build();

    // 3. 로그 출력 (logAndSave 메서드 대신 직접 구현)
    log.info("===== 전체 키워드 TOP10 =====");
    snapshot.getTopKeywords().stream().limit(10)
        .forEach(k -> log.info("keyword={}, count={}", k.getKeyword(), k.getCount()));

    // 4. MongoDB 저장
    mongoTemplate.save(snapshot);

    return RepeatStatus.FINISHED;
  }

  // 결과 파싱용 헬퍼 메서드들
  private List<KeywordCount> parseTotalKeywords(SearchResponse<Void> response) {
    return response.aggregations().get("total_keywords").sterms().buckets().array().stream()
        .map(b -> new KeywordCount(b.key().stringValue(), b.docCount()))
        .toList();
  }

  private List<GradeSnapshot> parseGradeSnapshots(SearchResponse<Void> response) {
    return response.aggregations().get("by_grade").sterms().buckets().array().stream()
        .map(gradeBucket -> {
          List<KeywordCount> keywords = gradeBucket.aggregations().get("grade_keywords")
              .sterms().buckets().array().stream()
              .map(k -> new KeywordCount(k.key().stringValue(), k.docCount()))
              .toList();
          return new GradeSnapshot(gradeBucket.key().stringValue(), keywords);
        })
        .toList();
  }
}


//    var reader = readerConfig.dailyConsultationReader();
//    reader.open(chunkContext.getStepContext().getStepExecution().getExecutionContext());
//
//    KeywordAggregator aggregator = new KeywordAggregator();
//
//    try {
//      Consultation item;
//
//      while ((item = reader.read()) != null) {
//        String content = item.getContent();
//        if (content == null || content.length() < 2) {
//          continue;
//        }
//
//        // 에러 발생해도 finally 블록에서 reader 닫힘.
//        List<String> keywords = analyzeService.analyze(content);
//        aggregator.accumulate(keywords, item.getGradeCode());
//      }
//
//    } finally {
//      reader.close();
//    }
//
//    List<KeywordCount> topKeywords =
//        aggregator.getTotalKeywordCount().entrySet().stream()
//            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
////            .limit(10)
//            .map(e -> new KeywordCount(e.getKey(), e.getValue()))
//            .toList();
//
//    List<GradeSnapshot> gradeSnapshots =
//        aggregator.getByGradeCode().entrySet().stream()
//            .map(entry -> {
//
//              List<KeywordCount> list =
//                  entry.getValue().entrySet().stream()
//                      .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
//                      .map(e -> new KeywordCount(e.getKey(), e.getValue()))
//                      .toList();
//
//              return new GradeSnapshot(entry.getKey(), list);
//            })
//            .toList();
//
//    DailyReportSnapshot snapshot =
//        DailyReportSnapshot.builder()
//            .date(LocalDate.now().minusDays(1))
//            .topKeywords(topKeywords)
//            .byGradeCode(gradeSnapshots)
//            .build();
//
//
//    // ===== 전체 키워드 TOP10 로그 =====
//    log.info("===== 전체 키워드 TOP10 =====");
//
//    snapshot.getTopKeywords()
//        .stream()
//        .limit(10)
//        .forEach(k ->
//            log.info("keyword={}, count={}", k.getKeyword(), k.getCount())
//        );
//
//
//// ===== 고객 등급별 TOP5 로그 =====
//    snapshot.getByGradeCode()
//        .forEach(grade -> {
//
//          log.info("===== 고객등급 {} TOP5 키워드 =====", grade.getGradeCode());
//
//          grade.getKeywords()
//              .stream()
//              .limit(5)
//              .forEach(k ->
//                  log.info("keyword={}, count={}", k.getKeyword(), k.getCount())
//              );
//        });
//
//    mongoTemplate.save(snapshot);
//
//    return RepeatStatus.FINISHED;
//  }
//}