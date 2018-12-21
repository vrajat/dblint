package io.dblint.mart.analyses.redshift;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.redshift.RedshiftCsv;
import io.dblint.mart.metricsink.redshift.SplitUserQuery;
import io.dblint.mart.metricsink.redshift.UserQuery;
import io.dblint.mart.sqlplanner.RedshiftClassifier;
import io.dblint.mart.sqlplanner.enums.AnalyticsEnum;
import io.dblint.mart.sqlplanner.enums.EnumContext;
import io.dblint.mart.sqlplanner.enums.QueryType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DagGenerator {
  Logger logger = LoggerFactory.getLogger(DagGenerator.class);

  Counter numSplitQueries;
  Counter numQueries;
  Counter numParsed;
  Counter numInserts;

  RedshiftClassifier classifier;

  class QueryInfo {
    final UserQuery query;

    public QueryInfo(UserQuery query) {
      this.query = query;
    }
  }

  DagGenerator(MetricRegistry registry) {
    numSplitQueries = registry.counter("io.dblint.dagGenerator.numSplitQueries");
    numQueries = registry.counter("io.dblint.dagGenerator.numQueries");
    numParsed = registry.counter("io.dblint.dagGenerator.numParsed");
    numInserts = registry.counter("io.dblint.DagGenerator.numInserts");

    classifier = new RedshiftClassifier();
  }

  List<UserQuery> generateDag(InputStream is) throws IOException {
    List<SplitUserQuery> splitUserQueries = RedshiftCsv.getQueries(is);
    numSplitQueries.inc(splitUserQueries.size());
    logger.info("numSplitQueries: " + numSplitQueries.getCount());

    List<UserQuery> userQueries = combineSplits(splitUserQueries);
    numQueries.inc(userQueries.size());
    logger.info("numQueries: " + numQueries.getCount());

    parse(userQueries);

    logger.info("numParsed: " + numParsed.getCount());
    logger.info("numInserts: " + numInserts.getCount());
    return userQueries;
  }

  private List<UserQuery> combineSplits(List<SplitUserQuery> splitUserQueries) {
    Map<Integer, List<SplitUserQuery>> groupByQueryId = splitUserQueries.stream().collect(
        Collectors.groupingBy(e -> e.queryId)
    );

    logger.debug("groupByQueryId map size:" + groupByQueryId.size());

    Map<Integer, List<SplitUserQuery>> groupBySortedByQueryId = groupByQueryId.entrySet().stream()
        .collect(
            Collectors.toMap(Map.Entry::getKey,
                e -> e.getValue().stream().sorted().collect(Collectors.toList()))
        );
    logger.debug("groupBySortedByQueryId map size:" + groupBySortedByQueryId.size());

    List<UserQuery> userQueries = new ArrayList<>(groupByQueryId.size());

    groupBySortedByQueryId.forEach((key, value) -> {
      SplitUserQuery splitUserQuery = value.get(0);
      UserQuery userQuery = new UserQuery(
          splitUserQuery.queryId, splitUserQuery.userId, 0, 0,
          splitUserQuery.startTime, splitUserQuery.endTime, splitUserQuery.duration,
          splitUserQuery.db, false, "");

      value.stream().forEach(s -> userQuery.addQueryFragment(s.query));
      userQueries.add(userQuery);
    });

    return userQueries;
  }

  private List<QueryInfo> parse(List<UserQuery> queries) {
    List<QueryInfo> queryInfos = new ArrayList<>();
    queries.forEach((query) -> {
      try {
        List<QueryType> types = classifier.classify(query.query, EnumContext.EMPTY_CONTEXT);
        numParsed.inc();
        types.stream()
            .filter(type -> type == AnalyticsEnum.INSERT).findFirst()
            .ifPresent(type -> {
              queryInfos.add(new QueryInfo(query));
              numInserts.inc();
            });
      } catch (SqlParseException exception) {
        // logger.warn(exception.getMessage());
      }
    });
    return queryInfos;
  }
}
