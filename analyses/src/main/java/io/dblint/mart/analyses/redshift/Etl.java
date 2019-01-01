package io.dblint.mart.analyses.redshift;

import com.google.common.graph.ImmutableGraph;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.redshift.Agent;
import io.dblint.mart.metricsink.redshift.UserQuery;
import io.dblint.mart.metricsink.util.MetricAgentException;
import io.dblint.mart.sqlplanner.RedshiftClassifier;
import io.dblint.mart.sqlplanner.visitors.InsertVisitor;
import org.apache.calcite.sql.parser.SqlParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class Etl {
  private static Logger logger = LoggerFactory.getLogger(Etl.class);

  Counter numQueries;
  Counter numParsed;
  Counter numInserts;
  Counter numInsertsWithSelects;
  Counter numLongInserts;

  RedshiftClassifier classifier;

  Etl(MetricRegistry registry) {
    numQueries = registry.counter("io.dblint.dagGenerator.numQueries");
    numParsed = registry.counter("io.dblint.dagGenerator.numParsed");
    numInserts = registry.counter("io.dblint.DagGenerator.numInserts");
    numInsertsWithSelects = registry.counter("io.dblint.DagGenerator.numInsertSelects");
    numLongInserts = registry.counter("io.dblint.DagGenerator.numLongInserts");

    classifier = new RedshiftClassifier();
  }

  void analyze(Agent agent) throws IOException, MetricAgentException {
    List<UserQuery> userQueries = agent.getQueries();
    numQueries.inc(userQueries.size());

    List<QueryInfo> queryInfos = null;
    try {
      longRunningQueries(userQueries);
      queryInfos = parse(userQueries);
      ImmutableGraph<String> dag = DagGenerator.buildGraph(queryInfos);
      Gantt.sort(queryInfos);
    } catch (Exception exc) {
      logger.error(exc.getMessage(), exc);
    }

    logger.info("numQueries: " + numQueries.getCount());
    logger.info("numParsed: " + numParsed.getCount());
    logger.info("numInserts: " + numInserts.getCount());
    logger.info("numInsertWithSelects: " + numInsertsWithSelects.getCount());
    logger.info("numLongInserts: " + numLongInserts.getCount());
  }

  private void longRunningQueries(List<UserQuery> queries) {
    logger.info("Long Duration Queries");
    queries.stream()
        .sorted((q1, q2) -> ((Double)q2.duration).compareTo(q1.duration))
        .limit(20)
        .forEach(q -> logger.info(q.toString()));
  }

  private List<QueryInfo> parse(List<UserQuery> queries) {
    List<QueryInfo> queryInfos = new ArrayList<>();
    queries.forEach((query) -> {
      try {
        InsertVisitor visitor = classifier.classifyInsert(query.query);
        numParsed.inc();

        if (visitor.isPassed()) {
          if (visitor.getSources().size() > 0) {
            numInsertsWithSelects.inc();
            if (Duration.between(query.endTime, query.startTime).getSeconds() > 60) {
              queryInfos.add(new QueryInfo(query, visitor.getTargetTable(), visitor.getSources()));
              numLongInserts.inc();
            }
          }
          numInserts.inc();
        }
      } catch (SqlParseException exception) {
        logger.warn(query.query);
        logger.warn(exception.getMessage());
        logger.warn("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
      }
    });
    return queryInfos;
  }
}
