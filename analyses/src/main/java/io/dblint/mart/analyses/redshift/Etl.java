package io.dblint.mart.analyses.redshift;

import com.google.common.graph.ImmutableGraph;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.redshift.Agent;
import io.dblint.mart.metricsink.redshift.UserQuery;
import io.dblint.mart.metricsink.util.MetricAgentException;
import io.dblint.mart.sqlplanner.redshift.QueryClasses;
import io.dblint.mart.sqlplanner.redshift.RedshiftClassifier;
import org.apache.calcite.sql.parser.SqlParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

class Etl {
  private static Logger logger = LoggerFactory.getLogger(Etl.class);

  private Counter numQueries;
  private Counter numParsed;
  private Counter numInserts;
  private Counter numInsertsWithSelects;
  private Counter numLongDml;
  private Counter numMaintenanceQueries;
  private Counter numCtasQueries;

  private RedshiftClassifier classifier;

  Etl(MetricRegistry registry) {
    numQueries = registry.counter("io.dblint.Etl.numQueries");
    numParsed = registry.counter("io.dblint.Etl.numParsed");
    numInserts = registry.counter("io.dblint.Etl.numInserts");
    numInsertsWithSelects = registry.counter("io.dblint.Etl.numInsertSelects");
    numLongDml = registry.counter("io.dblint.Etl.numLongDml");
    numMaintenanceQueries = registry.counter("io.dblint.Etl.numMaintenanceQueries");
    numCtasQueries = registry.counter("io.dblint.Etl.numCtas");

    classifier = new RedshiftClassifier();
  }

  void analyze(Agent agent) throws IOException, MetricAgentException {
    List<UserQuery> userQueries = agent.getQueries(
        LocalDateTime.of(2018, 12, 10, 0, 0),
        LocalDateTime.of(2018, 12, 25, 0, 0)
    );
    numQueries.inc(userQueries.size());

    List<QueryInfo> queryInfos = null;
    try {
      longRunningQueries(userQueries);
      queryInfos = parse(userQueries);
      ImmutableGraph<String> dag = DagGenerator.buildGraph(queryInfos);
      //Gantt.sort(queryInfos);
    } catch (Exception exc) {
      logger.error(exc.getMessage(), exc);
    }

    logger.info("numQueries: " + numQueries.getCount());
    logger.info("numMaintenanceQueries: " + numMaintenanceQueries.getCount());
    logger.info("numParsed: " + numParsed.getCount());
    logger.info("numInserts: " + numInserts.getCount());
    logger.info("numInsertWithSelects: " + numInsertsWithSelects.getCount());
    logger.info("numCtas: " + numCtasQueries.getCount());
    logger.info("numLongDml: " + numLongDml.getCount());
  }

  private void longRunningQueries(List<UserQuery> queries) {
    logger.info("Long Duration Queries");
    queries.stream()
        .sorted((q1, q2) -> ((Double)q2.duration).compareTo(q1.duration))
        .limit(20)
        .forEach(q -> logger.info(q.toString()));
  }

  List<QueryInfo> parse(List<UserQuery> queries) {
    List<QueryInfo> queryInfos = new ArrayList<>();
    queries.forEach((query) -> {
      try {
        QueryClasses classes = classifier.classify(query.query);
        numParsed.inc();

        if (classes.maintenanceContext.isPassed()) {
          numMaintenanceQueries.inc();
        }

        if (classes.insertContext.isPassed()) {
          logger.debug("Passed Insert Query");
          logger.debug(classes.insertContext.getTargetTable());
          if (classes.insertContext.getSources().size() > 0) {
            logger.debug("Num Sources: " + classes.insertContext.getSources().size());
            numInsertsWithSelects.inc();
            if (Duration.between(query.startTime, query.endTime).getSeconds() > 60) {
              queryInfos.add(new QueryInfo(query, classes));
              numLongDml.inc();
            }
          }
          numInserts.inc();
        }

        if (classes.ctasContext.isPassed()) {
          numCtasQueries.inc();
          if (Duration.between(query.startTime, query.endTime).getSeconds() > 60) {
            queryInfos.add(new QueryInfo(query, classes));
            numLongDml.inc();
          }
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
