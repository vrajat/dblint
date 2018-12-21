package io.dblint.mart.analyses.redshift;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.redshift.RedshiftCsv;
import io.dblint.mart.metricsink.redshift.SplitUserQuery;
import io.dblint.mart.metricsink.redshift.UserQuery;
import io.dblint.mart.sqlplanner.RedshiftClassifier;
import io.dblint.mart.sqlplanner.visitors.InsertVisitor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class DagGenerator {
  Logger logger = LoggerFactory.getLogger(DagGenerator.class);

  Counter numQueries;
  Counter numParsed;
  Counter numInserts;

  RedshiftClassifier classifier;

  class QueryInfo {
    final UserQuery query;
    final SqlNode targetTable;
    final SqlNode source;

    public QueryInfo(UserQuery query, SqlNode targetTable, SqlNode source) {
      this.query = query;
      this.targetTable = targetTable;
      this.source = source;
    }
  }

  DagGenerator(MetricRegistry registry) {
    numQueries = registry.counter("io.dblint.dagGenerator.numQueries");
    numParsed = registry.counter("io.dblint.dagGenerator.numParsed");
    numInserts = registry.counter("io.dblint.DagGenerator.numInserts");

    classifier = new RedshiftClassifier();
  }

  List<UserQuery> generateDag(InputStream is) throws IOException {
    List<UserQuery> userQueries = RedshiftCsv.getQueries(is);
    numQueries.inc(userQueries.size());
    logger.info("numQueries: " + numQueries.getCount());

    parse(userQueries);

    logger.info("numParsed: " + numParsed.getCount());
    logger.info("numInserts: " + numInserts.getCount());
    return userQueries;
  }

  private List<QueryInfo> parse(List<UserQuery> queries) {
    List<QueryInfo> queryInfos = new ArrayList<>();
    queries.forEach((query) -> {
      try {
        InsertVisitor visitor = new InsertVisitor();
        SqlNode sqlNode = classifier.parser.parse(query.query);
        numParsed.inc();

        sqlNode.accept(visitor);
        if (visitor.isPassed()) {
          logger.debug(sqlNode.toSqlString(SqlDialect.DatabaseProduct.REDSHIFT.getDialect())
              .getSql());
          logger.debug(visitor.getTargetTable()
              .toSqlString(SqlDialect.DatabaseProduct.REDSHIFT.getDialect()).getSql());
          logger.debug(visitor.getSource()
              .toSqlString(SqlDialect.DatabaseProduct.REDSHIFT.getDialect()).getSql());
          queryInfos.add(new QueryInfo(query, visitor.getTargetTable(), visitor.getSource()));
          numInserts.inc();
        }
      } catch (SqlParseException exception) {
        // logger.warn(exception.getMessage());
      }
    });
    return queryInfos;
  }
}
