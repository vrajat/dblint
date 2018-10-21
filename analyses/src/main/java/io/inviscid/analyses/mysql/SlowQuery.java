package io.inviscid.analyses.mysql;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import io.inviscid.metricsink.mysql.SchemaParser;
import io.inviscid.metricsink.mysql.UserQuery;
import io.inviscid.sqlplanner.MySqlClassifier;
import io.inviscid.sqlplanner.QanException;
import io.inviscid.sqlplanner.enums.MySqlEnum;
import io.inviscid.sqlplanner.enums.QueryType;
import io.inviscid.sqlplanner.planner.MartColumn;
import io.inviscid.sqlplanner.planner.MartSchema;
import io.inviscid.sqlplanner.planner.MartTable;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SlowQuery {
  private static Logger logger = LoggerFactory.getLogger(SlowQuery.class);

  SchemaPlus schema;
  MySqlClassifier classifier;
  Map<String, QueryStats> aggQueryStats;
  Counter numQueries;
  Counter parseExceptions;

  SlowQuery(SchemaParser.Database database, MetricRegistry registry) throws QanException {
    schema = createSchema(database);
    classifier = new MySqlClassifier(schema);
    aggQueryStats = new HashMap<>();
    numQueries = registry.counter("inviscid.slowQuery.numQueries");
    parseExceptions = registry.counter("inviscid.slowQuery.parseExceptions");
  }

  private SchemaPlus createSchema(SchemaParser.Database database) throws QanException {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    MartSchema martSchema = new MartSchema(database.name);
    SchemaPlus mySqlSchemaPlus = rootSchema.add("mysql", martSchema);
    martSchema.setSchemaPlus(mySqlSchemaPlus);

    for (SchemaParser.TableStructure ts : database.tables) {
      List<MartColumn> columns = new ArrayList<>();
      for (SchemaParser.Field f : ts.fieldList) {
        MartColumn col = new MartColumn(f.field, f.type);
        columns.add(col);
      }

      String primaryKey = null;
      List<String> secondaryKeys = new ArrayList<>();

      if (ts.keys != null) {
        for (SchemaParser.Key key : ts.keys) {
          if (key.keyName == "PRIMARY") {
            primaryKey = key.columnName;
          } else {
            secondaryKeys.add(key.columnName);
          }
        }
      }

      MartTable martTable = new MartTable(martSchema, ts.name, columns,
          primaryKey, secondaryKeys);
      martSchema.addTable(martTable);
    }

    return mySqlSchemaPlus;
  }

  void analyze(List<UserQuery> queries) {
    for (UserQuery query : queries) {
      if (query.getRowsExamined() > 1000) {
        for (String sql : query.getQueries()) {
          numQueries.inc();
          logger.info(sql);
          try {
            String digest = classifier.planner.digest(sql,
                SqlDialect.DatabaseProduct.MYSQL.getDialect());
            List<QueryType> types = classifier.classify(sql);
            QueryStats queryStats;
            if (aggQueryStats.containsKey(digest)) {
              queryStats = aggQueryStats.get(digest);
            } else {
              queryStats = new QueryStats(digest);
              aggQueryStats.put(digest, queryStats);
            }
            queryStats.addLockTime(query.getLockTime())
                .addQueryTime(query.getQueryTime())
                .addNumQueries(1)
                .addRowsSent(query.getRowsSent())
                .addRowsExamined(query.getRowsExamined());
            if (types.contains(MySqlEnum.BAD_NOINDEX)) {
              logger.warn("Query has no index scans");
            } else {
              queryStats.addIndexUsed(1);
            }
          } catch (SqlParseException | ValidationException
              | RelConversionException | QanException exc) {
            logger.error("Failed to parse query", exc);
            parseExceptions.inc();
          }
        }
      }
    }
  }

  Map<String, QueryStats> getAggQueryStats() {
    return aggQueryStats;
  }
}
