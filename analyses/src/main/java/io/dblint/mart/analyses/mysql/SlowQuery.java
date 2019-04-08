package io.dblint.mart.analyses.mysql;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.mysql.QueryAttribute;
import io.dblint.mart.metricsink.mysql.SchemaParser;
import io.dblint.mart.metricsink.mysql.UserQuery;
import io.dblint.mart.sqlplanner.MySqlClassifier;
import io.dblint.mart.sqlplanner.QanException;
import io.dblint.mart.sqlplanner.enums.MySqlEnum;
import io.dblint.mart.sqlplanner.enums.MySqlEnumContext;
import io.dblint.mart.sqlplanner.enums.QueryType;
import io.dblint.mart.sqlplanner.planner.MartColumn;
import io.dblint.mart.sqlplanner.planner.MartSchema;
import io.dblint.mart.sqlplanner.planner.MartTable;

import io.dblint.mart.sqlplanner.planner.Parser;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
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
  Parser parser;
  Map<String, QueryStats> aggQueryStats;
  Counter numQueries;
  Counter parseExceptions;

  /**
   * A SlowQuery analyzer for MySQL.
   * @param database Database schema of MySQL
   * @param registry A metric registry to store relevant counters
   * @throws QanException Throw an exception if a calcite schema creation fails
   */
  public SlowQuery(SchemaParser.Database database, MetricRegistry registry) throws QanException {
    schema = createSchema(database);
    classifier = new MySqlClassifier(schema);
    aggQueryStats = new HashMap<>();
    numQueries = registry.counter("io.dblint.slowQuery.numQueries");
    parseExceptions = registry.counter("io.dblint.slowQuery.parseExceptions");
    parser = null;
  }

  /**
   * A SlowQuery analyzer for MySQL.
   */
  public SlowQuery() {
    schema = null;
    classifier = null;
    aggQueryStats = null;
    numQueries = null;
    parseExceptions = null;
    parser = new Parser(SqlBabelParserImpl.FACTORY);

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
          ts.options.rows.doubleValue(), primaryKey, secondaryKeys);
      martSchema.addTable(martTable);
    }

    return mySqlSchemaPlus;
  }

  /**
   * Analyze a SQL statement and return all known attributes.
   * @param sql A string containing a SQL statement
   * @return QueryAttribute class
   * @throws SqlParseException Throws exception if not valid SQL syntax
   */
  public QueryAttribute analyze(String sql) throws SqlParseException {
    return new QueryAttribute(
        parser.digest(sql,
            SqlDialect.DatabaseProduct.MYSQL.getDialect())
    );
  }
}
