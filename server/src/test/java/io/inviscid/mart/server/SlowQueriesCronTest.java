package io.inviscid.mart.server;

import javax.xml.stream.XMLStreamException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.inviscid.metricsink.mysql.SchemaParser;
import io.inviscid.metricsink.mysql.SlowQueryLogParser;
import io.inviscid.metricsink.mysql.UserQuery;
import io.inviscid.sqlplanner.MySqlClassifier;
import io.inviscid.sqlplanner.QanException;
import io.inviscid.sqlplanner.enums.MySqlEnum;
import io.inviscid.sqlplanner.enums.QueryType;
import io.inviscid.sqlplanner.planner.MartColumn;
import io.inviscid.sqlplanner.planner.MartSchema;
import io.inviscid.sqlplanner.planner.MartTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlowQueriesCronTest {
  private static Logger logger = LoggerFactory.getLogger(SlowQueriesCronTest.class);

  SchemaPlus createSchema(SchemaParser.Database database) throws QanException {
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

  @Disabled
  @Test
  @Tag("cmdline")
  void cmdLineTest() throws XMLStreamException, IOException, QanException {
    logger.info(System.getProperty("schemaFile"));
    SchemaParser.Database database = SchemaParser.parseMySqlDump(
        new FileInputStream(System.getProperty("schemaFile")));

    SchemaPlus schema = createSchema(database);
    MySqlClassifier mySqlClassifier = new MySqlClassifier(schema);

    logger.info(System.getProperty("slowFile"));
    SlowQueryLogParser parser = new SlowQueryLogParser();
    List<UserQuery> queries = parser.parseLog(
        new FileInputStream(System.getProperty("slowFile"))
    );

    for (UserQuery query : queries) {
      if (query.getRowsExamined() > 1000) {
        for (String sql : query.getQueries()) {
          logger.info(sql);
          try {
            List<QueryType> types = mySqlClassifier.classify(sql);
            if (types.contains(MySqlEnum.BAD_NOINDEX)) {
              logger.warn("Query has no index scans");
            }
          } catch (SqlParseException | QanException exc) {
            logger.error("Failed to parse query", exc);
          }
        }
      }
    }
  }
}
