package io.dblint.mart.sqlplanner;

import io.dblint.mart.sqlplanner.enums.QueryType;
import io.dblint.mart.sqlplanner.enums.RedshiftEnum;

import java.util.List;

import io.dblint.mart.sqlplanner.planner.RedshiftParser;
import io.dblint.mart.sqlplanner.visitors.InsertVisitor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedshiftClassifier extends AnalyticsClassifier {
  private static Logger logger = LoggerFactory.getLogger(RedshiftClassifier.class);

  public RedshiftClassifier() {
    super();
  }

  @Override
  protected SqlParserImplFactory getFactory() {
    return RedshiftParser.FACTORY;
  }

  @Override
  List<QueryType> classifyImpl(SqlNode parseTree) {
    List<QueryType> typeList = super.classifyImpl(parseTree);
    for (RedshiftEnum redshiftEnum : RedshiftEnum.values()) {
      if (redshiftEnum.isPassed(parseTree)) {
        typeList.add(redshiftEnum);
      }
    }
    return typeList;
  }

  /**
   * Classify whether a query is an insert statement.
   * @param query Query String
   * @return Returns the visitor with additional info about the insert statement
   * @throws SqlParseException Exception thrown if there is a syntax error
   */
  public InsertVisitor classifyInsert(String query) throws SqlParseException {
    InsertVisitor visitor = new InsertVisitor();
    SqlNode sqlNode = parser.parse(query);

    sqlNode.accept(visitor);
    if (visitor.isPassed()) {
      logger.debug("Passed Insert Query");
      logger.debug(query);
      logger.debug(sqlNode.toSqlString(SqlDialect.DatabaseProduct.REDSHIFT.getDialect())
          .getSql());
      logger.debug(visitor.getTargetTable());
      if (visitor.getSources().size() > 0) {
        logger.debug("Num Sources: " + visitor.getSources().size());
      }
    }
    return visitor;
  }
}
