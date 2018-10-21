package io.inviscid.sqlplanner.planner;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class Parser {
  private static Logger logger = LoggerFactory.getLogger(Parser.class);

  public final SqlParser sqlParser;

  Quoting quoting = Quoting.DOUBLE_QUOTE;
  Casing unquotedCasing = Casing.TO_UPPER;
  Casing quotedCasing = Casing.UNCHANGED;
  SqlConformance conformance = SqlConformanceEnum.DEFAULT;

  /**
   * Sole Constructor for a SQL Parser based on Apache Calcite.
   * Uses default values for various parameters for the Calcite Parser.
   */
  public Parser() {
    this.sqlParser = SqlParser.create("",
        SqlParser.configBuilder()
            .setParserFactory(SqlParserImpl.FACTORY)
            .setQuoting(quoting)
            .setUnquotedCasing(unquotedCasing)
            .setQuotedCasing(quotedCasing)
            .setConformance(conformance)
            .build());
  }

  private String trim(String sql) {
    sql = sql.trim();
    List<Character> chars = Arrays.asList(';');
    boolean found = true;
    while (found) {
      found = false;
      for (Character character : chars) {
        if (sql.charAt(sql.length() - 1) == character) {
          sql = sql.substring(0, sql.length() - 1);
          found = true;
        }
      }
    }
    return sql;
  }

  private String handleNewLine(String sql) {
    return sql.replaceAll("\\\\n", "\n").replaceAll("\\r", "\r");
  }

  /**
   * Parse a SQL string using Apache Calcite.
   * @param sql String containing the SQL query
   * @return SqlNode as Root of the parse tree
   * @throws SqlParseException A parse exception if parsing fails
   */
  public SqlNode parse(String sql) throws SqlParseException {
    String processedSql = trim(handleNewLine(sql));
    try {
      return sqlParser.parseQuery(processedSql);
    } catch (SqlParseException parseExc) {
      logger.error(processedSql);
      throw parseExc;
    }
  }
}
