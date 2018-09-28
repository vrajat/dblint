package io.inviscid.qan;

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

  /**
   * Parse a SQL string using Apache Calcite.
   * @param sql String containing the SQL query
   * @return SqlNode as Root of the parse tree
   * @throws SqlParseException A parse exception if parsing fails
   */
  public SqlNode parse(String sql) throws SqlParseException {
    try {
      return sqlParser.parseQuery(sql);
    } catch (SqlParseException parseExc) {
      logger.error(sql);
      throw parseExc;
    }
  }
}
