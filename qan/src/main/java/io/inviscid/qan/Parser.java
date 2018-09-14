package io.inviscid.qan;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

public class Parser {
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

  public SqlNode parse(String sql) throws SqlParseException {
    return sqlParser.parseQuery(sql);
  }
}
