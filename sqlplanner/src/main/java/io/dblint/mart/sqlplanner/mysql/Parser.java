package io.dblint.mart.sqlplanner.mysql;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

public class Parser extends io.dblint.mart.sqlplanner.planner.Parser {
  /**
   * Create a Parser that understands MySQL dialect.
   */
  public Parser() {
    super(
        SqlBabelParserImpl.FACTORY,
        Quoting.BACK_TICK,
        Casing.TO_UPPER,
        Casing.UNCHANGED,
        SqlConformanceEnum.LENIENT
    );
  }
}
