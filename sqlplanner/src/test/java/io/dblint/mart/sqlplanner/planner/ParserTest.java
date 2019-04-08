package io.dblint.mart.sqlplanner.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.dblint.mart.sqlplanner.utils.SelectProvider;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ParserTest {
  private static Logger logger = LoggerFactory.getLogger(ParserTest.class);

  @ParameterizedTest(name="[{index}] {0}")
  @ArgumentsSource(SelectProvider.class)
  @Tags({@Tag("/parseSuccess.sql"), @Tag("/tpcds.sql")})
  void parseSuccessTest(String name, String sql) throws SqlParseException {
    Parser parser = new Parser();
    logger.info(name);
    SqlNode sqlNode = parser.parse(sql);
    assertNotNull(sqlNode);
  }

  @Test
  void testError() {
    Parser parser = new Parser();
    Assertions.assertThrows(SqlParseException.class, () -> parser.parse("select from"));
  }

  @Test
  void digestTest() throws SqlParseException {
    Parser parser = new Parser();
    String digest = parser.digest("select i_color from item where i_color = 'abc'",
        SqlDialect.DatabaseProduct.MYSQL.getDialect());
    assertEquals("SELECT `I_COLOR`\n"
        + "FROM `ITEM`\n"
        + "WHERE `I_COLOR` = ?", digest);
  }

  @Test
  void digestWithMultipleFilters() throws SqlParseException {
    Parser parser = new Parser();
    String digest = parser.digest("select i_color from item where i_item_id = 'abc' "
            + "and i_color = 'blue'",
        SqlDialect.DatabaseProduct.MYSQL.getDialect());
    assertEquals("SELECT `I_COLOR`\n"
        + "FROM `ITEM`\n"
        + "WHERE `I_ITEM_ID` = ? AND `I_COLOR` = ?", digest);
  }

  @Test
  void prettyTest() throws SqlParseException {
    Parser parser = new Parser();
    String digest = parser.pretty("select i_color from item where i_color = 'abc'",
        SqlDialect.DatabaseProduct.MYSQL.getDialect());
    assertEquals("SELECT `I_COLOR`\n"
        + "FROM `ITEM`\n"
        + "WHERE `I_COLOR` = 'abc'", digest);
  }
}