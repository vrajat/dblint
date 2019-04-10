package io.dblint.mart.sqlplanner.mysql;

import io.dblint.mart.sqlplanner.utils.SelectProvider;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class ParserTest {

  private static Logger logger = LoggerFactory.getLogger(ParserTest.class);

  @ParameterizedTest(name="[{index}] {0}")
  @ArgumentsSource(SelectProvider.class)
  @Tags({@Tag("/parseMysqlSuccess.sql")})
  void parseSuccessTest(String name, String sql) throws SqlParseException {
    Parser parser = new Parser();
    logger.info(name);
    SqlNode sqlNode = parser.parse(sql);
    assertNotNull(sqlNode);
  }

  @ParameterizedTest(name="[{index}] {0}")
  @ArgumentsSource(SelectProvider.class)
  @Tags({@Tag("/parseMysqlSuccess.sql")})
  void parseDigestTest(String name, String sql) throws SqlParseException {
    Parser parser = new Parser();
    logger.info(name);
    String digest = parser.digest(sql, SqlDialect.DatabaseProduct.MYSQL.getDialect());
    assertNotNull(digest);
  }
}
