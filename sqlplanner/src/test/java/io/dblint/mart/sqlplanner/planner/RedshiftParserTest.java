package io.dblint.mart.sqlplanner.planner;

import io.dblint.mart.sqlplanner.utils.SqlProvider;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RedshiftParserTest {
  private static Logger logger = LoggerFactory.getLogger(RedshiftParserTest.class);

  @ParameterizedTest(name="[{index}] {0}")
  @ArgumentsSource(SqlProvider.class)
  @Tags({@Tag("/parseRedshiftSuccess.yaml")})
  public void parseSuccessTest(String name, String targetTable,
                               List<String> sources, String query) throws SqlParseException {
    Parser parser = new Parser(io.dblint.mart.sqlplanner.planner.RedshiftParser.FACTORY);
    logger.info(name);
    SqlNode sqlNode = parser.parse(query);
    assertNotNull(sqlNode);
  }
}
