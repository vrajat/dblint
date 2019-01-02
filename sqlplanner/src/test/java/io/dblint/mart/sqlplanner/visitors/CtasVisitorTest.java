package io.dblint.mart.sqlplanner.visitors;

import io.dblint.mart.sqlplanner.planner.Parser;
import io.dblint.mart.sqlplanner.planner.RedshiftParser;
import io.dblint.mart.sqlplanner.utils.SqlProvider;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CtasVisitorTest {
  private static Logger logger = LoggerFactory.getLogger(InsertVisitorTest.class);

  private static Parser parser;
  @BeforeAll
  static void setParser() {
    parser = new Parser(RedshiftParser.FACTORY);
  }

  @ParameterizedTest(name="[{index}] {0}")
  @ArgumentsSource(SqlProvider.class)
  @Tags({@Tag("/ctasSuccess.yaml")})
  void sanityTest(String name, String targetTable,
                  List<String> sourceTables, String query) throws SqlParseException {
    SqlNode node = parser.parse(query);
    CtasVisitor visitor = new CtasVisitor();
    node.accept(visitor);

    logger.debug("Expected:" + sourceTables.size());
    logger.debug("Actual: " + visitor.getSources().size());

    assertTrue(visitor.passed);
    assertEquals(targetTable, visitor.getTargetTable().toString());

    Iterator<String> expected = sourceTables.iterator();
    Iterator<String> actual = visitor.getSources().iterator();
    while (expected.hasNext() && actual.hasNext()) {
      assertEquals(expected.next(), actual.next());
    }
    assertFalse(expected.hasNext());
    assertFalse(actual.hasNext());
  }

  @Test
  void simpleCreateShouldNotPass() throws SqlParseException {
    SqlNode node = parser.parse("create table c (a int, b int)");
    CtasVisitor visitor = new CtasVisitor();
    node.accept(visitor);
    assertFalse(visitor.isPassed());
  }
}
