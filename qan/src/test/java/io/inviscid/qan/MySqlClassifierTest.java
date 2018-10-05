package io.inviscid.qan;

import io.inviscid.qan.planner.Parser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class MySqlClassifierTest {
  @Test
  void sanityTest() throws SqlParseException {
    Parser parser = new Parser();
    SqlNode sqlNode = parser.parse("select x from y where z = 10");


  }
}
