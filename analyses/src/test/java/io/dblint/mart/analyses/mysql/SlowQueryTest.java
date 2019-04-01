package io.dblint.mart.analyses.mysql;

import io.dblint.mart.metricsink.mysql.QueryAttribute;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SlowQueryTest {
  private static Logger logger = LoggerFactory.getLogger(SlowQueryTest.class);

  @Test
  void testAnalyze() throws SqlParseException {
    QueryAttribute queryAttribute = new SlowQuery().analyze(
        "select a,b from d where c = 5"
    );
    assertEquals("SELECT `A`, `B`\n"
        + "FROM `D`\n"
        + "WHERE `C` = ?", queryAttribute.digest);
    assertEquals("bbdd1e7260fdd5fc159a12248d059e4a1a294ecd52c8287ed2e71708908dd142",
        queryAttribute.digestHash);

  }
}
