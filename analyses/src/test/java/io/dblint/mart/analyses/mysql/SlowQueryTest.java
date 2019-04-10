package io.dblint.mart.analyses.mysql;

import com.codahale.metrics.MetricRegistry;
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
    QueryAttribute queryAttribute = new SlowQuery(new MetricRegistry()).analyze(
        "select a,b from d where c = 5"
    );
    assertEquals("SELECT `A`, `B`\n"
        + "FROM `D`\n"
        + "WHERE `C` = ?", queryAttribute.digest);
    assertEquals("bbdd1e7260fdd5fc159a12248d059e4a1a294ecd52c8287ed2e71708908dd142",
        queryAttribute.digestHash);

  }

  @Test
  void testInsert() throws SqlParseException {
    QueryAttribute queryAttribute = new SlowQuery(new MetricRegistry()).analyze(
        "INSERT INTO `django_session` (`session_key`, `session_data`, `expire_date`) "
        + "VALUES ('tqbz3o7dvpdergag7ls6nvmlnj2fnfjb', 'Zjg3MjM0MTdkODRmNDk1ZmIzZmYyMGM4MDVjN"
        + "GY4MmNkNDE1YTBjNjqAAn1xAVgPAAAAX3Nlc3Npb25fZXhwaXJ5cQJKAHUSAHMu', "
        + "'2019-03-12 09:02:50.287595');"
    );
    assertEquals("INSERT INTO `django_session` (`session_key`, `session_data`, `expire_date`)\n"
        + "VALUES ROW(?, ?, ?)", queryAttribute.digest);
    assertEquals("9a604ff5459c644611fc9a17da8a63639ebcb4401b47468ae604026860edaadb",
        queryAttribute.digestHash);
  }

  @Test
  void testUpdate() throws SqlParseException {
    QueryAttribute queryAttribute = new SlowQuery(new MetricRegistry()).analyze(
        "UPDATE `django_session` SET `session_data` = 'NDUzYjc0ZmZhYTU0YzYxZDhlZjQ4YzQyMjYzOGFm'"
        + ", `expire_date` = '2019-04-08 15:04:57.832310' WHERE "
        + "`django_session`.`session_key` = 'od8c8hiweja7srrh3ue8yi0mkd2dhl6u'"
    );
    assertEquals(
        "UPDATE `django_session` SET `session_data` = ?\n"
            + ", `expire_date` = ?\nWHERE "
            + "`django_session`.`session_key` = ?"

        ,queryAttribute.digest);
    assertEquals("cd754453505f9196a6b27c9c11ea3830be59cb020839274bf35b203786c50963",
        queryAttribute.digestHash);
  }

  @Test
  void testDelete() throws SqlParseException {
    QueryAttribute queryAttribute = new SlowQuery(new MetricRegistry()).analyze(
        "DELETE FROM `django_session` WHERE `django_session`.`session_key` IN "
        + "('yo983e7woyuek1fcfopm0xgpnmrmdzbc')"
    );
    assertEquals("DELETE FROM `django_session`\nWHERE `django_session`.`session_key` IN (?)"
        ,queryAttribute.digest);
    assertEquals("3e4392897396ec8857506e61456b3d2e70ef64ff9456bced464f28fcf8c7d42a",
        queryAttribute.digestHash);
  }
}
