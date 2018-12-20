package io.dblint.mart.metricsink.redshift;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RedshiftCsvTest {
  Logger logger = LoggerFactory.getLogger(RedshiftCsvTest.class);
  @Test
  void query12Test() throws IOException {
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("redshift_queries.csv");
    List<SplitUserQuery> queryList = RedshiftCsv.getQueries(inputStream);
    Iterator<SplitUserQuery> iterator = queryList.iterator();
    while(iterator.hasNext()) {
      logger.debug(iterator.next().toString());
    }
    assertEquals(4, queryList.size());
  }

}