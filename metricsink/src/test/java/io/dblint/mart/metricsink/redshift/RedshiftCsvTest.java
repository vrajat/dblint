package io.dblint.mart.metricsink.redshift;

import io.dblint.mart.metricsink.util.MetricAgentException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RedshiftCsvTest {
  Logger logger = LoggerFactory.getLogger(RedshiftCsvTest.class);
  @Test
  void splitQuery12Test() throws IOException {
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("redshift_queries.csv");
    List<SplitUserQuery> queryList = new RedshiftCsv(inputStream).getSplitQueries();
    Iterator<SplitUserQuery> iterator = queryList.iterator();
    while(iterator.hasNext()) {
      logger.debug(iterator.next().toString());
    }
    assertEquals(4, queryList.size());
  }

  private static UserQuery userQuery = new UserQuery(
      100, 5052, 0, 0, LocalDateTime.parse("2018-09-19 13:08:03", Jdbi.dateTimeFormatter),
      LocalDateTime.parse("2018-09-19 13:46:51", Jdbi.dateTimeFormatter),
      90.0, "default", false, "-- query12\nSELECT\ni_item_id ,\ni_item_desc ,\ni_category "
      + ",\ni_class ,\ni_current_price ,\nSum(ws_ext_sales_price) AS itemrevenue ,"
      + "\nSum(ws_ext_sales_price)*100/Sum(Sum(ws_ext_sales_price)) OVER (partition BY i_class) "
      + "AS revenueratio\nFROM web_sales ,\nitem ,\ndate_dim\nWHERE ws_item_sk = i_item_sk"
      + "\nAND i_category IN (''Home'',\n''Men'',\n''Women'')\nAND ws_sold_date_sk = d_date_sk"
      + "\nAND Cast(d_date AS DATE) BETWEEN Cast(''2000-05-11'' AS DATE) AND "
      + "(\nCast(''2000-06-11'' AS DATE))\nGROUP BY i_item_id ,\ni_item_desc ,\ni_category ,\n"
      + "i_class ,\ni_current_price\nORDER BY i_category ,\ni_class ,\ni_item_id \ni_item_desc ,\n"
      + "revenueratio\nLIMIT 100;\n"
  );
  @Test
  void query12Test() throws MetricAgentException {
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("redshift_queries.csv");
    List<UserQuery> userQueries = new RedshiftCsv(inputStream).getQueries(
        LocalDateTime.of(2018,12,19, 0, 0),
        LocalDateTime.of(2018,12,21, 0, 0)
    );

    assertEquals(1, userQueries.size());
    assertEquals(userQuery, userQueries.get(0));
  }
}