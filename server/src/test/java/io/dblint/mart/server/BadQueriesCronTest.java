package io.dblint.mart.server;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.redshift.MySqlSink;
import io.dblint.mart.metricsink.redshift.RedshiftDb;
import io.dblint.mart.metricsink.redshift.UserQuery;
import org.junit.jupiter.api.Test;

class BadQueriesCronTest {
  @Test
  void testNumQueriesMetric() {
    MetricRegistry metricRegistry = new MetricRegistry();
    RedshiftDb redshiftDb = mock(RedshiftDb.class);
    MySqlSink mySqlSink = mock(MySqlSink.class);

    Iterator<UserQuery> mockIterator = mock(Iterator.class);
    List<UserQuery> userQueryList = mock(ArrayList.class);

    when(userQueryList.size()).thenReturn(2);
    when(userQueryList.iterator()).thenReturn(mockIterator);
    when(redshiftDb.getQueries())
        .thenReturn(userQueryList);

    BadQueriesCron badQueriesCron = new BadQueriesCron(60, metricRegistry, redshiftDb, mySqlSink);
    badQueriesCron.run();

    SortedMap<String, Counter> counters = metricRegistry.getCounters();

    assertEquals(5, counters.size());
    assertEquals(2, counters.get("inviscid.bad_queries_cron.num_queries_processed").getCount());
  }

  @Test
  void testNumParseExceptionsMetric() {
    MetricRegistry metricRegistry = new MetricRegistry();
    RedshiftDb redshiftDb = mock(RedshiftDb.class);
    MySqlSink mySqlSink = mock(MySqlSink.class);

    List<UserQuery> userQueryList = Arrays.asList(
        new UserQuery(1, 1, 1, 1, LocalDateTime.now(), LocalDateTime.now(),
        10, "db", false, "select 1 from tbl"),
        new UserQuery(2, 1, 1, 1, LocalDateTime.now(), LocalDateTime.now(),
            10, "db", false, "select x1 tbl where h = 0")
    );

    when(redshiftDb.getQueries())
        .thenReturn(userQueryList);

    BadQueriesCron badQueriesCron = new BadQueriesCron(60, metricRegistry, redshiftDb, mySqlSink);
    badQueriesCron.run();

    SortedMap<String, Counter> counters = metricRegistry.getCounters();

    assertEquals(2, counters.get("inviscid.bad_queries_cron.num_queries_processed").getCount());
    assertEquals(1, counters.get("inviscid.bad_queries_cron.num_parse_exception").getCount());
  }
}