package io.inviscid.mart.server;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import io.inviscid.metricsink.redshift.MySqlSink;
import io.inviscid.metricsink.redshift.RedshiftDb;
import io.inviscid.metricsink.redshift.UserQuery;
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
    when(redshiftDb.getQueries(any(LocalDateTime.class), any(LocalDateTime.class)))
        .thenReturn(userQueryList);

    BadQueriesCron badQueriesCron = new BadQueriesCron(60, metricRegistry, redshiftDb, mySqlSink);
    badQueriesCron.run();

    SortedMap<String, Counter> counters = metricRegistry.getCounters();

    assertEquals(5, counters.size());
    assertEquals(2, counters.get("inviscid.bad_queries_cron.num_queries_processed").getCount());
  }
}