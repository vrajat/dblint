package com.dblint.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.dblint.metricsink.redshift.MySqlSink;
import com.dblint.metricsink.redshift.RedshiftDb;
import com.dblint.metricsink.redshift.RunningQuery;
import com.dblint.metricsink.redshift.UserConnection;
import org.junit.jupiter.api.Test;

class ConnectionsCronTest {
  @Test
  void testConnectionRowsMetric() {
    MetricRegistry metricRegistry = new MetricRegistry();
    RedshiftDb redshiftDb = mock(RedshiftDb.class);
    MySqlSink mySqlSink = mock(MySqlSink.class);

    Iterator<UserConnection> userIterator = mock(Iterator.class);
    List<UserConnection> userConnectionList = mock(ArrayList.class);

    Iterator<RunningQuery> queryIterator = mock(Iterator.class);
    List<RunningQuery> queries = mock(ArrayList.class);

    when(userConnectionList.size()).thenReturn(2);
    when(userConnectionList.iterator()).thenReturn(userIterator);
    when(redshiftDb.getUserConnections()).thenReturn(userConnectionList);

    when(queries.size()).thenReturn(2);
    when(queries.iterator()).thenReturn(queryIterator);
    when(redshiftDb.getRunningQueries()).thenReturn(queries);

    ConnectionsCron connectionsCron = new ConnectionsCron(mySqlSink, redshiftDb,
        60, metricRegistry);
    connectionsCron.run();

    SortedMap<String, Counter> counters = metricRegistry.getCounters();

    assertEquals(4, counters.size());
    assertEquals(2, counters.get("inviscid.connectionsCron.connectionRows").getCount());
    assertEquals(2, counters.get("inviscid.connectionsCron.connectionRows").getCount());
  }
}