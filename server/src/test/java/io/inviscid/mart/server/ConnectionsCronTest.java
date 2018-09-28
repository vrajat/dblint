package io.inviscid.mart.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import io.inviscid.metricsink.redshift.MySqlSink;
import io.inviscid.metricsink.redshift.RedshiftDb;
import io.inviscid.metricsink.redshift.UserConnection;
import org.junit.jupiter.api.Test;

class ConnectionsCronTest {
  @Test
  void testConnectionRowsMetric() {
    MetricRegistry metricRegistry = new MetricRegistry();
    RedshiftDb redshiftDb = mock(RedshiftDb.class);
    MySqlSink mySqlSink = mock(MySqlSink.class);

    Iterator<UserConnection> mockIterator = mock(Iterator.class);
    List<UserConnection> userConnectionList = mock(ArrayList.class);

    when(userConnectionList.size()).thenReturn(2);
    when(userConnectionList.iterator()).thenReturn(mockIterator);
    when(redshiftDb.getUserConnections()).thenReturn(userConnectionList);

    ConnectionsCron connectionsCron = new ConnectionsCron(mySqlSink, redshiftDb,
        60, metricRegistry);
    connectionsCron.run();

    SortedMap<String, Counter> counters = metricRegistry.getCounters();

    assertEquals(3, counters.size());
    assertEquals(2, counters.get("inviscid.connections_cron.connection_rows").getCount());
  }
}