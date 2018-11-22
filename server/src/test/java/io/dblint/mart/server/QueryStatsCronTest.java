package io.dblint.mart.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.redshift.MySqlSink;
import io.dblint.mart.metricsink.redshift.QueryStats;
import io.dblint.mart.metricsink.redshift.RedshiftDb;
import org.junit.jupiter.api.Test;

class QueryStatsCronTest {

  @Test
  void testNumQueriesMetric() {
    MetricRegistry metricRegistry = new MetricRegistry();
    RedshiftDb redshiftDb = mock(RedshiftDb.class);
    MySqlSink mySqlSink = mock(MySqlSink.class);

    Iterator<QueryStats> mockIterator = mock(Iterator.class);
    List<QueryStats> queryStatsList = mock(ArrayList.class);

    when(queryStatsList.size()).thenReturn(2);
    when(queryStatsList.iterator()).thenReturn(mockIterator);
    when(redshiftDb.getQueryStats(anyBoolean(), any(LocalDateTime.class), any(LocalDateTime.class)))
        .thenReturn(queryStatsList);

    QueryStatsCron queryStatsCron = new QueryStatsCron(60, metricRegistry, redshiftDb, mySqlSink);
    queryStatsCron.run();

    SortedMap<String, Counter> counters = metricRegistry.getCounters();

    assertEquals(3, counters.size());
    assertEquals(2, counters.get("inviscid.query_stats_cron.num_queries").getCount());
  }
}