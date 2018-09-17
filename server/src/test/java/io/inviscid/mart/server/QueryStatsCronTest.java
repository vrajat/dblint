package io.inviscid.mart.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;

import com.codahale.metrics.MetricRegistry;
import io.inviscid.mart.server.configuration.MySqlConfiguration;
import io.inviscid.mart.server.configuration.QueryStatsCronConfiguration;
import io.inviscid.mart.server.configuration.RedshiftConfiguration;
import io.inviscid.metricsink.redshift.MySqlSink;
import io.inviscid.metricsink.redshift.RedshiftDb;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class QueryStatsCronTest {
  static QueryStatsCronConfiguration qcsConfig;

  @BeforeAll
  static void setUp() {
    RedshiftConfiguration redshiftConfiguration = new RedshiftConfiguration();
    redshiftConfiguration.setUrl("jdbc:h2:mem:QCSTRedshift");
    redshiftConfiguration.setUser("");
    redshiftConfiguration.setPassword("");

    MySqlConfiguration mySqlConfiguration = new MySqlConfiguration();
    mySqlConfiguration.setUrl("jdbc:h2:mem:QCSTRedshift");
    mySqlConfiguration.setUser("");
    mySqlConfiguration.setPassword("");

    qcsConfig = new QueryStatsCronConfiguration();
    qcsConfig.setFrequencyMin(1);
    qcsConfig.setMySql(mySqlConfiguration);
    qcsConfig.setRedshift(redshiftConfiguration);
  }

  @Test
  void singleRunTest() {
    MetricRegistry metricRegistry = new MetricRegistry();
    RedshiftDb redshiftDbMock = mock(RedshiftDb.class);
    MySqlSink mySqlSinkMock = mock(MySqlSink.class);

    QueryStatsCron queryStatsCron = new QueryStatsCron(qcsConfig, metricRegistry,
        redshiftDbMock, mySqlSinkMock);

    queryStatsCron.run();

    assertEquals(1, queryStatsCron.iterations.getCount());
    assertEquals(0, queryStatsCron.failedIterations.getCount());
  }

  @Test
  void singleRunExceptionTest() {
    MetricRegistry metricRegistry = new MetricRegistry();
    RedshiftDb redshiftDbMock = mock(RedshiftDb.class);
    MySqlSink mySqlSinkMock = mock(MySqlSink.class);

    when(redshiftDbMock.getQueryStats(eq(false),
        any(LocalDateTime.class), any(LocalDateTime.class)))
        .thenThrow(new RuntimeException("Mock Exception"));
    QueryStatsCron queryStatsCron = new QueryStatsCron(qcsConfig, metricRegistry,
        redshiftDbMock, mySqlSinkMock);

    queryStatsCron.run();

    assertEquals(1, queryStatsCron.iterations.getCount());
    assertEquals(1, queryStatsCron.failedIterations.getCount());
  }
}