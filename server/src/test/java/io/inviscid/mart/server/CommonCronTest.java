package io.inviscid.mart.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import io.inviscid.mart.server.configuration.JdbcConfiguration;
import io.inviscid.metricsink.redshift.MySqlSink;
import io.inviscid.metricsink.redshift.RedshiftDb;

import java.time.LocalDateTime;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CommonCronTest {
  static int frequency;
  static JdbcConfiguration redshiftConfiguration;
  static JdbcConfiguration mySqlConfiguration;

  @BeforeAll
  static void setUp() {
    redshiftConfiguration = new JdbcConfiguration();
    redshiftConfiguration.setUrl("jdbc:h2:mem:QCSTRedshift");
    redshiftConfiguration.setUser("");
    redshiftConfiguration.setPassword("");

    mySqlConfiguration = new JdbcConfiguration();
    mySqlConfiguration.setUrl("jdbc:h2:mem:QCSMySQL");
    mySqlConfiguration.setUser("");
    mySqlConfiguration.setPassword("");

    frequency = 60;
  }

  private static Stream<Arguments> cronProvider() {
    return Stream.of(
        Arguments.of(new QueryStatsCron(frequency, new MetricRegistry(),
            mock(RedshiftDb.class), mock(MySqlSink.class))),
        Arguments.of(new BadQueriesCron(frequency, new MetricRegistry(),
            mock(RedshiftDb.class), mock(MySqlSink.class))),
        Arguments.of(new ConnectionsCron(mock(MySqlSink.class), mock(RedshiftDb.class),
            frequency, new MetricRegistry()))
    );
  }

  @ParameterizedTest
  @DisplayName("{arguments}")
  @MethodSource("cronProvider")
  void singleRunTest(Cron cron) {
    cron.run();

    assertEquals(1, cron.iterations.getCount());
    assertEquals(0, cron.failedIterations.getCount());
  }

  @ParameterizedTest
  @DisplayName("{arguments}")
  @MethodSource("cronProvider")
  void singleRunExceptionTest(Cron cron) {
    when(cron.redshiftDb.getQueryStats(eq(false),
        any(LocalDateTime.class), any(LocalDateTime.class)))
        .thenThrow(new RuntimeException("Mock Exception"));

    when(cron.redshiftDb.getQueries(any(LocalDateTime.class), any(LocalDateTime.class)))
        .thenThrow(new RuntimeException("Mock Exception"));

    when(cron.redshiftDb.getUserConnections()).thenThrow(new RuntimeException("Mock Exception"));

    cron.run();

    assertEquals(1, cron.iterations.getCount());
    assertEquals(1, cron.failedIterations.getCount());
  }
}