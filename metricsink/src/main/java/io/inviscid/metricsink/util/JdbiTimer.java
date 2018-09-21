package io.inviscid.metricsink.util;

import com.codahale.metrics.MetricRegistry;
import io.inviscid.metricsink.util.strategies.SmartNameStrategy;
import io.inviscid.metricsink.util.strategies.StatementNameStrategy;

import org.jdbi.v3.core.statement.SqlLogger;
import org.jdbi.v3.core.statement.StatementContext;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class JdbiTimer implements SqlLogger {
  MetricRegistry metricRegistry;
  StatementNameStrategy statementNameStrategy;

  public JdbiTimer(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
    this.statementNameStrategy = new SmartNameStrategy();
  }

  /**
   * Calculate elapsed time of a SQL sql in JDBI after execution.
   * @param statementContext JDBI StatementContext with info about sql execution.
   */
  public void logAfterExecution(StatementContext statementContext) {
    String statementName = statementNameStrategy.getStatementName(statementContext);
    if (statementName != null) {
      metricRegistry.timer(statementName).update(statementContext.getElapsedTime(ChronoUnit.NANOS),
          TimeUnit.NANOSECONDS);
    }
  }
}
