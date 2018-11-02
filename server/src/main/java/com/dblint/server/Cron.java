package com.dblint.server;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.dblint.metricsink.redshift.MySqlSink;
import com.dblint.metricsink.redshift.RedshiftDb;

import java.time.LocalDateTime;

public abstract class Cron implements Runnable {
  final MySqlSink mySqlSink;
  final RedshiftDb redshiftDb;
  final int frequency;
  Counter iterations;
  Counter failedIterations;
  LocalDateTime startRange;

  /**
   * An abstract class for a Cron implemented with ScheduledExecutor Service.
   * @param mySqlSink The MySQL database where metrics will be stored
   * @param redshiftDb The RedShift database under inspection
   * @param frequency Frequency of cron in minutes
   * @param metricRegistry MetricRegistry for DropWizard App
   */
  public Cron(MySqlSink mySqlSink, RedshiftDb redshiftDb,
              int frequency, MetricRegistry metricRegistry,
              String metricNamespace) {
    this.mySqlSink = mySqlSink;
    this.redshiftDb = redshiftDb;
    this.frequency = frequency;
    iterations = metricRegistry.counter("inviscid." + metricNamespace + ".iterations");
    failedIterations = metricRegistry.counter("inviscid" + metricNamespace + ".failedIterations");
    startRange = LocalDateTime.now();
  }

  public long getIterations() {
    return iterations.getCount();
  }

  public long getFailedIterations() {
    return failedIterations.getCount();
  }
}
