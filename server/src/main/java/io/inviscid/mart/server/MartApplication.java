package io.inviscid.mart.server;

import io.dropwizard.Application;
import io.dropwizard.lifecycle.setup.ScheduledExecutorServiceBuilder;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.inviscid.mart.server.configuration.QueryStatsCronConfiguration;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MartApplication extends Application<MartConfiguration> {

  public static void main(final String[] args) throws Exception {
    new MartApplication().run(args);
  }

  @Override
  public String getName() {
    return "mart";
  }

  @Override
  public void initialize(final Bootstrap<MartConfiguration> bootstrap) {
      // TODO: application initialization
  }

  @Override
  public void run(final MartConfiguration configuration,
                  final Environment environment) {
    ScheduledExecutorServiceBuilder serviceBuilder = environment.lifecycle()
        .scheduledExecutorService("query_stats_cron");
    ScheduledExecutorService scheduledExecutorService = serviceBuilder.build();

    QueryStatsCronConfiguration qcsConfig = configuration.queryStatsCron;
    QueryStatsCron queryStatsCron = new QueryStatsCron(qcsConfig, environment.metrics());
    queryStatsCron.initialize();

    scheduledExecutorService.scheduleAtFixedRate(queryStatsCron,
        qcsConfig.getFrequencyMin(), qcsConfig.getFrequencyMin(), TimeUnit.MINUTES);
    environment.healthChecks().register("QueryStatsCron", new CronHealthCheck(queryStatsCron));
  }
}
