package com.dblint.server;

import com.dblint.metricsink.redshift.MySqlSink;
import com.dblint.metricsink.redshift.RedshiftDb;
import com.dblint.server.configuration.JdbcConfiguration;
import com.dblint.server.resources.RedshiftResource;

import io.dropwizard.Application;
import io.dropwizard.lifecycle.setup.ExecutorServiceBuilder;
import io.dropwizard.lifecycle.setup.ScheduledExecutorServiceBuilder;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.util.concurrent.ExecutorService;
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

    JdbcConfiguration redShift = configuration.redshift;
    JdbcConfiguration mySql = configuration.mySql;

    RedshiftDb redshiftDb = new RedshiftDb(redShift.getUrl(), redShift.getUser(),
            redShift.getPassword(), environment.metrics());
    MySqlSink mySqlSink = new MySqlSink(mySql.getUrl(), mySql.getUser(),
            mySql.getPassword(), environment.metrics());
    mySqlSink.initialize();

    ScheduledExecutorServiceBuilder serviceBuilder = environment.lifecycle()
        .scheduledExecutorService("mart_application");
    ScheduledExecutorService scheduledExecutorService = serviceBuilder.build();

    ExecutorServiceBuilder executorServiceBuilder = environment.lifecycle()
        .executorService("mart_resource");
    ExecutorService executorService = executorServiceBuilder.build();

    if (configuration.queryStatsCron != null) {
      QueryStatsCron queryStatsCron = new QueryStatsCron(configuration.queryStatsCron.frequencyMin,
          environment.metrics(), redshiftDb, mySqlSink);

      scheduledExecutorService.scheduleAtFixedRate(queryStatsCron,
          configuration.queryStatsCron.delayMin, configuration.queryStatsCron.frequencyMin,
          TimeUnit.MINUTES);
      environment.healthChecks().register("QueryStatsCron", new CronHealthCheck(queryStatsCron));

    }

    if (configuration.badQueriesCron != null) {
      BadQueriesCron badQueriesCron = new BadQueriesCron(configuration.badQueriesCron.frequencyMin,
          environment.metrics(), redshiftDb, mySqlSink);

      scheduledExecutorService.scheduleAtFixedRate(badQueriesCron,
          configuration.badQueriesCron.delayMin, configuration.badQueriesCron.frequencyMin,
          TimeUnit.MINUTES);
      environment.healthChecks().register("BadQueriesCron", new CronHealthCheck(badQueriesCron));
    }

    if (configuration.connectionsCron != null) {
      ConnectionsCron connectionsCron = new ConnectionsCron(mySqlSink, redshiftDb,
          configuration.connectionsCron.frequencyMin, environment.metrics());

      scheduledExecutorService.scheduleAtFixedRate(connectionsCron,
          configuration.connectionsCron.delayMin, configuration.connectionsCron.frequencyMin,
          TimeUnit.MINUTES);
      environment.healthChecks().register("ConnectionsCron", new CronHealthCheck(connectionsCron));
    }

    {
      ConnectionsCron cron = new ConnectionsCron(mySqlSink, redshiftDb, 0, environment.metrics());
      RedshiftResource resource = new RedshiftResource(cron, executorService);
      environment.jersey().register(resource);
      environment.healthChecks().register("Redshift Resource High CPU", new CronHealthCheck(cron));
    }
  }
}
