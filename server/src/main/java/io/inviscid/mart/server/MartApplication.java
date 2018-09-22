package io.inviscid.mart.server;

import io.dropwizard.Application;
import io.dropwizard.lifecycle.setup.ScheduledExecutorServiceBuilder;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.inviscid.mart.server.configuration.JdbcConfiguration;
import io.inviscid.metricsink.redshift.MySqlSink;
import io.inviscid.metricsink.redshift.RedshiftDb;

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
        .scheduledExecutorService("query_stats_cron");
    ScheduledExecutorService scheduledExecutorService = serviceBuilder.build();

    QueryStatsCron queryStatsCron = new QueryStatsCron(configuration.queryStatsCronMin,
        environment.metrics(), redshiftDb, mySqlSink);

    scheduledExecutorService.scheduleAtFixedRate(queryStatsCron,
        0, configuration.queryStatsCronMin, TimeUnit.MINUTES);
    environment.healthChecks().register("QueryStatsCron", new CronHealthCheck(queryStatsCron));

    BadQueriesCron badQueriesCron = new BadQueriesCron(configuration.badQueriesCronMin,
        environment.metrics(), redshiftDb, mySqlSink);

    scheduledExecutorService.scheduleAtFixedRate(badQueriesCron,
        0, configuration.badQueriesCronMin, TimeUnit.MINUTES);
    environment.healthChecks().register("BadQueriesCron", new CronHealthCheck(badQueriesCron));
  }
}
