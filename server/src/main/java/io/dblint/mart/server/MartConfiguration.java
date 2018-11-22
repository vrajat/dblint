package io.dblint.mart.server;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.dblint.mart.server.configuration.CronConfiguration;
import io.dblint.mart.server.configuration.JdbcConfiguration;
import io.dropwizard.Configuration;

public class MartConfiguration extends Configuration {
  @JsonProperty
  JdbcConfiguration redshift;

  @JsonProperty
  JdbcConfiguration mySql;

  @JsonProperty
  CronConfiguration queryStatsCron;

  @JsonProperty
  CronConfiguration badQueriesCron;

  @JsonProperty
  CronConfiguration connectionsCron;
}
