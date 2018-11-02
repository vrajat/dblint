package com.dblint.server;

import com.dblint.server.configuration.CronConfiguration;
import com.dblint.server.configuration.JdbcConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
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
