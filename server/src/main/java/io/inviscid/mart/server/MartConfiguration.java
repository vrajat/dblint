package io.inviscid.mart.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.inviscid.mart.server.configuration.JdbcConfiguration;

public class MartConfiguration extends Configuration {
  @JsonProperty
  JdbcConfiguration redshift;

  @JsonProperty
  JdbcConfiguration mySql;

  @JsonProperty
  int queryStatsCronMin;

  @JsonProperty
  int badQueriesCronMin;
}
