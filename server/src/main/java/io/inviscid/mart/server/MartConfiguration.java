package io.inviscid.mart.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.inviscid.mart.server.configuration.QueryStatsCronConfiguration;

public class MartConfiguration extends Configuration {
  @JsonProperty
  QueryStatsCronConfiguration queryStatsCron;
}
