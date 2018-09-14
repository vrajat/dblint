package io.inviscid.mart.server.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

import javax.validation.constraints.NotNull;

public class QueryStatsCronConfiguration extends Configuration {
  @JsonProperty
  @NotNull
  int frequencyMin;

  @JsonProperty
  RedshiftConfiguration redshift;

  @JsonProperty
  MySqlConfiguration mySql;

  public int getFrequencyMin() {
    return frequencyMin;
  }

  public RedshiftConfiguration getRedshift() {
    return redshift;
  }

  public MySqlConfiguration getMySql() {
    return mySql;
  }

  public void setFrequencyMin(int frequencyMin) {
    this.frequencyMin = frequencyMin;
  }

  public void setRedshift(RedshiftConfiguration redshiftConfiguration) {
    this.redshift = redshiftConfiguration;
  }

  public void setMySql(MySqlConfiguration mySql) {
    this.mySql = mySql;
  }
}
