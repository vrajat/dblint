package io.inviscid.mart.server.configuration;

import org.hibernate.validator.constraints.NotEmpty;

public class CronConfiguration {
  @NotEmpty
  public int delayMin;

  @NotEmpty
  public int frequencyMin;
}
