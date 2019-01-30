package io.dblint.mart.server.pojo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RegisterInfo {
  public final String feature;
  public final String email;

  @JsonCreator
  public RegisterInfo(
      @JsonProperty("feature") String feature,
      @JsonProperty("email") String email) {
    this.feature = feature;
    this.email = email;
  }
}
