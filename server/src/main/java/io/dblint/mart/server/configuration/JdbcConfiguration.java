package io.dblint.mart.server.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

public class JdbcConfiguration extends Configuration {
  @JsonProperty
  @NotEmpty
  private String url;

  @JsonProperty
  @NotEmpty
  private String user;

  @JsonProperty
  @NotEmpty
  private String password;

  public String getUrl() {
    return url;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public void setPassword(String password) {
    this.password = password;
  }
}
