package io.dblint.mart.metricsink.mysql;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

public class QueryAttribute {
  public final String digest;
  public final String digestHash;

  public QueryAttribute(String digest) {
    this.digest = digest;
    digestHash = Hashing.sha256().hashString(this.digest, StandardCharsets.UTF_8).toString();
  }
}
