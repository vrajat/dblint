package com.dblint.server.pojo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SqlQuery {
  public final String sql;
  public final String dialect;

  public SqlQuery(String sql) {
    this(sql, "mysql");
  }

  @JsonCreator
  public SqlQuery(@JsonProperty("sql") String sql,
                  @JsonProperty("dialect") String dialect) {
    this.sql = sql;
    this.dialect = dialect;
  }
}
