package com.dblint.server.pojo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class QueryResponse {
  public final String sql;
  public final String errorMessage;
  public final boolean success;

  /**
   * Create a query response object.
   * @param response Response or error message
   * @param success Boolean if processing was successful
   */
  public QueryResponse(String response, boolean success) {
    this.success = success;
    if (success) {
      this.sql = response;
      this.errorMessage = null;
    } else {
      this.errorMessage = response;
      this.sql = null;
    }
  }

  /**
   * Create response object from json strings.
   * @param sql SQL string in response
   * @param errorMessage Error message if processing failed
   * @param success Whether processing was successful
   */
  @JsonCreator
  public QueryResponse(@JsonProperty("sql") String sql,
                       @JsonProperty("errorMessage") String errorMessage,
                       @JsonProperty("success") boolean success) {
    this.sql = sql;
    this.errorMessage = errorMessage;
    this.success = success;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    QueryResponse that = (QueryResponse) obj;
    return success == that.success
       && Objects.equals(sql, that.sql)
       && Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sql, errorMessage, success);
  }
}
