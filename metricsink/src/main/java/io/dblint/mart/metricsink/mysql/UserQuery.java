package io.dblint.mart.metricsink.mysql;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class UserQuery {
  LocalDateTime time;
  String userHost;
  String ipAddress;
  String id;
  Double queryTime;
  Double lockTime;
  Long rowsSent;
  Long rowsExamined;
  String query;

  public LocalDateTime getTime() {
    return time;
  }

  public void setTime(String time) {
    this.time = LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(time)),
        ZoneId.of("UTC"));
  }

  public String getUserHost() {
    return userHost;
  }

  public void setUserHost(String userHost) {
    this.userHost = userHost;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Double getQueryTime() {
    return queryTime;
  }

  public void setQueryTime(String queryTime) {
    this.queryTime = Double.parseDouble(queryTime);
  }

  public Double getLockTime() {
    return lockTime;
  }

  public void setLockTime(String lockTime) {
    this.lockTime = Double.parseDouble(lockTime);
  }

  public long getRowsSent() {
    return rowsSent.longValue();
  }

  public void setRowsSent(String rowsSent) {
    this.rowsSent = Long.parseLong(rowsSent);
  }

  public long getRowsExamined() {
    return rowsExamined.longValue();
  }

  public void setRowsExamined(String rowsExamined) {
    this.rowsExamined = Long.parseLong(rowsExamined);
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
  }
}
