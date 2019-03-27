package io.dblint.mart.metricsink.mysql;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class UserQuery {
  ZonedDateTime time;
  String userHost;
  String ipAddress;
  String connectionId;
  Double queryTime;
  Double lockTime;
  Long rowsSent;
  Long rowsExamined;
  String query;

  public ZonedDateTime getTime() {
    return time;
  }

  public void setTime(String time) {
    this.time = ZonedDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(time)),
        ZoneId.of("UTC"));
  }

  public String getUserHost() {
    return userHost;
  }

  public void setUserHost(String userHost) {
    this.userHost = userHost;
  }

  public String getConnectionId() {
    return connectionId;
  }

  public void setConnectionId(String connectionId) {
    this.connectionId = connectionId;
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
