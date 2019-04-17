package io.dblint.mart.metricsink.mysql;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class UserQuery {
  protected static DateTimeFormatter dateFormat =
      DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss");

  private int id;
  ZonedDateTime logTime;
  private String userHost;
  private String ipAddress;
  private String connectionId;
  private Double queryTime;
  private Double lockTime;
  private long rowsSent;
  private long rowsExamined;
  private String query;
  private String digestHash;

  public String getLogTime() {
    return logTime.withZoneSameInstant(ZoneOffset.ofHoursMinutes(5, 30)).format(dateFormat);
  }

  public void setLogTime(String logTime) {
    this.logTime = ZonedDateTime.of(
        LocalDateTime.parse(logTime, dateFormat), ZoneOffset.ofHoursMinutes(5, 30));
  }

  public ZonedDateTime getZonedLogTime() {
    return this.logTime;
  }

  public void setZonedLogTime(ZonedDateTime logTime) {
    this.logTime = logTime;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
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

  public void setQueryTime(Double queryTime) {
    this.queryTime = queryTime;
  }

  public Double getLockTime() {
    return lockTime;
  }

  public void setLockTime(Double lockTime) {
    this.lockTime = lockTime;
  }

  public long getRowsSent() {
    return rowsSent;
  }

  public void setRowsSent(long rowsSent) {
    this.rowsSent = rowsSent;
  }

  public long getRowsExamined() {
    return rowsExamined;
  }

  public void setRowsExamined(long rowsExamined) {
    this.rowsExamined = rowsExamined;
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

  public String getDigestHash() {
    return digestHash;
  }

  public void setDigestHash(String digestHash) {
    this.digestHash = digestHash;
  }
}
