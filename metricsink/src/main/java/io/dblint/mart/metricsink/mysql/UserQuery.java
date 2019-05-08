package io.dblint.mart.metricsink.mysql;

public class UserQuery extends Logged {
  private String userHost;
  private String ipAddress;
  private String connectionId;
  private Double queryTime;
  private Double lockTime;
  private long rowsSent;
  private long rowsExamined;
  private String query;
  private String digestHash;

  public UserQuery() {
    super(null);
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
