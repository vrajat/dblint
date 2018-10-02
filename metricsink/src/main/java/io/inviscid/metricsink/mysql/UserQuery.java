package io.inviscid.metricsink.mysql;

import java.util.ArrayList;
import java.util.List;

public class UserQuery {
  String time;
  String userHost;
  String ipAddress;
  String id;
  String queryTime;
  String lockTime;
  String rowsSent;
  String rowsExamined;
  List<String> queries;

  UserQuery() {
    queries = new ArrayList<>();
  }

  public String getTime() {
    return time;
  }

  public void setTime(String time) {
    this.time = time;
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

  public String getQueryTime() {
    return queryTime;
  }

  public void setQueryTime(String queryTime) {
    this.queryTime = queryTime;
  }

  public String getLockTime() {
    return lockTime;
  }

  public void setLockTime(String lockTime) {
    this.lockTime = lockTime;
  }

  public String getRowsSent() {
    return rowsSent;
  }

  public void setRowsSent(String rowsSent) {
    this.rowsSent = rowsSent;
  }

  public String getRowsExamined() {
    return rowsExamined;
  }

  public void setRowsExamined(String rowsExamined) {
    this.rowsExamined = rowsExamined;
  }

  public List<String> getQueries() {
    return queries;
  }

  public void setQueries(List<String> queries) {
    this.queries = queries;
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
  }
}
