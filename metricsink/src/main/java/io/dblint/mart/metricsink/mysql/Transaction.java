package io.dblint.mart.metricsink.mysql;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class Transaction {
  static DateTimeFormatter dateFormat =
      DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss");
  int id;
  String databaseId;
  String thread;
  String query;
  ZonedDateTime startTime;
  ZonedDateTime waitStartTime;
  String lockMode;
  String lockType;
  String lockTable;
  String lockIndex;
  String lockData;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getDatabaseId() {
    return databaseId;
  }

  public void setDatabaseId(String databaseId) {
    this.databaseId = databaseId;
  }

  public String getThread() {
    return thread;
  }

  public void setThread(String thread) {
    this.thread = thread;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getStartTime() {
    return startTime.withZoneSameInstant(ZoneOffset.ofHoursMinutes(5, 30)).format(dateFormat);
  }

  public void setStartTime(String logTime) {
    this.startTime = ZonedDateTime.of(
        LocalDateTime.parse(logTime, dateFormat), ZoneOffset.ofHoursMinutes(5, 30));
  }

  public ZonedDateTime getZonedStartTime() {
    return startTime;
  }

  public void setZonedStartTime(ZonedDateTime startTime) {
    this.startTime = startTime;
  }

  public String getWaitStartTime() {
    return waitStartTime == null ? null :
        waitStartTime.withZoneSameInstant(ZoneOffset.ofHoursMinutes(5, 30)).format(dateFormat);
  }

  /**
   * Set Log Time from String. Typically for JDBI.
   * @param logTime Log time as a timestamp string
   */
  public void setWaitStartTime(String logTime) {
    this.waitStartTime = logTime == null ? null :
        ZonedDateTime.of(LocalDateTime.parse(logTime, dateFormat),
            ZoneOffset.ofHoursMinutes(5, 30));
  }

  public ZonedDateTime getZonedWaitStartTime() {
    return waitStartTime;
  }

  public void setZonedWaitStartTime(ZonedDateTime waitStartTime) {
    this.waitStartTime = waitStartTime;
  }

  public String getLockMode() {
    return lockMode;
  }

  public void setLockMode(String lockMode) {
    this.lockMode = lockMode;
  }

  public String getLockType() {
    return lockType;
  }

  public void setLockType(String lockType) {
    this.lockType = lockType;
  }

  public String getLockTable() {
    return lockTable;
  }

  public void setLockTable(String lockTable) {
    this.lockTable = lockTable;
  }

  public String getLockIndex() {
    return lockIndex;
  }

  public void setLockIndex(String lockIndex) {
    this.lockIndex = lockIndex;
  }

  public String getLockData() {
    return lockData;
  }

  public void setLockData(String lockData) {
    this.lockData = lockData;
  }
}
