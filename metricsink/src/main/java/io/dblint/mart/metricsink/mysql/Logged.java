package io.dblint.mart.metricsink.mysql;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class Logged {
  protected static DateTimeFormatter dateFormat =
      DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss");

  protected int id;
  protected ZonedDateTime logTime;

  Logged(ZonedDateTime logTime) {
    this.logTime = logTime;
  }

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
}
