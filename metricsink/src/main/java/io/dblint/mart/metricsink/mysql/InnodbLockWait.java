package io.dblint.mart.metricsink.mysql;

import java.time.ZonedDateTime;

public class InnodbLockWait extends Logged {

  private Transaction waiting;
  private Transaction blocking;

  InnodbLockWait(Transaction waiting, Transaction blocking,
                        ZonedDateTime time) {
    super(time);
    this.waiting = waiting;
    this.blocking = blocking;
  }

  public void setBlocking(Transaction transaction) {
    this.blocking = transaction;
  }

  public int getBlockingId() {
    return blocking.id;
  }

  public String getBlockingDatabaseId() {
    return blocking.databaseId;
  }

  public Transaction getBlocking() {
    return blocking;
  }

  public void setWaiting(Transaction transaction) {
    this.waiting = transaction;
  }

  public String getWaitingDatabaseId() {
    return waiting.databaseId;
  }

  public int getWaitingId() {
    return waiting.id;
  }

  public Transaction getWaiting() {
    return waiting;
  }

}
