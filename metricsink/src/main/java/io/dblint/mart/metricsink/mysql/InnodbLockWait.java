package io.dblint.mart.metricsink.mysql;

import java.time.ZonedDateTime;

public class InnodbLockWait extends Logged {

  public final Transaction waiting;
  public final Transaction blocking;

  InnodbLockWait(Transaction waiting, Transaction blocking,
                        ZonedDateTime time) {
    this.waiting = waiting;
    this.blocking = blocking;
    this.logTime = time;
  }

  public String getBlockingId() {
    return blocking.id;
  }

  public String getWaitingId() {
    return waiting.id;
  }
}
