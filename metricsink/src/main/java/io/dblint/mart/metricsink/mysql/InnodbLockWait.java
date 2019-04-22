package io.dblint.mart.metricsink.mysql;

import java.time.ZonedDateTime;

public class InnodbLockWait {

  public final Transaction waiting;
  public final Transaction blocking;
  public final ZonedDateTime time;

  InnodbLockWait(Transaction waiting, Transaction blocking,
                        ZonedDateTime time) {
    this.waiting = waiting;
    this.blocking = blocking;
    this.time = time;
  }
}
