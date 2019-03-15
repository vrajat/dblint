package io.dblint.mart.metricsink.mysql;

import java.time.LocalDateTime;

public class InnodbLockWait {

  public static class Transaction {
    public final String id;
    public final String thread;
    public final String query;

    Transaction(String id, String thread, String query) {
      this.id = id;
      this.thread = thread;
      this.query = query;
    }
  }

  public final Transaction waiting;
  public final Transaction blocking;
  public final LocalDateTime time;

  InnodbLockWait(Transaction waiting, Transaction blocking,
                        LocalDateTime time) {
    this.waiting = waiting;
    this.blocking = blocking;
    this.time = time;
  }
}
