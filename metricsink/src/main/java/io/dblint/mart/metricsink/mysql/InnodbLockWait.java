package io.dblint.mart.metricsink.mysql;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;

public class InnodbLockWait {

  public static class Transaction {
    public final String id;
    public final String thread;
    public final String query;
    public final ZonedDateTime startTime;
    public final ZonedDateTime waitStartTime;
    public final String lockMode;
    public final String lockType;
    public final String lockTable;
    public final String lockIndex;
    public final String lockData;


    Transaction(String id, String thread, String query,
                ZonedDateTime startTime, ZonedDateTime waitStartTime,
                String lockMode, String lockType, String lockTable,
                String lockIndex, String lockData) {
      this.id = id;
      this.thread = thread;
      this.query = query;
      this.startTime = startTime;
      this.waitStartTime = waitStartTime;
      this.lockMode = lockMode;
      this.lockType = lockType;
      this.lockTable = lockTable;
      this.lockIndex = lockIndex;
      this.lockData = lockData;
    }
  }

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
