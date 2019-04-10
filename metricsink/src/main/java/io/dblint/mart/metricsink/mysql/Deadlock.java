package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Deadlock implements Comparable<Deadlock> {
  public static class Transaction {
    private static Pattern idPattern = Pattern.compile(
        "^TRANSACTION ([0-9]+),.+"
    );

    String id;
    String query;
    List<Lock> holdingLocks = new ArrayList<>();
    List<Lock> waitingLocks = new ArrayList<>();

    /**
     * Set the MySQL query ID.
     * @param line Line containing the query id. The complete line is expected
     * @throws MetricAgentException An exception if the line does not match the regex pattern
     */
    public void setId(String line) throws MetricAgentException {
      Matcher matcher = idPattern.matcher(line);
      if (matcher.matches()) {
        id = matcher.group(1);
        return;
      }

      throw new MetricAgentException("Transaction ID not found in '" + line + "'");
    }

    public void setQuery(String query) {
      this.query = query;
    }

    public List<Lock> getHoldingLocks() {
      return holdingLocks;
    }

    void addHoldingLocks(Lock holdingLock) {
      this.holdingLocks.add(holdingLock);
    }

    public List<Lock> getWaitingLocks() {
      return waitingLocks;
    }

    void addWaitingLock(Lock waitingLock) {
      this.waitingLocks.add(waitingLock);
    }

    public String getId() {
      return id;
    }

    public String getQuery() {
      return query;
    }
  }

  public static class Lock {
    public final String id;
    public final String spaceId;
    public final String pageNo;
    public final String numBits;
    public final String index;
    public final String schema;
    public final String table;
    public final String lockType;

    Lock(String id, String spaceId, String pageNo, String numBits,
                String index, String schema, String table, String lockType) {
      this.id = id;
      this.spaceId = spaceId;
      this.pageNo = pageNo;
      this.numBits = numBits;
      this.index = index;
      this.schema = schema;
      this.table = table;
      this.lockType = lockType;
    }
  }

  public final List<Transaction> transactions;
  public final ZonedDateTime time;

  public Deadlock(List<Transaction> transactions, ZonedDateTime time) {
    this.transactions = transactions;
    this.time = time;
  }

  @Override
  public int compareTo(Deadlock deadlock) {
    return time.compareTo(deadlock.time);
  }
}
