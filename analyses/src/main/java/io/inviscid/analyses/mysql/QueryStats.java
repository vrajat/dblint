package io.inviscid.analyses.mysql;

import com.codahale.metrics.Timer;
import io.inviscid.sqlplanner.visitors.MySqlIndexVisitor;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class QueryStats implements Comparable<QueryStats> {
  final String digest;
  Timer queryTime;
  Timer lockTime;
  long rowsSent;
  long rowsExamined;
  long numQueries;
  long indexUsed;
  Set<MySqlIndexVisitor.Index> missingIndexes;

  QueryStats(String digest) {
    this.digest = digest;
    queryTime = new Timer();
    lockTime = new Timer();
    rowsSent = 0;
    rowsExamined = 0;
    numQueries = 0;
    indexUsed = 0;
    missingIndexes = new HashSet<>();
  }

  public QueryStats addQueryTime(Double time) {
    queryTime.update(time.longValue(), TimeUnit.SECONDS);
    return this;
  }

  public QueryStats addLockTime(Double time) {
    lockTime.update(time.longValue(), TimeUnit.SECONDS);
    return this;
  }

  public QueryStats addRowsSent(long rowsSent) {
    this.rowsSent += rowsSent;
    return this;
  }

  public QueryStats addRowsExamined(long rowsExamined) {
    this.rowsExamined += rowsExamined;
    return this;
  }

  public QueryStats addNumQueries(long numQueries) {
    this.numQueries += numQueries;
    return this;
  }

  public QueryStats addIndexUsed(long indexUsed) {
    this.indexUsed += indexUsed;
    return this;
  }

  public QueryStats addMissingIndex(MySqlIndexVisitor.Index index) {
    missingIndexes.add(index);
    return this;
  }

  public String getDigest() {
    return digest;
  }

  public Timer getQueryTime() {
    return queryTime;
  }

  public Timer getLockTime() {
    return lockTime;
  }

  public long getRowsSent() {
    return rowsSent;
  }

  public long getRowsExamined() {
    return rowsExamined;
  }

  public long getNumQueries() {
    return numQueries;
  }

  public long getIndexUsed() {
    return indexUsed;
  }

  @Override
  public int compareTo(QueryStats other) {
    return Double.compare((this.queryTime.getMeanRate() * this.numQueries),
        (other.queryTime.getMeanRate() * other.numQueries));
  }

  @Override
  public String toString() {
    return "QueryStats{"
        + "digest='" + digest + '\''
        + ", queryTime=" + queryTime.getMeanRate() * numQueries
        + ", lockTime=" + lockTime.getMeanRate() * numQueries
        + ", rowsSent=" + rowsSent
        + ", rowsExamined=" + rowsExamined
        + ", numQueries=" + numQueries
        + ", indexUsed=" + indexUsed
        + ", missingIndexes=" + missingIndexes
        + '}';
  }
}
