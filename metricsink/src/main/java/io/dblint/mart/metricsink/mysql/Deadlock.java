package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Deadlock {
  public static class Transaction {
    private static Pattern idPattern = Pattern.compile(
        "^TRANSACTION ([0-9]+),.+"
    );

    String id;
    String query;

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
  }

  public static class Lock {
    private static Pattern pattern = Pattern.compile(
        "^RECORD LOCKS space id ([0-9]+) page no ([0-9]+) n bits ([0-9]+) index `PRIMARY` of table "
        + "`(\\w+)`.`(\\w+)` trx id (\\d+) lock_mode X locks rec but not gap waiting"
    );

    public final String id;
    public final String spaceId;
    public final String pageNo;
    public final String numBits;
    public final String index;
    public final String schema;
    public final String table;
    Transaction holder;
    List<Transaction> waitList;

    Lock(String line) throws MetricAgentException {
      Matcher matcher = pattern.matcher(line);
      if (matcher.find()) {
        spaceId = matcher.group(1);
        pageNo = matcher.group(2);
        numBits = matcher.group(3);
        index = "PRIMARY";
        schema = matcher.group(4);
        table = matcher.group(5);
        id = matcher.group(6);
      } else {
        throw new MetricAgentException("Line did not match Lock pattern: '" + line
            + "' at " + matcher.toString());
      }
    }

    public Transaction getHolder() {
      return holder;
    }

    public void setHolder(Transaction holder) {
      this.holder = holder;
    }

    public List<Transaction> getWaitList() {
      return waitList;
    }

    public void addWaitList(Transaction txn) {
      this.waitList.add(txn);
    }
  }
}
