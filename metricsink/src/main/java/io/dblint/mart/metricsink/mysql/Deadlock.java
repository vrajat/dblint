package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Deadlock {
  private static Logger logger = LoggerFactory.getLogger(Deadlock.class);

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

    public void addHoldingLocks(Lock holdingLock) {
      this.holdingLocks.add(holdingLock);
    }

    public List<Lock> getWaitingLocks() {
      return waitingLocks;
    }

    public void addWaitingLock(Lock waitingLock) {
      this.waitingLocks.add(waitingLock);
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

    public Lock(String id, String spaceId, String pageNo, String numBits,
                String index, String schema, String table) {
      this.id = id;
      this.spaceId = spaceId;
      this.pageNo = pageNo;
      this.numBits = numBits;
      this.index = index;
      this.schema = schema;
      this.table = table;
    }
  }

  private static Pattern transactionStart = Pattern.compile(
      "^\\*\\*\\* \\([0-9]+\\) TRANSACTION:"
  );
  private static Pattern lockWaiting = Pattern.compile(
      "^\\*\\*\\* \\([0-9]\\) WAITING FOR THIS LOCK TO BE GRANTED:"
  );
  private static Pattern lockHolding = Pattern.compile(
      "^\\*\\*\\* \\([0-9]\\) HOLDS THE LOCK\\(S\\):"
  );
  private static Pattern rollBack = Pattern.compile(
      "^\\*\\*\\* WE ROLL BACK TRANSACTION \\(([0-9]+)\\)"
  );

  private static Pattern pattern = Pattern.compile(
      "^RECORD LOCKS space id ([0-9]+) page no ([0-9]+) n bits ([0-9]+) index `(\\w+)` of table "
          + "`(\\w+)`.`(\\w+)` trx id (\\d+) lock_mode X locks rec but not gap(\\bwaiting\\b)*"
  );

  List<Transaction> transactions = new ArrayList<>();

  static Lock parseLock(BufferedReader bufferedReader) throws IOException, MetricAgentException {
    String line = bufferedReader.readLine();
    Matcher matcher = pattern.matcher(line);
    Lock lock;
    if (matcher.find()) {
      lock = new Lock(
          matcher.group(7),
          matcher.group(1),
          matcher.group(2),
          matcher.group(3),
          matcher.group(4),
          matcher.group(5),
          matcher.group(6));
    } else {
      throw new MetricAgentException("Line did not match Lock pattern: '" + line
          + "' at " + matcher.toString());
    }

    while (bufferedReader.ready() && !line.isEmpty()) {
      line = bufferedReader.readLine();
    }

    return lock;
  }

  static Transaction parseTransaction(BufferedReader bufferedReader) throws IOException, MetricAgentException {
    Deadlock.Transaction transaction = new Deadlock.Transaction();
    transaction.setId(bufferedReader.readLine());

    // Ignore these lines
    // mysql tables in use 3, locked 3
    bufferedReader.readLine();
    // LOCK WAIT 519 lock struct(s), heap size 63016, 4 row lock(s)
    bufferedReader.readLine();
    // MySQL thread id 29932874, OS thread handle 0x2b91f3982700, query id 27905488590 \
    // 172.11.1.1 dbadmin updating
    bufferedReader.readLine();

    transaction.setQuery(bufferedReader.readLine());
    logger.debug("Parsed Transaction metadata");

    String line = bufferedReader.readLine();
    while (line != null && !line.isEmpty()) {
      final boolean waiting = lockWaiting.matcher(line).matches();
      final boolean holding = lockHolding.matcher(line).matches();

      if (!waiting && !holding) {
        throw new MetricAgentException("Expected Lock Wait or Hold information. Found `"
            + line + "`");
      }
      Deadlock.Lock lock = parseLock(bufferedReader);
      if (waiting) {
        transaction.addWaitingLock(lock);
      } else {
        transaction.addHoldingLocks(lock);
      }
      logger.debug("Parsed a lock");
      line = bufferedReader.readLine();
    }

    return transaction;
  }

  void parse(BufferedReader bufferedReader) throws IOException, MetricAgentException {
    //Read empty line
    bufferedReader.readLine();
    boolean foundRollBack = false;
    while (!foundRollBack) {
      String line = bufferedReader.readLine();
      if (transactionStart.matcher(line).matches()) {
        transactions.add(parseTransaction(bufferedReader));
      } else if (rollBack.matcher(line).matches()) {
        foundRollBack = true;
      } else {
        throw new MetricAgentException("Unexpected line `" + line + "`");
      }
    }
  }
}
