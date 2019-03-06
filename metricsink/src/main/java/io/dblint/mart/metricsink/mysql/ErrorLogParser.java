package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ErrorLogParser {
  private static Logger logger = LoggerFactory.getLogger(ErrorLogParser.class);
  private static Pattern deadlockStart = Pattern.compile(
      "InnoDB: transactions deadlock detected, dumping detailed information.");

  private static DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss");

  static boolean isALog(String line) {
    try {
      LocalDate.parse(line.substring(0, 18), dateFormat);
      return true;
    } catch (DateTimeParseException exc) {
      return false;
    }
  }

  static boolean newDeadlockSection(String line) {
    return deadlockStart.matcher(line).find();
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
          + "`(\\w+)`.`(\\w+)` trx id (\\d+) lock_mode X( locks rec but not gap)*(\\bwaiting\\b)*"
  );

  private static Pattern recordLock = Pattern.compile(
      "^Record lock, heap no"
  );

  static boolean parseRecordLock(RewindBufferedReader bufferedReader) throws IOException {
    String line = bufferedReader.readLine();
    if (line != null && recordLock.matcher(line).find()) {
      logger.debug("Found Record Lock");
      while (bufferedReader.ready() && line != null && !line.isEmpty()) {
        line = bufferedReader.readLine();
      }
      return true;
    }

    bufferedReader.rewind(line);
    return false;
  }

  static Deadlock.Lock parseLock(RewindBufferedReader bufferedReader)
      throws IOException, MetricAgentException {
    String line = bufferedReader.readLine();
    Matcher matcher = pattern.matcher(line);
    Deadlock.Lock lock;
    if (matcher.find()) {
      lock = new Deadlock.Lock(
          matcher.group(7),
          matcher.group(1),
          matcher.group(2),
          matcher.group(3),
          matcher.group(4),
          matcher.group(5),
          matcher.group(6));
    } else {
      throw new MetricAgentException("Line (" + bufferedReader.getLineNumber() + ") "
          + "did not match Lock pattern: '" + line
          + "' at " + matcher.toString());
    }

    while (parseRecordLock(bufferedReader)) {}

    return lock;
  }

  static Deadlock.Transaction parseTransaction(RewindBufferedReader bufferedReader)
      throws IOException, MetricAgentException {
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
        bufferedReader.rewind(line);
        return transaction;
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

  static Deadlock parseDeadlock(RewindBufferedReader bufferedReader)
      throws IOException, MetricAgentException {
    List<Deadlock.Transaction> transactions = new ArrayList<>();
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
        throw new MetricAgentException("Unexpected line (" + bufferedReader.getLineNumber() + ")`"
            + line + "`");
      }
    }
    return new Deadlock(transactions);
  }

  /**
   * Parse a MySQL error log.
   * @param bufferedReader Reader for the error log
   * @return List of deadlocks found in error log
   * @throws IOException Exceptions w.r.t log IO
   */
  public static List<Deadlock> parse(RewindBufferedReader bufferedReader)
      throws IOException, MetricAgentException {
    List<Deadlock> deadlocks = new ArrayList<>();
    while (bufferedReader.ready()) {
      if (newDeadlockSection(bufferedReader.readLine())) {
        deadlocks.add(parseDeadlock(bufferedReader));
      }
    }

    return deadlocks;
  }
}
