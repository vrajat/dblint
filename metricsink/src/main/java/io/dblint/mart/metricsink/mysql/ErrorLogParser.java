package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Pattern;

public class ErrorLogParser {
  private static Logger logger = LoggerFactory.getLogger(ErrorLogParser.class);
  private static Pattern deadlockStart = Pattern.compile(
      "InnoDB: transactions deadlock detected, dumping detailed information.");
  private static Pattern transactionStart = Pattern.compile(
      "\\*\\*\\* \\([0-9]+\\) TRANSACTION:"
  );
  private static Pattern lockWaiting = Pattern.compile(
      "\\*\\*\\* \\([0-9]\\) WAITING FOR THIS LOCK TO BE GRANTED:"
  );

  static boolean newDeadlockSection(String line) {
    return deadlockStart.matcher(line).matches();
  }

  static void parseDeadlock(BufferedReader bufferedReader)
      throws IOException, MetricAgentException {
    while (bufferedReader.ready()) {
      if (transactionStart.matcher(bufferedReader.readLine()).matches()) {
        Deadlock.Transaction transaction = new Deadlock.Transaction();
        transaction.setId(bufferedReader.readLine());

        // Ignore these lines
        // mysql tables in use 3, locked 3
        // LOCK WAIT 519 lock struct(s), heap size 63016, 4 row lock(s)
        // MySQL thread id 29932874, OS thread handle 0x2b91f3982700, query id 27905488590 \
        // 172.11.1.1 dbadmin updating
        transaction.setQuery(bufferedReader.readLine());
        final boolean waiting = lockWaiting.matcher(bufferedReader.readLine()).matches();
        Deadlock.Lock lock = new Deadlock.Lock(bufferedReader.readLine());

        if (waiting) {
          lock.addWaitList(transaction);
        } else {
          lock.setHolder(transaction);
        }
      }
    }
  }

  /**
   * Parse a MySQL error log.
   * @param is Inputstream to the error log
   * @throws IOException Exceptions w.r.t log IO
   */
  public static void parser(InputStream is) throws IOException, MetricAgentException {
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    while (br.ready()) {
      if (newDeadlockSection(br.readLine())) {
        parseDeadlock(br);
      }
    }
  }
}
