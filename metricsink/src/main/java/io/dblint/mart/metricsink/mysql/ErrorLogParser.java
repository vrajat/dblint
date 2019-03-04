package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
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
    return deadlockStart.matcher(line).matches();
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
        Deadlock deadlock = new Deadlock();
        deadlock.parse(br);
      }
    }
  }
}
