package com.dblint.metricsink.mysql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SlowQueryLogParser {
  static Pattern tsLine = Pattern.compile("^# Time: ([0-9: ]{15})");
  static Pattern uhLine = Pattern.compile("# User@Host: (.*\\[.*\\])\\s*@\\s*"
      + "\\[(.*)\\]\\s*Id:\\s*(\\d+)");
  static Pattern queryMetadata = Pattern.compile("# Query_time: ([\\d\\.]+)\\s*"
      + "Lock_time: ([\\d\\.]+)\\s*Rows_sent: (\\d+)\\s*Rows_examined: (\\d+)");
  static Pattern setStatement = Pattern.compile("SET\\s+(.*);");
  static Pattern useStatement = Pattern.compile("USE\\s+([.;]*)");
  static Pattern comments = Pattern.compile("/\\*(.*)\\*/");

  static boolean parseTsLine(String line, UserQuery userQuery) {
    Matcher matcher = tsLine.matcher(line);
    if (matcher.find()) {
      userQuery.setTime(matcher.group(1));
      return true;
    }
    return false;
  }

  static boolean parseUhLine(String line, UserQuery userQuery) {
    Matcher matcher = uhLine.matcher(line);
    if (matcher.find()) {
      userQuery.setUserHost(matcher.group(1));
      userQuery.setIpAddress(matcher.group(2));
      userQuery.setId(matcher.group(3));
      return true;
    }
    return false;
  }

  static boolean parseQueryMetadataLine(String line, UserQuery userQuery) {
    Matcher matcher = queryMetadata.matcher(line);
    if (matcher.find()) {
      userQuery.setQueryTime(matcher.group(1));
      userQuery.setLockTime(matcher.group(2));
      userQuery.setRowsSent(matcher.group(3));
      userQuery.setRowsExamined(matcher.group(4));
      return true;
    }
    return false;
  }

  static boolean parseSetUseStatment(String line) {
    return setStatement.matcher(line).matches() || useStatement.matcher(line).matches();
  }

  static String replaceComments(String line) {
    return line.replaceAll("/\\*.*?\\*/", "");
  }

  static boolean newSection(String line) {
    return tsLine.matcher(line).matches() || uhLine.matcher(line).matches();
  }

  /**
   * Parses a MySql slow query log file and return a list of queries.
   * @param is InputStream of the slow query log file
   * @return A list of queries in the log file
   * @throws IOException An exception is thrown if the log file cannot be read successfully
   */
  public static List<UserQuery> parseLog(InputStream is) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    List<UserQuery> userQueries = new ArrayList<>();

    //Read file header
    int headerLines = 3;
    while (br.ready() && headerLines > 0) {
      br.readLine();
      headerLines--;
    }

    if (headerLines > 0) {
      throw new IOException("Incomplete slow query log");
    }
    UserQuery userQuery = null;

    while (br.ready()) {
      String line = br.readLine();
      if (newSection(line)) {
        if (userQuery != null) {
          userQueries.add(userQuery);
        }
        userQuery = new UserQuery();
        if (parseTsLine(line, userQuery)) {
          if (!br.ready()) {
            throw new IOException("Incomplete query log. Did not find CONNECTION LINE");
          }
          line = br.readLine();
        }
        if (parseUhLine(line, userQuery)) {
          if (!br.ready()) {
            throw new IOException("Incomplete query log. Did not find METADATA LINE");
          }
          line = br.readLine();
        } else {
          throw new IOException("Expected CONNECTION LINE but did not match");
        }

        if (!parseQueryMetadataLine(line, userQuery)) {
          throw new IOException("Expected METADATA LINE but did not match");
        }
      } else if (userQuery == null) {
        throw new IOException("Did not find a new query section");
      } else if (!parseSetUseStatment(line)) {
        userQuery.queries.add(replaceComments(line));
      }
    }
    if (userQuery != null) {
      userQueries.add(userQuery);
    }
    return userQueries;
  }
}
