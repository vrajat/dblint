package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class RowParser<T> {
  private static Logger logger = LoggerFactory.getLogger(RowParser.class);
  protected static Pattern row = Pattern.compile(
      "^\\*+\\s([0-9]+)\\.\\srow\\s\\*+$"
  );

  protected static Pattern now = Pattern.compile(
      "^now\\(\\)"
  );

  static DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss");

  static boolean newTimeSection(RewindBufferedReader reader)
    throws IOException {
    String line = reader.readLine();
    Matcher rowMatcher = row.matcher(line);
    if (rowMatcher.matches()) {
      logger.debug("Row matched");

      line = reader.readLine();
      if (now.matcher(line).find()) {
        reader.rewind(line);
        return true;
      }
    }

    return false;
  }

  static String getLineOrThrow(RewindBufferedReader reader)
      throws IOException, MetricAgentException {
    String line = reader.readLine();
    if (line == null) {
      throw new MetricAgentException("Hit EOF unexpectedly");
    }
    return line;
  }

  String [] parseColumn(String line)
      throws MetricAgentException {
    String [] parts = line.split(":", 2);
    if (parts.length != 2) {
      throw new MetricAgentException("Line could not parsed to a column: `" + line + "'");
    }
    return parts;
  }

  List<T> parseTimeSection(RewindBufferedReader reader)
    throws IOException, MetricAgentException {
    String line = reader.readLine();
    String [] parts = line.split(":", 2);
    if (parts.length != 2) {
      throw new MetricAgentException("Line (" + reader.getLineNumber() + ") "
          + "did not match now pattern: '" + line);
    }
    ZonedDateTime time = ZonedDateTime.of(LocalDateTime.parse(parts[1].trim(), dateFormat),
        ZoneOffset.ofHoursMinutes(5, 30));
    List<T> list = new ArrayList<>();
    line = reader.readLine();
    while (line != null && !line.isEmpty() && row.matcher(line).matches()) {
      list.add(parseRow(reader, time));
      line = reader.readLine();
    }

    return list;
  }

  /**
   * Parse the output of a SQL query on information schema to get lock waits.
   * @param reader Reader pointing to the output
   * @return A list of Lock Wait graphs
   * @throws IOException Thrown if Reader cannot read data from stream
   * @throws MetricAgentException Thrown if parser cannot parse the stream
   */
  public List<T> parse(RewindBufferedReader reader)
      throws IOException, MetricAgentException {
    List<T> list = new ArrayList<>();
    while (reader.ready()) {
      if (newTimeSection(reader)) {
        list.addAll(parseTimeSection(reader));
      }
    }
    return list;
  }


  abstract T parseRow(RewindBufferedReader reader, ZonedDateTime time)
      throws IOException, MetricAgentException;
}
