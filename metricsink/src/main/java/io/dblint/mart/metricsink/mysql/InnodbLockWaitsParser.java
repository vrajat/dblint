package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InnodbLockWaitsParser {
  // Output of query
  /*
  SELECT
  r.trx_id waiting_trx_id,
  r.trx_mysql_thread_id waiting_thread,
  r.trx_query waiting_query,
  r.trx_started waiting_trx_started,
  r.trx_wait_started waiting_trx_wait_started,
  wl.lock_mode waiting_lock_mode,
  wl.lock_type waiting_lock_type,
  wl.lock_table waiting_lock_table,
  wl.lock_index waiting_lock_index,
  wl.lock_data waiting_lock_data,
  b.trx_id blocking_trx_id,
  b.trx_mysql_thread_id blocking_thread,
  b.trx_query blocking_query,
  b.trx_started blocking_trx_started,
  bl.lock_mode blocking_lock_mode,
  bl.lock_type blocking_lock_type,
  bl.lock_table blocking_lock_table,
  bl.lock_index blocking_lock_index,
  bl.lock_data blocking_lock_data
FROM       information_schema.innodb_lock_waits w
INNER JOIN information_schema.innodb_trx b
  ON b.trx_id = w.blocking_trx_id
INNER JOIN information_schema.innodb_trx r
  ON r.trx_id = w.requesting_trx_id
INNER JOIN information_schema.innodb_locks wl
  ON w.requested_lock_id = wl.lock_id
INNER JOIN information_schema.innodb_locks bl
  ON w.blocking_lock_id = bl.lock_id
   */
  private static Logger logger = LoggerFactory.getLogger(InnodbLockWaitsParser.class);

  private static Pattern row = Pattern.compile(
      "^\\*+\\s([0-9]+)\\.\\srow\\s\\*+$"
  );

  private static Pattern now = Pattern.compile(
      "^now\\(\\)"
  );

  static Pattern trxStarted = Pattern.compile(
      "^\\s*(block|wait)ing_trx_started"
  );

  private static DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss");

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

  static List<InnodbLockWait> parseTimeSection(RewindBufferedReader reader)
    throws IOException, MetricAgentException {
    String line = reader.readLine();
    String [] parts = line.split(":", 2);
    if (parts.length != 2) {
      throw new MetricAgentException("Line (" + reader.getLineNumber() + ") "
          + "did not match now pattern: '" + line);
    }
    LocalDateTime time = LocalDateTime.parse(parts[1].trim(), dateFormat);
    List<InnodbLockWait> lockWaits = new ArrayList<>();
    line = reader.readLine();
    while (line != null && !line.isEmpty() && row.matcher(line).matches()) {
      lockWaits.add(parseRow(reader, time));
      line = reader.readLine();
    }

    return lockWaits;
  }

  private static String getLineOrThrow(RewindBufferedReader reader)
      throws IOException, MetricAgentException {
    String line = reader.readLine();
    if (line == null) {
      throw new MetricAgentException("Hit EOF unexpectedly");
    }
    return line;
  }

  static String [] parseColumn(String line)
      throws MetricAgentException {
    String [] parts = line.split(":", 2);
    if (parts.length != 2) {
      throw new MetricAgentException("Line could not parsed to a column: `" + line + "'");
    }
    return parts;
  }

  static InnodbLockWait.Transaction parseTransaction(RewindBufferedReader reader)
      throws IOException, MetricAgentException {
    String line = getLineOrThrow(reader);
    final String id = parseColumn(line)[1].trim();

    line = getLineOrThrow(reader);
    final String thread = parseColumn(line)[1].trim();

    line = getLineOrThrow(reader);
    StringBuilder query = new StringBuilder(parseColumn(line)[1]);

    line = reader.readLine();
    while (line != null && !line.isEmpty()
        && !trxStarted.matcher(line).find()) {
      query.append("\n");
      query.append(line);
      line = reader.readLine();
    }
    if (line == null) {
      throw new MetricAgentException("Hit EOF unexpectedly while extracting query");
    }
    final LocalDateTime transactionStarted = LocalDateTime.parse(
        parseColumn(line)[1].trim(), dateFormat);

    LocalDateTime waitStarted = null;
    line = getLineOrThrow(reader);
    String [] columns = parseColumn(line);
    if (columns[0].contains("waiting_trx_wait_started")) {
      waitStarted = LocalDateTime.parse(parseColumn(line)[1].trim(), dateFormat);
      line = getLineOrThrow(reader);
      columns = parseColumn(line);
    }

    final String lockMode = columns[1].trim();

    line = getLineOrThrow(reader);
    final String lockType = parseColumn(line)[1].trim();

    line = getLineOrThrow(reader);
    final String lockTable = parseColumn(line)[1].trim();

    line = getLineOrThrow(reader);
    final String lockIndex = parseColumn(line)[1].trim();

    line = getLineOrThrow(reader);
    final String lockData = parseColumn(line)[1].trim();

    return new InnodbLockWait.Transaction(id, thread, query.toString(), transactionStarted,
        waitStarted, lockMode, lockType, lockTable, lockIndex, lockData);
  }

  static InnodbLockWait parseRow(RewindBufferedReader reader, LocalDateTime time)
    throws IOException, MetricAgentException {
    InnodbLockWait.Transaction waiting = parseTransaction(reader);
    InnodbLockWait.Transaction blocking = parseTransaction(reader);
    return new InnodbLockWait(waiting, blocking, time);
  }

  /**
   * Parse the output of a SQL query on information schema to get lock waits.
   * @param reader Reader pointing to the output
   * @return A list of Lock Wait graphs
   * @throws IOException Thrown if Reader cannot read data from stream
   * @throws MetricAgentException Thrown if parser cannot parse the stream
   */
  public static List<InnodbLockWait> parse(RewindBufferedReader reader)
      throws IOException, MetricAgentException {
    List<InnodbLockWait> waits = new ArrayList<>();
    while (reader.ready()) {
      if (newTimeSection(reader)) {
        waits.addAll(parseTimeSection(reader));
      }
    }
    return waits;
  }
}
