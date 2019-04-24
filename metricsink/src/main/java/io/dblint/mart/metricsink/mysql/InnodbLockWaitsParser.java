package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.regex.Pattern;

public class InnodbLockWaitsParser extends RowParser<InnodbLockWait> {
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
  static Pattern trxStarted = Pattern.compile(
      "^\\s*(block|wait)ing_trx_started"
  );


  Transaction parseTransaction(RewindBufferedReader reader)
      throws IOException, MetricAgentException {
    Transaction transaction = new Transaction();

    String line = getLineOrThrow(reader);
    transaction.setId(parseColumn(line)[1].trim());

    line = getLineOrThrow(reader);
    transaction.setThread(parseColumn(line)[1].trim());

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
    transaction.setQuery(query.toString());

    transaction.setZonedStartTime(ZonedDateTime.of(LocalDateTime.parse(
        parseColumn(line)[1].trim(), dateFormat), ZoneOffset.UTC));

    ZonedDateTime waitStarted = null;
    line = getLineOrThrow(reader);
    String [] columns = parseColumn(line);
    if (columns[0].contains("waiting_trx_wait_started")) {
      waitStarted = ZonedDateTime.of(LocalDateTime.parse(parseColumn(line)[1].trim(), dateFormat),
          ZoneOffset.UTC);
      line = getLineOrThrow(reader);
      columns = parseColumn(line);
    }
    transaction.setZonedWaitStartTime(waitStarted);

    transaction.setLockMode(columns[1].trim());

    line = getLineOrThrow(reader);
    transaction.setLockType(parseColumn(line)[1].trim());

    line = getLineOrThrow(reader);
    transaction.setLockTable(parseColumn(line)[1].trim());

    line = getLineOrThrow(reader);
    transaction.setLockIndex(parseColumn(line)[1].trim());

    line = getLineOrThrow(reader);
    transaction.setLockData(parseColumn(line)[1].trim());

    return transaction;
  }

  InnodbLockWait parseRow(RewindBufferedReader reader, ZonedDateTime time)
    throws IOException, MetricAgentException {
    Transaction waiting = parseTransaction(reader);
    Transaction blocking = parseTransaction(reader);
    return new InnodbLockWait(waiting, blocking, time);
  }
}
