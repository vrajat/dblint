package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class LongTxnParser extends RowParser<LongTxnParser.LongTxn> {
  public static class LongTxn {
    public final ZonedDateTime timeStamp;
    public final Transaction transaction;

    public LongTxn(ZonedDateTime timeStamp, Transaction transaction) {
      this.timeStamp = timeStamp;
      this.transaction = transaction;
    }
  }

  LongTxn parseRow(RewindBufferedReader reader, ZonedDateTime timeStamp)
      throws IOException, MetricAgentException {
    String line = getLineOrThrow(reader);
    final String id = parseColumn(line)[1].trim();

    line = getLineOrThrow(reader);
    final ZonedDateTime transactionStarted = ZonedDateTime.of(LocalDateTime.parse(
        parseColumn(line)[1].trim(), dateFormat), ZoneOffset.UTC);

    line = getLineOrThrow(reader);
    final String thread = parseColumn(line)[1].trim();

    line = getLineOrThrow(reader);
    final String lockMode = parseColumn(line)[1].trim();

    line = getLineOrThrow(reader);
    final String lockType = parseColumn(line)[1].trim();

    line = getLineOrThrow(reader);
    final String lockTable = parseColumn(line)[1].trim();

    line = getLineOrThrow(reader);
    final String lockData = parseColumn(line)[1].trim();

    line = getLineOrThrow(reader);
    StringBuilder query = new StringBuilder(parseColumn(line)[1]);
    line = reader.readLine();
    while (line != null && !line.isEmpty() && !row.matcher(line).find()) {
      query.append("\n");
      query.append(line);
      line = reader.readLine();
    }

    if (line != null) {
      reader.rewind(line);
    }

    return new LongTxn(timeStamp, new Transaction(id, thread, query.toString(), transactionStarted,
        null, lockMode, lockType, lockTable, null, lockData));
  }
}
