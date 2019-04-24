package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.regex.Pattern;

public class LongTxnParser extends RowParser<LongTxnParser.LongTxn> {
  public static class LongTxn extends Logged {
    public final Transaction transaction;

    public LongTxn(Transaction transaction, ZonedDateTime timeStamp) {
      this.logTime = timeStamp;
      this.transaction = transaction;
    }

    public String getTransactionId() {
      return transaction.id;
    }
  }

  static Pattern trxStarted = Pattern.compile(
      "^\\s*trx_started"
  );

  LongTxn parseRow(RewindBufferedReader reader, ZonedDateTime timeStamp)
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

    line = getLineOrThrow(reader);
    transaction.setLockMode(parseColumn(line)[1].trim());

    line = getLineOrThrow(reader);
    transaction.setLockType(parseColumn(line)[1].trim());

    line = getLineOrThrow(reader);
    transaction.setLockTable(parseColumn(line)[1].trim());

    line = getLineOrThrow(reader);
    transaction.setLockData(parseColumn(line)[1].trim());


    //Transaction State
    line = getLineOrThrow(reader);
    //Operation State
    line = getLineOrThrow(reader);
    //UTC Timestamp
    line = getLineOrThrow(reader);

    return new LongTxn(transaction, timeStamp);
  }
}
