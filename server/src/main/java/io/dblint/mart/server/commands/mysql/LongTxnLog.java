package io.dblint.mart.server.commands.mysql;

import io.dblint.mart.metricsink.mysql.LongTxnParser;
import io.dblint.mart.metricsink.mysql.RewindBufferedReader;
import io.dblint.mart.metricsink.mysql.Sink;
import io.dblint.mart.metricsink.mysql.Transaction;
import io.dblint.mart.metricsink.util.MetricAgentException;
import org.jdbi.v3.core.Handle;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;

public class LongTxnLog extends LogParser<LongTxnParser, LongTxnParser.LongTxn> {

  public LongTxnLog() {
    super("long_txns", "Parse Long Transactions log");
    this.parserT = new LongTxnParser();
  }

  @Override
  List<LongTxnParser.LongTxn> parse(RewindBufferedReader reader)
      throws IOException, MetricAgentException {
    return this.parserT.parse(reader);
  }

  @Override
  void output(OutputStream os) throws IOException {}

  @Override
  void outputSql(Sink sink, Handle handle, LongTxnParser.LongTxn item) throws MetricAgentException {
    long tid = sink.insertTransaction(handle, item.getTransaction());
    Optional<Transaction> transaction = sink.getTransaction(handle, tid);
    if (!transaction.isPresent()) {
      throw new MetricAgentException("Failed to insert transaction for LongTxn");
    }

    item.setTransaction(transaction.get());
    sink.insertLongTxn(handle, item);
  }
}
