package io.dblint.mart.server.commands.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dblint.mart.metricsink.mysql.InnodbLockWait;
import io.dblint.mart.metricsink.mysql.InnodbLockWaitsParser;
import io.dblint.mart.metricsink.mysql.RewindBufferedReader;
import io.dblint.mart.metricsink.mysql.Sink;
import io.dblint.mart.metricsink.util.MetricAgentException;
import org.jdbi.v3.core.Handle;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class InnodbLockWaitLog extends LogParser<InnodbLockWaitsParser, InnodbLockWait> {
  /**
   * A command to parse innodb lock wait query output.
   */
  public InnodbLockWaitLog() {
    super("innodb_lock_waits", "Analyze Innodb Lock Wait Information");
    this.parserT = new InnodbLockWaitsParser();
  }

  @Override
  protected List<InnodbLockWait> parse(RewindBufferedReader reader)
      throws IOException, MetricAgentException {
    return this.parserT.parse(reader);
  }

  @Override
  protected void output(OutputStream os) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(os, this.list);
  }

  @Override
  void outputSql(Sink sink, Handle handle, InnodbLockWait item) {
    if (!sink.getTransaction(handle, item.blocking.getId()).isPresent()) {
      sink.insertTransaction(handle, item.blocking);
    }

    if (!sink.getTransaction(handle, item.waiting.getId()).isPresent()) {
      sink.insertTransaction(handle, item.waiting);
    }

    sink.insertLockWait(handle, item);
  }
}
