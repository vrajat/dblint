package io.dblint.mart.server.commands.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dblint.mart.metricsink.mysql.InnodbLockWait;
import io.dblint.mart.metricsink.mysql.InnodbLockWaitsParser;
import io.dblint.mart.metricsink.mysql.RewindBufferedReader;
import io.dblint.mart.metricsink.util.MetricAgentException;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

public class InnodbLockWaitLog extends LogParser {
  private List<InnodbLockWait> lockWaits;

  /**
   * A command to parse innodb lock wait query output.
   */
  public InnodbLockWaitLog() {
    super("innodb_lock_waits", "Analyze Queries in MySQL slow query log");
    this.lockWaits = new ArrayList<>();
  }


  @Override
  protected void process(Reader reader)
      throws IOException, MetricAgentException {

    this.lockWaits.addAll(InnodbLockWaitsParser.parse(
        new RewindBufferedReader(reader)
        )
    );
  }

  @Override
  protected void output(OutputStream os) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(os, lockWaits);
  }
}
