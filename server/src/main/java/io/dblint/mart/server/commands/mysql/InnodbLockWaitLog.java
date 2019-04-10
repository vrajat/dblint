package io.dblint.mart.server.commands.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dblint.mart.metricsink.mysql.InnodbLockWait;
import io.dblint.mart.metricsink.mysql.InnodbLockWaitsParser;
import io.dblint.mart.metricsink.mysql.RewindBufferedReader;
import io.dblint.mart.metricsink.mysql.Sink;
import io.dblint.mart.metricsink.util.MetricAgentException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class InnodbLockWaitLog extends LogParser {
  private List<InnodbLockWait> lockWaits;

  /**
   * A command to parse innodb lock wait query output.
   */
  public InnodbLockWaitLog() {
    super("innodb_lock_waits", "Analyze Innodb Lock Wait Information");
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
  protected void filter(ZonedDateTime start, ZonedDateTime end) {
    lockWaits = lockWaits.stream()
        .filter(lw -> lw.time.isAfter(start) && lw.time.isBefore(end))
        .collect(Collectors.toList());
  }

  @Override
  protected void output(OutputStream os) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(os, lockWaits);
  }

  @Override
  void outputSql(Sink sink, Namespace namespace) {}
}
