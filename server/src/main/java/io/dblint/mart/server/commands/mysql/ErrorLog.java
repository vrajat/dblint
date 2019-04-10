package io.dblint.mart.server.commands.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dblint.mart.metricsink.mysql.Deadlock;
import io.dblint.mart.metricsink.mysql.ErrorLogParser;
import io.dblint.mart.metricsink.mysql.RewindBufferedReader;
import io.dblint.mart.metricsink.mysql.Sink;
import io.dblint.mart.metricsink.util.MetricAgentException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ErrorLog extends LogParser {
  private static Logger logger = LoggerFactory.getLogger(ErrorLog.class);
  private List<Deadlock> deadlocks = new ArrayList<>();

  public ErrorLog() {
    super("errorlog", "Analyze Error Log for deadlocks in innodb");
  }

  @Override
  protected void process(Reader reader)
      throws IOException, MetricAgentException {

    this.deadlocks.addAll(ErrorLogParser.parse(
        new RewindBufferedReader(reader)
    ));
  }

  @Override
  protected void filter(ZonedDateTime start, ZonedDateTime end) {
    this.deadlocks = this.deadlocks.stream()
        .filter(lock -> lock.time.isAfter(start) && lock.time.isBefore(end))
        .collect(Collectors.toList());
  }

  @Override
  protected void output(OutputStream os) throws IOException {
    Collections.sort(deadlocks);
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(os, this.deadlocks);
  }

  @Override
  void outputSql(Sink sink, Namespace namespace) {}
}
