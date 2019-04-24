package io.dblint.mart.server.commands.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dblint.mart.metricsink.mysql.Deadlock;
import io.dblint.mart.metricsink.mysql.ErrorLogParser;
import io.dblint.mart.metricsink.mysql.RewindBufferedReader;
import io.dblint.mart.metricsink.mysql.Sink;
import io.dblint.mart.metricsink.util.MetricAgentException;
import org.jdbi.v3.core.Handle;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

public class ErrorLog extends LogParser<ErrorLogParser, Deadlock> {

  public ErrorLog() {
    super("errorlog", "Analyze Error Log for deadlocks in innodb");
  }

  @Override
  protected List<Deadlock> parse(RewindBufferedReader reader)
      throws IOException, MetricAgentException {
    return ErrorLogParser.parse(new RewindBufferedReader(reader));
  }

  @Override
  protected void output(OutputStream os) throws IOException {
    Collections.sort(this.list);
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(os, this.list);
  }

  @Override
  void outputSql(Sink sink, Handle handle, Deadlock item) {}
}
