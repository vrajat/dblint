package io.dblint.mart.server.commands.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dblint.mart.metricsink.mysql.RewindBufferedReader;
import io.dblint.mart.metricsink.mysql.Sink;
import io.dblint.mart.metricsink.mysql.SlowQueryLogParser;
import io.dblint.mart.metricsink.mysql.UserQuery;
import io.dblint.mart.metricsink.util.MetricAgentException;
import org.jdbi.v3.core.Handle;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class SlowQueryLog extends LogParser<SlowQueryLogParser, UserQuery> {
  /**
   * A command to parse slow query logs.
   */
  public SlowQueryLog() {
    super("parse-slow-query", "Parse MySQL slow query log");
  }

  @Override
  protected List<UserQuery> parse(RewindBufferedReader reader)
      throws IOException, MetricAgentException {

    return this.parserT.parseLog(
        new RewindBufferedReader(reader)
    );
  }

  @Override
  protected void output(OutputStream os) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(os, this.list);
  }

  @Override
  protected void outputSql(Sink sink, Handle handle, UserQuery userQuery) {
    sink.insertUserQuery(handle, userQuery);
  }
}
