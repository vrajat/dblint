package io.dblint.mart.server.commands.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dblint.mart.metricsink.mysql.RewindBufferedReader;
import io.dblint.mart.metricsink.mysql.SlowQueryLogParser;
import io.dblint.mart.metricsink.mysql.UserQuery;
import io.dblint.mart.metricsink.util.MetricAgentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

public class SlowQueryLog extends LogParser {
  private static Logger logger = LoggerFactory.getLogger(SlowQueryLog.class);
  private List<UserQuery> queries;

  /**
   * A command to parse slow query logs.
   */
  public SlowQueryLog() {
    super("slowquerylog", "Analyze Queries in MySQL slow query log");
    this.queries = new ArrayList<>();
  }


  @Override
  protected void process(Reader reader)
      throws IOException, MetricAgentException {

    SlowQueryLogParser parser = new SlowQueryLogParser();
    this.queries.addAll(parser.parseLog(
        new RewindBufferedReader(reader)
        )
    );
  }

  @Override
  protected void output(OutputStream os) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(os, queries);
  }
}
