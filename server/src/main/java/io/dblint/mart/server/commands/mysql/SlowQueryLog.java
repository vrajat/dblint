package io.dblint.mart.server.commands.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dblint.mart.analyses.mysql.SlowQuery;
import io.dblint.mart.metricsink.mysql.QueryAttribute;
import io.dblint.mart.metricsink.mysql.RewindBufferedReader;
import io.dblint.mart.metricsink.mysql.Sink;
import io.dblint.mart.metricsink.mysql.SlowQueryLogParser;
import io.dblint.mart.metricsink.mysql.UserQuery;
import io.dblint.mart.metricsink.util.MetricAgentException;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.calcite.sql.parser.SqlParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SlowQueryLog extends LogParser {
  private static Logger logger = LoggerFactory.getLogger(SlowQueryLog.class);
  private List<UserQuery> queries;

  /**
   * A command to parse slow query logs.
   */
  public SlowQueryLog() {
    super("parse-slow-query", "Parse MySQL slow query log");
    this.queries = new ArrayList<>();
  }

  @Override
  public void configure(Subparser subparser) {
    super.configure(subparser);
    subparser.addArgument("-a", "--analyze")
        .type(boolean.class)
        .action(Arguments.storeTrue());
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
  protected void filter(ZonedDateTime start, ZonedDateTime end) {
    this.queries = this.queries.stream()
        .filter(query -> query.getLogTime().isAfter(start) && query.getLogTime().isBefore(end))
        .collect(Collectors.toList());
  }

  @Override
  protected void output(OutputStream os) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(os, queries);
  }

  @Override
  protected void outputSql(Sink sink, Namespace namespace) {
    sink.useHandle(handle ->
        queries.forEach(q -> {
          int id = sink.insertUserQuery(handle, q);
          if (namespace.getBoolean("analyze")) {
            q.setId(id);
            SlowQuery slowQuery = new SlowQuery(this.registry);
            try {
              QueryAttribute attribute = slowQuery.analyze(q.getQuery());
              sink.setQueryAttribute(handle, q, attribute);
            } catch (SqlParseException | UnsupportedOperationException
                | NullPointerException exc) {
              logger.error("Failed to analyze query '" + q.getId() + "'." + exc.getMessage());
            }
          }
        })
    );
  }
}
