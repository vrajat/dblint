package io.dblint.mart.server.commands.mysql;

import com.codahale.metrics.Counter;
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

  Counter numInserted;
  Counter numAnalyzed;
  Counter numParsed;

  /**
   * A command to parse slow query logs.
   */
  public SlowQueryLog() {
    super("parse-slow-query", "Parse MySQL slow query log");
    this.queries = new ArrayList<>();
    this.numAnalyzed = this.registry.counter("slowQueryLog.numAnalyzed");
    this.numInserted = this.registry.counter("slowQueryLog.numInserted");
    this.numParsed = this.registry.counter("slowQueryLog.numParsed");
  }

  @Override
  protected void process(Reader reader)
      throws IOException, MetricAgentException {

    SlowQueryLogParser parser = new SlowQueryLogParser();
    List<UserQuery> queries = parser.parseLog(
        new RewindBufferedReader(reader)
    );

    logger.info("Parsed " + queries.size());
    numParsed.inc(queries.size());
    this.queries.addAll(queries);
  }

  @Override
  protected void filter(ZonedDateTime start, ZonedDateTime end) {
    this.queries = this.queries.stream()
        .filter(query -> query.getZonedLogTime().isAfter(start)
            && query.getZonedLogTime().isBefore(end))
        .collect(Collectors.toList());
  }

  @Override
  protected void output(OutputStream os) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(os, queries);
  }

  @Override
  protected void outputSql(Sink sink, Namespace namespace) {
    sink.useTransaction(handle ->
        this.queries.forEach(q -> {
          int id = sink.insertUserQuery(handle, q);
          numInserted.inc();
        })
    );
  }
}
