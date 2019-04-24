package io.dblint.mart.server.commands.mysql;

import com.codahale.metrics.Counter;
import io.dblint.mart.metricsink.mysql.Logged;
import io.dblint.mart.metricsink.mysql.RewindBufferedReader;
import io.dblint.mart.metricsink.mysql.Sink;
import io.dblint.mart.metricsink.util.MetricAgentException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.jdbi.v3.core.Handle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

abstract class LogParser<ParserT, LoggedT extends Logged> extends TimeRange {
  private static Logger logger = LoggerFactory.getLogger(LogParser.class);

  protected List<LoggedT> list;
  protected ParserT parserT;
  protected Counter numParsed;
  protected Counter numInserted;

  LogParser(String name, String description) {
    super(name, description);
    this.list = new ArrayList<>();
    this.numParsed = this.registry.counter("logParser.numParsed");
    this.numInserted = this.registry.counter("logParser.numInserted");
  }

  @Override
  public void configure(Subparser subparser) {
    super.configure(subparser);
    MutuallyExclusiveGroup group = subparser.addMutuallyExclusiveGroup("input")
        .required(true);
    group.addArgument("-l", "--log")
        .metavar("log")
        .type(String.class)
        .help("Path to Error Log");

    group.addArgument("-d", "--log-dir")
        .type(String.class)
        .help("Path to Error Log directory");

    subparser.addArgument("-o", "--output")
        .metavar("output")
        .type(String.class)
        .help("Path to output file");

    subparser.addArgument("--output-type")
        .metavar("outputType")
        .type(String.class)
        .choices("json", "sqlite")
        .setDefault("sqlite")
        .help("Output Type");
  }

  abstract List<LoggedT> parse(RewindBufferedReader reader)
      throws IOException, MetricAgentException;

  protected void filter(ZonedDateTime start, ZonedDateTime end) {
    this.list = this.list.stream()
        .filter(lt -> lt.getZonedLogTime().isAfter(start) && lt.getZonedLogTime().isBefore(end))
        .collect(Collectors.toList());
  }

  @Override
  public void run(Namespace namespace)
      throws IOException, MetricAgentException {
    logger.debug(namespace.toString());
    if (namespace.getString("log") != null) {
      logger.info(namespace.getString("log"));
      List<LoggedT> newItems = this.parse(new RewindBufferedReader(
          new FileReader(namespace.getString("log"))));
      logger.info("Parsed " + newItems.size());
      numParsed.inc(newItems.size());
      this.list.addAll(newItems);
    } else {
      logger.info(namespace.getString("log_dir"));
      File folder = new File(namespace.getString("log_dir"));
      for (File f : folder.listFiles()) {
        logger.info("Processing " + f.getName());
        try {
          List<LoggedT> newItems = this.parse(new RewindBufferedReader(new FileReader(f)));
          logger.info("Parsed " + newItems.size());
          numParsed.inc(newItems.size());
          this.list.addAll(newItems);
        } catch (MetricAgentException me) {
          logger.error("Failed to parse " + f.getName(), me);
        }
      }
    }

    String startTime = namespace.getString("startTime");
    String endTime = namespace.getString("endTime");

    if (startTime != null && endTime != null) {
      filter(ZonedDateTime.of(LocalDateTime.parse(startTime, dateFormat),
          ZoneOffset.ofHoursMinutes(5, 30)),
          ZonedDateTime.of(LocalDateTime.parse(endTime, dateFormat),
              ZoneOffset.ofHoursMinutes(5, 30)));
    }

    if (namespace.getString("output_type").equals("sqlite")) {
      logger.info("Insert queries into database " + namespace.getString("output"));
      Sink sink = new Sink("jdbc:sqlite:" + namespace.getString("output"), "", "", this.registry);
      sink.initialize();
      sink.useTransaction(handle ->
          this.list.forEach(item -> {
            try {
              outputSql(sink, handle, item);
              numInserted.inc();
            } catch (IOException exp) {
              logger.error("Insert failed", exp);
            }
          }));
    } else {
      output(new FileOutputStream(namespace.getString("output")));
    }
    super.logRegistry();
  }

  abstract void output(OutputStream os) throws IOException;

  abstract void outputSql(Sink sink, Handle handle, LoggedT item) throws IOException;
}
