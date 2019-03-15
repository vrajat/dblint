package io.dblint.mart.server.commands.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;
import io.dblint.mart.server.MartConfiguration;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

abstract class LogParser extends TimeRange {
  private static Logger logger = LoggerFactory.getLogger(SlowQueryLog.class);
  private static DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss");

  LogParser(String name, String description) {
    super(name, description);
  }

  @Override
  public void configure(Subparser subparser) {
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
  }

  abstract void process(Reader reader) throws IOException, MetricAgentException;

  abstract void output(OutputStream os) throws IOException;

  abstract void filter(LocalDateTime start, LocalDateTime end);

  @Override
  protected void run(Bootstrap<MartConfiguration> bootstrap, Namespace namespace,
                     MartConfiguration configuration)
      throws IOException, MetricAgentException {
    logger.debug(namespace.toString());
    if (namespace.getString("log") != null) {
      logger.info(namespace.getString("log"));
      process(new FileReader(namespace.getString("log")));
    } else {
      logger.info(namespace.getString("log_dir"));
      File folder = new File(namespace.getString("log_dir"));
      for (File f : folder.listFiles()) {
        logger.info("Processing " + f.getName());
        process(new FileReader(f));
      }
    }

    String startTime = namespace.getString("startTime");
    String endTime = namespace.getString("endTime");

    if (startTime != null && endTime != null) {
      filter(LocalDateTime.parse(startTime, dateFormat),
          LocalDateTime.parse(endTime, dateFormat));
    }
    output(new FileOutputStream(namespace.getString("output")));
  }
}
