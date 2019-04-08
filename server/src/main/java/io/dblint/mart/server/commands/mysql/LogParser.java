package io.dblint.mart.server.commands.mysql;

import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.mysql.Sink;
import io.dblint.mart.metricsink.util.MetricAgentException;
import io.dblint.mart.server.MartConfiguration;
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
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

abstract class LogParser extends TimeRange {
  private static Logger logger = LoggerFactory.getLogger(LogParser.class);

  LogParser(String name, String description) {
    super(name, description);
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

  abstract void process(Reader reader) throws IOException, MetricAgentException;

  abstract void output(OutputStream os) throws IOException;

  abstract void outputSql(Sink sink) throws IOException;

  abstract void filter(ZonedDateTime start, ZonedDateTime end);

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
        try {
          process(new FileReader(f));
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
      Sink sink = new Sink("jdbc:sqlite:" + namespace.getString("output"), "", "", this.registry);
      sink.initialize();
      outputSql(sink);
    } else {
      output(new FileOutputStream(namespace.getString("output")));
    }
    super.logRegistry();
  }
}
