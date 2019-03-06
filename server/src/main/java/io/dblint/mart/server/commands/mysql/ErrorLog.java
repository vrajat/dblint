package io.dblint.mart.server.commands.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dblint.mart.metricsink.mysql.Deadlock;
import io.dblint.mart.metricsink.mysql.ErrorLogParser;
import io.dblint.mart.metricsink.mysql.RewindBufferedReader;
import io.dblint.mart.metricsink.util.MetricAgentException;
import io.dblint.mart.server.MartConfiguration;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class ErrorLog extends ConfiguredCommand<MartConfiguration> {
  private static Logger logger = LoggerFactory.getLogger(ErrorLog.class);

  public ErrorLog() {
    super("errorlog", "Analyze Error Log for deadlocks in innodb");
  }

  @Override
  public void configure(Subparser subparser) {
    subparser.addArgument("-l", "--log")
        .metavar("log")
        .type(String.class)
        .help("Path to Error Log");

    subparser.addArgument("-o", "--output")
        .metavar("output")
        .type(String.class)
        .help("Path to output file");
  }

  @Override
  protected void run(Bootstrap<MartConfiguration> bootstrap, Namespace namespace,
                     MartConfiguration configuration) throws IOException, MetricAgentException {
    logger.info(namespace.getString("log"));
    List<Deadlock> deadlocks = ErrorLogParser.parse(
        new RewindBufferedReader(new FileReader(namespace.getString("log")))
    );

    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(new FileOutputStream(namespace.getString("output")), deadlocks);
  }
}
