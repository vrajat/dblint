package io.dblint.mart.server.commands.mysql;

import io.dblint.mart.metricsink.mysql.ErrorLogParser;
import io.dblint.mart.server.MartConfiguration;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;

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
  }

  @Override
  protected void run(Bootstrap<MartConfiguration> bootstrap, Namespace namespace,
                     MartConfiguration configuration) throws IOException {
    logger.info(namespace.getString("log"));
    ErrorLogParser.parser(
        new FileInputStream(namespace.getString("log"))
    );
  }
}
