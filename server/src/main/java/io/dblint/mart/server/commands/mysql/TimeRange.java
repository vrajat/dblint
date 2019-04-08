package io.dblint.mart.server.commands.mysql;

import io.dblint.mart.server.MartConfiguration;
import io.dropwizard.cli.ConfiguredCommand;
import net.sourceforge.argparse4j.inf.Subparser;

import java.time.format.DateTimeFormatter;

public abstract class TimeRange extends ConfiguredCommand<MartConfiguration> {
  protected static DateTimeFormatter dateFormat =
      DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss");

  TimeRange(String name, String description) {
    super(name, description);
  }

  @Override
  public void configure(Subparser subparser) {
    super.configure(subparser);
    subparser.addArgument("-s", "--startTime")
        .metavar("startTime")
        .type(String.class)
        .help("Start Time in YYYY-MM-DD HH:MM:SS");

    subparser.addArgument("-e", "--endTime")
        .metavar("endTime")
        .type(String.class)
        .help("End Time in YYYY-MM-DD HH:MM:SS");
  }
}
