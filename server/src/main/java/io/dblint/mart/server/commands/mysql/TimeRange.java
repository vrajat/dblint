package io.dblint.mart.server.commands.mysql;

import io.dblint.mart.server.MartConfiguration;
import io.dropwizard.cli.ConfiguredCommand;
import net.sourceforge.argparse4j.inf.Subparser;

public abstract class TimeRange extends ConfiguredCommand<MartConfiguration> {
  TimeRange(String name, String description) {
    super(name, description);
  }

  @Override
  public void configure(Subparser subparser) {
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
