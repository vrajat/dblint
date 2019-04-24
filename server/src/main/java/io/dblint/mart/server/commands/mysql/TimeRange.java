package io.dblint.mart.server.commands.mysql;

import io.dblint.mart.server.commands.Command;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;

public abstract class TimeRange extends Command {
  private static Logger logger = LoggerFactory.getLogger(TimeRange.class);

  protected static DateTimeFormatter dateFormat =
      DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss");

  TimeRange(String name, String description) {
    super(name, description);
  }

  /**
   * Configure the argparser4J.
   * @param subparser subparser object
   */
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

  void logRegistry() {
    registry.getCounters().forEach((name, counter) -> {
      logger.info(name + ":" + counter.getCount());
    });
  }
}
