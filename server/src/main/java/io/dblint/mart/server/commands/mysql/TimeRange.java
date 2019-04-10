package io.dblint.mart.server.commands.mysql;

import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.server.MartConfiguration;
import io.dropwizard.cli.ConfiguredCommand;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;

public abstract class TimeRange extends ConfiguredCommand<MartConfiguration> {
  private static Logger logger = LoggerFactory.getLogger(TimeRange.class);

  protected static DateTimeFormatter dateFormat =
      DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss");

  protected MetricRegistry registry;

  TimeRange(String name, String description) {
    super(name, description);
    registry = new MetricRegistry();
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

  protected void logRegistry() {
    registry.getCounters().forEach((name, counter) -> {
      logger.info(name + ":" + counter.getCount());
    });
  }
}
