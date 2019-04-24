package io.dblint.mart.server.commands;

import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.util.MetricAgentException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.IOException;

public abstract class Command {
  protected MetricRegistry registry;
  private final String name;
  private final String description;

  /**
   * A command class for MySQL family.
   * @param name Name of the command
   * @param description Description of the command
   */
  public Command(String name, String description) {
    this.name = name;
    this.description = description;
    registry = new MetricRegistry();
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public abstract void configure(Subparser subparser);

  public abstract void run(Namespace namespace) throws IOException, MetricAgentException;

}
