package io.dblint.mart.server.commands;

import io.dblint.mart.server.MartConfiguration;
import io.dropwizard.cli.Command;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.impl.action.HelpArgumentAction;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.util.Map;
import java.util.SortedMap;

public class HierarchicalCommand extends ConfiguredCommand<MartConfiguration> {
  private static final String COMMAND_NAME_ATTR = "subcommand";

  private final SortedMap<String, Command> commands;

  protected HierarchicalCommand(String name, String description, SortedMap<String,
      Command> commands) {
    super(name, description);
    this.commands = commands;

  }

  private void addCommand(Subparser subparser, Command command) {
    commands.put(command.getName(), command);
    subparser.addSubparsers().help("available commands");

    final Subparser commandSubparser =
        subparser.addSubparsers().addParser(command.getName(), false);

    command.configure(commandSubparser);

    commandSubparser.addArgument("-h", "--help")
        .action(new HelpArgumentAction())
        .help("show this help message and exit")
        .setDefault(Arguments.SUPPRESS);

    commandSubparser.description(command.getDescription())
        .setDefault(COMMAND_NAME_ATTR, command.getName())
        .defaultHelp(true);
  }

  @Override
  public void configure(Subparser subparser) {
    for (Map.Entry<String, Command> command : commands.entrySet()) {
      addCommand(subparser, command.getValue());
    }
  }

  @Override
  protected void run(Bootstrap<MartConfiguration> bootstrap, Namespace namespace,
                     MartConfiguration configuration) throws Exception {
    final Command command = commands.get(namespace.getString(COMMAND_NAME_ATTR));
    command.run(bootstrap, namespace);
  }
}
