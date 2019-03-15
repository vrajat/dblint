package io.dblint.mart.server.commands.mysql;

import io.dblint.mart.server.MartApplication;
import io.dblint.mart.server.MartConfiguration;
import io.dblint.mart.server.commands.MySqlCommands;
import io.dropwizard.cli.Cli;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.util.JarLocation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ParserTest {
  protected final PrintStream originalOut = System.out;
  protected final PrintStream originalErr = System.err;
  protected final InputStream originalIn = System.in;

  protected final ByteArrayOutputStream stdOut = new ByteArrayOutputStream();
  protected final ByteArrayOutputStream stdErr = new ByteArrayOutputStream();
  protected Cli cli;

  @BeforeEach
  public void setUp() {
    // Setup necessary mock
    final JarLocation location = mock(JarLocation.class);
    when(location.getVersion()).thenReturn(Optional.of("1.0.0"));

    // Add commands you want to test
    final Bootstrap<MartConfiguration> bootstrap = new Bootstrap<>(new MartApplication());
    bootstrap.addCommand(new MySqlCommands());

    // Redirect stdout and stderr to our byte streams
    System.setOut(new PrintStream(stdOut));
    System.setErr(new PrintStream(stdErr));

    // Build what'll run the command and interpret arguments
    cli = new Cli(location, bootstrap, stdOut, stdErr);
  }

  @AfterEach
  public void teardown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
    System.setIn(originalIn);
  }
}
