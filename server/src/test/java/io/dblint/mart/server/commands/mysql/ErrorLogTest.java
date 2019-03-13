package io.dblint.mart.server.commands.mysql;

import io.dblint.mart.server.MartApplication;
import io.dblint.mart.server.MartConfiguration;
import io.dblint.mart.server.commands.MySqlCommands;
import io.dropwizard.cli.Cli;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.util.JarLocation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ErrorLogTest {
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private final InputStream originalIn = System.in;

  private final ByteArrayOutputStream stdOut = new ByteArrayOutputStream();
  private final ByteArrayOutputStream stdErr = new ByteArrayOutputStream();
  private Cli cli;

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

  @Test
  void testPositive(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("deadlocks.json");
    String input = getClass().getClassLoader().getResource("errorlog_01").getPath();
    final boolean success = cli.run( "mysql", "errorlog", "--log",
        input, "--output", output.toString());

    assertTrue(success);
  }
}
