package io.dblint.mart.server.commands.mysql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ErrorLogTest extends ParserTest {
  @Test
  void testPositive(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("deadlocks.json");
    String input = getClass().getClassLoader().getResource("errorlog_01").getPath();
    final boolean success = cli.run( "mysql", "errorlog", "--log",
        input, "--output", output.toString());

    assertTrue(success);
  }
}
