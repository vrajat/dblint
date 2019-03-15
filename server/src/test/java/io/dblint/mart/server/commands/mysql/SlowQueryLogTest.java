package io.dblint.mart.server.commands.mysql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SlowQueryLogTest extends ParserTest {
  @Test
  void testPositive(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("slow_queries.json");
    String input = getClass().getClassLoader().getResource("slowlog_01").getPath();
    final boolean success = cli.run( "mysql", "slowquerylog", "--log",
        input, "--output", output.toString());

    System.out.println(stdErr.toString());
    System.out.println(stdOut.toString());
    assertEquals("", stdErr.toString());
    assertTrue(success);
  }
}
