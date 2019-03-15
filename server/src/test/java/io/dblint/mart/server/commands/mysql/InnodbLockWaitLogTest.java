package io.dblint.mart.server.commands.mysql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InnodbLockWaitLogTest extends ParserTest {
  @Test
  void testPositive(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("lockWaits.json");
    String input = getClass().getClassLoader().getResource("innodblwlog_01").getPath();
    final boolean success = cli.run( "mysql", "innodb_lock_waits", "--log",
        input, "--output", output.toString());

    assertEquals("", stdErr.toString());
    assertTrue(success);
  }
}
