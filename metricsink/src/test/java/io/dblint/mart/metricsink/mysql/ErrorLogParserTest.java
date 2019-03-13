package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ErrorLogParserTest {
  @Test
  void positiveDeadLockSectionStart() {
    assertTrue(ErrorLogParser.newDeadlockSection(
        "InnoDB: transactions deadlock detected, dumping detailed information."));
  }

  @Test
  void timestampDeadLockSectionStart() {
    assertTrue(ErrorLogParser.newDeadlockSection(
        "2019-02-25 07:58:01 2b91d350d700InnoDB: transactions deadlock detected, "
            + "dumping detailed information."));
  }

  @Test
  void testRecordLock() throws IOException {
    RewindBufferedReader bufferedReader = new RewindBufferedReader(
        new StringReader(
            "Record lock, heap no 163 PHYSICAL RECORD: n_fields 3; compact format; info bits 32\n"
        )
    );
    assertTrue(ErrorLogParser.parseRecordLock(bufferedReader));
  }

  @Test
  void negativeDeadlockSectionStart() {
    assertFalse(ErrorLogParser.newDeadlockSection("blah"));
  }

  static Stream<Arguments> transactionProvider() {
    return Stream.of(
        Arguments.arguments("transaction_01", "261737481082", 1, 0),
        Arguments.arguments("transaction_02", "261737481145", 1, 1),
        Arguments.arguments("transaction_03", "261775914978", 0, 1)
    );
  }

  @ParameterizedTest
  @MethodSource("transactionProvider")
  void testTransaction(String file, String transactionId, int expectedWaiting, int expectedHolding)
      throws IOException, MetricAgentException {
    RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("error_logs/" + file)));
    assertNotNull(reader);

    Deadlock.Transaction transaction = ErrorLogParser.parseTransaction(reader);
    assertEquals(transactionId, transaction.id);
    assertEquals(expectedWaiting, transaction.waitingLocks.size());
    assertEquals(expectedHolding, transaction.holdingLocks.size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"lock_01", "lock_02"})
  void testLock(String file) throws IOException, MetricAgentException {
    RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("error_logs/" + file)));

    assertNotNull(reader);

    Deadlock.Lock lock = ErrorLogParser.parseLock(reader);
    assertEquals("12042", lock.spaceId);
    assertEquals("63840", lock.pageNo);
    assertEquals("200", lock.numBits);
    assertEquals("PRIMARY", lock.index);
    assertEquals("schema", lock.schema);
    assertEquals("tablename", lock.table);
    assertEquals("261737481427", lock.id);
    assertEquals("X", lock.lockType);
  }

  @Test
  void testCustomIndex() throws IOException, MetricAgentException {
    RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("error_logs/lock_03")));

    assertNotNull(reader);

    Deadlock.Lock lock = ErrorLogParser.parseLock(reader);
    assertEquals("12042", lock.spaceId);
    assertEquals("59420", lock.pageNo);
    assertEquals("1192", lock.numBits);
    assertEquals("table_f67c5d8", lock.index);
    assertEquals("schema", lock.schema);
    assertEquals("table", lock.table);
    assertEquals("261737481145", lock.id);
    assertEquals("X", lock.lockType);
  }

  @Test
  void testSLock() throws IOException, MetricAgentException {
    RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("error_logs/lock_04")));

    assertNotNull(reader);

    Deadlock.Lock lock = ErrorLogParser.parseLock(reader);
    assertEquals("11953", lock.spaceId);
    assertEquals("323928", lock.pageNo);
    assertEquals("1272", lock.numBits);
    assertEquals("order_id", lock.index);
    assertEquals("schema", lock.schema);
    assertEquals("table", lock.table);
    assertEquals("264357785633", lock.id);
    assertEquals("S", lock.lockType);
  }

  @Test
  void testDeadlock() throws IOException, MetricAgentException {
    RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("error_logs/deadlock_01")));
    assertNotNull(reader);

    Deadlock deadlock = ErrorLogParser.parseDeadlock(reader,
        LocalDateTime.of(2018, 3, 8, 9, 39));
    assertEquals(2, deadlock.transactions.size());
  }

  @Test
  void testErrorLog() throws IOException, MetricAgentException {
    RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("error_logs/errorlog_01")));
    assertNotNull(reader);

    List<Deadlock> deadlocks = ErrorLogParser.parse(reader);
    assertEquals(2, deadlocks.size());
  }
}