package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.junit.jupiter.api.Assertions.*;

class ErrorLogParserTest {
  @Test
  void positiveDeadLockSectionStart() {
    assertTrue(ErrorLogParser.newDeadlockSection(
        "InnoDB: transactions deadlock detected, dumping detailed information."));
  }

  @Test
  void negativeDeadlockSectionStart() {
    assertFalse(ErrorLogParser.newDeadlockSection("blah"));
  }

  @Test
  void testWaitingTransaction() throws IOException, MetricAgentException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("error_logs/transaction_01.log")));
    assertNotNull(reader);

    Deadlock.Transaction transaction = Deadlock.parseTransaction(reader);
    assertEquals("261737481082", transaction.id);
    assertEquals(1, transaction.waitingLocks.size());
    assertTrue(transaction.holdingLocks.isEmpty());
  }

  @Test
  void testHoldingTransaction() throws IOException, MetricAgentException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("error_logs/transaction_02.log")));
    assertNotNull(reader);

    Deadlock.Transaction transaction = Deadlock.parseTransaction(reader);
    assertEquals("261737481145", transaction.id);
    assertEquals(1, transaction.waitingLocks.size());
    assertEquals(1, transaction.holdingLocks.size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"lock_01.log", "lock_02.log"})
  void testLock(String file) throws IOException, MetricAgentException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("error_logs/" + file)));

    assertNotNull(reader);

    Deadlock.Lock lock = Deadlock.parseLock(reader);
    assertEquals("12042", lock.spaceId);
    assertEquals("63840", lock.pageNo);
    assertEquals("200", lock.numBits);
    assertEquals("PRIMARY", lock.index);
    assertEquals("schema", lock.schema);
    assertEquals("tablename", lock.table);
    assertEquals("261737481427", lock.id);
  }

  @Test
  void testCustomIndex() throws IOException, MetricAgentException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("error_logs/lock_03.log")));

    assertNotNull(reader);

    Deadlock.Lock lock = Deadlock.parseLock(reader);
    assertEquals("12042", lock.spaceId);
    assertEquals("59420", lock.pageNo);
    assertEquals("1192", lock.numBits);
    assertEquals("table_f67c5d8", lock.index);
    assertEquals("schema", lock.schema);
    assertEquals("table", lock.table);
    assertEquals("261737481145", lock.id);
  }
}