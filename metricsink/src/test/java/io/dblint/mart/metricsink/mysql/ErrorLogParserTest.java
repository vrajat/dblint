package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;
import org.junit.jupiter.api.Test;

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
  void testTransaction() throws MetricAgentException {
    Deadlock.Transaction transaction = new Deadlock.Transaction();
    transaction.setId("TRANSACTION 261737481082, ACTIVE 0 sec fetching rows");
    assertEquals("261737481082", transaction.id);
  }

  @Test
  void testLock() throws MetricAgentException {
    Deadlock.Lock lock = new Deadlock.Lock("RECORD LOCKS space id 12042 page no 63840 n bits 200 "
        + "index `PRIMARY` of table `schema`.`tablename` trx id 261737481082 "
        + "lock_mode X locks rec but not gap waiting");
    assertEquals("12042", lock.spaceId);
    assertEquals("63840", lock.pageNo);
    assertEquals("200", lock.numBits);
    assertEquals("PRIMARY", lock.index);
    assertEquals("schema", lock.schema);
    assertEquals("tablename", lock.table);
    assertEquals("261737481082", lock.id);
  }
}