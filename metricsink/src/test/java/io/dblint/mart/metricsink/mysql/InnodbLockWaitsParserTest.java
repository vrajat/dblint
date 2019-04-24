package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class InnodbLockWaitsParserTest {
  @Test
  void newSectionTest() throws IOException {
    String lines = "*************************** 1. row ***************************\n"
        + "now(): 2019-03-13 22:02:01\n";

    RewindBufferedReader bufferedReader = new RewindBufferedReader(
        new InputStreamReader(new ByteArrayInputStream(lines.getBytes()))
    );
    assertTrue(InnodbLockWaitsParser.newTimeSection(bufferedReader));
  }

  @Test
  void testTrxStarted() {
    String line =  "     waiting_trx_started: 2019-03-18 02:41:01\n";
    assertTrue(InnodbLockWaitsParser.trxStarted.matcher(line).find());
  }

  @Test
  void parseColumnTest() throws MetricAgentException {
    String line = " waiting_trx_id: 281417201150\n";
    assertEquals(2, new InnodbLockWaitsParser().parseColumn(line).length);
  }

  @Test
  void parseWaitingTransactionTest() throws IOException, MetricAgentException {
    String lines = " waiting_trx_id: 281417201150\n"
        + " waiting_thread: 55762544\n"
        + "  waiting_query: UPDATE `table` SET `arrival_time` = '21:57:27.543000'\n"
        + "     waiting_trx_started: 2019-03-18 02:41:01\n"
        + "waiting_trx_wait_started: 2019-03-18 02:41:01\n"
        + "       waiting_lock_mode: X\n"
        + "       waiting_lock_type: RECORD\n"
        + "      waiting_lock_table: `schema`.`table`\n"
        + "      waiting_lock_index: PRIMARY\n"
        + "       waiting_lock_data: 45\n";
    RewindBufferedReader bufferedReader = new RewindBufferedReader(
        new InputStreamReader(new ByteArrayInputStream(lines.getBytes()))
    );

    Transaction transaction =
        new InnodbLockWaitsParser().parseTransaction(bufferedReader);

    assertEquals("281417201150", transaction.id);
    assertEquals("55762544", transaction.thread);
    assertEquals(" UPDATE `table` SET `arrival_time` = '21:57:27.543000'", transaction.query);
    assertEquals(ZonedDateTime.of(
        LocalDateTime.of(2019, 3, 18, 2, 41, 1), ZoneOffset.UTC), transaction.startTime);
    assertEquals(ZonedDateTime.of(
        LocalDateTime.of(2019, 3, 18, 2, 41, 1), ZoneOffset.UTC), transaction.waitStartTime);
    assertEquals("X", transaction.lockMode);
    assertEquals("RECORD", transaction.lockType);
    assertEquals("`schema`.`table`", transaction.lockTable);
    assertEquals("PRIMARY", transaction.lockIndex);
    assertEquals("45", transaction.lockData);
  }

  @Test
  void parseBlockingTransaction() throws IOException, MetricAgentException {
    String lines = "         blocking_trx_id: 285543495612\n"
        + "         blocking_thread: 62265470\n"
        + "          blocking_query: NULL\n"
        + "    blocking_trx_started: 2019-03-18 02:41:01\n"
        + "      blocking_lock_mode: X\n"
        + "      blocking_lock_type: RECORD\n"
        + "     blocking_lock_table: `schema`.`table`\n"
        + "     blocking_lock_index: PRIMARY\n"
        + "      blocking_lock_data: 45\n";

    RewindBufferedReader bufferedReader = new RewindBufferedReader(
        new InputStreamReader(new ByteArrayInputStream(lines.getBytes()))
    );

    Transaction transaction =
        new InnodbLockWaitsParser().parseTransaction(bufferedReader);

    assertEquals("285543495612", transaction.id);
    assertEquals("62265470", transaction.thread);
    assertEquals(" NULL", transaction.query);
    assertEquals(ZonedDateTime.of(LocalDateTime.of(2019, 3, 18, 2, 41, 1), ZoneOffset.UTC),
        transaction.startTime);
    assertNull(transaction.waitStartTime);
    assertEquals("X", transaction.lockMode);
    assertEquals("RECORD", transaction.lockType);
    assertEquals("`schema`.`table`", transaction.lockTable);
    assertEquals("PRIMARY", transaction.lockIndex);
    assertEquals("45", transaction.lockData);
  }

  @Test
  void parseMultiLineQueryTest() throws IOException, MetricAgentException {
     RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
         this.getClass().getClassLoader().getResourceAsStream("innodb_locks/transaction_02")));
     assertNotNull(reader);

    Transaction transaction =
        new InnodbLockWaitsParser().parseTransaction(reader);

    String[] lines = transaction.query.split("\r\n|\r|\n");
    assertEquals(9, lines.length);
  }

  @Test
  void parseRowTest() throws IOException, MetricAgentException {
    RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("innodb_locks/row_01")));
    assertNotNull(reader);

    InnodbLockWait lockWait = new InnodbLockWaitsParser().parseRow(reader,
        ZonedDateTime.of(LocalDateTime.of(2019, 3, 15, 1, 0, 0),
            ZoneOffset.ofHoursMinutes(5, 30)));

    assertNotNull(lockWait.blocking);
    assertNotNull(lockWait.waiting);
  }

  @Test
  void parseTimeSectionTest() throws IOException, MetricAgentException {
    RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("innodb_locks/timeSection_01")));
    assertNotNull(reader);

    List<InnodbLockWait> waitList = new InnodbLockWaitsParser().parseTimeSection(reader);

    assertEquals(2, waitList.size());
  }
}
