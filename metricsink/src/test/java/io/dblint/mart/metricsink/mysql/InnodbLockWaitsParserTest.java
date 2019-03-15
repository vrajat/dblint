package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
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
  void parseColumnTest() throws MetricAgentException {
    String line = " waiting_trx_id: 281417201150\n";
    assertEquals(2, InnodbLockWaitsParser.parseColumn(line).length);
  }

  @Test
  void parseTransactionTest() throws IOException, MetricAgentException {
    String lines = " waiting_trx_id: 281417201150\n"
        + " waiting_thread: 55762544\n"
        + "  waiting_query: UPDATE `table` SET `arrival_time` = '21:57:27.543000'\n";

    RewindBufferedReader bufferedReader = new RewindBufferedReader(
        new InputStreamReader(new ByteArrayInputStream(lines.getBytes()))
    );

    InnodbLockWait.Transaction transaction =
        InnodbLockWaitsParser.parseTransaction(bufferedReader);

    assertEquals("281417201150", transaction.id);
    assertEquals("55762544", transaction.thread);
    assertEquals(" UPDATE `table` SET `arrival_time` = '21:57:27.543000'", transaction.query);
  }

  @Test
  void parseMultiLineQueryTest() throws IOException, MetricAgentException {
     RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
         this.getClass().getClassLoader().getResourceAsStream("innodb_locks/transaction_02")));
     assertNotNull(reader);

    InnodbLockWait.Transaction transaction =
        InnodbLockWaitsParser.parseTransaction(reader);

    String[] lines = transaction.query.split("\r\n|\r|\n");
    assertEquals(9, lines.length);
  }

  @Test
  void parseRowTest() throws IOException, MetricAgentException {
    RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("innodb_locks/row_01")));
    assertNotNull(reader);

    InnodbLockWait lockWait = InnodbLockWaitsParser.parseRow(reader,
        LocalDateTime.of(2019, 3, 15, 1, 0, 0));

    assertNotNull(lockWait.blocking);
    assertNotNull(lockWait.waiting);
  }

  @Test
  void parseTimeSectionTest() throws IOException, MetricAgentException {
    RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("innodb_locks/timeSection_01")));
    assertNotNull(reader);

    List<InnodbLockWait> waitList = InnodbLockWaitsParser.parseTimeSection(reader);

    assertEquals(2, waitList.size());
  }
}
