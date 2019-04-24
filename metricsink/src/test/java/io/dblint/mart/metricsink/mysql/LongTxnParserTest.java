package io.dblint.mart.metricsink.mysql;

import io.dblint.mart.metricsink.util.MetricAgentException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LongTxnParserTest {
  @Test
  void parseEmptyRow() throws IOException, MetricAgentException {
     RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
         this.getClass().getClassLoader().getResourceAsStream("longtxns/empty_01")));
     assertNotNull(reader);

    List<LongTxnParser.LongTxn> longTxns = new LongTxnParser().parse(reader);
    assertTrue(longTxns.isEmpty());
  }

  @Test
  void parseTwoEmptyRow() throws IOException, MetricAgentException {
     RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
         this.getClass().getClassLoader().getResourceAsStream("longtxns/empty_02")));
     assertNotNull(reader);

    List<LongTxnParser.LongTxn> longTxns = new LongTxnParser().parse(reader);
    assertTrue(longTxns.isEmpty());
  }

  @Test
  void parseRow() throws IOException, MetricAgentException {
    RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("longtxns/row_01")));
    assertNotNull(reader);
    LongTxnParser.LongTxn txn = new LongTxnParser().parseRow(reader,
        ZonedDateTime.of(LocalDateTime.of(2019, 3, 15, 1, 0, 0),
            ZoneOffset.ofHoursMinutes(5, 30)));

    assertEquals("142437810275", txn.transaction.id);
    assertEquals(ZonedDateTime.of(LocalDateTime.of(2019, 4, 24, 7, 1, 5),
        ZoneOffset.UTC), txn.transaction.startTime);
    assertEquals("21323342", txn.transaction.thread);
  }

  @Test
  void parseOneSnapshot() throws IOException, MetricAgentException {
     RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
         this.getClass().getClassLoader().getResourceAsStream("longtxns/snapshot_01")));
     assertNotNull(reader);

    List<LongTxnParser.LongTxn> longTxns = new LongTxnParser().parse(reader);
    assertEquals(2, longTxns.size());
  }

  @Test
  void parseTwoSnapshot() throws IOException, MetricAgentException {
     RewindBufferedReader reader = new RewindBufferedReader(new InputStreamReader(
         this.getClass().getClassLoader().getResourceAsStream("longtxns/snapshot_02")));
     assertNotNull(reader);

    List<LongTxnParser.LongTxn> longTxns = new LongTxnParser().parse(reader);
    assertEquals(3, longTxns.size());
  }

}
