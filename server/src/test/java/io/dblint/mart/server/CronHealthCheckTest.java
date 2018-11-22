package io.dblint.mart.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

class CronHealthCheckTest {

  @Test
  void zeroIterationsTest() {
    Cron cron = mock(QueryStatsCron.class);
    when(cron.getIterations()).thenReturn(0L);
    when(cron.getFailedIterations()).thenReturn(0L);

    CronHealthCheck cronHealthCheck = new CronHealthCheck(cron);
    assertTrue(cronHealthCheck.check().isHealthy());
  }

  @Test
  void checkAllSucceededTest() {
    Cron cron = mock(QueryStatsCron.class);
    when(cron.getIterations()).thenReturn(100L);
    when(cron.getFailedIterations()).thenReturn(0L);

    CronHealthCheck cronHealthCheck = new CronHealthCheck(cron);
    assertTrue(cronHealthCheck.check().isHealthy());
  }

  @Test
  void fewFailedTest() {
    Cron cron = mock(QueryStatsCron.class);
    when(cron.getIterations()).thenReturn(100L);
    when(cron.getFailedIterations()).thenReturn(8L);

    CronHealthCheck cronHealthCheck = new CronHealthCheck(cron);
    assertTrue(cronHealthCheck.check().isHealthy());
  }

  @Test
  void failedAtLimitTest() {
    Cron cron = mock(QueryStatsCron.class);
    when(cron.getIterations()).thenReturn(100L);
    when(cron.getFailedIterations()).thenReturn(10L);

    CronHealthCheck cronHealthCheck = new CronHealthCheck(cron);
    assertTrue(cronHealthCheck.check().isHealthy());
  }

  @Test
  void failedOverLimitTest() {
    Cron cron = mock(QueryStatsCron.class);
    when(cron.getIterations()).thenReturn(100L);
    when(cron.getFailedIterations()).thenReturn(11L);

    CronHealthCheck cronHealthCheck = new CronHealthCheck(cron);
    assertFalse(cronHealthCheck.check().isHealthy());
    assertEquals("Failed Percentage is 0.110000", cronHealthCheck.check().getMessage());
  }

  @Test
  void customLimitSucceededTest() {
    Cron cron = mock(QueryStatsCron.class);
    when(cron.getIterations()).thenReturn(100L);
    when(cron.getFailedIterations()).thenReturn(11L);

    CronHealthCheck cronHealthCheck = new CronHealthCheck(cron, 0.15);
    assertTrue(cronHealthCheck.check().isHealthy());
  }

  @Test
  void failedAtCustomTest() {
    Cron cron = mock(QueryStatsCron.class);
    when(cron.getIterations()).thenReturn(100L);
    when(cron.getFailedIterations()).thenReturn(15L);

    CronHealthCheck cronHealthCheck = new CronHealthCheck(cron, 0.15);
    assertTrue(cronHealthCheck.check().isHealthy());
  }

  @Test
  void failedOverCustomLimitTest() {
    Cron cron = mock(QueryStatsCron.class);
    when(cron.getIterations()).thenReturn(100L);
    when(cron.getFailedIterations()).thenReturn(16L);

    CronHealthCheck cronHealthCheck = new CronHealthCheck(cron);
    assertFalse(cronHealthCheck.check().isHealthy());
    assertEquals("Failed Percentage is 0.160000", cronHealthCheck.check().getMessage());
  }

}