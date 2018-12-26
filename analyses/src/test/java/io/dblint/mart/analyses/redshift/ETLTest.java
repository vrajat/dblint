package io.dblint.mart.analyses.redshift;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ETLTest {
  Logger logger = LoggerFactory.getLogger(ETLTest.class);

  @Disabled
  @Tag("cmdLine")
  @Test
  void cmdLineTest() throws IOException {
    assertNotNull(System.getProperty("csvFile"));
    logger.info(System.getProperty("csvFile"));
    InputStream inputStream = new FileInputStream(System.getProperty("csvFile"));
    MetricRegistry registry = new MetricRegistry();

    Etl etl = new Etl(registry);
    etl.analyze(inputStream);
  }
}
