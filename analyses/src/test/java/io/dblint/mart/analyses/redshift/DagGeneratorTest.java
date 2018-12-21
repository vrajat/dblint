package io.dblint.mart.analyses.redshift;

import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.redshift.UserQuery;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class DagGeneratorTest {
  Logger logger = LoggerFactory.getLogger(DagGeneratorTest.class);

  @Disabled
  @Tag("cmdLine")
  @Test
  void cmdLineTest() throws IOException {
    logger.info(System.getProperty("csvFile"));
    InputStream inputStream = new FileInputStream(System.getProperty("csvFile"));
    MetricRegistry registry = new MetricRegistry();

    DagGenerator generator = new DagGenerator(registry);
    List<UserQuery> queryList = generator.generateDag(inputStream);

    logger.info("Number of records: " + queryList.size());
  }
}
