package io.dblint.mart.analyses.mysql;

import javax.xml.stream.XMLStreamException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.mysql.SchemaParser;
import io.dblint.mart.metricsink.mysql.SlowQueryLogParser;
import io.dblint.mart.metricsink.mysql.UserQuery;
import io.dblint.mart.sqlplanner.QanException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlowQueryTest {
  private static Logger logger = LoggerFactory.getLogger(SlowQueryTest.class);
}
