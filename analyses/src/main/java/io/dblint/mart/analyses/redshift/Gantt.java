package io.dblint.mart.analyses.redshift;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Gantt {
  private static Logger logger = LoggerFactory.getLogger(Gantt.class);

  static void sort(List<QueryInfo> queries) {
    queries.sort(Comparator.naturalOrder());
    queries.forEach(q -> logger.info(q.classes.insertContext.getTargetTable()
        + "("
        + q.classes.insertContext.getSources().stream().collect(Collectors.joining(","))
        + "),"
        + q.query.startTime + ","
        + q.query.endTime));
  }
}
