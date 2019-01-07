package io.dblint.mart.analyses.redshift;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Gantt {
  private static Logger logger = LoggerFactory.getLogger(Gantt.class);

  static class Entry {
    public final String target;
    public final String startTime;
    public final String endTime;

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("Y-M-d H:m:s");

    public Entry(String target, LocalDateTime startTime, LocalDateTime endTime) {
      this.target = target;
      this.startTime = startTime.format(formatter);
      this.endTime = endTime.format(formatter);
    }
  }

  static List<Entry> sort(List<QueryInfo> queries) {
    List<Entry> entries = new ArrayList<>();

    queries.sort(Comparator.naturalOrder());
    queries.forEach((query) -> {
      String target = null;
      List<String> sources = null;
      assert (query.classes.insertContext.isPassed()
          || query.classes.ctasContext.isPassed());

      if (query.classes.insertContext.isPassed()) {
        target = query.classes.insertContext.getTargetTable();
        sources = query.classes.insertContext.getSources();
      } else if (query.classes.ctasContext.isPassed()) {
        target = query.classes.ctasContext.getTargetTable();
        sources = query.classes.ctasContext.getSources();
      }
      entries.add(new Entry(
          target
              + "("
              + sources.stream().collect(Collectors.joining(","))
              + "),",
          query.query.startTime,
          query.query.endTime
      ));
    });

    return entries;
  }
}
