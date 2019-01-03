package io.dblint.mart.metricsink.redshift;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.dblint.mart.metricsink.util.MetricAgentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RedshiftCsv implements Agent {
  private static Logger logger = LoggerFactory.getLogger(RedshiftCsv.class);

  final InputStream inputStream;

  public RedshiftCsv(InputStream is) {
    this.inputStream = is;
  }

  /**
   * Parse CSV file and map to SplitUserQuery.
   * In SplitUserQuery, query text is not already combined
   * @return A list of SplitUserQueries
   * @throws IOException Exception thrown if source cannot be read successfully.
   */
  List<SplitUserQuery> getSplitQueries() throws IOException {
    CsvMapper mapper = new CsvMapper();
    CsvSchema schema = CsvSchema.emptySchema().withHeader();
    MappingIterator<SplitUserQuery> iterator = mapper.readerFor(SplitUserQuery.class).with(schema)
        .readValues(inputStream);

    List<SplitUserQuery> queries = new ArrayList<>();
    while (iterator.hasNext()) {
      queries.add(iterator.next());
    }

    logger.debug("NumSplitQueries: " + queries.size());
    return queries;
  }

  @Override
  public List<UserQuery> getQueries(LocalDateTime rangeStart, LocalDateTime rangeEnd)
      throws MetricAgentException {
    try {
      return combineSplits(getSplitQueries());
    } catch (IOException exc) {
      throw new MetricAgentException(exc);
    }
  }

  private static List<UserQuery> combineSplits(List<SplitUserQuery> splitUserQueries) {
    Map<Integer, List<SplitUserQuery>> groupByQueryId = splitUserQueries.stream().collect(
        Collectors.groupingBy(e -> e.queryId)
    );

    logger.debug("groupByQueryId map size:" + groupByQueryId.size());

    Map<Integer, List<SplitUserQuery>> groupBySortedByQueryId = groupByQueryId.entrySet().stream()
        .collect(
            Collectors.toMap(Map.Entry::getKey,
                e -> e.getValue().stream().sorted().collect(Collectors.toList()))
        );
    logger.debug("groupBySortedByQueryId map size:" + groupBySortedByQueryId.size());

    List<UserQuery> userQueries = new ArrayList<>(groupByQueryId.size());

    groupBySortedByQueryId.forEach((key, value) -> {
      SplitUserQuery splitUserQuery = value.get(0);
      UserQuery userQuery = new UserQuery(
          splitUserQuery.queryId, splitUserQuery.userId, 0, 0,
          splitUserQuery.startTime, splitUserQuery.endTime, splitUserQuery.duration,
          splitUserQuery.db, false, "");

      value.stream().forEach(s -> userQuery.addQueryFragment(s.query.replace("\\n", "\n")));
      userQueries.add(userQuery);
    });

    return userQueries;
  }
}
