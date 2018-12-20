package io.dblint.mart.metricsink.redshift;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class RedshiftCsv {

  /**
   * Parse CSV file and map to SplitUserQuery.
   * In SplitUserQuery, query text is not already combined
   * @param inputStream Inputstream of CSV source
   * @return A list of SplitUserQueries
   * @throws IOException Exception thrown if source cannot be read successfully.
   */
  public static List<SplitUserQuery> getQueries(InputStream inputStream) throws IOException {
    CsvMapper mapper = new CsvMapper();
    CsvSchema schema = CsvSchema.emptySchema().withHeader();
    MappingIterator<SplitUserQuery> iterator = mapper.readerFor(SplitUserQuery.class).with(schema)
        .readValues(inputStream);

    List<SplitUserQuery> queries = new ArrayList<>();
    while (iterator.hasNext()) {
      queries.add(iterator.next());
    }

    return queries;
  }
}
