package io.inviscid.sqlplanner;

import io.inviscid.sqlplanner.enums.AnalyticsEnum;
import io.inviscid.sqlplanner.enums.QueryType;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

/**
 * Created by rvenkatesh on 9/9/18.
 */
public class AnalyticsClassifier extends Classifier {
  AnalyticsClassifier() {
    super();
  }

  @Override
  public List<QueryType> classify(String sql) throws SqlParseException {
    return classifyImpl(parser.parse(sql));
  }

  List<QueryType> classifyImpl(SqlNode parseTree) {
    List<QueryType> typeList = new ArrayList<>();
    for (AnalyticsEnum analyticsEnum: AnalyticsEnum.values()) {
      if (analyticsEnum.isPassed(parseTree)) {
        typeList.add(analyticsEnum);
      }
    }

    return typeList;
  }
}
