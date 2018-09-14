package io.inviscid.qan;

import io.inviscid.qan.enums.AnalyticsEnum;
import io.inviscid.qan.enums.QueryType;

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
    List<QueryType> typeList = new ArrayList<>();
    SqlNode parseTree = parser.parse(sql);
    for (AnalyticsEnum analyticsEnum: AnalyticsEnum.values()) {
      if (analyticsEnum.isPassed(parseTree)) {
        typeList.add(analyticsEnum);
      }
    }

    return typeList;
  }
}
