package io.inviscid.qan;

import io.inviscid.qan.enums.QueryType;
import io.inviscid.qan.enums.RedshiftEnum;

import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;


public class RedshiftClassifier extends AnalyticsClassifier {
  RedshiftClassifier() {
    super();
  }

  @Override
  public List<QueryType> classify(String sql) throws SqlParseException {
    List<QueryType> typeList = super.classify(sql);
    SqlNode parseTree = parser.parse(sql);
    for (RedshiftEnum redshiftEnum : RedshiftEnum.values()) {
      if (redshiftEnum.isPassed(parseTree)) {
        typeList.add(redshiftEnum);
      }
    }
    return typeList;
  }
}
