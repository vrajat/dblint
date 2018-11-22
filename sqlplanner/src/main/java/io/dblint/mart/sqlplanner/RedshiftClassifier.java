package io.dblint.mart.sqlplanner;

import io.dblint.mart.sqlplanner.enums.QueryType;
import io.dblint.mart.sqlplanner.enums.RedshiftEnum;

import java.util.List;

import org.apache.calcite.sql.SqlNode;

public class RedshiftClassifier extends AnalyticsClassifier {
  public RedshiftClassifier() {
    super();
  }

  @Override
  List<QueryType> classifyImpl(SqlNode parseTree) {
    List<QueryType> typeList = super.classifyImpl(parseTree);
    for (RedshiftEnum redshiftEnum : RedshiftEnum.values()) {
      if (redshiftEnum.isPassed(parseTree)) {
        typeList.add(redshiftEnum);
      }
    }
    return typeList;
  }
}
