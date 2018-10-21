package io.inviscid.sqlplanner;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import io.inviscid.sqlplanner.enums.AnalyticsEnum;
import io.inviscid.sqlplanner.enums.QueryType;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.parser.SqlParseException;

import org.junit.jupiter.api.Test;

class AnalyticsClassifierTest {
  @Test
  public void lookupOnlyTest() throws SqlParseException {
    AnalyticsClassifier analyticsClassifier = new AnalyticsClassifier();
    List<QueryType> queryTypeList = analyticsClassifier.classify("select a from b where c = 10");

    List<QueryType> expected = new ArrayList<>();
    expected.add(AnalyticsEnum.LOOKUP);
    assertIterableEquals(expected, queryTypeList);
  }

}