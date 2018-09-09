package io.inviscid.qan;

import io.inviscid.qan.enums.AnalyticsEnum;
import io.inviscid.qan.enums.QueryType;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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