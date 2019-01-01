package io.dblint.mart.sqlplanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dblint.mart.sqlplanner.enums.AnalyticsEnum;
import io.dblint.mart.sqlplanner.enums.EnumContext;
import io.dblint.mart.sqlplanner.enums.QueryType;
import io.dblint.mart.sqlplanner.enums.RedshiftEnum;

import java.util.ArrayList;
import java.util.List;

import io.dblint.mart.sqlplanner.redshift.QueryClasses;
import io.dblint.mart.sqlplanner.redshift.RedshiftClassifier;
import io.dblint.mart.sqlplanner.visitors.InsertVisitor;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

class RedshiftClassifierTest {
  @Test
  public void overLimitTest() throws SqlParseException {
    RedshiftClassifier redshiftClassifier = new RedshiftClassifier();

    QueryClasses classes = redshiftClassifier.classify("select x from "
        + "a1 join a2 on a1.i1 = a2.i "
        + "join a3 on a1.i2 = a3.i "
        + "join a4 on a1.i3 = a4.i "
        + "join a5 on a1.i4 = a5.i "
        + "join a6 on a1.i5 = a6.i "
        + "join a7 on a1.i6 = a7.i "
        + "join a8 on a1.i7 = a8.i "
        + "join a9 on a1.i8 = a9.i "
        + "join a10 on a1.i9 = a10.i "
        + "join a11 on a1.i10 = a11.i "
        + "join a12 on a1.i11 = a12.i "
        + "join a13 on a1.i12 = a13.i");
    assertFalse(classes.insertContext.isPassed());
  }

  @Test
  public void underLimitTest() throws SqlParseException {
    RedshiftClassifier redshiftClassifier = new RedshiftClassifier();

    QueryClasses classes = redshiftClassifier.classify(
        "insert into c select * from a join b on a.id = b.id");
    InsertVisitor visitor = classes.insertContext;
    assertTrue(visitor.isPassed());
    assertEquals(visitor.getTargetTable(), "C");

    List<String> expectedSources = new ArrayList<>();
    expectedSources.add("A");
    expectedSources.add("B");

    assertIterableEquals(expectedSources, visitor.getSources());
  }
}