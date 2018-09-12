package io.inviscid.qan.visitors;

import io.inviscid.qan.Parser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RedshiftTooManyJoinsVisitorTest {
  @Test
  public void elevenJoinTest() throws SqlParseException {
    Parser parser = new Parser();
    SqlNode parseTree = parser.parse("select x from " +
        "a1 join a2 on a1.i1 = a2.i " +
        "join a3 on a1.i2 = a3.i " +
        "join a4 on a1.i3 = a4.i " +
        "join a5 on a1.i4 = a5.i " +
        "join a6 on a1.i5 = a6.i " +
        "join a7 on a1.i6 = a7.i " +
        "join a8 on a1.i7 = a8.i " +
        "join a9 on a1.i8 = a9.i " +
        "join a10 on a1.i9 = a10.i " +
        "join a11 on a1.i10 = a11.i " +
        "join a12 on a1.i11 = a12.i");
    RedshiftTooManyJoinsVisitor redshiftTooManyJoinsVisitor = new RedshiftTooManyJoinsVisitor();
    parseTree.accept(redshiftTooManyJoinsVisitor);
    assertTrue(redshiftTooManyJoinsVisitor.isPassed());
  }

  @Test
  public void tenJoinTest() throws SqlParseException {
    Parser parser = new Parser();
    SqlNode parseTree = parser.parse("select x from " +
        "a1 join a2 on a1.i1 = a2.i " +
        "join a3 on a1.i2 = a3.i " +
        "join a4 on a1.i3 = a4.i " +
        "join a5 on a1.i4 = a5.i " +
        "join a6 on a1.i5 = a6.i " +
        "join a7 on a1.i6 = a7.i " +
        "join a8 on a1.i7 = a8.i " +
        "join a9 on a1.i8 = a9.i " +
        "join a10 on a1.i9 = a10.i " +
        "join a11 on a1.i10 = a11.i");
    RedshiftTooManyJoinsVisitor redshiftTooManyJoinsVisitor = new RedshiftTooManyJoinsVisitor();
    parseTree.accept(redshiftTooManyJoinsVisitor);
    assertFalse(redshiftTooManyJoinsVisitor.isPassed());
  }

  @Test
  public void twoLimitFailTest() throws SqlParseException {
    Parser parser = new Parser();
    SqlNode parseTree = parser.parse("select x from " +
        "a1 join a2 on a1.i1 = a2.i " +
        "join a3 on a1.i2 = a3.i " +
        "join a4 on a1.i3 = a4.i ");

    RedshiftTooManyJoinsVisitor redshiftTooManyJoinsVisitor = new RedshiftTooManyJoinsVisitor(2);
    parseTree.accept(redshiftTooManyJoinsVisitor);
    assertTrue(redshiftTooManyJoinsVisitor.isPassed());
  }

  @Test
  public void twoLimitPassTest() throws SqlParseException {
    Parser parser = new Parser();
    SqlNode parseTree = parser.parse("select x from " +
        "a1 join a2 on a1.i1 = a2.i " +
        "join a3 on a1.i2 = a3.i ");

    RedshiftTooManyJoinsVisitor redshiftTooManyJoinsVisitor = new RedshiftTooManyJoinsVisitor(2);
    parseTree.accept(redshiftTooManyJoinsVisitor);
    assertFalse(redshiftTooManyJoinsVisitor.isPassed());
  }
}
