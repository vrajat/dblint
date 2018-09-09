package io.inviscid.qan.visitors;

import org.apache.calcite.sql.util.SqlBasicVisitor;

/**
 * Created by rvenkatesh on 9/9/18.
 */
public abstract class ClassifyingVisitor extends SqlBasicVisitor<Void> {
  protected boolean passed;

  ClassifyingVisitor(boolean passed) {
    this.passed = passed;
  }

  public boolean isPassed() {
    return passed;
  }
}