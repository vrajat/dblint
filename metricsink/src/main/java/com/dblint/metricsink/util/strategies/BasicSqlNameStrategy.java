package com.dblint.metricsink.util.strategies;

/**
 * Collects metrics by respective SQLObject methods.
 */
public class BasicSqlNameStrategy extends DelegatingStatementNameStrategy {

  public BasicSqlNameStrategy() {
    super(DefaultNameStrategy.CHECK_EMPTY,
                DefaultNameStrategy.SQL_OBJECT);
  }
}
