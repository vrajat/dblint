package io.dblint.mart.metricsink.util.strategies;

import org.jdbi.v3.core.statement.StatementContext;

/**
 * Interface for strategies to statement contexts to metric names.
 */
@FunctionalInterface
public interface StatementNameStrategy {
  String getStatementName(StatementContext statementContext);
}
