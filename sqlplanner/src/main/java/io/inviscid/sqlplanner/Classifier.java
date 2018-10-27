package io.inviscid.sqlplanner;

import io.inviscid.sqlplanner.enums.EnumContext;
import io.inviscid.sqlplanner.enums.QueryType;

import java.util.List;

import io.inviscid.sqlplanner.planner.Parser;
import org.apache.calcite.sql.parser.SqlParseException;

public abstract class Classifier {
  public final Parser parser;

  Classifier() {
    this.parser = new Parser();
  }

  public abstract List<QueryType> classify(String sql, EnumContext context)
      throws SqlParseException, QanException;


}
