package com.dblint.sqlplanner;

import com.dblint.sqlplanner.enums.EnumContext;
import com.dblint.sqlplanner.enums.QueryType;

import java.util.List;

import com.dblint.sqlplanner.planner.Parser;
import org.apache.calcite.sql.parser.SqlParseException;

public abstract class Classifier {
  public final Parser parser;

  Classifier() {
    this.parser = new Parser();
  }

  public abstract List<QueryType> classify(String sql, EnumContext context)
      throws SqlParseException, QanException;


}
