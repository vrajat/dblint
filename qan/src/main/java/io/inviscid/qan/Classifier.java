package io.inviscid.qan;

import io.inviscid.qan.enums.QueryType;

import java.util.List;

import io.inviscid.qan.planner.Parser;
import org.apache.calcite.sql.parser.SqlParseException;

public abstract class Classifier {
  public final Parser parser;

  Classifier() {
    this.parser = new Parser();
  }

  public abstract List<QueryType> classify(String sql) throws SqlParseException, QanException;


}
