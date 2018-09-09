package io.inviscid.qan;

import io.inviscid.qan.enums.QueryType;
import org.apache.calcite.sql.parser.SqlParseException;

import java.util.List;

public abstract class Classifier {
  public final Parser parser;

  Classifier() {
    this.parser = new Parser();
  }

  public abstract List<QueryType> classify(String sql) throws SqlParseException;


}
