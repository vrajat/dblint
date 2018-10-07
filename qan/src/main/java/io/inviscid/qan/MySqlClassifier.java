package io.inviscid.qan;

import io.inviscid.qan.enums.MySqlEnum;
import io.inviscid.qan.enums.QueryType;
import io.inviscid.qan.planner.Planner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import java.util.ArrayList;
import java.util.List;

public class MySqlClassifier extends Classifier {
  Planner planner;

  public MySqlClassifier(SchemaPlus schemaPlus) {
    planner = new Planner(schemaPlus);
  }

  @Override
  public List<QueryType> classify(String sql) throws SqlParseException, QanException {
    List<QueryType> queryTypes = new ArrayList<>();
    try {
      RelNode relNode = planner.optimize(sql);
      for (MySqlEnum sqlEnum : MySqlEnum.values()) {
        if (sqlEnum.isPassed(relNode)) {
          queryTypes.add(sqlEnum);
        }
      }

      return queryTypes;
    } catch (ValidationException | RelConversionException exc) {
      throw new QanException(exc);
    }
  }
}
