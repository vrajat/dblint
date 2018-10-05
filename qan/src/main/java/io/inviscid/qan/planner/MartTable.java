package io.inviscid.qan.planner;

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class MartTable extends AbstractTable
    implements QueryableTable, TranslatableTable {

  protected static final Logger LOG = LoggerFactory.getLogger(MartTable.class);

  protected final MartSchema schema;
  protected final String name;
  protected final List<MartColumn> columns;

  /**
   * Create a MartTable that stores information about a table.
   * @param schema Schema of the table.
   * @param name Name of the table.
   * @param columns List of columns of type MartColumn
   */
  public MartTable(MartSchema schema, String name, List<MartColumn> columns) {
    this.schema = schema;
    this.name = name;
    this.columns = columns;
  }

  /**
   * Returns an enumerable over a given projection of the fields.
   * Called from generated code.
   */
  public Enumerable<Object> project(final int[] fields) {
    return new AbstractEnumerable<Object>() {
      public org.apache.calcite.linq4j.Enumerator enumerator() {
        return new MartEnumerator();
      }
    };
  }

  @Override
  public Expression getExpression(SchemaPlus schema, String tableName,
                                  Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  @Override
  public Type getElementType() {
    return Object[].class;
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    // Request all fields.
    return EnumerableTableScan.create(context.getCluster(), relOptTable);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final List<String> names = new ArrayList<>();
    final List<RelDataType> types = new ArrayList<>();
    for (MartColumn col : this.columns) {
      final FieldType fieldType = FieldType.of(col.type);
      if (fieldType == null) {
        LOG.error("Field Type is null for " + col.type);
      }
      final RelDataType type = fieldType.toType((JavaTypeFactory) typeFactory);
      types.add(type);
      names.add(col.name);
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  public List<MartColumn> getColumns() {
    return columns;
  }

  /**
   * Get the ordinal number of column.
   * @param columnName Name of the column
   * @return An integer representing the ordinal number
   */
  public int getFieldOrdinal(String columnName) {
    int count = 0;
    for (MartColumn column : columns) {
      if (columnName.equals(column.name)) {
        return count;
      }
      count++;
    }

    throw new RuntimeException("Column " + columnName + " not found in " + this.toString());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (this.getClass() != obj.getClass()) {
      return false;
    }
    MartTable other = (MartTable) obj;
    return schema.equals(other.schema) && name.equals(other.name) && columns.equals(other.columns);
  }

  @Override
  public int hashCode() {
    return schema.hashCode() + name.hashCode() * 31 + columns.hashCode() * 47;
  }

  class MartEnumerator implements Enumerator<Object> {
    MartEnumerator() {}

    public Object current() {
      return null;
    }

    public boolean moveNext() {
      return false;
    }

    public void reset() {
      throw new UnsupportedOperationException();
    }

    public void close() {
    }
  }
}

