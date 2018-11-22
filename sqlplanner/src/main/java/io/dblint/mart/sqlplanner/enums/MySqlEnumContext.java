package io.dblint.mart.sqlplanner.enums;

import io.dblint.mart.sqlplanner.visitors.MySqlIndexVisitor;

import java.util.Set;

public class MySqlEnumContext extends EnumContext {
  Set<MySqlIndexVisitor.Index> indices;

  public Set<MySqlIndexVisitor.Index> getIndices() {
    return indices;
  }

  public void setIndices(Set<MySqlIndexVisitor.Index> indices) {
    this.indices = indices;
  }
}
