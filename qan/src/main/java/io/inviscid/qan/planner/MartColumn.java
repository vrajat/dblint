package io.inviscid.qan.planner;

public class MartColumn {
  public final String name;
  public final int  type;

  /**
   * Represents a column.
   * @param name Name of the column
   * @param type Type of the column
   */
  public MartColumn(String name, int type) {
    this.name = name.toUpperCase();
    this.type = type;
  }
}
