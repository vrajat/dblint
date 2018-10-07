package io.inviscid.qan.planner;

import java.sql.Types;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.inviscid.qan.QanException;

public class MartColumn {
  private static final Map<Pattern, Integer> typeMap;

  static {
    typeMap = new HashMap<>();
    typeMap.put(Pattern.compile("int(\\(*\\d*\\)*)\\s*(unsigned)*"), Types.INTEGER);
    typeMap.put(Pattern.compile("datetime(\\(*\\d*\\)*)"), Types.DOUBLE);
    typeMap.put(Pattern.compile("varchar(\\(*\\d*\\)*)"), Types.VARCHAR);
    typeMap.put(Pattern.compile("char(\\(*\\d*\\)*)"), Types.VARCHAR);
    typeMap.put(Pattern.compile("(long)*text"), Types.VARCHAR);
    typeMap.put(Pattern.compile("date"), Types.DATE);
    typeMap.put(Pattern.compile("tinyint(\\(*\\d*\\)*)\\s*(unsigned)*"), Types.TINYINT);
    typeMap.put(Pattern.compile("smallint(\\(*\\d*\\)*)\\s*(unsigned)*"), Types.SMALLINT);
    typeMap.put(Pattern.compile("bigint(\\(*\\d*\\)*)\\s*(unsigned)*"), Types.BIGINT);
    typeMap.put(Pattern.compile("time(\\(*\\d*\\)*)"), Types.TIME);
    typeMap.put(Pattern.compile("decimal(\\(*.*?\\)*)"), Types.DECIMAL);
    typeMap.put(Pattern.compile("double"), Types.DOUBLE);
    typeMap.put(Pattern.compile("point"), Types.BLOB);
    typeMap.put(Pattern.compile("polygon"), Types.BLOB);
  }

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

  /**
   * Represents a column.
   * @param name Name of the column
   * @param typeStr Type of the column
   * @throws QanException Throws an exception if type could not be processed.
   */
  public MartColumn(String name, String typeStr) throws QanException {
    this.name = name.toUpperCase();
    this.type = getType(typeStr);
  }

  int getType(String typeStr) throws QanException {
    IntSummaryStatistics statistics = typeMap.entrySet().stream()
        .filter(map -> map.getKey().matcher(typeStr).matches())
        .map(map -> map.getValue())
        .collect(Collectors.summarizingInt(Integer::intValue));

    if (statistics.getCount() > 1) {
      throw new QanException("Internal error. Found more than one match." + statistics.toString());
    } else if (statistics.getCount() == 0) {
      throw new QanException("Type: " + typeStr + " is not supported");
    }
    return statistics.getMax();
  }
}
