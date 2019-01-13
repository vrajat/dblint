package io.dblint.mart.analyses.redshift;

import io.dblint.mart.metricsink.redshift.UserQuery;
import io.dblint.mart.sqlplanner.redshift.QueryClasses;

import java.util.List;

class QueryInfo implements Comparable<QueryInfo> {
  public final UserQuery query;
  final QueryClasses classes;

  public QueryInfo(UserQuery query, QueryClasses classes) {
    this.query = query;
    this.classes = classes;
  }

  @Override
  public int compareTo(QueryInfo queryInfo) {
    return this.query.startTime.compareTo(queryInfo.query.startTime);
  }
}
