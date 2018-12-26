package io.dblint.mart.analyses.redshift;

import io.dblint.mart.metricsink.redshift.UserQuery;

import java.util.List;

class QueryInfo implements Comparable<QueryInfo> {
  final UserQuery query;
  final String targetTable;
  final List<String> sources;

  public QueryInfo(UserQuery query, String targetTable, List<String> sources) {
    this.query = query;
    this.targetTable = targetTable;
    this.sources = sources;
  }

  @Override
  public int compareTo(QueryInfo queryInfo) {
    return this.query.startTime.compareTo(queryInfo.query.startTime);
  }
}
