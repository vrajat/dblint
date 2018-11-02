package com.dblint.metricsink.redshift;

import org.jdbi.v3.core.mapper.reflect.JdbiConstructor;

import java.time.LocalDateTime;


public class UserQuery implements Jdbi {
  public final int queryId;
  public final int userId;
  public final int transactionId;
  public final int pid;
  public final LocalDateTime startTime;
  public final LocalDateTime endTime;
  public final long duration;
  public final String db;
  public final boolean aborted;
  public final String query;

  /**
   * Construct a UserQuery Row.
   * @param queryId Query ID
   * @param userId ID of user who submitted the query
   * @param transactionId Transaction ID of the query
   * @param pid PID of process
   * @param startTime Start time of the query
   * @param endTime End time of the the query
   * @param duration Duration of the query (secs)
   * @param db Redshift database
   * @param aborted Whether aborted or not
   * @param query SQL text
   */
  @JdbiConstructor
  public UserQuery(
      int queryId, int userId, int transactionId, int pid, LocalDateTime startTime,
      LocalDateTime endTime, long duration, String db, boolean aborted, String query) {
    this.queryId = queryId;
    this.userId = userId;
    this.transactionId = transactionId;
    this.pid = pid;
    this.startTime = startTime;
    this.endTime = endTime;
    this.duration = duration;
    this.db = db;
    this.aborted = aborted;
    this.query = query;
  }

  static String getExtractQuery(LocalDateTime rangeStart, LocalDateTime rangeEnd) {
    return String.format(
        extractQuery,
        rangeStart.format(dateTimeFormatter), rangeEnd.format(dateTimeFormatter)
    );
  }

  private static String extractQuery = "WITH query_sql AS (\n"
      + "  SELECT\n"
      + "    query,\n"
      + "    LISTAGG(text) WITHIN GROUP (ORDER BY sequence) AS query_text\n"
      // + "GROUP_CONCAT(text ORDER BY sequence) AS query_text\n"
      + "  FROM stl_querytext\n"
      + "  GROUP BY query\n"
      + ")\n"
      + "SELECT\n"
      + "  q.query as query_id,\n"
      + "  userid as user_id,\n"
      + "  xid as transaction_id,\n"
      + "  pid,\n"
      + "  starttime as start_time,\n"
      + "  endtime as end_time,\n"
      + "  DATEDIFF(milliseconds, starttime, endtime)/1000.0 AS duration,\n"
      + "  TRIM(database) AS db,\n"
      + "  (CASE aborted WHEN 1 THEN TRUE ELSE FALSE END) AS aborted,\n"
      + "  qs.query_text as query\n"
      + "FROM\n"
      + "  stl_query q JOIN query_sql qs ON (q.query = qs.query)\n"
      + "WHERE\n"
      + " endtime between '%s' and '%s'";

  static String insertQuery = "insert into bad_user_queries(query_id, user_id, transaction_id, "
      + "pid, start_time, end_time, duration, db, aborted, query) "
      + " values (:queryId, :userId, :transactionId, :pid, :startTime, :endTime, :duration, "
      + " :db, :aborted, :query)";
}
