package io.dblint.mart.metricsink.redshift;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SplitUserQuery implements Jdbi {
  public final int queryId;
  public final int sequence;
  public final int userId;
  public final String startTime;
  public final String endTime;
  public final long duration;
  public final String db;
  public final String query;

  /**
   * Construct a UserQuery Row.
   * @param queryId Query ID
   * @param sequence Sequence of the query fragment
   * @param userId ID of user who submitted the query
   * @param startTime Start time of the query
   * @param endTime End time of the the query
   * @param duration Duration of the query (secs)
   * @param db Redshift database
   * @param query SQL text
   */
  @JsonCreator
  public SplitUserQuery(@JsonProperty("query_id") int queryId,
                        @JsonProperty("sequence") int sequence,
                        @JsonProperty("user_id") int userId,
                        @JsonProperty("start_time") String startTime,
                        @JsonProperty("end_time") String endTime,
                        @JsonProperty("duration") long duration,
                        @JsonProperty("db") String db,
                        @JsonProperty("query") String query) {
    this.queryId = queryId;
    this.userId = userId;
    this.sequence = sequence;
    this.startTime = startTime;
    this.endTime = endTime;
    this.duration = duration;
    this.db = db;
    this.query = query;
  }

  private static String extractQuery = "SELECT "
      + "q.query, qs.sequence, q.userid,  q.starttime,  q.endtime, "
      + "DATEDIFF(milliseconds, starttime, endtime)/1000.0 AS duration,"
      + " TRIM(database) AS database, qs.text"
      + " FROM"
      + " stl_query q JOIN stl_querytext qs ON (q.query = qs.query)"
      + " WHERE"
      + " endtime between '%s' and '%s'"
      + " ORDER BY starttime;";

  @Override
  public String toString() {
    return "SplitUserQuery{"
        + "queryId=" + queryId
        + ", sequence=" + sequence
        + ", userId=" + userId
        + ", startTime='" + startTime + '\''
        + ", endTime='" + endTime + '\''
        + ", duration=" + duration
        + ", db='" + db + '\''
        + ", query='" + query + '\''
        + '}';
  }
}
