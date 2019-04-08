package io.dblint.mart.metricsink.mysql;

import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.util.DbSink;
import org.flywaydb.core.Flyway;
import org.jdbi.v3.core.mapper.reflect.BeanMapper;
import org.jdbi.v3.core.mapper.reflect.ConstructorMapper;
import org.jdbi.v3.core.mapper.reflect.FieldMapper;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

public class Sink extends DbSink {

  public Sink(String url, String user, String password,
                   MetricRegistry metricRegistry) {
    super(url, user, password, metricRegistry, new Flyway());
  }

  /**
   * Create a Sink for MySQL metrics.
   *
   * @param url URL of the MySQL Database
   * @param user user of the MySQL Database
   * @param password password of the MySQL database
   * @param metricRegistry MetricRegistry to store JDBI metrics
   * @param flyway Migrations library to setup the MySQL database
   */

  public Sink(String url, String user, String password,
                   MetricRegistry metricRegistry, Flyway flyway) {
    super(url, user, password, metricRegistry, flyway);
  }

  @Override
  protected String getMigrationsPath() {
    return "db/mySqlMigrations";
  }

  /**
   * Get a list of mysql user queries.
   * @param start Start time of range
   * @param end End time of range
   * @return List of UserQuery
   */
  public List<UserQuery> selectUserQueries(ZonedDateTime start, ZonedDateTime end) {
    return jdbi.withHandle(handle -> {
      handle.registerRowMapper(BeanMapper.factory(UserQuery.class));
      return handle.createQuery("select * from user_queries where log_time/1000 "
              + "between :start and :end")
          .bind("start", start.toEpochSecond())
          .bind("end", end.toEpochSecond())
          .mapTo(UserQuery.class)
          .list();
    });
  }

  /**
   * Get a UserQuery by id.
   * @param id Long id of the query
   * @return A UserQuery
   */
  public Optional<UserQuery> selectUserQuery(long id) {
    return jdbi.withHandle(handle -> {
      handle.registerRowMapper(FieldMapper.factory(UserQuery.class));
      return handle.createQuery("select * from user_queries where id = :id")
          .bind("id", id)
          .mapTo(UserQuery.class)
          .findFirst();
    });
  }

  /**
   * Insert one UserQuery row into user_queries table.
   * @param userQuery A POJO of User Query
   */
  public long insertUserQuery(UserQuery userQuery) {
    return jdbi.withHandle(handle -> {
      handle.registerRowMapper(FieldMapper.factory(UserQuery.class));
      return handle.createUpdate("insert into user_queries("
          + "log_time,"
          + "user_host,"
          + "ip_address,"
          + "connection_id,"
          + "query_time,"
          + "lock_time,"
          + "rows_sent,"
          + "rows_examined,"
          + "query,"
          + "digest_hash"
          + ") values ("
          + ":logTime, :userHost, :ipAddress, :connectionId, :queryTime, :lockTime, :rowsSent, "
          + ":rowsExamined, :query, :digestHash)")
          .bindBean(userQuery)
          .executeAndReturnGeneratedKeys()
          .mapTo(long.class)
          .findOnly();
    });
  }

  /**
   * Update one UserQuery record.
   * @param userQuery A POJO of user query
   */
  public void updateUserQuery(UserQuery userQuery) {
    jdbi.useHandle(handle -> {
      handle.registerRowMapper(FieldMapper.factory(UserQuery.class));
      handle.createUpdate("update user_queries set "
          + "log_time=:logTime,"
          + "user_host=:userHost,"
          + "ip_address=:ipAddress,"
          + "connection_id=:connectionId,"
          + "query_time=:queryTime,"
          + "lock_time=:lockTime,"
          + "rows_sent=:rowsSent,"
          + "rows_examined=:rowsExamined,"
          + "query=:query,"
          + "digest_hash=:digestHash"
          + " where id=:id")
          .bindBean(userQuery)
          .execute();
    });
  }

  /**
   * Insert one row of QueryAttribute to query_attributes.
   * @param queryAttribute The POJO to insert
   */
  public Optional<Long> setQueryAttribute(
      UserQuery userQuery,
      QueryAttribute queryAttribute) {
    return jdbi.inTransaction(handle -> {
      Optional<Long> attributeOptional = Optional.empty();

      handle.registerRowMapper(FieldMapper.factory(UserQuery.class));
      handle.registerRowMapper(ConstructorMapper.factory(QueryAttribute.class));

      Optional<QueryAttribute> attribute = handle.createQuery("select * from query_attributes "
            + "where digest_hash = :digestHash")
          .bind("digestHash", queryAttribute.digestHash)
          .mapTo(QueryAttribute.class)
          .findFirst();

      if (!attribute.isPresent()) {
        attributeOptional = Optional.of(handle.createUpdate("insert into query_attributes ("
          + "digest, "
          + "digest_hash"
          + ") values ("
          + ":digest, :digestHash)")
          .bindFields(queryAttribute)
          .executeAndReturnGeneratedKeys()
          .mapTo(Long.class)
          .findOnly());
      }
      handle.createUpdate("update user_queries set "
          + "digest_hash=:digestHash"
          + " where id=:id")
          .bind("id", userQuery.getId())
          .bind("digestHash", queryAttribute.digestHash)
          .execute();

      return attributeOptional;
    });
  }
}
