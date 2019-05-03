package io.dblint.mart.metricsink.mysql;

import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.util.DbSink;
import io.dblint.mart.metricsink.util.MetricAgentException;
import org.flywaydb.core.Flyway;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.mapper.reflect.BeanMapper;
import org.jdbi.v3.core.mapper.reflect.ConstructorMapper;
import org.jdbi.v3.core.mapper.reflect.FieldMapper;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

public class Sink extends DbSink {
  private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("Y-MM-dd HH:mm:ss");

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
  protected void registerMappers(Handle handle) {
    handle.registerRowMapper(FieldMapper.factory(UserQuery.class));
    handle.registerRowMapper(ConstructorMapper.factory(QueryAttribute.class));
    handle.registerRowMapper(BeanMapper.factory(Transaction.class));
    handle.registerRowMapper(BeanMapper.factory(LongTxnParser.LongTxn.class));
    handle.registerRowMapper(BeanMapper.factory(InnodbLockWait.class));
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
  public List<UserQuery> selectUserQueries(LocalDateTime start, LocalDateTime end) {
    return jdbi.withHandle(handle -> {
      handle.registerRowMapper(BeanMapper.factory(UserQuery.class));
      return handle.createQuery("select * from user_queries where log_time "
              + "between :start and :end and digest_hash is null")
          .bind("start", start.format(formatter))
          .bind("end", end.format(formatter))
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
      handle.registerRowMapper(BeanMapper.factory(UserQuery.class));
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
  public int insertUserQuery(Handle handle, UserQuery userQuery) {
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
        .mapTo(int.class)
        .findOnly();
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
      Handle handle,
      UserQuery userQuery,
      QueryAttribute queryAttribute) {
    Optional<Long> attributeOptional = Optional.empty();

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
  }

  /**
   * Insert a transaction into a database.
   * @param handle JDBI Handle that manages the connection to the database
   * @param transaction Transaction to be inserted
   */
  public long insertTransaction(Handle handle, Transaction transaction) {
    return handle.createUpdate("insert into transactions("
        + "database_id,"
        + "thread,"
        + "query,"
        + "start_time,"
        + "wait_start_time,"
        + "lock_mode,"
        + "lock_type,"
        + "lock_table,"
        + "lock_index,"
        + "lock_data) values (:databaseId, :thread, :query, :startTime, :waitStartTime,"
        + ":lockMode, :lockType, :lockTable, :lockIndex, :lockData)")
        .bindBean(transaction)
        .executeAndReturnGeneratedKeys()
        .mapTo(long.class)
        .findOnly();
  }

  /**
   * Get a @Transaction object from a database.
   * @param handle Connection to the database managed by JDBI
   * @param id ID of the transaction
   * @return Returns an Optional with the Transaction object
   */
  public Optional<Transaction> getTransaction(Handle handle, long id) {
    return handle.createQuery("select * from transactions "
        + "where id = :id")
        .bind("id", id)
        .mapTo(Transaction.class)
        .findFirst();
  }

  /**
   * Get a @Transaction object from a database.
   * @param handle Connection to the database managed by JDBI
   * @param transaction  Transaction object with database id and start time
   * @return Returns an Optional with the Transaction object
   */
  Optional<Transaction> getTransaction(Handle handle, Transaction transaction) {
    return handle.createQuery("select * from transactions "
        + "where database_id = :databaseId and start_time = :startTime")
        .bind("databaseId", transaction.databaseId)
        .bind("startTime", transaction.getStartTime())
        .mapTo(Transaction.class)
        .findFirst();
  }

  /**
   * Insert Or Get a transaction depending on if it exists in the database.
   * @param handle Connection to the database managed by JDBI
   * @param transaction  Transaction object with database id and start time
   * @return An inserted transaction with primary key filled in
   * @throws MetricAgentException Throw an exception if a transaction is not found unexpectedly
   */
  public Transaction insertOrGetTransaction(Handle handle, Transaction transaction)
      throws MetricAgentException {
    Optional<Transaction> optional;
    if (!getTransaction(handle, transaction).isPresent()) {
      long tid = insertTransaction(handle, transaction);
      optional = getTransaction(handle, tid);
    } else {
      optional = getTransaction(handle, transaction);
    }
    if (!optional.isPresent()) {
      throw new MetricAgentException("Failed to insert Transaction for LongTxn");
    }
    return optional.get();
  }

  /**
   * Insert a LongTxn object to the database.
   * @param handle Connection to the database managed by JDBI
   * @param longTxn Object to be stored
   * @return Returns an ID to the new row
   */
  public long insertLongTxn(Handle handle, LongTxnParser.LongTxn longTxn) {
    return handle.createUpdate("insert into long_txns("
        + "log_time,"
        + "transaction_id,"
        + "database_id) values (:logTime, :transactionId, :databaseId)")
        .bindBean(longTxn)
        .executeAndReturnGeneratedKeys()
        .mapTo(long.class)
        .findOnly();
  }

  /**
   * Insert a LockWait object to the database.
   * @param handle Connection to the database managed by JDBI
   * @param lockWait Object to be stored
   * @return Returns an ID to the new row
   */
  public long insertLockWait(Handle handle, InnodbLockWait lockWait) {
    return handle.createUpdate("insert into lock_waits("
        + "log_time,"
        + "waiting_id,"
        + "blocking_id,"
        + "waiting_database_id,"
        + "blocking_database_id)"
        + " values (:logTime, :waitingId, :blockingId, :waitingDatabaseId, :blockingDatabaseId)")
        .bindBean(lockWait)
        .executeAndReturnGeneratedKeys()
        .mapTo(long.class)
        .findOnly();
  }
}
