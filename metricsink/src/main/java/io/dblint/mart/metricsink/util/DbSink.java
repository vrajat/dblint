package io.dblint.mart.metricsink.util;

import com.codahale.metrics.MetricRegistry;
import org.flywaydb.core.Flyway;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.HandleCallback;
import org.jdbi.v3.core.HandleConsumer;
import org.jdbi.v3.core.Jdbi;

public abstract class DbSink {
  final String url;
  final String user;
  final String password;
  final Flyway flyway;
  protected final Jdbi jdbi;

  /**
   * Create a MySqlSink for Redshift metrics.
   *
   * @param url URL of the MySQL Database
   * @param user user of the MySQL Database
   * @param password password of the MySQL database
   * @param metricRegistry MetricRegistry to store JDBI metrics
   * @param flyway Migrations library to setup the MySQL database
   */

  protected DbSink(String url, String user, String password,
                   MetricRegistry metricRegistry, Flyway flyway) {
    this.url = url;
    this.user = user;
    this.password = password;
    this.flyway = flyway;
    this.jdbi = Jdbi.create(url, user, password);
    this.jdbi.setSqlLogger(new JdbiTimer(metricRegistry));
  }

  /**
   * Setup MySQL with tables to store metrics.
   */
  public void initialize() {
    flyway.setDataSource(url, user, password);
    flyway.setLocations(this.getMigrationsPath());
    flyway.migrate();
  }

  public void close() {
    flyway.clean();
  }

  /**
   * A convenience function which manages the lifecycle of a handle and yields it to a callback
   * for use by clients.
   *
   * @param callback A callback which will receive an open Handle
   * @param <R> type returned by the callback
   * @param <X> exception type thrown by the callback, if any.
   *
   * @return the value returned by callback
   *
   * @throws X any exception thrown by the callback
   */
  public <R, X extends Exception> R withHandle(HandleCallback<R, X> callback) throws X {
    return this.jdbi.withHandle(handle -> {
      this.registerMappers(handle);
      return callback.withHandle(handle);
    });
  }

  /**
   * A convenience function which manages the lifecycle of a handle and yields it to a callback
   * for use by clients.
   *
   * @param callback A callback which will receive an open Handle
   * @param <X> exception type thrown by the callback, if any.
   *
   * @throws X any exception thrown by the callback
   */
  public <X extends Exception> void useHandle(HandleConsumer<X> callback) throws X {
    this.jdbi.useHandle(handle -> {
      this.registerMappers(handle);
      callback.useHandle(handle);
    });
  }

  protected abstract void registerMappers(Handle handle);

  protected abstract String getMigrationsPath();
}
