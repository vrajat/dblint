package io.inviscid.metricsink.redshift;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.reflect.ConstructorMapper;

import java.util.List;

public class RedshiftDb {
  final String url;
  final String user;
  final String password;
  final Jdbi jdbi;

  /**
   * Manage a connection to a Redshift database.
   * @param url URL of the Redshift database
   * @param user User of the Redshift database
   * @param password Password of the Redshift database
   */
  public RedshiftDb(String url, String user, String password) {
    this.url = url;
    this.user = user;
    this.password = password;
    this.jdbi = Jdbi.create(url, user, password);
  }

  /**
   * Get QueryStats for a specific time period from Redshift.
   * @param inTest Test parameter to choose a H2 compliant query
   * @return List of QueryStats
   */
  public List<QueryStats> getQueryStats(boolean inTest) {
    return jdbi.withHandle(handle -> {
      handle.registerRowMapper(ConstructorMapper.factory(QueryStats.class));
      return handle.createQuery(inTest ? QueryStats.getExtractQueryinTest()
          : QueryStats.getExtractQuery())
          .mapTo(QueryStats.class)
          .list();
    });
  }
}
