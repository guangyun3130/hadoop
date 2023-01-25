/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.federation.router.security.token;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.SQLDelegationTokenSecretManager;


/**
 * Interface to provide SQL connections to the {@link SQLDelegationTokenSecretManagerImpl}.
 */
public interface SQLConnectionFactory {
  String CONNECTION_URL = SQLDelegationTokenSecretManager.SQL_DTSM_CONF_PREFIX
      + "connection.url";
  String CONNECTION_USERNAME = SQLDelegationTokenSecretManager.SQL_DTSM_CONF_PREFIX
      + "connection.username";
  String CONNECTION_PASSWORD = SQLDelegationTokenSecretManager.SQL_DTSM_CONF_PREFIX
      + "connection.password";
  String CONNECTION_DRIVER = SQLDelegationTokenSecretManager.SQL_DTSM_CONF_PREFIX
      + "connection.driver";

  Connection getConnection() throws SQLException;
  void shutdown();

  default Connection getConnection(boolean autocommit) throws SQLException {
    Connection connection = getConnection();
    connection.setAutoCommit(autocommit);
    return connection;
  }
}

/**
 * Class that relies on a MysqlDataSource to provide SQL connections.
 */
class MysqlDataSourceConnectionFactory implements SQLConnectionFactory {
  private final MysqlDataSource dataSource;

  MysqlDataSourceConnectionFactory(Configuration conf) {
    this.dataSource = new MysqlDataSource();
    this.dataSource.setUrl(conf.get(CONNECTION_URL));
    this.dataSource.setUser(conf.get(CONNECTION_USERNAME));
    this.dataSource.setPassword(conf.get(CONNECTION_PASSWORD));
  }

  @Override
  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  @Override
  public void shutdown() {
    // Nothing to shut down
  }
}

/**
 * Class that relies on a HikariDataSource to provide SQL connections.
 */
class HikariDataSourceConnectionFactory implements SQLConnectionFactory {
  protected final static String HIKARI_PROPS = SQLDelegationTokenSecretManager.SQL_DTSM_CONF_PREFIX
      + "connection.hikari.";
  private final HikariDataSource dataSource;

  HikariDataSourceConnectionFactory(Configuration conf) {
    Properties properties = new Properties();
    properties.setProperty("jdbcUrl", conf.get(CONNECTION_URL));
    properties.setProperty("username", conf.get(CONNECTION_USERNAME));
    properties.setProperty("password", conf.get(CONNECTION_PASSWORD));
    properties.setProperty("driverClassName", conf.get(CONNECTION_DRIVER));

    // Include hikari connection properties
    properties.putAll(conf.getPropsWithPrefix(HIKARI_PROPS));

    HikariConfig hikariConfig = new HikariConfig(properties);
    this.dataSource = new HikariDataSource(hikariConfig);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  @Override
  public void shutdown() {
    // Close database connections
    dataSource.close();
  }

  @VisibleForTesting
  HikariDataSource getDataSource() {
    return dataSource;
  }
}
