/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.core.util.db;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jcr.RepositoryException;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.DelegatingConnection;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.jackrabbit.core.config.DataSourceConfig;
import org.apache.jackrabbit.core.config.DataSourceConfig.DataSourceDefinition;
import org.apache.jackrabbit.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory for new database connections.
 * Supported are regular JDBC drivers, as well as
 * JNDI resources.
 *
 * FIXME: the registry currently is ClassLoader wide. I.e., if you start two repositories
 * then you share the registered datasources...
 */
public final class ConnectionFactory {

    /**
     * System property to disable the pooling of PreparedStatements.
     */
    private static final String SYSTEM_PROPERTY_POOL_PREPARED_STATEMENTS = "org.apache.jackrabbit.core.util.db.driver.datasource.poolPreparedStatements";
    
    private static final Logger log = LoggerFactory.getLogger(ConnectionFactory.class);

    /**
     * The lock to protect the fields of this class.
     */
    private final Object lock = new Object();

    /**
     * The data sources without logical name. The keys in the map are based on driver-url-user combination.
     */
    private final Map<String, DataSource> keyToDataSource = new HashMap<String, DataSource>();

    /**
     * The configured data sources with logical name. The keys in the map are the logical name.
     */
    private final Map<String, DataSource> nameToDataSource = new HashMap<String, DataSource>();

    /**
     * The configured data source defs. The keys in the map are the logical name.
     */
    private final Map<String, DataSourceDefinition> nameToDataSourceDef = new HashMap<String, DataSourceDefinition>();

    /**
     * The list of data sources created by this factory.
     */
    private final List<BasicDataSource> created = new ArrayList<BasicDataSource>();

    private boolean closed = false;

    /**
     * Registers a number of data sources.
     *
     * @param dsc the {@link DataSourceConfig} which contains the configuration
     */
    public void registerDataSources(DataSourceConfig dsc) throws RepositoryException {
        synchronized (lock) {
            sanityCheck();
            for (DataSourceDefinition def : dsc.getDefinitions()) {
                Class<?> driverClass = getDriverClass(def.getDriver());
                if (driverClass != null
                        && Context.class.isAssignableFrom(driverClass)) {
                    DataSource ds = getJndiDataSource((Class<Context>) driverClass, def.getUrl());
                    nameToDataSource.put(def.getLogicalName(), ds);
                    nameToDataSourceDef.put(def.getLogicalName(), def);
                } else {
                    BasicDataSource bds =
                        getDriverDataSource(driverClass, def.getUrl(), def.getUser(), def.getPassword());
                    if (def.getMaxPoolSize() > 0) {
                        bds.setMaxTotal(def.getMaxPoolSize());
                    }
                    if (def.getValidationQuery() != null && !"".equals(def.getValidationQuery().trim())) {
                        bds.setValidationQuery(def.getValidationQuery());
                    }
                    nameToDataSource.put(def.getLogicalName(), bds);
                    nameToDataSourceDef.put(def.getLogicalName(), def);
                }
            }
        }
    }

    /**
     * Retrieves a configured data source by logical name.
     *
     * @param logicalName the name of the {@code DataSource}
     * @return a {@code DataSource}
     * @throws RepositoryException if there is no {@code DataSource} with the given name
     */
    public DataSource getDataSource(String logicalName) throws RepositoryException {
        synchronized (lock) {
            sanityCheck();
            DataSource ds = nameToDataSource.get(logicalName);
            if (ds == null) {
                throw new RepositoryException("DataSource with logicalName " + logicalName
                        + " has not been configured");
            }
            return ds;
        }
    }

    /**
     * @param logicalName the name of the {@code DataSource}
     * @return the configured database type
     * @throws RepositoryException if there is no {@code DataSource} with the given name
     */
    public String getDataBaseType(String logicalName) throws RepositoryException {
        synchronized (lock) {
            sanityCheck();
            DataSourceDefinition def = nameToDataSourceDef.get(logicalName);
            if (def == null) {
                throw new RepositoryException("DataSource with logicalName " + logicalName
                        + " has not been configured");
            }
            return def.getDbType();
        }
    }

    /**
     * Retrieve a {@code DataSource} for the specified properties.
     * This can be a JNDI Data Source as well. To do that,
     * the driver class name must reference a {@code javax.naming.Context} class
     * (for example {@code javax.naming.InitialContext}), and the URL must be the JNDI URL
     * (for example {@code java:comp/env/jdbc/Test}).
     *
     * @param driver the JDBC driver or the Context class
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @return the {@code DataSource}
     * @throws RepositoryException if the driver could not be loaded
     * @throws SQLException if the connection could not be established
     */
    public DataSource getDataSource(String driver, String url, String user, String password)
            throws RepositoryException, SQLException    {
        final String key = driver + url + user;
        synchronized(lock) {
            sanityCheck();
            DataSource ds = keyToDataSource.get(key);
            if (ds == null) {
                ds = createDataSource(
                        driver, url, user, Base64.decodeIfEncoded(password));
                keyToDataSource.put(key, ds);
            }
            return ds;
        }
    }

    /**
     *
     */
    public void close() {
        synchronized(lock) {
            sanityCheck();
            for (BasicDataSource ds : created) {
                try {
                    ds.close();
                } catch (SQLException e) {
                    log.error("failed to close " + ds, e);
                }
            }
            keyToDataSource.clear();
            nameToDataSource.clear();
            nameToDataSourceDef.clear();
            created.clear();
            closed = true;
        }
    }

    /**
     * Needed for pre-10R2 Oracle blob support....:(
     *
     * This method actually assumes that we are using commons DBCP 1.2.2.
     *
     * @param con the commons-DBCP {@code DelegatingConnection} to unwrap
     * @return the unwrapped connection
     */
    public static Connection unwrap(Connection con) throws SQLException {
        if (con instanceof DelegatingConnection) {
            return ((DelegatingConnection)con).getInnermostDelegate();
        } else {
            throw new SQLException("failed to unwrap connection of class " + con.getClass().getName() +
                ", expected it to be a " + DelegatingConnection.class.getName());
        }
    }

    private void sanityCheck() {
        if (closed) {
            throw new IllegalStateException("this factory has already been closed");
        }
    }

    /**
     * Create a new pooling data source or finds an existing JNDI data source (depends on driver).
     *
     * @param driver
     * @param url
     * @param user
     * @param password
     * @return
     * @throws RepositoryException
     */
    private DataSource createDataSource(String driver, String url, String user, String password)
            throws RepositoryException {
        Class<?> driverClass = getDriverClass(driver);
        if (driverClass != null
                && Context.class.isAssignableFrom(driverClass)) {
            @SuppressWarnings("unchecked")
            DataSource database = getJndiDataSource((Class<Context>) driverClass, url);
            if (user == null && password == null) {
                return database;
            } else {
                return new DataSourceWrapper(database, user, password);
            }
        } else {
            return getDriverDataSource(driverClass, url, user, password);
        }
    }

    /**
     * Loads and returns the given JDBC driver (or JNDI context) class.
     * Returns <code>null</code> if a class name is not given.
     *
     * @param driver driver class name
     * @return driver class, or <code>null</code>
     * @throws RepositoryException if the class can not be loaded
     */
    private Class<?> getDriverClass(String driver)
            throws RepositoryException {
        try {
            if (driver != null && driver.length() > 0) {
                return Class.forName(driver);
            } else {
                return null;
            }
        } catch (ClassNotFoundException e) {
            throw new RepositoryException(
                    "Could not load JDBC driver class " + driver, e);
        }
    }

    /**
     * Returns the JDBC {@link DataSource} bound to the given name in
     * the JNDI {@link Context} identified by the given class.
     *
     * @param contextClass class that is instantiated to get the JNDI context
     * @param name name of the DataSource within the JNDI context
     * @return the DataSource bound in JNDI
     * @throws RepositoryException if the JNDI context can not be accessed,
     *                             or if the named DataSource is not found
     */
    private DataSource getJndiDataSource(
            Class<Context> contextClass, String name)
            throws RepositoryException {
        try {
            Object object = contextClass.getDeclaredConstructor().newInstance().lookup(name);
            if (object instanceof DataSource) {
                return (DataSource) object;
            } else {
                throw new RepositoryException(
                        "Object " + object + " with JNDI name "
                        + name + " is not a JDBC DataSource");
            }
        } catch (InstantiationException|IllegalAccessException|IllegalArgumentException|InvocationTargetException|NoSuchMethodException|SecurityException e) {
            throw new RepositoryException(
                    "Invalid JNDI context: " + contextClass.getName(), e);
        } catch (NamingException e) {
            throw new RepositoryException(
                    "JNDI name not found: " + name, e);
        }
    }

    /**
     * Creates and returns a pooling JDBC {@link DataSource} for accessing
     * the database identified by the given driver class and JDBC
     * connection URL. The driver class can be <code>null</code> if
     * a specific driver has not been configured.
     *
     * @param driverClass the JDBC driver class, or <code>null</code>
     * @param url the JDBC connection URL
     * @return pooling DataSource for accessing the specified database
     */
    private BasicDataSource getDriverDataSource(
            Class<?> driverClass, String url, String user, String password) {
        BasicDataSource ds = new BasicDataSource();
        created.add(ds);

        if (driverClass != null) {
        	Driver instance = null;
            try {
                // Workaround for Apache Derby:
                // The JDBC specification recommends the Class.forName
                // method without the .newInstance() method call,
                // but it is required after a Derby 'shutdown'
                instance = (Driver) driverClass.getDeclaredConstructor().newInstance();
            } catch (Throwable e) {
                // Ignore exceptions as there's no requirement for
                // a JDBC driver class to have a public default constructor
            }
            if (instance != null) {
                if (instance.jdbcCompliant()) {
                	// JCR-3445 At the moment the PostgreSQL isn't compliant because it doesn't implement this method...                	
                    ds.setValidationQueryTimeout(Duration.ofSeconds(3));
                }
            }
            ds.setDriverClassName(driverClass.getName());
        }

        ds.setUrl(url);
        ds.setUsername(user);
        ds.setPassword(password);
        ds.setDefaultAutoCommit(true);
        ds.setTestOnBorrow(false);
        ds.setTestWhileIdle(true);
        ds.setDurationBetweenEvictionRuns(Duration.ofMinutes(10));
        ds.setMinEvictableIdle(Duration.ofMinutes(1));
        ds.setMaxTotal(-1); // unlimited
        ds.setMaxIdle(GenericObjectPoolConfig.DEFAULT_MAX_IDLE + 10);
        ds.setValidationQuery(guessValidationQuery(url));
        ds.setAccessToUnderlyingConnectionAllowed(true);
        ds.setPoolPreparedStatements(Boolean.valueOf(System.getProperty(SYSTEM_PROPERTY_POOL_PREPARED_STATEMENTS, "true")));
        ds.setMaxOpenPreparedStatements(-1); // unlimited
        // this helps to discover unusable connections of a shut-down database without executing a validation query
        ds.setCacheState(false);
        return ds;
    }

    private String guessValidationQuery(String url) {
        if (url.contains("derby")) {
            return "values(1)";
        } else if (url.contains("mysql")) {
            return "select 1";
        } else if (url.contains("sqlserver") || url.contains("jtds")) {
            return "select 1";
        } else if (url.contains("oracle")) {
            return "select 'validationQuery' from dual";
        } else if (url.contains("postgresql")) {
            return "select 1";
        } else if (url.contains("h2")) {
            return "select 1";
        } else if (url.contains("db2")) {
            return "values(1)";
        }
        log.warn("Failed to guess validation query for URL " + url);
        return null;
    }
}
