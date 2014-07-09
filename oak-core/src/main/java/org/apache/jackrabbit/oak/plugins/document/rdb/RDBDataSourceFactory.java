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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Locale;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating {@link DataSource}s based on a JDBC connection URL.
 */
public class RDBDataSourceFactory {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RDBDataSourceFactory.class);

    public static DataSource forJdbcUrl(String url, String username, String passwd, String driverName) {

        // load driver class when specified
        if (driverName != null && !driverName.isEmpty()) {
            LOG.info("trying to load {}", driverName);

            try {
                Class.forName(driverName);
            } catch (ClassNotFoundException ex) {
                LOG.error("driver " + driverName + " not loaded", ex);
            }
        } else {
            // try to determine driver from JDBC URL
            String defaultDriver = driverForDBType(jdbctype(url));
            if (defaultDriver != null && !defaultDriver.isEmpty()) {
                LOG.info("trying to load {}", defaultDriver);

                try {
                    Class.forName(defaultDriver);
                } catch (ClassNotFoundException ex) {
                    LOG.error("driver " + defaultDriver + " not loaded", ex);
                }
            }
        }

        try {
            BasicDataSource bds = new BasicDataSource();
            LOG.debug("Getting driver for " + url);
            Driver d = DriverManager.getDriver(url);
            bds.setDriverClassName(d.getClass().getName());
            bds.setUsername(username);
            bds.setPassword(passwd);
            bds.setUrl(url);
            return new CloseableDataSource(bds);
        } catch (SQLException ex) {
            String message = "trying to obtain driver for " + url;
            LOG.info(message, ex);
            throw new DocumentStoreException(message, ex);
        }
    }

    public static DataSource forJdbcUrl(String url, String username, String passwd) {
        return forJdbcUrl(url, username, passwd, null);
    }

    private static String jdbctype(String jdbcurl) {
        if (jdbcurl == null) {
            return null;
        } else {
            String t = jdbcurl.toLowerCase(Locale.ENGLISH);
            if (!t.startsWith("jdbc:")) {
                return null;
            } else {
                t = t.substring("jbdc:".length());
                int p = t.indexOf(":");
                if (p <= 0) {
                    return null;
                } else {
                    return t.substring(0, p);
                }
            }
        }
    }

    private static String driverForDBType(String type) {
        if ("h2".equals(type)) {
            return "org.h2.Driver";
        } else if ("postgresql".equals(type)) {
            return "org.postgresql.Driver";
        } else if ("db2".equals(type)) {
            return "com.ibm.db2.jcc.DB2Driver";
        } else if ("mysql".equals(type)) {
            return "com.mysql.jdbc.Driver";
        } else if ("oracle".equals(type)) {
            return "oracle.jdbc.OracleDriver";
        } else {
            return "";
        }
    }

    /**
     * A {@link Closeable} {@link DataSource} based on a {@link BasicDataSource}
     * .
     */
    private static class CloseableDataSource implements DataSource, Closeable {

        private BasicDataSource ds;

        public CloseableDataSource(BasicDataSource ds) {
            this.ds = ds;
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return this.ds.getLogWriter();
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return this.ds.getLoginTimeout();
        }

        @Override
        public void setLogWriter(PrintWriter pw) throws SQLException {
            this.ds.setLogWriter(pw);
        }

        @Override
        public void setLoginTimeout(int t) throws SQLException {
            this.ds.setLoginTimeout(t);
        }

        @Override
        public boolean isWrapperFor(Class<?> c) throws SQLException {
            return this.ds.isWrapperFor(c);
        }

        @Override
        public <T> T unwrap(Class<T> c) throws SQLException {
            return this.ds.unwrap(c);
        }

        @Override
        public void close() throws IOException {
            try {
                this.ds.close();
            } catch (SQLException ex) {
                throw new IOException("closing data source " + this.ds, ex);
            }
        }

        @Override
        public Connection getConnection() throws SQLException {
            return this.ds.getConnection();
        }

        @Override
        public Connection getConnection(String user, String passwd) throws SQLException {
            return this.ds.getConnection(user, passwd);
        }

        // needed in Java 7...
        @SuppressWarnings("unused")
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public String toString() {
            return this.getClass().getName() + " wrapping a " + this.ds.toString();
        }
    }
}
