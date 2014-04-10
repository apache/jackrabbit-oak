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
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.jackrabbit.mk.api.MicroKernelException;

public class RDBDataSourceFactory {

    public static DataSource forJdbcUrl(String url, String username, String passwd) {
        try {
            BasicDataSource bds = new BasicDataSource();
            Driver d = DriverManager.getDriver(url);
            bds.setDriverClassName(d.getClass().getName());
            bds.setUsername(username);
            bds.setPassword(passwd);
            bds.setUrl(url);
            return new CloseableDataSource(bds);
        } catch (SQLException ex) {
            throw new MicroKernelException("trying to obtain driver for " + url, ex);
        }
    }

    /**
     * A {@link Closeable} {@link DataSource} based on a {@link BasicDataSource}. 
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
    }
}
