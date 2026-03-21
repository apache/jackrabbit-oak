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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * This class delegates all calls to the corresponding method on the wrapped {@code DataSource} except for the {@link #getConnection()} method,
 * which delegates to {@code DataSource#getConnection(String, String)} with the username and password
 * which are given on construction.
 */
public class DataSourceWrapper implements DataSource {

    private final DataSource dataSource;

    private final String username;

    private final String password;

    /**
     * @param dataSource the {@code DataSource} to wrap
     * @param username the username to use
     * @param password the password to use
     */
    public DataSourceWrapper(DataSource dataSource, String username, String password) {
        this.dataSource = dataSource;
        this.username = username;
        this.password = password;
    }

    /**
     * Java 6 method.
     * 
     * {@inheritDoc}
     */
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        throw new UnsupportedOperationException("Java 6 method not supported");
    }

    /**
     * Java 6 method.
     * 
     * {@inheritDoc}
     */
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        throw new UnsupportedOperationException("Java 6 method not supported");
    }

    /**
     * Unsupported Java 7 method.
     *
     * @see <a href="https://issues.apache.org/jira/browse/JCR-3167">JCR-3167</a>
     */
    public Logger getParentLogger() {
        throw new UnsupportedOperationException("Java 7 method not supported");
    }

    /**
     * {@inheritDoc}
     */
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection(username, password);
    }

    /**
     * {@inheritDoc}
     */
    public Connection getConnection(String username, String password) throws SQLException {
        return dataSource.getConnection(username, password);
    }

    /**
     * {@inheritDoc}
     */
    public PrintWriter getLogWriter() throws SQLException {
        return dataSource.getLogWriter();
    }

    /**
     * {@inheritDoc}
     */
    public int getLoginTimeout() throws SQLException {
        return dataSource.getLoginTimeout();
    }

    /**
     * {@inheritDoc}
     */
    public void setLogWriter(PrintWriter out) throws SQLException {
        dataSource.setLogWriter(out);
    }

    /**
     * {@inheritDoc}
     */
    public void setLoginTimeout(int seconds) throws SQLException {
        dataSource.setLoginTimeout(seconds);
    }

}
