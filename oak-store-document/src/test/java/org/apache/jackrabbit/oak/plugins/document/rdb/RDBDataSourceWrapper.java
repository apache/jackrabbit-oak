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
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * A wrapper for {@link DataSource} that offers logging of various
 * operations.
 * <p>
 * Note that the implementations currently focus on method invocations done
 * by {@link RDBDocumentStore} and thus may not be applicable for other use cases.
 */
public class RDBDataSourceWrapper implements DataSource, Closeable {

    // sample use in BasicDocumentStoreTest:

    // to start
    // if (super.rdbDataSource != null) {
    // ((RDBDataSourceWrapper) super.rdbDataSource).startLog();
    // }

    // to dump
    // if (super.rdbDataSource != null) {
    // RDBLogEntry.DUMP(System.err, ((RDBDataSourceWrapper)
    // super.rdbDataSource).stopLog());
    // }

    private final DataSource ds;
    private boolean batchResultPrecise = true;
    private boolean failAlterTableAddColumnStatements = false; 
    private String temporaryCommitException = null;
    private String temporaryUpdateException = null;

    // Logging

    private Map<Thread, List<RDBLogEntry>> loggerMap = new ConcurrentHashMap<Thread, List<RDBLogEntry>>();

    protected List<RDBLogEntry> getLog() {
        return loggerMap.get(Thread.currentThread());
    }

    private void startLog(Thread thread) {
        loggerMap.put(thread, new ArrayList<RDBLogEntry>());
    }

    /**
     * Start logging for the current thread.
     */
    public void startLog() {
        startLog(Thread.currentThread());
    }

    private List<RDBLogEntry> stopLog(Thread thread) {
        return loggerMap.remove(thread);
    }

    /**
     * End logging for the current thread and obtain log results.
     */
    public List<RDBLogEntry> stopLog() {
        return stopLog(Thread.currentThread());
    }

    /**
     * Set to {@code false} to simulate drivers/DBs that do not return the number of affected rows in {@link Statement#executeBatch()}.
     */
    public void setBatchResultPrecise(boolean precise) {
        this.batchResultPrecise = precise;
    }

    public boolean isBatchResultPrecise() {
        return this.batchResultPrecise;
    }

    /**
     * For the simulation of setups where we can't modify the DB.
     */
    public void setFailAlterTableAddColumnStatements(boolean failAlterTableAddColumnStatements) {
        this.failAlterTableAddColumnStatements = failAlterTableAddColumnStatements;
    }

    public boolean doFailAlterTableAddColumnStatements() {
        return this.failAlterTableAddColumnStatements;
    }

    public void setTemporaryCommitException(String exmsg) {
        this.temporaryCommitException = exmsg;
    }

    public String getTemporaryCommitException() {
        return this.temporaryCommitException;
    }

    public void setTemporaryUpdateException(String exmsg) {
        this.temporaryUpdateException = exmsg;
    }

    public String getTemporaryUpdateException() {
        return this.temporaryUpdateException;
    }

    // DataSource

    public RDBDataSourceWrapper(DataSource ds) {
        this.ds = ds;
    }

    public Connection getConnection() throws SQLException {
        long start = System.nanoTime();
        try {
            return new RDBConnectionWrapper(this, ds.getConnection());
        } finally {
            List<RDBLogEntry> l = getLog();
            if (l != null) {
                l.add(new RDBLogEntry(start, "got connection"));
            }
        }
    }

    public Connection getConnection(String username, String password) throws SQLException {
        return ds.getConnection(username, password);
    }

    public PrintWriter getLogWriter() throws SQLException {
        return ds.getLogWriter();
    }

    public int getLoginTimeout() throws SQLException {
        return ds.getLoginTimeout();
    }

    // needed in Java 7...
    @SuppressWarnings("unused")
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return ds.isWrapperFor(iface);
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        ds.setLogWriter(out);
    }

    public void setLoginTimeout(int seconds) throws SQLException {
        ds.setLoginTimeout(seconds);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return ds.unwrap(iface);
    }

    @Override
    public void close() throws IOException {
        if (ds instanceof Closeable) {
            ((Closeable) ds).close();
        }
    }

    @Override
    public String toString() {
        return this.getClass().getName() + " wrapping a " + this.ds.toString();
    }
}
