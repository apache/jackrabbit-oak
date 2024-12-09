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
import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

/**
 * Utility functions for connection handling.
 */
public class RDBConnectionHandler implements Closeable {

    private DataSource ds;
    private long closedTime = 0L;

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RDBConnectionHandler.class);

    /**
     * Closing a connection doesn't necessarily imply a {@link Connection#commit()} or {@link Connection#rollback()}.
     * This becomes a problem when the pool implemented by the {@link DataSource} re-uses the connection, and may
     * affect subsequent users of that connection. This system property allows to enable a check to be done upon
     * {@link #closeConnection(Connection)} so that problems can be detected early rather than late.
     * See also https://issues.apache.org/jira/browse/OAK-2337.
     */
    private static final boolean CHECKCONNECTIONONCLOSE = SystemPropertySupplier
            .create("org.apache.jackrabbit.oak.plugins.document.rdb.RDBConnectionHandler.CHECKCONNECTIONONCLOSE", Boolean.FALSE)
            .loggingTo(LOG).formatSetMessage((name, value) -> String
                    .format("Check connection on close enabled (system property %s set to '%s')", name, value))
            .get();

    public RDBConnectionHandler(@NotNull DataSource ds) {
        this.ds = ds;
    }

    /**
     * Obtain a {@link Connection} suitable for read-only operations.
     */
    public @NotNull Connection getROConnection() throws SQLException {
        Connection c = getConnection();
        c.setAutoCommit(false);
        setReadOnly(c, true);
        return c;
    }

    /**
     * Obtain a {@link Connection} suitable for read-write operations.
     */
    public @NotNull Connection getRWConnection() throws SQLException {
        Connection c = getConnection();
        c.setAutoCommit(false);
        setReadOnly(c, false);
        return c;
    }

    /**
     * Roll back the {@link Connection}.
     */
    public void rollbackConnection(@Nullable Connection c) {
        if (c != null) {
            try {
                c.rollback();
            } catch (SQLException ex) {
                LOG.error("error on rollback (ignored)", ex);
            }
        }
    }

    /**
     * Close the {@link Connection}.
     */
    public void closeConnection(Connection c) {
        if (c != null) {
            try {
                if (CHECKCONNECTIONONCLOSE) {
                    try {
                        setReadOnly(c, !c.isReadOnly());
                        setReadOnly(c, !c.isReadOnly());
                    } catch (SQLException ex2) {
                        LOG.error("got dirty connection", ex2);
                        throw new DocumentStoreException("dirty connection on close", ex2);
                    }
                }
                c.close();
            } catch (SQLException ex) {
                LOG.error("exception on connection close (ignored)", ex);
            }
        }
    }

    /**
     * Return current schema name or {@code null} when unavailable
     */
    @Nullable
    public String getSchema(Connection c) {
        try {
            return (String) c.getClass().getMethod("getSchema").invoke(c);
        } catch (Throwable ex) {
            // ignored, it's really best effort
            return null;
        }
    }

    public boolean isClosed() {
        return this.ds == null;
    }

    @Override
    public void close() {
        this.ds = null;
        this.closedTime = System.currentTimeMillis();
    }

    @NotNull
    private DataSource getDataSource() throws IllegalStateException {
        DataSource result = this.ds;
        if (result == null) {
            throw new IllegalStateException("Connection handler is already closed ("
                    + (System.currentTimeMillis() - this.closedTime) + "ms ago)");
        }
        return result;
    }

    @NotNull
    private Connection getConnection() throws IllegalStateException, SQLException {
        long ts = System.currentTimeMillis();
        dumpConnectionMap(ts);
        Connection c = getDataSource().getConnection();
        remember(c);
        if (LOG.isDebugEnabled()) {
            long elapsed = System.currentTimeMillis() - ts;
            if (elapsed >= 20) {
                LOG.debug("Obtaining a new connection from " + this.ds + " took " + elapsed + "ms", new Exception("call stack"));
            }
        }
        return c;
    }

    // workaround for broken connection wrappers
    // see https://issues.apache.org/jira/browse/OAK-2918

    private Boolean setReadOnlyThrows = null; // null if we haven't checked yet
    private Boolean setReadWriteThrows = null; // null if we haven't checked yet

    private void setReadOnly(Connection c, boolean ro) throws SQLException {

        if (ro) {
            if (this.setReadOnlyThrows == null) {
                // we don't know yet, so we try at least once
                try {
                    c.setReadOnly(true);
                    this.setReadOnlyThrows = Boolean.FALSE;
                } catch (SQLException ex) {
                    LOG.error("Connection class " + c.getClass()
                            + " erroneously throws SQLException on setReadOnly(true); not trying again");
                    this.setReadOnlyThrows = Boolean.TRUE;
                }
            } else if (!this.setReadOnlyThrows) {
                c.setReadOnly(true);
            }
        } else {
            if (this.setReadWriteThrows == null) {
                // we don't know yet, so we try at least once
                try {
                    c.setReadOnly(false);
                    this.setReadWriteThrows = Boolean.FALSE;
                } catch (SQLException ex) {
                    LOG.error("Connection class " + c.getClass()
                            + " erroneously throws SQLException on setReadOnly(false); not trying again");
                    this.setReadWriteThrows = Boolean.TRUE;
                }
            } else if (!this.setReadWriteThrows) {
                c.setReadOnly(false);
            }
        }
    }

    private static class ConnectionHolder {
        public String thread;
        public String caller;
        public long ts;

        public ConnectionHolder() {
            Thread t = Thread.currentThread();
            this.thread = t.getName();
            this.caller = getCaller(t.getStackTrace());
            this.ts = System.currentTimeMillis();
        }

        public long getTimestamp() {
            return ts;
        }

        public String dump(long now) {
            return "(thread=" + thread + ", caller=" + caller + ", age=" + (now - ts) + ")";
        }
    }

    // map holding references to currently open connections
    private ConcurrentMap<WeakReference<Connection>, ConnectionHolder> connectionMap = new ConcurrentHashMap<>();

    // time in millis for a connection in the map to be logged as "old"; note
    // that this is meant to catch both connection leaks and long-running
    // transactions
    private final int LOGTHRESHOLD = 100;

    private void dumpConnectionMap(long ts) {
        if (LOG.isTraceEnabled()) {
            connectionMap.forEach((k, v) -> {
                try {
                    Connection con = k.get();
                    if (con == null || con.isClosed()) {
                        connectionMap.remove(k);
                    }
                } catch (SQLException ex) {
                }
            });

            int size = connectionMap.size();
            if (size > 0) {
                int cnt = 0;
                StringBuilder sb = new StringBuilder();
                for (ConnectionHolder ch : connectionMap.values()) {
                    if (ts - ch.getTimestamp() >= LOGTHRESHOLD) {
                        if (cnt != 0) {
                            sb.append(", ");
                        }
                        cnt += 1;
                        sb.append(ch.dump(ts));
                    }
                }
                if (cnt > 0) {
                    LOG.trace(cnt + " connections with age >= " + LOGTHRESHOLD + "ms active while obtaining new connection: "
                            + sb.toString());
                }
            }
        }
    }

    private void remember(Connection c) {
        if (LOG.isTraceEnabled()) {
            connectionMap.put(new WeakReference<Connection>(c), new ConnectionHolder());
        }
    }

    private static String getCaller(StackTraceElement[] elements) {
        StringBuilder sb = new StringBuilder();
        String prevClass = null;
        for (StackTraceElement e : elements) {
            String cn = e.getClassName();
            if (!cn.startsWith(RDBConnectionHandler.class.getName()) && !(cn.startsWith(Thread.class.getName()))) {
                if (sb.length() != 0) {
                    sb.append(" ");
                }
                if (e.getClassName().equals(prevClass)) {
                    String loc;
                    if (e.isNativeMethod()) {
                        loc = "Native Method";
                    } else if (e.getFileName() == null) {
                        loc = "Unknown Source";
                    } else {
                        loc = e.getFileName() + ":" + e.getLineNumber();
                    }
                    sb.append('.').append(e.getMethodName()).append('(').append(loc).append(')');
                } else {
                    sb.append(e.toString());
                }
                prevClass = e.getClassName();
            }
        }
        return sb.toString();
    }
}
