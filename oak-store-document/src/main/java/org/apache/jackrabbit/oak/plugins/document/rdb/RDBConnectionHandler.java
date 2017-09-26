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
import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
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
    private static final boolean CHECKCONNECTIONONCLOSE = Boolean
            .getBoolean("org.apache.jackrabbit.oak.plugins.document.rdb.RDBConnectionHandler.CHECKCONNECTIONONCLOSE");

    public RDBConnectionHandler(@Nonnull DataSource ds) {
        this.ds = ds;
    }

    /**
     * Obtain a {@link Connection} suitable for read-only operations.
     */
    public @Nonnull Connection getROConnection() throws SQLException {
        Connection c = getConnection();
        c.setAutoCommit(false);
        setReadOnly(c, true);
        return c;
    }

    /**
     * Obtain a {@link Connection} suitable for read-write operations.
     */
    public @Nonnull Connection getRWConnection() throws SQLException {
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
    @CheckForNull
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
    public void close() throws IOException {
        this.ds = null;
        this.closedTime = System.currentTimeMillis();
    }

    @Nonnull
    private DataSource getDataSource() throws IllegalStateException {
        DataSource result = this.ds;
        if (result == null) {
            throw new IllegalStateException("Connection handler is already closed ("
                    + (System.currentTimeMillis() - this.closedTime) + "ms ago)");
        }
        return result;
    }

    @Nonnull
    private Connection getConnection() throws IllegalStateException, SQLException {
        long ts = System.currentTimeMillis();
        Connection c = getDataSource().getConnection();
        if (LOG.isDebugEnabled()) {
            long elapsed = System.currentTimeMillis() - ts;
            if (elapsed >= 100) {
                LOG.debug("Obtaining a new connection from " + this.ds + " took " + elapsed + "ms");
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
}
