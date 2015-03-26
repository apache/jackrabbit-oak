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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.slf4j.LoggerFactory;

/**
 * Utility functions for connection handling.
 */
public class RDBConnectionHandler {

    private final DataSource ds;

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
        Connection c = this.ds.getConnection();
        c.setAutoCommit(false);
        c.setReadOnly(true);
        return c;
    }

    /**
     * Obtain a {@link Connection} suitable for read-write operations.
     */
    public @Nonnull Connection getRWConnection() throws SQLException {
        Connection c = this.ds.getConnection();
        c.setAutoCommit(false);
        c.setReadOnly(false);
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
                        c.setReadOnly(!c.isReadOnly());
                        c.setReadOnly(!c.isReadOnly());
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
     * Closes a {@link Statement}, logging potential problems.
     * @return null
     */
    public <T extends Statement> T closeStatement(@CheckForNull T stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException ex) {
                LOG.debug("Closing statement", ex);
            }
        }
        return null;
    }

    /**
     * Closes a {@link ResultSet}, logging potential problems.
     * 
     * @return null
     */
    public ResultSet closeResultSet(@CheckForNull ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ex) {
                LOG.debug("Closing result set", ex);
            }
        }

        return null;
    }
}
