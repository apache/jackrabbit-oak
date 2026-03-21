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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains some database utility methods.
 */
public final class DbUtility {

    private static final Logger LOG = LoggerFactory.getLogger(DbUtility.class);

    /**
     * Private constructor for utility class pattern.
     */
    private DbUtility() {
    }

    /**
     * This is a utility method which closes the given resources without throwing exceptions. Any exceptions
     * encountered are logged instead.
     * 
     * @param rs the {@link ResultSet} to close, may be null
     */
    public static void close(ResultSet rs) {
        close(null, null, rs);
    }

    /**
     * This is a utility method which closes the given resources without throwing exceptions. Any exceptions
     * encountered are logged instead.
     * 
     * @param con the {@link Connection} to close, may be null
     * @param stmt the {@link Statement} to close, may be null
     * @param rs the {@link ResultSet} to close, may be null
     */
    public static void close(Connection con, Statement stmt, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            logException("failed to close ResultSet", e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                logException("failed to close Statement", e);
            } finally {
                try {
                    if (con != null && !con.isClosed()) {
                        con.close();
                    }
                } catch (SQLException e) {
                    logException("failed to close Connection", e);
                }
            }
        }
    }

    /**
     * Logs an SQL exception on error level, and debug level (more detail).
     * 
     * @param message the message
     * @param e the exception
     */
    public static void logException(String message, SQLException e) {
        if (message != null) {
            LOG.error(message);
        }
        LOG.error("       Reason: " + e.getMessage());
        LOG.error("   State/Code: " + e.getSQLState() + "/" + e.getErrorCode());
        LOG.debug("   dump:", e);
    }
}
