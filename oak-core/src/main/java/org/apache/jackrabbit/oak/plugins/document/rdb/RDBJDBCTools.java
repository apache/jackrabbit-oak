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
import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.Nonnull;

/**
 * Convenience methods dealing with JDBC specifics.
 */
public class RDBJDBCTools {

    protected static String jdbctype(String jdbcurl) {
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
                    return t;
                } else {
                    return t.substring(0, p);
                }
            }
        }
    }

    protected static String driverForDBType(String type) {
        if ("h2".equals(type)) {
            return "org.h2.Driver";
        } else if ("derby".equals(type)) {
            return "org.apache.derby.jdbc.EmbeddedDriver";
        } else if ("postgresql".equals(type)) {
            return "org.postgresql.Driver";
        } else if ("db2".equals(type)) {
            return "com.ibm.db2.jcc.DB2Driver";
        } else if ("mysql".equals(type)) {
            return "com.mysql.jdbc.Driver";
        } else if ("oracle".equals(type)) {
            return "oracle.jdbc.OracleDriver";
        } else if ("sqlserver".equals(type)) {
            return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        } else {
            return "";
        }
    }

    private static @Nonnull String checkLegalTableName(@Nonnull String tableName) throws IllegalArgumentException {
        for (int i = 0; i < tableName.length(); i++) {
            char c = tableName.charAt(i);
            if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (c == '_'))) {
                throw new IllegalArgumentException("Invalid character '" + c + "' in table name '" + tableName + "'");
            }
        }
        return tableName;
    }

    /**
     * Creates a table name based on an optional prefix and a base name.
     * 
     * @throws IllegalArgumentException
     *             upon illegal characters in name
     */
    protected static @Nonnull String createTableName(@Nonnull String prefix, @Nonnull String basename)
            throws IllegalArgumentException {
        String p = checkLegalTableName(prefix);
        String b = checkLegalTableName(basename);
        if (p.length() != 0 && !p.endsWith("_")) {
            p += "_";
        }
        return p + b;
    }

    /**
     * Return string representation of transaction isolation level.
     */
    protected static @Nonnull String isolationLevelToString(int isolationLevel) {
        String name;
        switch (isolationLevel) {
            case Connection.TRANSACTION_NONE:
                name = "TRANSACTION_NONE";
                break;
            case Connection.TRANSACTION_READ_COMMITTED:
                name = "TRANSACTION_READ_COMMITTED";
                break;
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                name = "TRANSACTION_READ_UNCOMMITTED";
                break;
            case Connection.TRANSACTION_REPEATABLE_READ:
                name = "TRANSACTION_REPEATABLE_READ";
                break;
            case Connection.TRANSACTION_SERIALIZABLE:
                name = "TRANSACTION_SERIALIZABLE";
                break;
            default:
                name = "unknown";
                break;
        }
        return String.format("%s (%d)", name, isolationLevel);
    }

    private static String dumpColumnMeta(String columnName, int type, String typeName, int precision) {
        boolean skipPrecision = precision == 0 || (type == Types.SMALLINT && precision == 5)
                || (type == Types.BIGINT && precision == 19);
        return skipPrecision ? String.format("%s %s", columnName, typeName)
                : String.format("%s %s(%d)", columnName, typeName, precision);
    }

    /**
     * Return approximated string representation of table DDL.
     */
    protected static String dumpResultSetMeta(ResultSetMetaData met) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%s.%s: ", met.getSchemaName(1).trim(), met.getTableName(1).trim()));
            Map<String, Integer> types = new TreeMap<String, Integer>();
            for (int i = 1; i <= met.getColumnCount(); i++) {
                if (i > 1) {
                    sb.append(", ");
                }
                sb.append(
                        dumpColumnMeta(met.getColumnName(i), met.getColumnType(i), met.getColumnTypeName(i), met.getPrecision(i)));
                types.put(met.getColumnTypeName(i), met.getColumnType(i));
            }
            sb.append(" /* " + types.toString() + " */");
            return sb.toString();
        } catch (SQLException ex) {
            return "Column metadata unavailable: " + ex.getMessage();
        }
    }

    /**
     * Return a string containing additional messages from chained exceptions.
     */
    protected static @Nonnull String getAdditionalMessages(SQLException ex) {
        List<String> messages = new ArrayList<String>();
        String message = ex.getMessage();
        SQLException next = ex.getNextException();
        while (next != null) {
            String m = next.getMessage();
            if (!message.equals(m)) {
                messages.add(m);
            }
            next = next.getNextException();
        }

        return messages.isEmpty() ? "" : messages.toString();
    }

    /**
     * Check whether the exception matches one of the given states.
     */
    protected static boolean matchesSQLState(SQLException ex, String... statePrefix) {
        String state = ex.getSQLState();
        if (state != null) {
            for (String sp : statePrefix) {
                if (state.startsWith(sp))
                    return true;
            }
        }
        return false;
    }

    /**
     * Generate version diagnostics.
     */
    protected static String versionCheck(DatabaseMetaData md, int xmaj, int xmin, String description) throws SQLException {
        int maj = md.getDatabaseMajorVersion();
        int min = md.getDatabaseMinorVersion();
        if (maj < xmaj || (maj == xmaj && min < xmin)) {
            return "Unsupported " + description + " version: " + maj + "." + min + ", expected at least " + xmaj + "." + xmin;
        }
        else {
            return "";
        }
    }
}
