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

import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

/**
 * Convenience methods dealing with JDBC specifics.
 */
public class RDBJDBCTools {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RDBJDBCTools.class);

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
     * Generate database + driver version diagnostics.
     * 
     * @param md
     *            metadata object
     * @param dbmax
     *            minimal DB major version number (where {@code -1} disables the
     *            check)
     * @param dbmin
     *            minimal DB minor version number
     * @param drmax
     *            minimal driver major version number (where {@code -1} disables
     *            the check)
     * @param drmin
     *            minimal driver minor version number
     * @param dbname
     *            database type
     * @return diagnostics (empty when there's nothing to complain about)
     */
    protected static String versionCheck(DatabaseMetaData md, int dbmax, int dbmin, int drmax, int drmin, String dbname)
            throws SQLException {
        StringBuilder result = new StringBuilder();

        if (dbmax != -1) {
            int maj = md.getDatabaseMajorVersion();
            int min = md.getDatabaseMinorVersion();

            if (maj < dbmax || (maj == dbmax && min < dbmin)) {
                result.append(
                        "Unsupported " + dbname + " version: " + maj + "." + min + ", expected at least " + dbmax + "." + dbmin);
            }
        }

        if (drmax != -1) {
            int maj = md.getDriverMajorVersion();
            int min = md.getDriverMinorVersion();

            if (maj < drmax || (maj == drmax && min < drmin)) {
                if (result.length() != 0) {
                    result.append(", ");
                }
                result.append("Unsupported " + dbname + " driver version: " + md.getDriverName() + " " + maj + "." + min
                        + ", expected at least " + drmax + "." + drmin);
            }
        }

        return result.toString();
    }

    /**
     * Generate database version diagnostics.
     * 
     * @param md
     *            metadata object
     * @param dbmax
     *            minimal DB major version number (where {@code -1} disables the
     *            check)
     * @param dbmin
     *            minimal DB minor version number
     * @param dbname
     *            database type
     * @return diagnostics (empty when there's nothing to complain about)
     */
    protected static String versionCheck(DatabaseMetaData md, int dbmax, int dbmin, String dbname) throws SQLException {
        return versionCheck(md, dbmax, dbmin, -1, -1, dbname);
    }

    /**
     * Closes a {@link Statement}, logging potential problems.
     * @return null
     */
    protected static <T extends Statement> T closeStatement(@CheckForNull T stmt) {
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
     * @return null
     */
    protected static ResultSet closeResultSet(@CheckForNull ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ex) {
                LOG.debug("Closing result set", ex);
            }
        }

        return null;
    }

    /**
     * Provides a component for a {@link PreparedStatement} and a method for
     * setting the parameters within this component
     */
    public interface PreparedStatementComponent {

        /**
         * @return a string suitable for inclusion into a
         *         {@link PreparedStatement}
         */
        @Nonnull
        public String getStatementComponent();

        /**
         * Set the parameters need by the statement component returned by
         * {@link #getStatementComponent()}
         * 
         * @param stmt
         *            the statement
         * @param startIndex
         *            of first parameter to set
         * @return index of next parameter to set
         * @throws SQLException
         */
        public int setParameters(PreparedStatement stmt, int startIndex) throws SQLException;
    }

    /**
     * Appends following SQL condition to the builder: {@code ID in (?,?,?)}.
     * The field name {@code ID} and the number of place holders is
     * configurable. If the number of place holders is greater than
     * {@code maxListLength}, then the condition will have following form:
     * {@code (ID in (?,?,?) or ID in (?,?,?) or ID in (?,?))}
     *
     * @param builder
     *            the condition will be appended here
     * @param field
     *            name of the field
     * @param placeholdersCount
     *            how many ? should be included
     * @param maxListLength
     *            what's the max number of ? in one list
     */
    protected static void appendInCondition(StringBuilder builder, String field, int placeholdersCount, int maxListLength) {
        if (placeholdersCount == 1) {
            builder.append(field).append(" = ?");
        } else if (placeholdersCount > 0) {

            if (placeholdersCount > maxListLength) {
                builder.append('(');
            }

            int i;
            for (i = 0; i < placeholdersCount / maxListLength; i++) {
                if (i > 0) {
                    builder.append(" or ");
                }
                appendInCondition(builder, field, maxListLength);
            }

            if (placeholdersCount % maxListLength > 0) {
                if (i > 0) {
                    builder.append(" or ");
                }
                appendInCondition(builder, field, placeholdersCount % maxListLength);
            }

            if (placeholdersCount > maxListLength) {
                builder.append(')');
            }
        }
    }

    private static void appendInCondition(StringBuilder builder, String field, int placeholdersCount) {
        builder.append(field).append(" in (");
        Joiner.on(',').appendTo(builder, limit(cycle('?'), placeholdersCount));
        builder.append(')');
    }

    // see <https://issues.apache.org/jira/browse/OAK-3843>
    public static final int MAX_IN_CLAUSE = Integer
            .getInteger("org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.MAX_IN_CLAUSE", 2048);

    public static PreparedStatementComponent createInStatement(final String fieldName, final Collection<String> values,
            final boolean binary) {

        if (values.size() > MAX_IN_CLAUSE) {
            throw new IllegalArgumentException("Maximum size of IN clause allowed is " + MAX_IN_CLAUSE + ", but " + values.size() + " was requested");
        }

        return new PreparedStatementComponent() {

            @Override
            public String getStatementComponent() {
                StringBuilder sb = new StringBuilder(values.size() * 3);
                // maximum "in" statement in Oracle takes 1000 values
                appendInCondition(sb, fieldName, values.size(), 1000);
                return sb.toString();
            }

            @Override
            public int setParameters(PreparedStatement stmt, int startIndex) throws SQLException {
                for (String value : values) {
                    if (binary) {
                        try {
                            stmt.setBytes(startIndex++, value.getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException ex) {
                            LOG.error("UTF-8 not supported??", ex);
                            throw new DocumentStoreException(ex);
                        }
                    } else {
                        stmt.setString(startIndex++, value);
                    }
                }
                return startIndex;
            }
        };
    }
}
