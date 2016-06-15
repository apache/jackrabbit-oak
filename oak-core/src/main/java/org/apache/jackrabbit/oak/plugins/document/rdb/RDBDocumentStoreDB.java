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

import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.closeResultSet;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.closeStatement;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.PreparedStatementComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines variation in the capabilities of different RDBs.
 */
public enum RDBDocumentStoreDB {

    DEFAULT("default") {
    },

    H2("H2") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            return RDBJDBCTools.versionCheck(md, 1, 4, description);
        }

        @Override
        public String getInitializationStatement() {
            return "create alias if not exists unix_timestamp as $$ long unix_timestamp() { return System.currentTimeMillis()/1000L; } $$;";
        }

        @Override
        public String getCurrentTimeStampInSecondsSyntax() {
            return "select unix_timestamp()";
        }
    },

    DERBY("Apache Derby") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            return RDBJDBCTools.versionCheck(md, 10, 11, description);
        }

        @Override
        public boolean allowsCaseInSelect() {
            return false;
        }
    },

    POSTGRES("PostgreSQL") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            String result = RDBJDBCTools.versionCheck(md, 9, 5, 9, 4, description);

            if (result.isEmpty()) {
                // special case: we need 9.4.1208 or newer (see OAK-3977)
                if (md.getDriverMajorVersion() == 9 && md.getDriverMinorVersion() == 4) {
                    String versionString = md.getDriverVersion();
                    String scanfor = "9.4.";
                    int p = versionString.indexOf(scanfor);
                    if (p >= 0) {
                        StringBuilder build = new StringBuilder();
                        for (char c : versionString.substring(p + scanfor.length()).toCharArray()) {
                            if (c >= '0' && c <= '9') {
                                build.append(c);
                            } else {
                                break;
                            }
                        }
                        if (Integer.parseInt(build.toString()) < 1208) {
                            result = "Unsupported " + description + " driver version: " + md.getDriverVersion() + ", found build "
                                    + build + ", but expected at least build 1208";
                        }
                    }
                }
            }

            return result;
        }

        @Override
        public String getCurrentTimeStampInSecondsSyntax() {
            return "select extract(epoch from now())::integer";
        }

        @Override
        public String getTableCreationStatement(String tableName) {
            return ("create table " + tableName + " (ID varchar(512) not null primary key, MODIFIED bigint, HASBINARY smallint, DELETEDONCE smallint, MODCOUNT bigint, CMODCOUNT bigint, DSIZE bigint, DATA varchar(16384), BDATA bytea)");
        }

        @Override
        public String getAdditionalDiagnostics(RDBConnectionHandler ch, String tableName) {
            Connection con = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;
            Map<String, String> result = new HashMap<String, String>();
            try {
                con = ch.getROConnection();
                String cat = con.getCatalog();
                stmt = con.prepareStatement("SELECT pg_encoding_to_char(encoding), datcollate FROM pg_database WHERE datname=?");
                stmt.setString(1, cat);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    result.put("pg_encoding_to_char(encoding)", rs.getString(1));
                    result.put("datcollate", rs.getString(2));
                }
                stmt.close();
                con.commit();
            } catch (SQLException ex) {
                LOG.debug("while getting diagnostics", ex);
            } finally {
                closeResultSet(rs);
                closeStatement(stmt);
                ch.closeConnection(con);
            }
            return result.toString();
        }
    },

    DB2("DB2") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            return RDBJDBCTools.versionCheck(md, 10, 5, description);
        }

        @Override
        public String getCurrentTimeStampInSecondsSyntax() {
            return "select cast (days(current_timestamp - current_timezone) - days('1970-01-01') as integer) * 86400 + midnight_seconds(current_timestamp - current_timezone) from sysibm.sysdummy1";
        }

        public String getTableCreationStatement(String tableName) {
            return "create table " + tableName
                    + " (ID varchar(512) not null, MODIFIED bigint, HASBINARY smallint, DELETEDONCE smallint, MODCOUNT bigint, CMODCOUNT bigint, DSIZE bigint, DATA varchar(16384), BDATA blob("
                    + 1024 * 1024 * 1024 + "))";
        }

        public List<String> getIndexCreationStatements(String tableName) {
            List<String> statements = new ArrayList<String>();
            String pkName = tableName + "_pk";
            statements.add("create unique index " + pkName + " on " + tableName + " ( ID ) cluster");
            statements.add("alter table " + tableName + " add constraint " + pkName + " primary key ( ID )");
            statements.addAll(super.getIndexCreationStatements(tableName));
            return statements;
        }

        @Override
        public String getAdditionalDiagnostics(RDBConnectionHandler ch, String tableName) {
            Connection con = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;
            Map<String, String> result = new HashMap<String, String>();
            try {
                con = ch.getROConnection();

                // schema name will only be available with JDK 1.7
                String conSchema = ch.getSchema(con);

                StringBuilder sb = new StringBuilder();
                sb.append("SELECT CODEPAGE, COLLATIONSCHEMA, COLLATIONNAME, TABSCHEMA FROM SYSCAT.COLUMNS WHERE COLNAME=? and COLNO=0 AND UPPER(TABNAME)=UPPER(?)");
                if (conSchema != null) {
                    conSchema = conSchema.trim();
                    sb.append(" AND UPPER(TABSCHEMA)=UPPER(?)");
                }
                stmt = con.prepareStatement(sb.toString());
                stmt.setString(1, "ID");
                stmt.setString(2, tableName);
                if (conSchema != null) {
                    stmt.setString(3, conSchema);
                }

                rs = stmt.executeQuery();
                while (rs.next() && result.size() < 20) {
                    String schema = rs.getString("TABSCHEMA").trim();
                    result.put(schema + ".CODEPAGE", rs.getString("CODEPAGE").trim());
                    result.put(schema + ".COLLATIONSCHEMA", rs.getString("COLLATIONSCHEMA").trim());
                    result.put(schema + ".COLLATIONNAME", rs.getString("COLLATIONNAME").trim());
                }
                stmt.close();
                con.commit();
            } catch (SQLException ex) {
                LOG.debug("while getting diagnostics", ex);
            } finally {
                closeResultSet(rs);
                closeStatement(stmt);
                ch.closeConnection(con);
            }
            return result.toString();
        }
    },

    ORACLE("Oracle") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            return RDBJDBCTools.versionCheck(md, 12, 1, 12, 1, description);
        }

        @Override
        public String getCurrentTimeStampInSecondsSyntax() {
            return "select (trunc(sys_extract_utc(systimestamp)) - to_date('01/01/1970', 'MM/DD/YYYY')) * 24 * 60 * 60 + to_number(to_char(sys_extract_utc(systimestamp), 'SSSSS')) from dual";
        }

        @Override
        public String getInitializationStatement() {
            // see https://issues.apache.org/jira/browse/OAK-1914
            // for some reason, the default for NLS_SORT is incorrect
            return ("ALTER SESSION SET NLS_SORT='BINARY'");
        }

        @Override
        public String getTableCreationStatement(String tableName) {
            // see https://issues.apache.org/jira/browse/OAK-1914
            return ("create table " + tableName + " (ID varchar(512) not null primary key, MODIFIED number, HASBINARY number, DELETEDONCE number, MODCOUNT number, CMODCOUNT number, DSIZE number, DATA varchar(4000), BDATA blob)");
        }

        @Override
        public String getAdditionalDiagnostics(RDBConnectionHandler ch, String tableName) {
            Connection con = null;
            Statement stmt = null;
            ResultSet rs = null;
            Map<String, String> result = new HashMap<String, String>();
            try {
                con = ch.getROConnection();
                stmt = con.createStatement();
                rs = stmt
                        .executeQuery("SELECT PARAMETER, VALUE from NLS_DATABASE_PARAMETERS WHERE PARAMETER IN ('NLS_COMP', 'NLS_CHARACTERSET')");
                while (rs.next()) {
                    result.put(rs.getString(1), rs.getString(2));
                }
                stmt.close();
                con.commit();
            } catch (SQLException ex) {
                LOG.debug("while getting diagnostics", ex);
            } finally {
                closeResultSet(rs);
                closeStatement(stmt);
                ch.closeConnection(con);
            }
            return result.toString();
        }
    },

    MYSQL("MySQL") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            return RDBJDBCTools.versionCheck(md, 5, 5, description);
        }

        @Override
        public String getCurrentTimeStampInSecondsSyntax() {
            return "select unix_timestamp()";
        }

        @Override
        public String getTableCreationStatement(String tableName) {
            // see https://issues.apache.org/jira/browse/OAK-1913
            return ("create table " + tableName + " (ID varbinary(512) not null primary key, MODIFIED bigint, HASBINARY smallint, DELETEDONCE smallint, MODCOUNT bigint, CMODCOUNT bigint, DSIZE bigint, DATA varchar(16000), BDATA longblob)");
        }

        @Override
        public FETCHFIRSTSYNTAX getFetchFirstSyntax() {
            return FETCHFIRSTSYNTAX.LIMIT;
        }

        @Override
        public PreparedStatementComponent getConcatQuery(final String appendData, final int dataOctetLimit) {
            return new PreparedStatementComponent() {

                @Override
                public String getStatementComponent() {
                    return "CONCAT(DATA, ?)";
                }

                @Override
                public int setParameters(PreparedStatement stmt, int startIndex) throws SQLException {
                    stmt.setString(startIndex++, appendData);
                    return startIndex;
                }
            };
        }

        @Override
        public String getAdditionalDiagnostics(RDBConnectionHandler ch, String tableName) {
            Connection con = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;
            Map<String, String> result = new HashMap<String, String>();
            try {
                con = ch.getROConnection();
                stmt = con.prepareStatement("SHOW TABLE STATUS LIKE ?");
                stmt.setString(1, tableName);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    result.put("collation", rs.getString("Collation"));
                }
                rs.close();
                stmt.close();
                stmt = con.prepareStatement(
                        "SHOW VARIABLES WHERE variable_name LIKE 'character\\_set\\_%' OR variable_name LIKE 'collation%' OR variable_name = 'max_allowed_packet'");
                rs = stmt.executeQuery();
                while (rs.next()) {
                    result.put(rs.getString(1), rs.getString(2));
                }
                stmt.close();
                con.commit();
            } catch (SQLException ex) {
                LOG.debug("while getting diagnostics", ex);
            } finally {
                closeResultSet(rs);
                closeStatement(stmt);
                ch.closeConnection(con);
            }
            return result.toString();
        }
    },

    MSSQL("Microsoft SQL Server") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            return RDBJDBCTools.versionCheck(md, 11, 0, description);
        }

        @Override
        public String getTableCreationStatement(String tableName) {
            // see https://issues.apache.org/jira/browse/OAK-2395
            return ("create table " + tableName + " (ID varbinary(512) not null primary key, MODIFIED bigint, HASBINARY smallint, DELETEDONCE smallint, MODCOUNT bigint, CMODCOUNT bigint, DSIZE bigint, DATA nvarchar(4000), BDATA varbinary(max))");
        }

        @Override
        public FETCHFIRSTSYNTAX getFetchFirstSyntax() {
            return FETCHFIRSTSYNTAX.TOP;
        }

        @Override
        public PreparedStatementComponent getConcatQuery(final String appendData, final int dataOctetLimit) {
            return new PreparedStatementComponent() {

                @Override
                // this statement ensures that SQL server will generate an exception on overflow
                public String getStatementComponent() {
                    return "CASE WHEN LEN(DATA) < ? THEN (DATA + CAST(? AS nvarchar(" + dataOctetLimit
                            + "))) ELSE (DATA + CAST(DATA AS nvarchar(max))) END";
                }

                @Override
                public int setParameters(PreparedStatement stmt, int startIndex) throws SQLException {
                    stmt.setInt(startIndex++, dataOctetLimit - appendData.length());
                    stmt.setString(startIndex++, appendData);
                    return startIndex;
                }
            };
        }

        @Override
        public String getCurrentTimeStampInSecondsSyntax() {
            return "select datediff(second, dateadd(second, datediff(second, getutcdate(), getdate()), '1970-01-01'), getdate())";
        }

        @Override
        public String getAdditionalDiagnostics(RDBConnectionHandler ch, String tableName) {
            Connection con = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;
            Map<String, String> result = new HashMap<String, String>();
            try {
                con = ch.getROConnection();
                String cat = con.getCatalog();
                stmt = con.prepareStatement("SELECT collation_name FROM sys.databases WHERE name=?");
                stmt.setString(1, cat);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    result.put("collation_name", rs.getString(1));
                }
                stmt.close();
                con.commit();
            } catch (SQLException ex) {
                LOG.debug("while getting diagnostics", ex);
            } finally {
                closeResultSet(rs);
                closeStatement(stmt);
                ch.closeConnection(con);
            }
            return result.toString();
        }
    };

    private static final Logger LOG = LoggerFactory.getLogger(RDBDocumentStoreDB.class);

    // whether to create indices
    private static final String CREATEINDEX = System.getProperty(
            "org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.CREATEINDEX", "");

    public enum FETCHFIRSTSYNTAX {
        FETCHFIRST, LIMIT, TOP
    };

    /**
     * Check the database brand and version
     */
    public String checkVersion(DatabaseMetaData md) throws SQLException {
        return "Unknown database type: " + md.getDatabaseProductName();
    }

    /**
     * Allows case in select. Default true.
     */
    public boolean allowsCaseInSelect() {
        return true;
    }

    /**
     * Query syntax for "FETCH FIRST"
     */
    public FETCHFIRSTSYNTAX getFetchFirstSyntax() {
        return FETCHFIRSTSYNTAX.FETCHFIRST;
    }

    /**
     * Query syntax for current time in ms since the epoch
     * 
     * @return the query syntax or empty string when no such syntax is available
     */
    public String getCurrentTimeStampInSecondsSyntax() {
        // unfortunately, we don't have a portable statement for this
        return "";
    }

    /**
     * Returns the CONCAT function or its equivalent function or sub-query. Note
     * that the function MUST NOT cause a truncated value to be written!
     *
     * @param appendData
     *            string to be inserted
     * @param dataOctetLimit
     *            expected capacity of data column
     */
    public PreparedStatementComponent getConcatQuery(final String appendData, final int dataOctetLimit) {

        return new PreparedStatementComponent() {

            @Override
            public String getStatementComponent() {
                return "DATA || CAST(? AS varchar(" + dataOctetLimit + "))";
            }

            @Override
            public int setParameters(PreparedStatement stmt, int startIndex) throws SQLException {
                stmt.setString(startIndex++, appendData);
                return startIndex;
            }
        };
    }

    /**
     * Query for any required initialization of the DB.
     * 
     * @return the DB initialization SQL string
     */
    public @Nonnull String getInitializationStatement() {
        return "";
    }

    /**
     * Table creation statement string
     *
     * @param tableName
     * @return the table creation string
     */
    public String getTableCreationStatement(String tableName) {
        return "create table "
                + tableName
                + " (ID varchar(512) not null primary key, MODIFIED bigint, HASBINARY smallint, DELETEDONCE smallint, MODCOUNT bigint, CMODCOUNT bigint, DSIZE bigint, DATA varchar(16384), BDATA blob("
                + 1024 * 1024 * 1024 + "))";
    }

    public List<String> getIndexCreationStatements(String tableName) {
        if (CREATEINDEX.equals("modified-id")) {
            return Collections.singletonList("create index " + tableName + "_MI on " + tableName + " (MODIFIED, ID)");
        } else if (CREATEINDEX.equals("id-modified")) {
            return Collections.singletonList("create index " + tableName + "_MI on " + tableName + " (ID, MODIFIED)");
        } else if (CREATEINDEX.equals("modified")) {
            return Collections.singletonList("create index " + tableName + "_MI on " + tableName + " (MODIFIED)");
        } else {
            return Collections.emptyList();
        }
    }

    public String getAdditionalDiagnostics(RDBConnectionHandler ch, String tableName) {
        return "";
    }

    protected String description;

    private RDBDocumentStoreDB(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return this.description;
    }

    @Nonnull
    public static RDBDocumentStoreDB getValue(String desc) {
        for (RDBDocumentStoreDB db : RDBDocumentStoreDB.values()) {
            if (db.description.equals(desc)) {
                return db;
            } else if (db == DB2 && desc.startsWith("DB2/")) {
                return db;
            }
        }

        LOG.error("DB type " + desc + " unknown, trying default settings");
        DEFAULT.description = desc + " - using default settings";
        return DEFAULT;
    }
}
