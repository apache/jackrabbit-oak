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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.PreparedStatementComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

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
        public String getTableCreationStatement(String tableName, int schema) {
            return ("create table " + tableName
                    + " (ID varchar(512) not null primary key, MODIFIED bigint, HASBINARY smallint, DELETEDONCE smallint, MODCOUNT bigint, CMODCOUNT bigint, DSIZE bigint, "
                    + (schema >= 1 ? "VERSION smallint, " : "")
                    + (schema >= 2 ? "SDTYPE smallint, SDMAXREVTIME bigint, " : "")
                    + "DATA varchar(16384), BDATA bytea)");
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

        @Override
        public Map<String, String> getAdditionalStatistics(RDBConnectionHandler ch, String catalog, String tableName) {
            Map<String, String> result = new HashMap<String, String>();
            Connection con = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;
            SortedSet<String> indexNames = Collections.emptySortedSet();

            // get index names
            try {
                SortedSet<String> in = new TreeSet<String>();
                con = ch.getROConnection();
                stmt = con.prepareStatement("SELECT indexname FROM pg_indexes WHERE tablename=?");
                stmt.setString(1, tableName.toLowerCase(Locale.ENGLISH));
                rs = stmt.executeQuery();
                while (rs.next()) {
                    in.add(rs.getString(1));
                }
                con.commit();
                indexNames = in;
            } catch (SQLException ex) {
                LOG.debug("while getting diagnostics", ex);
            } finally {
                closeResultSet(rs);
                closeStatement(stmt);
                ch.closeConnection(con);
            }

            // table data
            try {
                StringBuilder query = new StringBuilder("SELECT pg_total_relation_size(?), pg_table_size(?), pg_indexes_size(?)");
                indexNames.forEach(name -> query.append(", pg_relation_size(?)"));
                con = ch.getROConnection();
                stmt = con.prepareStatement(query.toString());
                int i = 1;
                stmt.setString(i++, tableName);
                stmt.setString(i++, tableName);
                stmt.setString(i++, tableName);
                for(String name : indexNames) {
                    stmt.setString(i++, name);
                }
                rs = stmt.executeQuery();
                while (rs.next()) {
                    i = 1;
                    result.put("storageSize", rs.getString(i++));
                    result.put("size", rs.getString(i++));
                    result.put("totalIndexSize", rs.getString(i++));
                    for (String name : indexNames) {
                        result.put("indexSizes." + name, rs.getString(i++));
                    }
                }
                con.commit();
            } catch (SQLException ex) {
                LOG.debug("while getting diagnostics", ex);
            } finally {
                closeResultSet(rs);
                closeStatement(stmt);
                ch.closeConnection(con);
            }
            return result;
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

        @Override
        public String getTableCreationStatement(String tableName, int schema) {
            return "create table " + tableName
                    + " (ID varchar(512) not null, MODIFIED bigint, HASBINARY smallint, DELETEDONCE smallint, MODCOUNT bigint, CMODCOUNT bigint, DSIZE bigint, "
                    + (schema >= 1 ? "VERSION smallint, " : "")
                    + (schema >= 2 ? "SDTYPE smallint, SDMAXREVTIME bigint, " : "")
                    + "DATA varchar(16384), BDATA blob(" + 1024 * 1024 * 1024 + "))";
        }

        @Override
        public List<String> getIndexCreationStatements(String tableName, int schema) {
            List<String> statements = new ArrayList<String>();
            String pkName = tableName + "_pk";
            statements.add("create unique index " + pkName + " on " + tableName + " ( ID ) cluster");
            statements.add("alter table " + tableName + " add constraint " + pkName + " primary key ( ID )");
            statements.addAll(super.getIndexCreationStatements(tableName, schema));
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

        @Override
        public Map<String, String> getAdditionalStatistics(RDBConnectionHandler ch, String catalog, String tableName) {

            Map<String, String> result = new HashMap<String, String>();

            Connection con = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;

            // table data
            String tableStats = System.getProperty(SYSPROP_PREFIX + ".DB2.TABLE_STATS",
                    "card npages mpages fpages overflow pctfree avgrowsize stats_time");

            try {
                con = ch.getROConnection();
                stmt = con.prepareStatement("SELECT * FROM syscat.tables WHERE tabschema=? and tabname=?");
                stmt.setString(1, catalog.toUpperCase(Locale.ENGLISH));
                stmt.setString(2, tableName.toUpperCase(Locale.ENGLISH));
                rs = stmt.executeQuery();
                while (rs.next()) {
                    String data = extractFields(rs, tableStats);
                    result.put("_data", data);
                }
                con.commit();
            } catch (SQLException ex) {
                LOG.debug("while getting diagnostics", ex);
            } finally {
                closeResultSet(rs);
                closeStatement(stmt);
                ch.closeConnection(con);
            }

            // index data
            String indexStats = System.getProperty(SYSPROP_PREFIX + ".DB2.INDEX_STATS",
                    "indextype colnames pctfree clusterratio nleaf nlevels fullkeycard density indcard numrids numrids_deleted avgleafkeysize avgnleafkeysize remarks stats_time");

            try {
                con = ch.getROConnection();
                stmt = con.prepareStatement("SELECT * FROM syscat.indexes WHERE tabschema=? and tabname=?");
                stmt.setString(1, catalog.toUpperCase(Locale.ENGLISH));
                stmt.setString(2, tableName.toUpperCase(Locale.ENGLISH));
                rs = stmt.executeQuery();
                while (rs.next()) {
                    String index = rs.getString("indname");
                    String data = extractFields(rs, indexStats);
                    result.put("index." + index + "._data", data);
                }
                con.commit();
            } catch (SQLException ex) {
                LOG.debug("while getting diagnostics", ex);
            } finally {
                closeResultSet(rs);
                closeStatement(stmt);
                ch.closeConnection(con);
            }

            return result;
        }
    },

    ORACLE("Oracle") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            return RDBJDBCTools.versionCheck(md, 12, 1, 12, 2, description);
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
        public String getTableCreationStatement(String tableName, int schema) {
            // see https://issues.apache.org/jira/browse/OAK-1914
            return ("create table " + tableName
                    + " (ID varchar(512) not null primary key, MODIFIED number, HASBINARY number, DELETEDONCE number, MODCOUNT number, CMODCOUNT number, DSIZE number, "
                    + (schema >= 1 ? "VERSION number, " : "")
                    + (schema >= 2 ? "SDTYPE number, SDMAXREVTIME number, " : "")
                    + "DATA varchar(4000), BDATA blob)");
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

        @Override
        public Map<String, String> getAdditionalStatistics(RDBConnectionHandler ch, String catalog, String tableName) {

            Map<String, String> result = new HashMap<String, String>();

            Connection con = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;

            // table data
            String tableStats = System.getProperty(SYSPROP_PREFIX + ".ORACLE.TABLE_STATS",
                    "num_rows blocks avg_row_len sample_size last_analyzed");

            try {
                con = ch.getROConnection();
                stmt = con.prepareStatement("SELECT * FROM user_tables WHERE table_name=?");
                stmt.setString(1, tableName.toUpperCase(Locale.ENGLISH));
                rs = stmt.executeQuery();
                while (rs.next()) {
                    String data = extractFields(rs, tableStats);
                    result.put("_data", data.toString());
                }
                con.commit();
            } catch (SQLException ex) {
                LOG.debug("while getting diagnostics", ex);
            } finally {
                closeResultSet(rs);
                closeStatement(stmt);
                ch.closeConnection(con);
            }

            // index data
            String indexStats = System.getProperty(SYSPROP_PREFIX + ".ORACLE.INDEX_STATS",
                    "blevel leaf_blocks distinct_keys avg_leaf_blocks_per_key avg_data_blocks_per_key clustering_factor num_rows sample_size last_analyzed");

            try {
                con = ch.getROConnection();
                stmt = con.prepareStatement("SELECT * FROM user_indexes WHERE table_name=?");
                stmt.setString(1, tableName.toUpperCase(Locale.ENGLISH));
                rs = stmt.executeQuery();
                while (rs.next()) {
                    String index = rs.getString("index_name");
                    String data = extractFields(rs, indexStats);
                    result.put("index." + index + "._data", data);
                }
                con.commit();
            } catch (SQLException ex) {
                LOG.debug("while getting diagnostics", ex);
            } finally {
                closeResultSet(rs);
                closeStatement(stmt);
                ch.closeConnection(con);
            }

            return result;
        }

        @Override
        public String getSmallintType() {
            return "number";
        }

        @Override
        public String getBigintType() {
            return "number";
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
        public String getTableCreationStatement(String tableName, int schema) {
            // see https://issues.apache.org/jira/browse/OAK-1913
            return ("create table " + tableName
                    + " (ID varbinary(512) not null primary key, MODIFIED bigint, HASBINARY smallint, DELETEDONCE smallint, MODCOUNT bigint, CMODCOUNT bigint, DSIZE bigint, "
                    + (schema >= 1 ? "VERSION smallint, " : "")
                    + (schema >= 2 ? "SDTYPE smallint, SDMAXREVTIME bigint, " : "")
                    + "DATA varchar(16000), BDATA longblob)");
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
                rs.close();
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

        @Override
        public Map<String, String> getAdditionalStatistics(RDBConnectionHandler ch, String catalog, String tableName) {

            Map<String, String> result = new HashMap<String, String>();

            Connection con = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;

            // table data
            String tableStats = System.getProperty(SYSPROP_PREFIX + ".MYSQL.TABLE_STATS",
                    "engine version row_format rows avg_row_length data_length index_length data_free collation");

            try {
                con = ch.getROConnection();
                stmt = con.prepareStatement("show table status from " + catalog + " where name=?");
                stmt.setString(1, tableName.toUpperCase(Locale.ENGLISH));
                rs = stmt.executeQuery();
                while (rs.next()) {
                    String data = extractFields(rs, tableStats);
                    result.put("_data", data.toString());
                }
                con.commit();
            } catch (SQLException ex) {
                LOG.debug("while getting diagnostics", ex);
            } finally {
                closeResultSet(rs);
                closeStatement(stmt);
                ch.closeConnection(con);
            }

            // index data
            String indexStats = System.getProperty(SYSPROP_PREFIX + ".MYSQL.INDEX_STATS",
                    "column_name cardinality index_type sub_part");

            try {
                con = ch.getROConnection();
                stmt = con.prepareStatement("show index from " + tableName + " in " + catalog);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    String index = rs.getString("key_name");
                    String data = extractFields(rs, indexStats);
                    result.put("index." + index + "._data", data);
                }
                con.commit();
            } catch (SQLException ex) {
                LOG.debug("while getting diagnostics", ex);
            } finally {
                closeResultSet(rs);
                closeStatement(stmt);
                ch.closeConnection(con);
            }

            return result;
        }
    },

    MSSQL("Microsoft SQL Server") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            return RDBJDBCTools.versionCheck(md, 11, 0, description);
        }

        @Override
        public String getTableCreationStatement(String tableName, int schema) {
            // see https://issues.apache.org/jira/browse/OAK-2395
            return ("create table " + tableName
                    + " (ID varbinary(512) not null primary key, MODIFIED bigint, HASBINARY smallint, DELETEDONCE smallint, MODCOUNT bigint, CMODCOUNT bigint, DSIZE bigint, "
                    + (schema >= 1 ? "VERSION smallint, " : "")
                    + (schema >= 2 ? "SDTYPE smallint, SDMAXREVTIME bigint, " : "")
                    + "DATA nvarchar(4000), BDATA varbinary(max))");
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
                rs.close();
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

    private static final String SYSPROP_PREFIX = "org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore";

    // whether to create indices
    private static final String CREATEINDEX = System.getProperty(SYSPROP_PREFIX + ".CREATEINDEX", "");

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
    public String getTableCreationStatement(String tableName, int schema) {
        return "create table " + tableName
                + " (ID varchar(512) not null primary key, MODIFIED bigint, HASBINARY smallint, DELETEDONCE smallint, MODCOUNT bigint, CMODCOUNT bigint, DSIZE bigint, "
                + (schema >= 1 ? "VERSION smallint, " : "")
                + (schema >= 2 ? "SDTYPE smallint, SDMAXREVTIME bigint, " : "")
                + "DATA varchar(16384), BDATA blob(" + 1024 * 1024 * 1024 + "))";
    }

    public List<String> getIndexCreationStatements(String tableName, int level) {
        List<String> result = Lists.newArrayList();
        if (CREATEINDEX.equals("modified-id")) {
            result.add("create index " + tableName + "_MI on " + tableName + " (MODIFIED, ID)");
        } else if (CREATEINDEX.equals("id-modified")) {
            result.add("create index " + tableName + "_MI on " + tableName + " (ID, MODIFIED)");
        } else if (CREATEINDEX.equals("modified")) {
            result.add("create index " + tableName + "_MI on " + tableName + " (MODIFIED)");
        }
        if (level == 2) {
            result.add("create index " + tableName + "_VSN on " + tableName + " (VERSION)");
            result.add("create index " + tableName + "_SDT on " + tableName + " (SDTYPE)");
            result.add("create index " + tableName + "_SDM on " + tableName + " (SDMAXREVTIME)");
        }
        return result;
    }

    /**
     * Returns additional DB-specific statistics, augmenting the return value of
     * {@link RDBDocumentStore#getStats()}.
     * <p>
     * Where applicable, the following fields are returned similar to the output
     * for MongoDB:
     * <dl>
     * <dt>storageSize</dt>
     * <dd>total size of table</dd>
     * <dt>size</dt>
     * <dd>size of table (excl. indexes)</dd>
     * <dt>totalIndexSize</dt>
     * <dd>total size of all indexes</dd>
     * <dt>indexSizes.<em>indexName</em></dt>
     * <dd>size of individual indexes</dd>
     * </dl>
     * <p>
     * Additionally, a information obtained from the databases system
     * tables/views can be included:
     * <dl>
     * <dt>_data</dt>
     * <dd>table specific data</dd>
     * <dt><em>indexName</em>._data</dt>
     * <dd>index specific data</dd>
     * </dl>
     * <p>
     * These fields will just contain DB-specific name/value pairs obtained from
     * the database. The exact fields to fetch are preconfigured by can be tuned
     * using system properties, such as:
     * 
     * <pre>
     * -Dorg.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.DB2.TABLE_STATS="card npages mpages fpages overflow pctfree avgrowsize stats_time"
     * -Dorg.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.DB2.INDEX_STATS="indextype colnames pctfree clusterratio nleaf nlevels fullkeycard density indcard numrids numrids_deleted avgleafkeysize avgnleafkeysize remarks stats_time"
     * </pre>
     * <p>
     * (this currently applies to DB types: {@link #DB2}, {@link #ORACLE}, and
     * {@link #MYSQL}).
     * <p>
     * See links below for the definition of the individual fields:
     * <ul>
     * <li>{@link #POSTGRES} - <a href=
     * "https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBOBJECT">PostgreSQL
     * 9.6.6 Documentation - 9.26.7. Database Object Management Functions</a>
     * <li>{@link #DB2} - DB2 10.5 for Linux, UNIX, and Windows: <a href=
     * "https://www.ibm.com/support/knowledgecenter/en/SSEPGG_10.5.0/com.ibm.db2.luw.sql.ref.doc/doc/r0001063.html">SYSCAT.TABLES
     * catalog view</a>, <a href=
     * "https://www.ibm.com/support/knowledgecenter/en/SSEPGG_10.5.0/com.ibm.db2.luw.sql.ref.doc/doc/r0001047.html">SYSCAT.INDEXES
     * catalog view</a>
     * <li>{@link #ORACLE} - Oracle Database Online Documentation 12c Release 1
     * (12.1): <a href=
     * "https://docs.oracle.com/database/121/REFRN/GUID-6823CD28-0681-468E-950B-966C6F71325D.htm#REFRN20286">3.118
     * ALL_TABLES</a>, <a href=
     * "https://docs.oracle.com/database/121/REFRN/GUID-E39825BA-70AC-45D8-AF30-C7FF561373B6.htm#REFRN20088">2.127
     * ALL_INDEXES</a>
     * <li>{@link #MYSQL} - MySQL 5.7 Reference Manual: <a href=
     * "https://dev.mysql.com/doc/refman/5.7/en/show-table-status.html">13.7.5.36
     * SHOW TABLE STATUS Syntax</a>, <a href=
     * "https://dev.mysql.com/doc/refman/5.7/en/show-index.html">13.7.5.22 SHOW
     * INDEX Syntax</a>
     * </ul>
     */
    public String getAdditionalDiagnostics(RDBConnectionHandler ch, String tableName) {
        return "";
    }

    public Map<String, String> getAdditionalStatistics(RDBConnectionHandler ch, String catalog, String tableName) {
        return Collections.emptyMap();
    }

    public String getSmallintType() {
        return "smallint";
    }

    public String getBigintType() {
        return "bigint";
    }

    /**
     * Statements needed to upgrade the DB
     *
     * @return the table modification string
     */
    public List<String> getTableUpgradeStatements(String tableName, int level) {
        String smallint = getSmallintType();
        String bigint = getBigintType();
        if (level == 1) {
            return Collections.singletonList("alter table " + tableName + " add VERSION " + smallint);
        } else if (level == 2) {
            String[] statements = new String[] { "alter table " + tableName + " add SDTYPE " + smallint,
                    "alter table " + tableName + " add SDMAXREVTIME " + bigint,
                    "create index " + tableName + "_VSN on " + tableName + " (VERSION)",
                    "create index " + tableName + "_SDT on " + tableName + " (SDTYPE)",
                    "create index " + tableName + "_SDM on " + tableName + " (SDMAXREVTIME)", };
            return Arrays.asList(statements);
        } else {
            throw new IllegalArgumentException("level must be 1 or 2");
        }
    }

    protected String description;

    protected String extractFields(ResultSet rs, String indexStats) throws SQLException {
        StringBuilder data = new StringBuilder();
        for (String f : indexStats.split(" ")) {
            String fn = f.trim();
            if (!fn.isEmpty()) {
                if (data.length() != 0) {
                    data.append(", ");
                }
                String v = rs.getString(fn);
                data.append(fn).append(": ").append(v == null ? v : v.trim());
            }
        }
        return data.toString();
    }

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
