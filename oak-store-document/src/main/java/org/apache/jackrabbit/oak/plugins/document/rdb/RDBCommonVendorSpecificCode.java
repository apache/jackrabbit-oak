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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum RDBCommonVendorSpecificCode {

    DEFAULT() {
    },

    DB2() {
        @Override
        public Map<String, String> getAdditionalDiagnostics(RDBConnectionHandler ch, String tableName) {
            Connection con = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;
            Map<String, String> result = new HashMap<>();
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
            return result;
        }
    },

    MSSQL() {
        @Override
        public Map<String, String> getAdditionalDiagnostics(RDBConnectionHandler ch, String tableName) {
            Connection con = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;
            Map<String, String> result = new HashMap<>();
            try {
                con = ch.getROConnection();
                String cat = con.getCatalog();
                stmt = con.prepareStatement("SELECT collation_name, create_date FROM sys.databases WHERE name=?");
                stmt.setString(1, cat);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    result.put("collation_name", rs.getString("collation_name"));
                    result.put("create_date", rs.getString("create_date"));
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
            return result;
        }
    },

    MYSQL() {
        @Override
        public Map<String, String> getAdditionalDiagnostics(RDBConnectionHandler ch, String tableName) {
            Connection con = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;
            Map<String, String> result = new HashMap<>();
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
            return result;
        }
    },

    ORACLE() {
        @Override
        public Map<String, String> getAdditionalDiagnostics(RDBConnectionHandler ch, String tableName) {
            Connection con = null;
            Statement stmt = null;
            ResultSet rs = null;
            Map<String, String> result = new HashMap<>();
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
            return result;
        }
    },

    POSTGRES() {
        @Override
        public Map<String, String> getAdditionalDiagnostics(RDBConnectionHandler ch, String tableName) {
            Connection con = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;
            Map<String, String> result = new HashMap<>();
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
            return result;
        }
    };

    private static final Logger LOG = LoggerFactory.getLogger(RDBCommonVendorSpecificCode.class);

    protected String description;

    private RDBCommonVendorSpecificCode() {
    }

    @NotNull
    public Map<String, String> getAdditionalDiagnostics(RDBConnectionHandler ch, String tableName) {
        return Collections.emptyMap();
    }
}
