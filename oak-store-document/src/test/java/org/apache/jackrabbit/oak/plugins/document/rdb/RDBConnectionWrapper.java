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

import java.lang.reflect.InvocationTargetException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class RDBConnectionWrapper implements Connection {

    private final RDBDataSourceWrapper datasource;
    private final Connection connection;
    private final long constart;
    private boolean isReadOnly = false;

    public RDBConnectionWrapper(RDBDataSourceWrapper datasource, Connection connection) {
        this.datasource = datasource;
        this.connection = connection;
        this.constart = System.nanoTime();
    }

    // needed in Java 7...
    @SuppressWarnings("unused")
    public void abort(Executor arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void clearWarnings() throws SQLException {
        connection.clearWarnings();
    }

    public void close() throws SQLException {
        long start = System.nanoTime();
        SQLException x = null;
        try {
            connection.close();
        } catch (SQLException ex) {
            x = ex;
            throw ex;
        } finally {
            List<RDBLogEntry> l = datasource.getLog();
            if (l != null) {
                String message = "connection.close() after " + ((start - constart) / 1000) + "us";
                if (x != null) {
                    message += " " + x.getMessage();
                }
                l.add(new RDBLogEntry(start, message));
            }
        }
    }

    public void commit() throws SQLException {
        long start = System.nanoTime();
        SQLException x = null;
        try {
            connection.commit();
            if (this.datasource.getTemporaryCommitException() != null && !isReadOnly) {
                throw new SQLException(this.datasource.getTemporaryCommitException());
            }
        } catch (SQLException ex) {
            x = ex;
            throw ex;
        } finally {
            List<RDBLogEntry> l = datasource.getLog();
            if (l != null) {
                String message = "connection.commit()";
                if (x != null) {
                    message += " " + x.getMessage();
                }
                l.add(new RDBLogEntry(start, message));
            }
        }
    }

    public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
        return connection.createArrayOf(arg0, arg1);
    }

    public Blob createBlob() throws SQLException {
        return connection.createBlob();
    }

    public Clob createClob() throws SQLException {
        return connection.createClob();
    }

    public NClob createNClob() throws SQLException {
        return connection.createNClob();
    }

    public SQLXML createSQLXML() throws SQLException {
        return connection.createSQLXML();
    }

    public Statement createStatement() throws SQLException {
        Statement statement = connection.createStatement();
        return new RDBStatementWrapper(statement, datasource.doFailAlterTableAddColumnStatements());
    }

    public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException {
        return connection.createStatement(arg0, arg1, arg2);
    }

    public Statement createStatement(int arg0, int arg1) throws SQLException {
        return connection.createStatement(arg0, arg1);
    }

    public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
        return connection.createStruct(arg0, arg1);
    }

    public boolean getAutoCommit() throws SQLException {
        return connection.getAutoCommit();
    }

    public String getCatalog() throws SQLException {
        return connection.getCatalog();
    }

    public Properties getClientInfo() throws SQLException {
        return connection.getClientInfo();
    }

    public String getClientInfo(String arg0) throws SQLException {
        return connection.getClientInfo(arg0);
    }

    public int getHoldability() throws SQLException {
        return connection.getHoldability();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return connection.getMetaData();
    }

    // needed in Java 7...
    @SuppressWarnings("unused")
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    // needed in Java 7...
    @SuppressWarnings("unused")
    public String getSchema() throws SQLException {
        try {
            return (String) connection.getClass().getMethod("getSchema").invoke(connection);
        } catch (InvocationTargetException ex) {
            if (ex.getCause() instanceof SQLException) {
                throw (SQLException) ex.getCause();
            } else {
                // best effort otherwise
                return null;
            }
        } catch (Throwable ex) {
            // best effort otherwise
            return null;
        }
    }

    public int getTransactionIsolation() throws SQLException {
        return connection.getTransactionIsolation();
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return connection.getTypeMap();
    }

    public SQLWarning getWarnings() throws SQLException {
        return connection.getWarnings();
    }

    public boolean isClosed() throws SQLException {
        return connection.isClosed();
    }

    public boolean isReadOnly() throws SQLException {
        return connection.isReadOnly();
    }

    public boolean isValid(int arg0) throws SQLException {
        return connection.isValid(arg0);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return connection.isWrapperFor(iface);
    }

    public String nativeSQL(String arg0) throws SQLException {
        return connection.nativeSQL(arg0);
    }

    public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException {
        return connection.prepareCall(arg0, arg1, arg2, arg3);
    }

    public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException {
        return connection.prepareCall(arg0, arg1, arg2);
    }

    public CallableStatement prepareCall(String arg0) throws SQLException {
        return connection.prepareCall(arg0);
    }

    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException {
        return connection.prepareStatement(arg0, arg1, arg2, arg3);
    }

    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2) throws SQLException {
        return connection.prepareStatement(arg0, arg1, arg2);
    }

    public PreparedStatement prepareStatement(String arg0, int arg1) throws SQLException {
        return connection.prepareStatement(arg0, arg1);
    }

    public PreparedStatement prepareStatement(String arg0, int[] arg1) throws SQLException {
        return connection.prepareStatement(arg0, arg1);
    }

    public PreparedStatement prepareStatement(String arg0, String[] arg1) throws SQLException {
        return connection.prepareStatement(arg0, arg1);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        long start = System.nanoTime();
        SQLException x = null;
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            return new RDBPreparedStatementWrapper(datasource, statement);
        } catch (SQLException ex) {
            x = ex;
            throw ex;
        } finally {
            List<RDBLogEntry> l = datasource.getLog();
            if (l != null) {
                String message = "connection.prepareStatement(\"" + sql + "\")";
                if (x != null) {
                    message += " " + x.getMessage();
                }
                l.add(new RDBLogEntry(start, message));
            }
        }
    }

    public void releaseSavepoint(Savepoint arg0) throws SQLException {
        connection.releaseSavepoint(arg0);
    }

    public void rollback() throws SQLException {
        long start = System.nanoTime();
        SQLException x = null;
        try {
            connection.rollback();
        } catch (SQLException ex) {
            x = ex;
            throw ex;
        } finally {
            List<RDBLogEntry> l = datasource.getLog();
            if (l != null) {
                String message = "connection.rollback()";
                if (x != null) {
                    message += " " + x.getMessage();
                }
                l.add(new RDBLogEntry(start, message));
            }
        }
    }

    public void rollback(Savepoint arg0) throws SQLException {
        connection.rollback(arg0);
    }

    public void setAutoCommit(boolean arg0) throws SQLException {
        connection.setAutoCommit(arg0);
    }

    public void setCatalog(String arg0) throws SQLException {
        connection.setCatalog(arg0);
    }

    public void setClientInfo(Properties arg0) throws SQLClientInfoException {
        connection.setClientInfo(arg0);
    }

    public void setClientInfo(String arg0, String arg1) throws SQLClientInfoException {
        connection.setClientInfo(arg0, arg1);
    }

    public void setHoldability(int arg0) throws SQLException {
        connection.setHoldability(arg0);
    }

    // needed in Java 7...
    @SuppressWarnings("unused")
    public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        this.isReadOnly = readOnly;
        connection.setReadOnly(readOnly);
    }

    public Savepoint setSavepoint() throws SQLException {
        return connection.setSavepoint();
    }

    public Savepoint setSavepoint(String arg0) throws SQLException {
        return connection.setSavepoint(arg0);
    }

    // needed in Java 7...
    @SuppressWarnings("unused")
    public void setSchema(String arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void setTransactionIsolation(int arg0) throws SQLException {
        connection.setTransactionIsolation(arg0);
    }

    public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
        connection.setTypeMap(arg0);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return connection.unwrap(iface);
    }
}
