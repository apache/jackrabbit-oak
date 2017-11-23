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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;

import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentStoreTest;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests measuring the performance of various {@link RDBDocumentStore}
 * operations.
 * <p>
 * These tests are disabled by default due to their long running time. On the
 * command line specify {@code -DRDBDocumentStorePerformanceTest=true} to enable
 * them.
 */
public class RDBDocumentStorePerformanceTest extends AbstractDocumentStoreTest {

    private static final Logger LOG = LoggerFactory.getLogger(RDBDocumentStorePerformanceTest.class);
    private static final boolean ENABLED = Boolean.getBoolean(RDBDocumentStorePerformanceTest.class.getSimpleName());

    public RDBDocumentStorePerformanceTest(DocumentStoreFixture dsf) {
        super(dsf);
        assumeTrue(ENABLED);
        assumeTrue(super.rdbDataSource != null);
    }

    @Test
    public void testPerfUpdateLimit() throws SQLException, UnsupportedEncodingException {
        internalTestPerfUpdateLimit("testPerfUpdateLimit", "raw row update (set long)", 0);
    }

    @Test
    public void testPerfUpdateLimitString() throws SQLException, UnsupportedEncodingException {
        internalTestPerfUpdateLimit("testPerfUpdateLimitString", "raw row update (set long/string)", 1);
    }

    @Test
    public void testPerfUpdateLimitStringBlob() throws SQLException, UnsupportedEncodingException {
        internalTestPerfUpdateLimit("testPerfUpdateLimitStringBlob", "raw row update (set long/string/blob)", 2);
    }

    @Test
    public void testPerfUpdateAppendString() throws SQLException, UnsupportedEncodingException {
        internalTestPerfUpdateLimit("testPerfUpdateAppendString", "raw row update (append string)", 3);
    }

    @Test
    public void testPerfUpdateGrowingDoc() throws SQLException, UnsupportedEncodingException {
        internalTestPerfUpdateLimit("testPerfUpdateGrowingDoc", "raw row update (string + blob)", 4);
    }

    private void internalTestPerfUpdateLimit(String name, String desc, int mode) throws SQLException, UnsupportedEncodingException {
        String key = name;
        Connection connection = null;
        String table = DocumentStoreFixture.TABLEPREFIX + "NODES";

        // create test node
        try {
            connection = super.rdbDataSource.getConnection();
            connection.setAutoCommit(false);
            // we use the same pool as the document store, and the
            // connection might have been returned in read-only mode
            connection.setReadOnly(false);
            PreparedStatement stmt = connection.prepareStatement("insert into " + table + " (ID, MODCOUNT, DATA) values (?, ?, ?)");
            try {
                setIdInStatement(stmt, 1, key);
                stmt.setLong(2, 0);
                stmt.setString(3, "X");
                stmt.executeUpdate();
                connection.commit();
            } finally {
                stmt = close(stmt);
            }
        } catch (SQLException ex) {
            // ignored
            // ex.printStackTrace();
        } finally {
            connection = close(connection);
        }

        removeMe.add(key);
        StringBuffer expect = new StringBuffer("X");

        String appendString = generateString(512, true);

        long duration = 1000;
        long end = System.currentTimeMillis() + duration;
        long cnt = 0;
        byte bdata[] = new byte[65536];
        String sdata = appendString;
        boolean needsConcat = super.dsname.contains("MySQL");
        boolean needsSQLStringConcat = super.dsname.contains("MSSql");
        int dataInChars = ((super.dsname.contains("Oracle") || (super.dsname.contains("MSSql"))) ? 4000 : 16384);
        int dataInBytes = dataInChars / 3;

        while (System.currentTimeMillis() < end) {

            try {
                connection = super.rdbDataSource.getConnection();
                connection.setAutoCommit(false);

                if (mode == 0) {
                    PreparedStatement stmt = connection.prepareStatement("update " + table + " set MODCOUNT = ? where ID = ?");
                    try {
                        stmt.setLong(1, cnt);
                        setIdInStatement(stmt, 2, key);
                        assertEquals(1, stmt.executeUpdate());
                        connection.commit();
                    } finally {
                        stmt = close(stmt);
                    }
                } else if (mode == 1) {
                    PreparedStatement stmt = connection
                            .prepareStatement("update " + table + " set MODCOUNT = ?, DATA = ? where ID = ?");
                    try {
                        stmt.setLong(1, cnt);
                        stmt.setString(2, "JSON data " + UUID.randomUUID());
                        setIdInStatement(stmt, 3, key);
                        assertEquals(1, stmt.executeUpdate());
                        connection.commit();
                    } finally {
                        stmt = close(stmt);
                    }
                } else if (mode == 2) {
                    PreparedStatement stmt = connection
                            .prepareStatement("update " + table + " set MODCOUNT = ?, DATA = ?, BDATA = ? where ID = ?");
                    try {
                        stmt.setLong(1, cnt);
                        stmt.setString(2, "JSON data " + UUID.randomUUID());
                        bdata[(int) cnt % bdata.length] = (byte) (cnt & 0xff);
                        stmt.setString(2, "JSON data " + UUID.randomUUID());
                        stmt.setBytes(3, bdata);
                        setIdInStatement(stmt, 4, key);
                        assertEquals(1, stmt.executeUpdate());
                        connection.commit();
                    } finally {
                        stmt = close(stmt);
                    }
                } else if (mode == 3) {
                    String t = "update " + table + " ";

                    t += "set DATA = ";
                    if (needsConcat) {
                        t += "CONCAT(DATA, ?) ";
                    } else if (needsSQLStringConcat) {
                        t += "CASE WHEN LEN(DATA) <= " + (dataInChars - appendString.length()) + " THEN (DATA + CAST(? AS nvarchar("
                                + 4000 + "))) ELSE (DATA + CAST(DATA AS nvarchar(max))) END";
                    } else {
                        t += "DATA || CAST(? as varchar(" + dataInChars + "))";
                    }

                    t += " where ID = ?";

                    PreparedStatement stmt = connection.prepareStatement(t);
                    try {
                        stmt.setString(1, appendString);
                        setIdInStatement(stmt, 2, key);
                        assertEquals(1, stmt.executeUpdate());
                        connection.commit();
                        expect.append(appendString);
                    } catch (SQLException ex) {
                        // ex.printStackTrace();
                        String state = ex.getSQLState();
                        if ("22001".equals(state)
                                /* everybody */ || ("72000".equals(state) && 1489 == ex.getErrorCode()) /* Oracle */) {
                            // overflow
                            stmt = close(stmt);
                            connection.rollback();
                            stmt = connection
                                    .prepareStatement("update " + table + " set MODCOUNT = MODCOUNT + 1, DATA = ? where ID = ?");
                            stmt.setString(1, "X");
                            setIdInStatement(stmt, 2, key);
                            assertEquals(1, stmt.executeUpdate());
                            connection.commit();
                            expect = new StringBuffer("X");
                        } else {
                            // ex.printStackTrace();
                            throw (ex);
                        }
                    } finally {
                        stmt = close(stmt);
                    }
                } else if (mode == 4) {
                    PreparedStatement stmt = connection.prepareStatement("update " + table
                            + " set MODIFIED = ?, HASBINARY = ?, MODCOUNT = ?, CMODCOUNT = ?, DSIZE = ?, DATA = ?, BDATA = ? where ID = ?");
                    try {
                        int si = 1;
                        stmt.setObject(si++, System.currentTimeMillis() / 5, Types.BIGINT);
                        stmt.setObject(si++, 0, Types.SMALLINT);
                        stmt.setObject(si++, cnt, Types.BIGINT);
                        stmt.setObject(si++, null, Types.BIGINT);
                        stmt.setObject(si++, sdata.length(), Types.BIGINT);

                        if (sdata.length() < dataInBytes) {
                            stmt.setString(si++, sdata);
                            stmt.setBinaryStream(si++, null, 0);
                        } else {
                            stmt.setString(si++, "null");
                            stmt.setBytes(si++, sdata.getBytes("UTF-8"));
                        }
                        setIdInStatement(stmt, si++, key);
                        assertEquals(1, stmt.executeUpdate());
                        connection.commit();
                        sdata += appendString;
                    } finally {
                        stmt = close(stmt);
                    }

                }
            } catch (SQLException ex) {
                LOG.error(ex.getMessage() + " " + ex.getSQLState() + " " + ex.getErrorCode(), ex);
            } finally {
                connection = close(connection);
            }

            cnt += 1;
        }

        // check persisted values
        if (mode == 3) {
            try {
                connection = super.rdbDataSource.getConnection();
                connection.setAutoCommit(false);
                PreparedStatement stmt = connection.prepareStatement("select DATA, MODCOUNT from " + table + " where ID = ?");
                ResultSet rs = null;
                try {
                    setIdInStatement(stmt, 1, key);
                    rs = stmt.executeQuery();
                    assertTrue("test record " + key + " not found in " + super.dsname, rs.next());
                    String got = rs.getString(1);
                    long modc = rs.getLong(2);
                    LOG.info("column reset " + modc + " times");
                    assertEquals(expect.toString(), got);
                } finally {
                    rs = close(rs);
                    stmt = close(stmt);
                }
            } finally {
                connection = close(connection);
            }
        }

        LOG.info(desc + " for " + super.dsname + " was " + cnt + " in " + duration + "ms (" + (cnt / (duration / 1000f)) + "/s)");
    }

    private void setIdInStatement(PreparedStatement stmt, int idx, String id) throws SQLException {
        boolean binaryId = super.dsname.contains("MySQL") || super.dsname.contains("MSSql");
        if (binaryId) {
            try {
                stmt.setBytes(idx, id.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException ex) {
                LOG.error("UTF-8 not supported??", ex);
                throw new DocumentStoreException(ex);
            }
        } else {
            stmt.setString(idx, id);
        }
    }

    private static Connection close(Connection c) {
        if (c != null) {
            try {
                c.close();
            } catch (SQLException ex) {
                // ignored
            }
        }
        return null;
    }

    private static PreparedStatement close(PreparedStatement s) {
        if (s != null) {
            try {
                s.close();
            } catch (SQLException ex) {
                // ignored
            }
        }
        return null;
    }

    private static ResultSet close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ex) {
                // ignored
            }
        }
        return null;
    }
}
