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

import static com.google.common.collect.ImmutableSet.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.Closeable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentStoreTest;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.QueryCondition;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.RDBTableMetaData;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * Tests checking certain JDBC related features.
 */
public class RDBDocumentStoreJDBCTest extends AbstractDocumentStoreTest {

    private RDBDocumentStoreJDBC jdbc;
    private RDBDocumentStoreDB dbInfo;
    private static final Logger LOG = LoggerFactory.getLogger(RDBDocumentStoreJDBCTest.class);

    @Rule
    public TestName name= new TestName();

    public RDBDocumentStoreJDBCTest(DocumentStoreFixture dsf) {
        super(dsf);
        assumeTrue(super.rdbDataSource != null);

        dbInfo = RDBDocumentStoreDB.getValue(((RDBDocumentStore) super.ds).getMetadata().get("db"));
        RDBDocumentSerializer ser = new RDBDocumentSerializer(super.ds);
        jdbc = new RDBDocumentStoreJDBC(dbInfo, ser, 100, 10000);
    }

    @Test
    public void conditionalRead() throws SQLException {

        String id = this.getClass().getName() + ".conditionalRead";
        super.ds.remove(Collection.NODES, id);
        UpdateOp op = new UpdateOp(id, true);
        op.set("_modified", 1L);
        removeMe.add(id);
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(op)));

        NodeDocument nd = super.ds.find(Collection.NODES, id, 0);
        assertNotNull(nd);
        Long lastmodcount = nd.getModCount();
        Long lastmodified = nd.getModified();
        assertNotNull(lastmodcount);
        assertNotNull(lastmodified);

        RDBTableMetaData tmd = ((RDBDocumentStore) super.ds).getTable(Collection.NODES);
        Connection con = super.rdbDataSource.getConnection();
        con.setReadOnly(true);
        try {
            RDBRow rMcNotMatch = jdbc.read(con, tmd, id, lastmodcount + 1, lastmodified);
            assertNotNull(rMcNotMatch.getData());

            RDBRow rMcNotGiven = jdbc.read(con, tmd, id, -1, lastmodified);
            assertNotNull(rMcNotGiven.getData());

            RDBRow rMcMatch = jdbc.read(con, tmd, id, lastmodcount, lastmodified);
            assertNull(rMcMatch.getData());

            RDBRow rMcMatchModNonmatch = jdbc.read(con, tmd, id, lastmodcount, lastmodified + 2);
            assertNotNull(rMcMatchModNonmatch.getData());
        } finally {
            con.close();
        }
    }

    @Test
    public void batchUpdateResult() throws SQLException {

        // https://issues.apache.org/jira/browse/OAK-3938
        assumeTrue(super.dsf != DocumentStoreFixture.RDB_ORACLE);

        String table = ((RDBDocumentStore) super.ds).getTable(Collection.NODES).getName();

        Connection con = super.rdbDataSource.getConnection();
        con.setReadOnly(false);
        try {
            PreparedStatement st = con.prepareStatement("DELETE FROM " + table + " WHERE ID in (?, ?, ?)");
            setIdInStatement(st, 1, "key-1");
            setIdInStatement(st, 2, "key-2");
            setIdInStatement(st, 3, "key-3");
            st.executeUpdate();
            st.close();
            con.commit();

            st = con.prepareStatement("INSERT INTO " + table + " (id) VALUES (?)");
            setIdInStatement(st, 1, "key-3");
            st.executeUpdate();
            st.close();
            con.commit();

            removeMe.add("key-3");

            PreparedStatement batchSt = con.prepareStatement("UPDATE " + table + " SET data = '{}' WHERE id = ?");
            setIdInStatement(batchSt, 1, "key-1");
            batchSt.addBatch();

            setIdInStatement(batchSt, 1, "key-2");
            batchSt.addBatch();

            setIdInStatement(batchSt, 1, "key-3");
            batchSt.addBatch();

            int[] batchResult = batchSt.executeBatch();
            batchSt.close();
            con.commit();

            // System.out.println(super.dsname + " " +
            // Arrays.toString(batchResult));

            assertEquals(3, batchResult.length);
            assertFalse("Row was updated although not present, status: " + batchResult[0], isSuccess(batchResult[0]));
            assertFalse("Row was updated although not present, status: " + batchResult[1], isSuccess(batchResult[1]));
            assertTrue("Row should be updated correctly.", isSuccess(batchResult[2]));
        } finally {
            con.close();
        }
    }

    @Test
    public void batchFailingInsertResult() throws SQLException {

        String table = ((RDBDocumentStore) super.ds).getTable(Collection.NODES).getName();

        Connection con = super.rdbDataSource.getConnection();
        con.setReadOnly(false);
        try {
            // remove key-1, key-2, key-3
            PreparedStatement st = con.prepareStatement("DELETE FROM " + table + " WHERE ID in (?, ?, ?)");
            setIdInStatement(st, 1, "key-1");
            setIdInStatement(st, 2, "key-2");
            setIdInStatement(st, 3, "key-3");
            st.executeUpdate();
            st.close();
            con.commit();

            removeMe.add("key-3");

            // insert key-3
            st = con.prepareStatement("INSERT INTO " + table + " (id) VALUES (?)");
            setIdInStatement(st, 1, "key-3");
            st.executeUpdate();
            st.close();
            con.commit();

            removeMe.add("key-1");
            removeMe.add("key-2");

            // try to insert key-1, key-2, key-3
            PreparedStatement batchSt = con.prepareStatement("INSERT INTO " + table + " (id) VALUES (?)");
            setIdInStatement(batchSt, 1, "key-1");
            batchSt.addBatch();

            setIdInStatement(batchSt, 1, "key-2");
            batchSt.addBatch();

            setIdInStatement(batchSt, 1, "key-3");
            batchSt.addBatch();

            int[] batchResult = null;
            try {
                batchSt.executeBatch();
                fail("Batch operation should fail");
            } catch (BatchUpdateException e) {
                batchResult = e.getUpdateCounts();
            }
            batchSt.close();
            con.commit();

            // System.out.println(super.dsname + " " + Arrays.toString(batchResult));

            boolean partialSuccess = false;

            if (batchResult.length >= 2) {
                if (isSuccess(batchResult[0]) && isSuccess(batchResult[1])) {
                    partialSuccess = true;
                }
            }

            if (batchResult.length == 3) {
                assertTrue("Row already exists, shouldn't be inserted.", !isSuccess(batchResult[2]));
            }

            PreparedStatement rst = con.prepareStatement("SELECT id FROM " + table + " WHERE id in (?, ?, ?)");
            setIdInStatement(rst, 1, "key-1");
            setIdInStatement(rst, 2, "key-2");
            setIdInStatement(rst, 3, "key-3");
            ResultSet results = rst.executeQuery();
            Set<String> ids = new HashSet<String>();
            while (results.next()) {
                ids.add(getIdFromRS(results, 1));
            }
            results.close();
            rst.close();

            if (partialSuccess) {
                assertEquals("Some of the rows weren't inserted.", of("key-1", "key-2", "key-3"), ids);
            }
            else {
                assertEquals("Failure reported, but rows inserted.", of("key-3"), ids);
            }
        } finally {
            con.close();
        }
    }

    @Test
    public void statementCloseTest() throws SQLException {

        // for now we just log the behavior, see https://bz.apache.org/bugzilla/show_bug.cgi?id=59850

        String table = ((RDBDocumentStore) super.ds).getTable(Collection.NODES).getName();

        Connection con = super.rdbDataSource.getConnection();
        con.setReadOnly(true);
        try {
            PreparedStatement st = con.prepareStatement("SELECT id from " + table + " WHERE id = ?");
            setIdInStatement(st, 1, "key-1");
            ResultSet rs = st.executeQuery();
            st.close();
            LOG.info(super.rdbDataSource + " on " + super.dsname + " - statement.close() closes ResultSet: " + rs.isClosed());
            con.commit();
        } finally {
            con.close();
        }
    }

    private class MyConnectionHandler extends RDBConnectionHandler {

        public AtomicInteger cnt = new AtomicInteger();

        public MyConnectionHandler(DataSource ds) {
            super(ds);
        }

        @Override
        public Connection getROConnection() throws SQLException {
            cnt.incrementAndGet();
            return super.getROConnection();
        }

        @Override
        public Connection getRWConnection() throws SQLException {
            throw new RuntimeException();
        }

        @Override
        public void closeConnection(Connection c) {
            super.closeConnection(c);
            cnt.decrementAndGet();
        }
    }

    @Test
    public void queryIteratorNotStartedTest() throws SQLException {
        insertTestResource(this.getClass().getName() + "." + name.getMethodName());

        MyConnectionHandler ch = new MyConnectionHandler(super.rdbDataSource);
        RDBTableMetaData tmd = ((RDBDocumentStore) super.ds).getTable(Collection.NODES);
        List<QueryCondition> conditions = Collections.emptyList();

        Iterator<RDBRow> qi = jdbc.queryAsIterator(ch, tmd, null, null, RDBDocumentStore.EMPTY_KEY_PATTERN, conditions,
                Integer.MAX_VALUE, null);
        assertTrue(qi instanceof Closeable);
        assertEquals(1, ch.cnt.get());
        Utils.closeIfCloseable(qi);
        assertEquals(0, ch.cnt.get());
    }

    @Test
    public void queryIteratorConsumedTest() throws SQLException {
        insertTestResource(this.getClass().getName() + "." + name.getMethodName());

        LogCustomizer customLogs = LogCustomizer.forLogger(RDBDocumentStoreJDBC.class.getName()).enable(Level.DEBUG)
                .contains("Query on ").create();
        customLogs.starting();

        MyConnectionHandler ch = new MyConnectionHandler(super.rdbDataSource);
        RDBTableMetaData tmd = ((RDBDocumentStore) super.ds).getTable(Collection.NODES);
        List<QueryCondition> conditions = Collections.emptyList();

        try {
            Iterator<RDBRow> qi = jdbc.queryAsIterator(ch, tmd, null, null, RDBDocumentStore.EMPTY_KEY_PATTERN, conditions,
                    Integer.MAX_VALUE, null);
            assertTrue(qi instanceof Closeable);
            assertEquals(1, ch.cnt.get());
            while (qi.hasNext()) {
                qi.next();
            }
            assertEquals(0, ch.cnt.get());
            assertEquals("should have a DEBUG level log entry", 1, customLogs.getLogs().size());
        } finally {
            customLogs.finished();
            customLogs = null;
        }
    }

    @Test
    public void queryIteratorNotConsumedTest() throws SQLException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        LogCustomizer customLogs = LogCustomizer.forLogger(RDBDocumentStoreJDBC.class.getName()).enable(Level.DEBUG).contains("finalizing unclosed").create();
        customLogs.starting();

        insertTestResource(this.getClass().getName() + "." + name.getMethodName());

        MyConnectionHandler ch = new MyConnectionHandler(super.rdbDataSource);
        RDBTableMetaData tmd = ((RDBDocumentStore) super.ds).getTable(Collection.NODES);
        List<QueryCondition> conditions = Collections.emptyList();
        Iterator<RDBRow> qi = jdbc.queryAsIterator(ch, tmd, null, null, RDBDocumentStore.EMPTY_KEY_PATTERN, conditions,
                Integer.MAX_VALUE, null);
        assertTrue(qi instanceof Closeable);
        assertEquals(1, ch.cnt.get());
        Method fin = qi.getClass().getDeclaredMethod("finalize");

        try {
            fin.setAccessible(true);
            fin.invoke(qi);

            assertTrue("finalizing non-consumed iterator should generate log entry", customLogs.getLogs().size() >= 1);
        } finally {
            Utils.closeIfCloseable(qi);
            fin.setAccessible(false);
            customLogs.finished();
        }
    }

    @Test
    public void queryCountTest() throws SQLException {
        insertTestResource(this.getClass().getName() + "." + name.getMethodName());

        Connection con = super.rdbDataSource.getConnection();
        try {
            con.setReadOnly(true);
            RDBTableMetaData tmd = ((RDBDocumentStore) super.ds).getTable(Collection.NODES);
            List<QueryCondition> conditions = Collections.emptyList();
            long cnt = jdbc.getLong(con, tmd, "count", "*", null, null, RDBDocumentStore.EMPTY_KEY_PATTERN, conditions);
            assertTrue(cnt > 0);
        } finally {
            con.close();
        }
    }

    @Test
    public void queryMinLastModifiedTest() throws SQLException {
        String baseName = this.getClass().getName() + "." + name.getMethodName();

        long magicValue = (long)(Math.random() * 100000);

        String baseNameNullModified = baseName + "-1";
        super.ds.remove(Collection.NODES, baseNameNullModified);
        UpdateOp op = new UpdateOp(baseNameNullModified, true);
        op.set(RDBDocumentStore.COLLISIONSMODCOUNT, magicValue);
        op.set(NodeDocument.DELETED_ONCE, true);
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(op)));
        removeMe.add(baseNameNullModified);

        String baseName10Modified = baseName + "-2";
        super.ds.remove(Collection.NODES, baseName10Modified);
        op = new UpdateOp(baseName10Modified, true);
        op.set(RDBDocumentStore.COLLISIONSMODCOUNT, magicValue);
        op.set(NodeDocument.MODIFIED_IN_SECS, 10);
        op.set(NodeDocument.DELETED_ONCE, true);
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(op)));
        removeMe.add(baseName10Modified);

        String baseName20Modified = baseName + "-3";
        super.ds.remove(Collection.NODES, baseName20Modified);
        op = new UpdateOp(baseName20Modified, true);
        op.set(RDBDocumentStore.COLLISIONSMODCOUNT, magicValue);
        op.set(NodeDocument.MODIFIED_IN_SECS, 20);
        op.set(NodeDocument.DELETED_ONCE, true);
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(op)));
        removeMe.add(baseName20Modified);

        String baseName5ModifiedNoDeletedOnce = baseName + "-4";
        super.ds.remove(Collection.NODES, baseName5ModifiedNoDeletedOnce);
        op = new UpdateOp(baseName5ModifiedNoDeletedOnce, true);
        op.set(RDBDocumentStore.COLLISIONSMODCOUNT, magicValue);
        op.set(NodeDocument.MODIFIED_IN_SECS, 5);
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(op)));
        removeMe.add(baseName5ModifiedNoDeletedOnce);

        LogCustomizer customLogs = LogCustomizer.forLogger(RDBDocumentStoreJDBC.class.getName()).enable(Level.DEBUG)
                .contains("Aggregate query").contains("min(MODIFIED)").create();
        customLogs.starting();
        Connection con = super.rdbDataSource.getConnection();
        try {
            con.setReadOnly(true);
            RDBTableMetaData tmd = ((RDBDocumentStore) super.ds).getTable(Collection.NODES);
            List<QueryCondition> conditions = new ArrayList<QueryCondition>();
            conditions.add(new QueryCondition(RDBDocumentStore.COLLISIONSMODCOUNT, "=", magicValue));
            long min = jdbc.getLong(con, tmd, "min", "_modified", null, null, RDBDocumentStore.EMPTY_KEY_PATTERN, conditions);
            assertEquals(5, min);
            con.commit();
        } finally {
            con.close();
            assertEquals("should have a DEBUG level log entry", 1, customLogs.getLogs().size());
            customLogs.finished();
            customLogs = null;
        }

        con = super.rdbDataSource.getConnection();
        try {
            con.setReadOnly(true);
            RDBTableMetaData tmd = ((RDBDocumentStore) super.ds).getTable(Collection.NODES);
            List<QueryCondition> conditions = new ArrayList<QueryCondition>();
            conditions.add(new QueryCondition(RDBDocumentStore.COLLISIONSMODCOUNT, "=", magicValue));
            conditions.add(new QueryCondition(NodeDocument.DELETED_ONCE, "=", 1));
            long min = jdbc.getLong(con, tmd, "min", "_modified", null, null, RDBDocumentStore.EMPTY_KEY_PATTERN, conditions);
            assertEquals(10, min);
            con.commit();
        } finally {
            con.close();
        }
}

    private void insertTestResource(String id) {
        super.ds.remove(Collection.NODES, id);
        UpdateOp op = new UpdateOp(id, true);
        removeMe.add(id);
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(op)));
    }

    private static boolean isSuccess(int result) {
        return result == 1 || result == Statement.SUCCESS_NO_INFO;
    }

    private void setIdInStatement(PreparedStatement stmt, int idx, String id) throws SQLException {
        boolean binaryId = ((RDBDocumentStore) super.ds).getTable(Collection.NODES).isIdBinary();
        if (binaryId) {
            try {
                stmt.setBytes(idx, id.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException ex) {
                throw new DocumentStoreException(ex);
            }
        } else {
            stmt.setString(idx, id);
        }
    }

    private String getIdFromRS(ResultSet rs, int idx) throws SQLException {
        boolean binaryId = ((RDBDocumentStore) super.ds).getTable(Collection.NODES).isIdBinary();
        if (binaryId) {
            try {
                return new String(rs.getBytes(idx), "UTF-8");
            } catch (UnsupportedEncodingException ex) {
                throw new DocumentStoreException(ex);
            }
        } else {
            return rs.getString(idx);
        }
    }
}
