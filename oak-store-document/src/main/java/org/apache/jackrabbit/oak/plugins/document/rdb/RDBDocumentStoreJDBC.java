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

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.CHAR2OCTETRATIO;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.asBytes;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.closeResultSet;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.closeStatement;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.QueryCondition;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.RDBTableMetaData;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStoreDB.FETCHFIRSTSYNTAX;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.PreparedStatementComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Implements (most) DB interactions used in {@link RDBDocumentStore}.
 */
public class RDBDocumentStoreJDBC {

    private static final Logger LOG = LoggerFactory.getLogger(RDBDocumentStoreJDBC.class);

    private static final String COLLISIONSMODCOUNT = RDBDocumentStore.COLLISIONSMODCOUNT;
    private static final String MODCOUNT = NodeDocument.MOD_COUNT;
    private static final String MODIFIED = NodeDocument.MODIFIED_IN_SECS;

    private static final int SCHEMAVERSION = RDBDocumentStore.SCHEMA;

    private final RDBDocumentStoreDB dbInfo;
    private final RDBDocumentSerializer ser;
    private final int queryHitsLimit, queryTimeLimit;

    public RDBDocumentStoreJDBC(RDBDocumentStoreDB dbInfo, RDBDocumentSerializer ser, int queryHitsLimit, int queryTimeLimit) {
        this.dbInfo = dbInfo;
        this.ser = ser;
        this.queryHitsLimit = queryHitsLimit;
        this.queryTimeLimit = queryTimeLimit;
    }

    public boolean appendingUpdate(Connection connection, RDBTableMetaData tmd, String id, Long modified,
            boolean setModifiedConditionally, Number hasBinary, Boolean deletedOnce, Long modcount, Long cmodcount,
            Long oldmodcount, String appendData) throws SQLException {
        String appendDataWithComma = "," + appendData;
        PreparedStatementComponent stringAppend = this.dbInfo.getConcatQuery(appendDataWithComma, tmd.getDataLimitInOctets());
        StringBuilder t = new StringBuilder();
        t.append("update " + tmd.getName() + " set ");
        t.append(setModifiedConditionally ? "MODIFIED = case when ? > MODIFIED then ? else MODIFIED end, " : "MODIFIED = ?, ");
        t.append("HASBINARY = ?, DELETEDONCE = ?, MODCOUNT = ?, CMODCOUNT = ?, DSIZE = DSIZE + ?, ");
        if (tmd.hasVersion()) {
            t.append("VERSION = " + SCHEMAVERSION + ", ");
        }
        t.append("DATA = " + stringAppend.getStatementComponent() + " ");
        t.append("where ID = ?");
        if (oldmodcount != null) {
            t.append(" and MODCOUNT = ?");
        }
        PreparedStatement stmt = connection.prepareStatement(t.toString());
        try {
            int si = 1;
            stmt.setObject(si++, modified, Types.BIGINT);
            if (setModifiedConditionally) {
                stmt.setObject(si++, modified, Types.BIGINT);
            }
            stmt.setObject(si++, hasBinaryAsNullOrInteger(hasBinary), Types.SMALLINT);
            stmt.setObject(si++, deletedOnceAsNullOrInteger(deletedOnce), Types.SMALLINT);
            stmt.setObject(si++, modcount, Types.BIGINT);
            stmt.setObject(si++, cmodcount == null ? Long.valueOf(0) : cmodcount, Types.BIGINT);
            stmt.setObject(si++, appendDataWithComma.length(), Types.BIGINT);
            si = stringAppend.setParameters(stmt, si);
            setIdInStatement(tmd, stmt, si++, id);

            if (oldmodcount != null) {
                stmt.setObject(si++, oldmodcount, Types.BIGINT);
            }
            int result = stmt.executeUpdate();
            if (result != 1) {
                LOG.debug("DB append update failed for " + tmd.getName() + "/" + id + " with oldmodcount=" + oldmodcount);
            }
            return result == 1;
        } finally {
            stmt.close();
        }
    }

    public int delete(Connection connection, RDBTableMetaData tmd, List<String> allIds) throws SQLException {
        int count = 0;

        for (List<String> ids : Lists.partition(allIds, RDBJDBCTools.MAX_IN_CLAUSE)) {
            PreparedStatement stmt;
            PreparedStatementComponent inClause = RDBJDBCTools.createInStatement("ID", ids, tmd.isIdBinary());
            String sql = "delete from " + tmd.getName() + " where " + inClause.getStatementComponent();
            stmt = connection.prepareStatement(sql);

            try {
                inClause.setParameters(stmt, 1);
                int result = stmt.executeUpdate();
                if (result != ids.size()) {
                    LOG.debug("DB delete failed for " + tmd.getName() + "/" + ids);
                }
                count += result;
            } finally {
                stmt.close();
            }
        }

        return count;
    }

    public int delete(Connection connection, RDBTableMetaData tmd, Map<String, Long> toDelete)
            throws SQLException, DocumentStoreException {
        PreparedStatement stmt = connection.prepareStatement("delete from " + tmd.getName() + " where ID=? and MODIFIED=?");
        try {
            for (Entry<String, Long> entry : toDelete.entrySet()) {
                setIdInStatement(tmd, stmt, 1, entry.getKey());
                stmt.setLong(2, entry.getValue());
                stmt.addBatch();
            }
            int[] rets = stmt.executeBatch();
            int updatedRows = 0;
            for (int ret : rets) {
                if (ret >= 0) {
                    updatedRows += ret;
                }
            }
            return updatedRows;
        } finally {
            stmt.close();
        }
    }

    public int deleteWithCondition(Connection connection, RDBTableMetaData tmd, List<QueryCondition> conditions)
            throws SQLException, DocumentStoreException {

        StringBuilder query = new StringBuilder("delete from " + tmd.getName());

        String whereClause = buildWhereClause(null, null, null, conditions);
        if (whereClause.length() != 0) {
            query.append(" where ").append(whereClause);
        }

        PreparedStatement stmt = connection.prepareStatement(query.toString());
        try {
            int si = 1;
            for (QueryCondition cond : conditions) {
                if (cond.getOperands().size() != 1) {
                    throw new DocumentStoreException("unexpected condition: " + cond);
                }
                stmt.setLong(si++, (Long)cond.getOperands().get(0));
            }
            return stmt.executeUpdate();
        } finally {
            stmt.close();
        }
    }

    public long determineServerTimeDifferenceMillis(Connection connection) {
        String sql = this.dbInfo.getCurrentTimeStampInSecondsSyntax();

        if (sql.isEmpty()) {
            LOG.debug("{}: unsupported database, skipping DB server time check", this.dbInfo.toString());
            return 0;
        } else {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                stmt = connection.prepareStatement(sql);
                long start = System.currentTimeMillis();
                rs = stmt.executeQuery();
                if (rs.next()) {
                    long roundtrip = System.currentTimeMillis() - start;
                    long serverTimeSec = rs.getInt(1);
                    long roundedTimeSec = ((start + roundtrip / 2) + 500) / 1000;
                    long resultSec = roundedTimeSec - serverTimeSec;
                    String message = String.format("instance timestamp: %d, DB timestamp: %d, difference: %d", roundedTimeSec,
                            serverTimeSec, resultSec);
                    if (Math.abs(resultSec) >= 2) {
                        LOG.info(message);
                    } else {
                        LOG.debug(message);
                    }
                    return resultSec * 1000;
                } else {
                    throw new DocumentStoreException("failed to determine server timestamp");
                }
            } catch (Exception ex) {
                LOG.error("Trying to determine time difference to server", ex);
                throw new DocumentStoreException(ex);
            } finally {
                closeResultSet(rs);
                closeStatement(stmt);
            }
        }
    }

    public <T extends Document> Set<String> insert(Connection connection, RDBTableMetaData tmd, List<T> documents) throws SQLException {
        int actualSchema = tmd.hasSplitDocs() ? 2 : 1;
        PreparedStatement stmt = connection.prepareStatement(
                "insert into " + tmd.getName() + "(ID, MODIFIED, HASBINARY, DELETEDONCE, MODCOUNT, CMODCOUNT, DSIZE, "
                        + (tmd.hasVersion() ? "VERSION, " : "") 
                        + (tmd.hasSplitDocs() ? "SDTYPE, SDMAXREVTIME, " : "")
                        + "DATA, BDATA) " + "values (?, ?, ?, ?, ?, ?, ?, "
                        + (tmd.hasVersion() ? (" " + actualSchema + ", ") : "")
                        + (tmd.hasSplitDocs() ? "?, ?, " : "")
                        + "?, ?)");

        List<T> sortedDocs = sortDocuments(documents);
        int[] results;
        try {
            for (T document : sortedDocs) {
                String data = this.ser.asString(document, tmd.getColumnOnlyProperties());
                String id = document.getId();
                Number hasBinary = (Number) document.get(NodeDocument.HAS_BINARY_FLAG);
                Boolean deletedOnce = (Boolean) document.get(NodeDocument.DELETED_ONCE);
                Long cmodcount = (Long) document.get(COLLISIONSMODCOUNT);

                int si = 1;
                setIdInStatement(tmd, stmt, si++, id);
                stmt.setObject(si++, document.get(MODIFIED), Types.BIGINT);
                stmt.setObject(si++, hasBinaryAsNullOrInteger(hasBinary), Types.SMALLINT);
                stmt.setObject(si++, deletedOnceAsNullOrInteger(deletedOnce), Types.SMALLINT);
                stmt.setObject(si++, document.get(MODCOUNT), Types.BIGINT);
                stmt.setObject(si++, cmodcount == null ? Long.valueOf(0) : cmodcount, Types.BIGINT);
                stmt.setObject(si++, data.length(), Types.BIGINT);
                if (tmd.hasSplitDocs()) {
                    stmt.setObject(si++, document.get(NodeDocument.SD_TYPE));
                    stmt.setObject(si++, document.get(NodeDocument.SD_MAX_REV_TIME_IN_SECS));
                }
                if (data.length() < tmd.getDataLimitInOctets() / CHAR2OCTETRATIO) {
                    stmt.setString(si++, data);
                    stmt.setBinaryStream(si++, null, 0);
                } else {
                    stmt.setString(si++, "\"blob\"");
                    byte[] bytes = asBytes(data);
                    stmt.setBytes(si++, bytes);
                }
                stmt.addBatch();
            }
            results = stmt.executeBatch();
        } catch (BatchUpdateException ex) {
            LOG.debug("Some of the batch updates failed", ex);
            results = ex.getUpdateCounts();
        } finally {
            stmt.close();
        }
        Set<String> succesfullyInserted = new HashSet<String>();
        for (int i = 0; i < results.length; i++) {
            int result = results[i];
            if (result != 1 && result != Statement.SUCCESS_NO_INFO) {
                LOG.debug("DB insert failed for {}: {}", tmd.getName(), sortedDocs.get(i).getId());
            } else {
                succesfullyInserted.add(sortedDocs.get(i).getId());
            }
        }
        return succesfullyInserted;
    }

    /**
     * Update a list of documents using JDBC batches. Some of the updates may fail because of the concurrent
     * changes. The method returns a set of successfully updated documents. It's the caller responsibility
     * to compare the set with the list of input documents, find out which documents conflicted and take
     * appropriate action.
     * <p>
     * If the {@code upsert} parameter is set to true, the method will also try to insert new documents, those
     * which modcount equals to 1.
     * <p>
     * The order of applying updates will be different than order of the passed list, so there shouldn't be two
     * updates related to the same document. An {@link IllegalArgumentException} will be thrown if there are.
     *
     * @param connection JDBC connection
     * @param tmd Table metadata
     * @param documents List of documents to update
     * @param upsert Insert new documents
     * @return set containing ids of successfully updated documents
     * @throws SQLException
     */
    public <T extends Document> Set<String> update(Connection connection, RDBTableMetaData tmd, List<T> documents, boolean upsert)
            throws SQLException {
        assertNoDuplicatedIds(documents);

        Set<String> successfulUpdates = new HashSet<String>();
        List<String> updatedKeys = new ArrayList<String>();
        int[] batchResults = new int[0];

        PreparedStatement stmt = connection.prepareStatement("update " + tmd.getName()
                + " set MODIFIED = ?, HASBINARY = ?, DELETEDONCE = ?, MODCOUNT = ?, CMODCOUNT = ?, DSIZE = ?, DATA = ?, "
                + (tmd.hasVersion() ? (" VERSION = " + SCHEMAVERSION + ", ") : "") + "BDATA = ? where ID = ? and MODCOUNT = ?");
        try {
            boolean batchIsEmpty = true;
            for (T document : sortDocuments(documents)) {
                Long modcount = (Long) document.get(MODCOUNT);
                if (modcount == 1) {
                    continue; // This is a new document. We'll deal with the inserts later.
                }

                String data = this.ser.asString(document, tmd.getColumnOnlyProperties());
                Number hasBinary = (Number) document.get(NodeDocument.HAS_BINARY_FLAG);
                Boolean deletedOnce = (Boolean) document.get(NodeDocument.DELETED_ONCE);
                Long cmodcount = (Long) document.get(COLLISIONSMODCOUNT);

                int si = 1;
                stmt.setObject(si++, document.get(MODIFIED), Types.BIGINT);
                stmt.setObject(si++, hasBinaryAsNullOrInteger(hasBinary), Types.SMALLINT);
                stmt.setObject(si++, deletedOnceAsNullOrInteger(deletedOnce), Types.SMALLINT);
                stmt.setObject(si++, modcount, Types.BIGINT);
                stmt.setObject(si++, cmodcount == null ? Long.valueOf(0) : cmodcount, Types.BIGINT);
                stmt.setObject(si++, data.length(), Types.BIGINT);

                if (data.length() < tmd.getDataLimitInOctets() / CHAR2OCTETRATIO) {
                    stmt.setString(si++, data);
                    stmt.setBinaryStream(si++, null, 0);
                } else {
                    stmt.setString(si++, "\"blob\"");
                    byte[] bytes = asBytes(data);
                    stmt.setBytes(si++, bytes);
                }

                setIdInStatement(tmd, stmt, si++, document.getId());
                stmt.setObject(si++, modcount - 1, Types.BIGINT);
                stmt.addBatch();
                updatedKeys.add(document.getId());

                batchIsEmpty = false;
            }
            if (!batchIsEmpty) {
                batchResults = stmt.executeBatch();
                connection.commit();
            }
        } catch (BatchUpdateException ex) {
            LOG.debug("Some of the batch updates failed", ex);
            batchResults = ex.getUpdateCounts();
        } finally {
            stmt.close();
        }

        for (int i = 0; i < batchResults.length; i++) {
            int result = batchResults[i];
            if (result == 1 || result == Statement.SUCCESS_NO_INFO) {
                successfulUpdates.add(updatedKeys.get(i));
            }
        }

        if (upsert) {
            List<T> toBeInserted = new ArrayList<T>(documents.size());
            for (T doc : documents) {
                if ((Long) doc.get(MODCOUNT) == 1) {
                    toBeInserted.add(doc);
                }
            }

            if (!toBeInserted.isEmpty()) {
                for (String id : insert(connection, tmd, toBeInserted)) {
                    successfulUpdates.add(id);
                }
            }
        }
        return successfulUpdates;
    }

    private static <T extends Document> void assertNoDuplicatedIds(List<T> documents) {
        if (newHashSet(transform(documents, idExtractor)).size() < documents.size()) {
            throw new IllegalArgumentException("There are duplicated ids in the document list");
        }
    }

    @Nonnull
    public List<RDBRow> query(Connection connection, RDBTableMetaData tmd, String minId, String maxId,
            List<String> excludeKeyPatterns, List<QueryCondition> conditions, int limit) throws SQLException {
        long start = System.currentTimeMillis();
        List<RDBRow> result = new ArrayList<RDBRow>();
        long dataTotal = 0, bdataTotal = 0;
        PreparedStatement stmt = null;
        String fields;
        if (tmd.hasSplitDocs()) {
            fields = "ID, MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, VERSION, SDTYPE, SDMAXREVTIME, DATA, BDATA";
        } else if (tmd.hasVersion()) {
            fields = "ID, MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, VERSION, DATA, BDATA";
        } else {
            fields = "ID, MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, DATA, BDATA";
        }
        ResultSet rs = null;
        try {
            stmt = prepareQuery(connection, tmd, fields, minId,
                    maxId, excludeKeyPatterns, conditions, limit, "ID");
            rs = stmt.executeQuery();
            while (rs.next() && result.size() < limit) {
                int field = 1;
                String id = getIdFromRS(tmd, rs, field++);

                if ((minId != null && id.compareTo(minId) < 0) || (maxId != null && id.compareTo(maxId) > 0)) {
                    throw new DocumentStoreException(
                            "unexpected query result: '" + minId + "' < '" + id + "' < '" + maxId + "' - broken DB collation?");
                }
                long modified = readLongFromResultSet(rs, field++);
                long modcount = readLongFromResultSet(rs, field++);
                long cmodcount = readLongFromResultSet(rs, field++);
                Long hasBinary = readLongOrNullFromResultSet(rs, field++);
                Boolean deletedOnce = readBooleanOrNullFromResultSet(rs, field++);
                long schemaVersion = tmd.hasVersion() ? readLongFromResultSet(rs, field++) : 0;
                long sdType = tmd.hasSplitDocs() ? readLongFromResultSet(rs, field++) : 0;
                long sdMaxRevTime = tmd.hasSplitDocs() ? readLongFromResultSet(rs, field++) : 0;
                String data = rs.getString(field++);
                byte[] bdata = rs.getBytes(field++);
                result.add(new RDBRow(id, hasBinary, deletedOnce, modified, modcount, cmodcount, schemaVersion, sdType,
                        sdMaxRevTime, data, bdata));
                dataTotal += data.length();
                bdataTotal += bdata == null ? 0 : bdata.length;
            }
        } finally {
            closeStatement(stmt);
            closeResultSet(rs);
        }

        long elapsed = System.currentTimeMillis() - start;

        if ((this.queryHitsLimit != 0 && result.size() > this.queryHitsLimit)
                || (this.queryTimeLimit != 0 && elapsed > this.queryTimeLimit)) {

            String params = String.format("params minid '%s' maxid '%s' excludeKeyPatterns %s conditions %s limit %d.", minId,
                    maxId, excludeKeyPatterns, conditions, limit);

            String resultRange = "";
            if (result.size() > 0) {
                resultRange = String.format(" Result range: '%s'...'%s'.", result.get(0).getId(),
                        result.get(result.size() - 1).getId());
            }

            String postfix = String.format(" Read %d chars from DATA and %d bytes from BDATA. Check calling method.", dataTotal,
                    bdataTotal);

            if (this.queryHitsLimit != 0 && result.size() > this.queryHitsLimit) {
                String message = String.format(
                        "Potentially excessive query on %s with %d hits (limited to %d, configured QUERYHITSLIMIT %d), elapsed time %dms, %s%s%s",
                        tmd.getName(), result.size(), limit, this.queryHitsLimit, elapsed, params, resultRange, postfix);
                LOG.info(message, new Exception("call stack"));
            }

            if (this.queryTimeLimit != 0 && elapsed > this.queryTimeLimit) {
                String message = String.format(
                        "Long running query on %s with %d hits (limited to %d), elapsed time %dms (configured QUERYTIMELIMIT %d), %s%s%s",
                        tmd.getName(), result.size(), limit, elapsed, this.queryTimeLimit, params, resultRange, postfix);
                LOG.info(message, new Exception("call stack"));
            }
        }

        return result;
    }

    public long getLong(Connection connection, RDBTableMetaData tmd, String aggregate, String field, String minId, String maxId,
            List<String> excludeKeyPatterns, List<QueryCondition> conditions) throws SQLException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        long start = System.currentTimeMillis();
        long result = -1;
        String selector = aggregate + "(" + ("*".equals(field) ? "*" : INDEXED_PROP_MAPPING.get(field)) + ")";
        try {
            stmt = prepareQuery(connection, tmd, selector, minId, maxId, excludeKeyPatterns, conditions, Integer.MAX_VALUE, null);
            rs = stmt.executeQuery();

            result = rs.next() ? rs.getLong(1) : -1;
            return result;
        } finally {
            closeStatement(stmt);
            closeResultSet(rs);
            if (LOG.isDebugEnabled()) {
                long elapsed = System.currentTimeMillis() - start;
                String params = String.format("params minid '%s' maxid '%s' excludeKeyPatterns %s conditions %s.", minId, maxId,
                        excludeKeyPatterns, conditions);
                LOG.debug("Aggregate query " + selector + " on " + tmd.getName() + " with " + params + " -> " + result + ", took "
                        + elapsed + "ms");
            }
        }
    }

    @Nonnull
    public Iterator<RDBRow> queryAsIterator(RDBConnectionHandler ch, RDBTableMetaData tmd, String minId, String maxId,
            List<String> excludeKeyPatterns, List<QueryCondition> conditions, int limit, String sortBy) throws SQLException {
        return new ResultSetIterator(ch, tmd, minId, maxId, excludeKeyPatterns, conditions, limit, sortBy);
    }

    private class ResultSetIterator implements Iterator<RDBRow>, Closeable {

        private RDBConnectionHandler ch;
        private Connection connection;
        private RDBTableMetaData tmd;
        private PreparedStatement stmt;
        private ResultSet rs;
        private RDBRow next = null;
        private Exception callstack = null;
        private long elapsed = 0;
        private String message = null;
        private long cnt = 0;

        public ResultSetIterator(RDBConnectionHandler ch, RDBTableMetaData tmd, String minId, String maxId,
                List<String> excludeKeyPatterns, List<QueryCondition> conditions, int limit, String sortBy) throws SQLException {
            long start = System.currentTimeMillis();
            try {
                this.ch = ch;
                this.connection = ch.getROConnection();
                this.tmd = tmd;
                String fields;
                if (tmd.hasSplitDocs()) {
                    fields = "ID, MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, VERSION, SDTYPE, SDMAXREVTIME, DATA, BDATA";
                } else if (tmd.hasVersion()) {
                    fields = "ID, MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, VERSION, DATA, BDATA";
                } else {
                    fields = "ID, MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, DATA, BDATA";
                }
                this.stmt = prepareQuery(connection, tmd, fields, minId, maxId, excludeKeyPatterns, conditions, limit, sortBy);
                this.rs = stmt.executeQuery();
                this.next = internalNext();
                this.message = String.format("Query on %s with params minid '%s' maxid '%s' excludeKeyPatterns %s conditions %s.",
                        tmd.getName(), minId, maxId, excludeKeyPatterns, conditions);
                if (LOG.isDebugEnabled()) {
                    callstack = new Exception("call stack");
                }
            } finally {
                this.elapsed += (System.currentTimeMillis() - start);
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public void remove() {
            throw new RuntimeException("remove not supported");
        }

        @Override
        public RDBRow next() {
            RDBRow result = next;
            if (next != null) {
                next = internalNext();
                this.cnt += 1;
                return result;
            } else {
                throw new NoSuchElementException("ResultSet exhausted");
            }
        }

        private RDBRow internalNext() {
            long start = System.currentTimeMillis();
            try {
                if (this.rs.next()) {
                    int field = 1;
                    String id = getIdFromRS(this.tmd, this.rs, field++);
                    long modified = readLongFromResultSet(this.rs, field++);
                    long modcount = readLongFromResultSet(this.rs, field++);
                    long cmodcount = readLongFromResultSet(this.rs, field++);
                    Long hasBinary = readLongOrNullFromResultSet(this.rs, field++);
                    Boolean deletedOnce = readBooleanOrNullFromResultSet(this.rs, field++);
                    long schemaVersion = tmd.hasVersion() ? readLongFromResultSet(rs, field++) : 0;
                    long sdType = tmd.hasSplitDocs() ? readLongFromResultSet(rs, field++) : 0;
                    long sdMaxRevTime = tmd.hasSplitDocs() ? readLongFromResultSet(rs, field++) : 0;
                    String data = this.rs.getString(field++);
                    byte[] bdata = this.rs.getBytes(field++);
                    return new RDBRow(id, hasBinary, deletedOnce, modified, modcount, cmodcount, schemaVersion, sdType,
                            sdMaxRevTime, data, bdata);
                } else {
                    this.rs = closeResultSet(this.rs);
                    this.stmt = closeStatement(this.stmt);
                    this.connection.commit();
                    internalClose();
                    return null;
                }
            } catch (SQLException ex) {
                LOG.debug("iterating through result set", ex);
                throw new RuntimeException(ex);
            } finally {
                this.elapsed += (System.currentTimeMillis() - start);
            }
        }

        @Override
        public void close() throws IOException {
            internalClose();
        }

        @Override
        public void finalize() throws Throwable {
            try {
                if (this.connection != null) {
                    if (this.callstack != null) {
                        LOG.error("finalizing unclosed " + this + "; check caller", this.callstack);
                    } else {
                        LOG.error("finalizing unclosed " + this);
                    }
                }
            } finally {
                super.finalize();
            }
        }

        private void internalClose() {
            this.rs = closeResultSet(this.rs);
            this.stmt = closeStatement(this.stmt);
            this.ch.closeConnection(this.connection);
            this.connection = null;
            if (LOG.isDebugEnabled()) {
                LOG.debug(this.message + " -> " + this.cnt + " results in " + elapsed + "ms");
            }
        }
    }

    @Nonnull
    private PreparedStatement prepareQuery(Connection connection, RDBTableMetaData tmd, String columns, String minId, String maxId,
            List<String> excludeKeyPatterns, List<QueryCondition> conditions, int limit, String sortBy) throws SQLException {

        StringBuilder selectClause = new StringBuilder();

        if (limit != Integer.MAX_VALUE && this.dbInfo.getFetchFirstSyntax() == FETCHFIRSTSYNTAX.TOP) {
            selectClause.append("TOP " + limit + " ");
        }

        selectClause.append(columns + " from " + tmd.getName());

        String whereClause = buildWhereClause(minId, maxId, excludeKeyPatterns, conditions);

        StringBuilder query = new StringBuilder();
        query.append("select ").append(selectClause);

        if (whereClause.length() != 0) {
            query.append(" where ").append(whereClause);
        }

        if (sortBy != null) {
            query.append(" order by ID");
        }

        if (limit != Integer.MAX_VALUE) {
            switch (this.dbInfo.getFetchFirstSyntax()) {
                case LIMIT:
                    query.append(" LIMIT " + limit);
                    break;
                case FETCHFIRST:
                    query.append(" FETCH FIRST " + limit + " ROWS ONLY");
                    break;
                default:
                    break;
            }
        }

        PreparedStatement stmt = connection.prepareStatement(query.toString());

        int si = 1;
        if (minId != null) {
            setIdInStatement(tmd, stmt, si++, minId);
        }
        if (maxId != null) {
            setIdInStatement(tmd, stmt, si++, maxId);
        }
        for (String keyPattern : excludeKeyPatterns) {
            setIdInStatement(tmd, stmt, si++, keyPattern);
        }
        for (QueryCondition cond : conditions) {
            for (Object o : cond.getOperands()) {
                stmt.setObject(si++, o);
            }
        }
        if (limit != Integer.MAX_VALUE) {
            stmt.setFetchSize(limit);
        }
        return stmt;
    }

    public List<RDBRow> read(Connection connection, RDBTableMetaData tmd, Collection<String> allKeys) throws SQLException {

        List<RDBRow> rows = new ArrayList<RDBRow>();

        for (List<String> keys : Iterables.partition(allKeys, RDBJDBCTools.MAX_IN_CLAUSE)) {
            PreparedStatementComponent inClause = RDBJDBCTools.createInStatement("ID", keys, tmd.isIdBinary());
            StringBuilder query = new StringBuilder();
            if (tmd.hasSplitDocs()) {
                query.append("select ID, MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, VERSION, SDTYPE, SDMAXREVTIME, DATA, BDATA from ");
            } else if (tmd.hasVersion()) {
                query.append("select ID, MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, VERSION, DATA, BDATA from ");
            } else {
                query.append("select ID, MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, DATA, BDATA from ");
            }
            query.append(tmd.getName());
            query.append(" where ").append(inClause.getStatementComponent());

            PreparedStatement stmt = connection.prepareStatement(query.toString());
            ResultSet rs = null;
            stmt.setPoolable(false);
            try {
                inClause.setParameters(stmt,  1);
                rs = stmt.executeQuery();

                while (rs.next()) {
                    int field = 1;
                    String id = getIdFromRS(tmd, rs, field++);
                    long modified = readLongFromResultSet(rs, field++);
                    long modcount = readLongFromResultSet(rs, field++);
                    long cmodcount = readLongFromResultSet(rs, field++);
                    Long hasBinary = readLongOrNullFromResultSet(rs, field++);
                    Boolean deletedOnce = readBooleanOrNullFromResultSet(rs, field++);
                    long schemaVersion = tmd.hasVersion() ? readLongFromResultSet(rs, field++) : 0;
                    long sdType = tmd.hasSplitDocs() ? readLongFromResultSet(rs, field++) : 0;
                    long sdMaxRevTime = tmd.hasSplitDocs() ? readLongFromResultSet(rs, field++) : 0;
                    String data = rs.getString(field++);
                    byte[] bdata = rs.getBytes(field++);
                    RDBRow row = new RDBRow(id, hasBinary, deletedOnce, modified, modcount, cmodcount, schemaVersion, sdType,
                            sdMaxRevTime, data, bdata);
                    rows.add(row);
                }
            } catch (SQLException ex) {
                LOG.debug("attempting to read " + keys, ex);
                // DB2 throws an SQLException for invalid keys; handle this more
                // gracefully
                if ("22001".equals(ex.getSQLState())) {
                    try {
                        connection.rollback();
                    } catch (SQLException ex2) {
                        LOG.debug("failed to rollback", ex2);
                    }
                    return null;
                } else {
                    throw (ex);
                }
            } finally {
                closeResultSet(rs);
                closeStatement(stmt);
            }
        }
        return rows;
    }

    @CheckForNull
    public RDBRow read(Connection connection, RDBTableMetaData tmd, String id, long lastmodcount, long lastmodified) throws SQLException {

        boolean useCaseStatement = lastmodcount != -1 && lastmodified >= 1;
        StringBuffer sql = new StringBuffer();
        String fields;
        if (tmd.hasSplitDocs()) {
            fields = "MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, VERSION, SDTYPE, SDMAXREVTIME, ";
        } else if (tmd.hasVersion()) {
            fields = "MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, VERSION, ";
        } else {
            fields = "MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, ";
        }

        sql.append("select ").append(fields);
        if (useCaseStatement) {
            // the case statement causes the actual row data not to be
            // sent in case we already have it
            sql.append("case when (MODCOUNT = ? and MODIFIED = ?) then null else DATA end as DATA, ");
            sql.append("case when (MODCOUNT = ? and MODIFIED = ?) then null else BDATA end as BDATA ");
        } else {
            // either we don't have a previous version of the document
            // or the database does not support CASE in SELECT
            sql.append("DATA, BDATA ");
        }
        sql.append("from " + tmd.getName() + " where ID = ?");
        PreparedStatement stmt = connection.prepareStatement(sql.toString());
        ResultSet rs = null;

        try {
            int si = 1;
            if (useCaseStatement) {
                stmt.setLong(si++, lastmodcount);
                stmt.setLong(si++, lastmodified);
                stmt.setLong(si++, lastmodcount);
                stmt.setLong(si++, lastmodified);
            }
            setIdInStatement(tmd, stmt, si, id);

            rs = stmt.executeQuery();
            if (rs.next()) {
                int field = 1;
                long modified = readLongFromResultSet(rs, field++);
                long modcount = readLongFromResultSet(rs, field++);
                long cmodcount = readLongFromResultSet(rs, field++);
                Long hasBinary = readLongOrNullFromResultSet(rs, field++);
                Boolean deletedOnce = readBooleanOrNullFromResultSet(rs, field++);
                long schemaVersion = tmd.hasVersion() ? readLongFromResultSet(rs, field++) : 0;
                long sdType = tmd.hasSplitDocs() ? readLongFromResultSet(rs, field++) : 0;
                long sdMaxRevTime = tmd.hasSplitDocs() ? readLongFromResultSet(rs, field++) : 0;
                String data = rs.getString(field++);
                byte[] bdata = rs.getBytes(field++);
                return new RDBRow(id, hasBinary, deletedOnce, modified, modcount, cmodcount, schemaVersion, sdType, sdMaxRevTime,
                        data, bdata);
            } else {
                return null;
            }
        } catch (SQLException ex) {
            LOG.debug("attempting to read " + id + " (id length is " + id.length() + ")", ex);
            // DB2 throws an SQLException for invalid keys; handle this more
            // gracefully
            if ("22001".equals(ex.getSQLState())) {
                try {
                    connection.rollback();
                } catch (SQLException ex2) {
                    LOG.debug("failed to rollback", ex2);
                }
                return null;
            } else {
                throw (ex);
            }
        } finally {
            closeResultSet(rs);
            closeStatement(stmt);
        }
    }

    public boolean update(Connection connection, RDBTableMetaData tmd, String id, Long modified, Number hasBinary,
            Boolean deletedOnce, Long modcount, Long cmodcount, Long oldmodcount, String data) throws SQLException {

        StringBuilder t = new StringBuilder();
        t.append("update " + tmd.getName() + " set ");
        t.append("MODIFIED = ?, HASBINARY = ?, DELETEDONCE = ?, MODCOUNT = ?, CMODCOUNT = ?, DSIZE = ?, DATA = ?, "
                + (tmd.hasVersion() ? (" VERSION = " + SCHEMAVERSION + ", ") : "") + "BDATA = ? ");
        t.append("where ID = ?");
        if (oldmodcount != null) {
            t.append(" and MODCOUNT = ?");
        }
        PreparedStatement stmt = connection.prepareStatement(t.toString());
        try {
            int si = 1;
            stmt.setObject(si++, modified, Types.BIGINT);
            stmt.setObject(si++, hasBinaryAsNullOrInteger(hasBinary), Types.SMALLINT);
            stmt.setObject(si++, deletedOnceAsNullOrInteger(deletedOnce), Types.SMALLINT);
            stmt.setObject(si++, modcount, Types.BIGINT);
            stmt.setObject(si++, cmodcount == null ? Long.valueOf(0) : cmodcount, Types.BIGINT);
            stmt.setObject(si++, data.length(), Types.BIGINT);

            if (data.length() < tmd.getDataLimitInOctets() / CHAR2OCTETRATIO) {
                stmt.setString(si++, data);
                stmt.setBinaryStream(si++, null, 0);
            } else {
                stmt.setString(si++, "\"blob\"");
                byte[] bytes = asBytes(data);
                stmt.setBytes(si++, bytes);
            }

            setIdInStatement(tmd, stmt, si++, id);

            if (oldmodcount != null) {
                stmt.setObject(si++, oldmodcount, Types.BIGINT);
            }
            int result = stmt.executeUpdate();
            if (result != 1) {
                LOG.debug("DB update failed for " + tmd.getName() + "/" + id + " with oldmodcount=" + oldmodcount);
            }
            return result == 1;
        } finally {
            stmt.close();
        }
    }

    private final static Map<String, String> INDEXED_PROP_MAPPING;
    static {
        Map<String, String> tmp = new HashMap<String, String>();
        tmp.put(MODIFIED, "MODIFIED");
        tmp.put(NodeDocument.HAS_BINARY_FLAG, "HASBINARY");
        tmp.put(NodeDocument.DELETED_ONCE, "DELETEDONCE");
        tmp.put(COLLISIONSMODCOUNT, "CMODCOUNT");
        tmp.put(NodeDocument.SD_TYPE, "SDTYPE");
        tmp.put(NodeDocument.SD_MAX_REV_TIME_IN_SECS, "SDMAXREVTIME");
        tmp.put(RDBDocumentStore.VERSIONPROP, "VERSION");
        INDEXED_PROP_MAPPING = Collections.unmodifiableMap(tmp);
    }

    private final static Set<String> SUPPORTED_OPS;
    static {
        Set<String> tmp = new HashSet<String>();
        tmp.add(">=");
        tmp.add(">");
        tmp.add("<=");
        tmp.add("<");
        tmp.add("=");
        tmp.add("in");
        tmp.add("is null");
        tmp.add("is not null");
        tmp.add("null or <");
        SUPPORTED_OPS = Collections.unmodifiableSet(tmp);
    }

    private static String buildWhereClause(String minId, String maxId, List<String> excludeKeyPatterns, List<QueryCondition> conditions) {
        StringBuilder result = new StringBuilder();

        String whereSep = "";
        if (minId != null) {
            result.append("ID > ?");
            whereSep = " and ";
        }
        if (maxId != null) {
            result.append(whereSep).append("ID < ?");
            whereSep = " and ";
        }
        if (excludeKeyPatterns != null && !excludeKeyPatterns.isEmpty()) {
            result.append(whereSep);
            whereSep = " and ";
            result.append("not (");
            for (int i = 0; i < excludeKeyPatterns.size(); i++) {
                result.append(i == 0 ? "" : " or ");
                result.append("ID like ?");
            }
            result.append(")");
        }
        for (QueryCondition cond : conditions) {
            String op = cond.getOperator();
            if (!SUPPORTED_OPS.contains(op)) {
                throw new DocumentStoreException("unsupported operator: " + op);
            }
            String indexedProperty = cond.getPropertyName();
            String column = INDEXED_PROP_MAPPING.get(indexedProperty);
            if (column != null) {
                String realOperand = op;
                boolean allowNull = false;
                if (op.startsWith("null or ")) {
                    realOperand = op.substring("null or ".length());
                    allowNull = true; 
                }
                result.append(whereSep);
                if (allowNull) {
                    result.append("(").append(column).append(" is null or ");
                }
                result.append(column).append(" ").append(realOperand);

                List<? extends Object> operands = cond.getOperands();
                if (operands.size() == 1) {
                    result.append(" ?");
                } else if (operands.size() > 1) {
                    result.append(" (");
                    for (int i = 0; i < operands.size(); i++) {
                        result.append("?");
                        if (i < operands.size() - 1) {
                            result.append(", ");
                        }
                    }
                    result.append(") ");
                }
                if (allowNull) {
                    result.append(")");
                }
                whereSep = " and ";
            } else {
                throw new DocumentStoreException("unsupported indexed property: " + indexedProperty);
            }
        }
        return result.toString();
    }

    private static String getIdFromRS(RDBTableMetaData tmd, ResultSet rs, int idx) throws SQLException {
        if (tmd.isIdBinary()) {
            try {
                return new String(rs.getBytes(idx), "UTF-8");
            } catch (UnsupportedEncodingException ex) {
                LOG.error("UTF-8 not supported??", ex);
                throw new DocumentStoreException(ex);
            }
        } else {
            return rs.getString(idx);
        }
    }

    private static void setIdInStatement(RDBTableMetaData tmd, PreparedStatement stmt, int idx, String id) throws SQLException {
        if (tmd.isIdBinary()) {
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

    private static long readLongFromResultSet(ResultSet res, int index) throws SQLException {
        long v = res.getLong(index);
        return res.wasNull() ? RDBRow.LONG_UNSET : v;
    }

    @CheckForNull
    private static Boolean readBooleanOrNullFromResultSet(ResultSet res, int index) throws SQLException {
        long v = res.getLong(index);
        return res.wasNull() ? null : Boolean.valueOf(v != 0);
    }

    @CheckForNull
    private static Long readLongOrNullFromResultSet(ResultSet res, int index) throws SQLException {
        long v = res.getLong(index);
        return res.wasNull() ? null : Long.valueOf(v);
    }

    private static final Integer INT_FALSE = 0;
    private static final Integer INT_TRUE = 1;

    @CheckForNull
    private static Integer deletedOnceAsNullOrInteger(Boolean b) {
        return b == null ? null : (b.booleanValue() ? INT_TRUE : INT_FALSE);
    }

    @CheckForNull
    private static Integer hasBinaryAsNullOrInteger(Number n) {
        return n == null ? null : (n.longValue() == 1 ? INT_TRUE : INT_FALSE);
    }

    private static <T extends Document> List<T> sortDocuments(Collection<T> documents) {
        List<T> result = new ArrayList<T>(documents);
        Collections.sort(result, new Comparator<T>() {
            @Override
            public int compare(T o1, T o2) {
                return o1.getId().compareTo(o2.getId());
            }
        });
        return result;
    }

    private static final Function<Document, String> idExtractor = new Function<Document, String>() {
        @Override
        public String apply(Document input) {
            return input.getId();
        }
    };
}
