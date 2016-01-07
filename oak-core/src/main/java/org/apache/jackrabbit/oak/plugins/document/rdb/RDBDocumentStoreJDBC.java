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

import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.CHAR2OCTETRATIO;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.asBytes;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.closeResultSet;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.closeStatement;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
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
            boolean setModifiedConditionally, Boolean hasBinary, Boolean deletedOnce, Long modcount, Long cmodcount,
            Long oldmodcount, String appendData) throws SQLException {
        String appendDataWithComma = "," + appendData;
        PreparedStatementComponent stringAppend = this.dbInfo.getConcatQuery(appendDataWithComma, tmd.getDataLimitInOctets());
        StringBuilder t = new StringBuilder();
        t.append("update " + tmd.getName() + " set ");
        t.append(setModifiedConditionally ? "MODIFIED = case when ? > MODIFIED then ? else MODIFIED end, " : "MODIFIED = ?, ");
        t.append("HASBINARY = ?, DELETEDONCE = ?, MODCOUNT = ?, CMODCOUNT = ?, DSIZE = DSIZE + ?, ");
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
            stmt.setObject(si++, hasBinary ? 1 : 0, Types.SMALLINT);
            stmt.setObject(si++, deletedOnce ? 1 : 0, Types.SMALLINT);
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

    public boolean batchedAppendingUpdate(Connection connection, RDBTableMetaData tmd, List<String> allIds, Long modified,
            boolean setModifiedConditionally, String appendData) throws SQLException {
        boolean result = true;
        for (List<String> ids : Lists.partition(allIds, RDBJDBCTools.MAX_IN_CLAUSE)) {
            String appendDataWithComma = "," + appendData;
            PreparedStatementComponent stringAppend = this.dbInfo.getConcatQuery(appendDataWithComma, tmd.getDataLimitInOctets());
            PreparedStatementComponent inClause = RDBJDBCTools.createInStatement("ID", ids, tmd.isIdBinary());
            StringBuilder t = new StringBuilder();
            t.append("update " + tmd.getName() + " set ");
            t.append(setModifiedConditionally ? "MODIFIED = case when ? > MODIFIED then ? else MODIFIED end, " : "MODIFIED = ?, ");
            t.append("MODCOUNT = MODCOUNT + 1, DSIZE = DSIZE + ?, ");
            t.append("DATA = " + stringAppend.getStatementComponent() + " ");
            t.append("where ").append(inClause.getStatementComponent());
            PreparedStatement stmt = connection.prepareStatement(t.toString());
            try {
                int si = 1;
                stmt.setObject(si++, modified, Types.BIGINT);
                if (setModifiedConditionally) {
                    stmt.setObject(si++, modified, Types.BIGINT);
                }
                stmt.setObject(si++, appendDataWithComma.length(), Types.BIGINT);
                si = stringAppend.setParameters(stmt, si);
                si = inClause.setParameters(stmt,  si);
                int count = stmt.executeUpdate();
                if (count != ids.size()) {
                    LOG.debug("DB update failed: only " + result + " of " + ids.size() + " updated. Table: " + tmd.getName() + ", IDs:"
                            + ids);
                    result = false;
                }
            } finally {
                stmt.close();
            }
        }
        return result;
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

    public int delete(Connection connection, RDBTableMetaData tmd, Map<String, Map<Key, Condition>> toDelete)
            throws SQLException, DocumentStoreException {
        String or = "";
        StringBuilder whereClause = new StringBuilder();
        for (Entry<String, Map<Key, Condition>> entry : toDelete.entrySet()) {
            whereClause.append(or);
            or = " or ";
            whereClause.append("ID=?");
            for (Entry<Key, Condition> c : entry.getValue().entrySet()) {
                if (!c.getKey().getName().equals(MODIFIED)) {
                    throw new DocumentStoreException("Unsupported condition: " + c);
                }
                whereClause.append(" and MODIFIED");
                if (c.getValue().type == Condition.Type.EQUALS && c.getValue().value instanceof Long) {
                    whereClause.append("=?");
                } else if (c.getValue().type == Condition.Type.EXISTS) {
                    whereClause.append(" is not null");
                } else {
                    throw new DocumentStoreException("Unsupported condition: " + c);
                }
            }
        }

        PreparedStatement stmt = connection.prepareStatement("delete from " + tmd.getName() + " where " + whereClause);
        try {
            int i = 1;
            for (Entry<String, Map<Key, Condition>> entry : toDelete.entrySet()) {
                setIdInStatement(tmd, stmt, i++, entry.getKey());
                for (Entry<Key, Condition> c : entry.getValue().entrySet()) {
                    if (c.getValue().type == Condition.Type.EQUALS) {
                        stmt.setLong(i++, (Long) c.getValue().value);
                    }
                }
            }
            return stmt.executeUpdate();
        } finally {
            stmt.close();
        }
    }

    public long determineServerTimeDifferenceMillis(Connection connection, RDBTableMetaData tmd) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        long result;
        try {
            String t = "select ";
            if (this.dbInfo.getFetchFirstSyntax() == FETCHFIRSTSYNTAX.TOP) {
                t += "TOP 1 ";
            }
            t += this.dbInfo.getCurrentTimeStampInMsSyntax() + " from " + tmd.getName();
            switch (this.dbInfo.getFetchFirstSyntax()) {
                case LIMIT:
                    t += " LIMIT 1";
                    break;
                case FETCHFIRST:
                    t += " FETCH FIRST 1 ROWS ONLY";
                    break;
                default:
                    break;
            }

            stmt = connection.prepareStatement(t);
            long start = System.currentTimeMillis();
            rs = stmt.executeQuery();
            if (rs.next()) {
                long roundtrip = System.currentTimeMillis() - start;
                long serverTime = rs.getTimestamp(1).getTime();
                long roundedTime = start + roundtrip / 2;
                result = roundedTime - serverTime;
                String msg = String.format("instance timestamp: %d, DB timestamp: %d, difference: %d", roundedTime, serverTime,
                        result);
                if (Math.abs(result) >= 2000) {
                    LOG.info(msg);
                } else {
                    LOG.debug(msg);
                }
            } else {
                throw new DocumentStoreException("failed to determine server timestamp");
            }
            return result;
        } catch (Exception ex) {
            LOG.error("Trying to determine time difference to server", ex);
            throw new DocumentStoreException(ex);
        } finally {
            closeResultSet(rs);
            closeStatement(stmt);
        }
    }

    public <T extends Document> Set<String> insert(Connection connection, RDBTableMetaData tmd, List<T> documents) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(
                "insert into " + tmd.getName() + "(ID, MODIFIED, HASBINARY, DELETEDONCE, MODCOUNT, CMODCOUNT, DSIZE, DATA, BDATA) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?)");

        try {
            for (T document : documents) {
                String data = this.ser.asString(document);
                String id = document.getId();
                Number hasBinary = (Number) document.get(NodeDocument.HAS_BINARY_FLAG);
                Boolean deletedOnce = (Boolean) document.get(NodeDocument.DELETED_ONCE);
                Long cmodcount = (Long) document.get(COLLISIONSMODCOUNT);

                int si = 1;
                setIdInStatement(tmd, stmt, si++, id);
                stmt.setObject(si++, document.get(MODIFIED), Types.BIGINT);
                stmt.setObject(si++, (hasBinary != null && hasBinary.intValue() == NodeDocument.HAS_BINARY_VAL) ? 1 : 0,
                        Types.SMALLINT);
                stmt.setObject(si++, (deletedOnce != null && deletedOnce) ? 1 : 0, Types.SMALLINT);
                stmt.setObject(si++, document.get(MODCOUNT), Types.BIGINT);
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
                stmt.addBatch();
            }
            int[] results = stmt.executeBatch();

            Set<String> succesfullyInserted = new HashSet<String>();
            for (int i = 0; i < documents.size(); i++) {
                int result = results[i];
                if (result != 1 && result != Statement.SUCCESS_NO_INFO) {
                    LOG.error("DB insert failed for {}: {}", tmd.getName(), documents.get(i).getId());
                } else {
                    succesfullyInserted.add(documents.get(i).getId());
                }
            }
            return succesfullyInserted;
        } finally {
            stmt.close();
        }
    }

    /**
     * Update a list of documents using JDBC batches. Some of the updates may fail because of the concurrent
     * changes. The method returns a set of successfully updated documents. It's the caller responsibility
     * to compare the set with the list of input documents, find out which documents conflicted and take
     * appropriate action.
     * <p>
     * If the {@code upsert} parameter is set to true, the method will also try to insert new documents, those
     * which modcount equals to 1.
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
        Set<String> successfulUpdates = new HashSet<String>();

        PreparedStatement stmt = connection.prepareStatement("update " + tmd.getName()
            + " set MODIFIED = ?, HASBINARY = ?, DELETEDONCE = ?, MODCOUNT = ?, CMODCOUNT = ?, DSIZE = ?, DATA = ?, BDATA = ? where ID = ? and MODCOUNT = ?");
        try {
            List<String> updatedKeys = new ArrayList<String>();
            for (T document : documents) {
                Long modcount = (Long) document.get(MODCOUNT);
                if (modcount == 1) {
                    continue; // This is a new document. We'll deal with the inserts later.
                }

                String data = this.ser.asString(document);
                Number hasBinary = (Number) document.get(NodeDocument.HAS_BINARY_FLAG);
                Boolean deletedOnce = (Boolean) document.get(NodeDocument.DELETED_ONCE);
                Long cmodcount = (Long) document.get(COLLISIONSMODCOUNT);

                int si = 1;
                stmt.setObject(si++, document.get(MODIFIED), Types.BIGINT);
                stmt.setObject(si++, (hasBinary != null && hasBinary.intValue() == NodeDocument.HAS_BINARY_VAL) ? 1 : 0,
                        Types.SMALLINT);
                stmt.setObject(si++, (deletedOnce != null && deletedOnce) ? 1 : 0, Types.SMALLINT);
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
            }

            int[] batchResults = stmt.executeBatch();

            for (int i = 0; i < batchResults.length; i++) {
                int result = batchResults[i];
                if (result == 1 || result == Statement.SUCCESS_NO_INFO) {
                    successfulUpdates.add(updatedKeys.get(i));
                }
            }
        } finally {
            stmt.close();
        }

        if (upsert) {
            List<T> remainingDocuments = new ArrayList<T>(documents.size() - successfulUpdates.size());
            for (T doc : documents) {
                if (!successfulUpdates.contains(doc.getId())) {
                    remainingDocuments.add(doc);
                }
            }

            if (!remainingDocuments.isEmpty()) {
                Set<String> documentsWithUpdatedModcount = new HashSet<String>();
                List<String> remainingDocumentIds = Lists.transform(remainingDocuments, idExtractor);
                for (List<String> keys : Lists.partition(remainingDocumentIds, RDBJDBCTools.MAX_IN_CLAUSE)) {
                    PreparedStatementComponent inClause = RDBJDBCTools.createInStatement("ID", keys, tmd.isIdBinary());
                    StringBuilder sql = new StringBuilder("select ID from ").append(tmd.getName());
                    sql.append(" where ").append(inClause.getStatementComponent());

                    PreparedStatement selectStmt = null;
                    ResultSet rs = null;
                    try {
                        selectStmt = connection.prepareStatement(sql.toString());
                        selectStmt.setPoolable(false);
                        inClause.setParameters(selectStmt, 1);
                        rs = selectStmt.executeQuery();
                        while (rs.next()) {
                            documentsWithUpdatedModcount.add(getIdFromRS(tmd, rs, 1));
                        }
                    } finally {
                        closeResultSet(rs);
                        closeStatement(selectStmt);
                    }
                }

                Iterator<T> it = remainingDocuments.iterator();
                while (it.hasNext()) {
                    if (documentsWithUpdatedModcount.contains(it.next().getId())) {
                        it.remove();
                    }
                }

                if (!remainingDocuments.isEmpty()) {
                    for (String id : insert(connection, tmd, remainingDocuments)) {
                        successfulUpdates.add(id);
                    }
                }
            }
        }
        return successfulUpdates;
    }

    private final static Map<String, String> INDEXED_PROP_MAPPING;
    static {
        Map<String, String> tmp = new HashMap<String, String>();
        tmp.put(MODIFIED, "MODIFIED");
        tmp.put(NodeDocument.HAS_BINARY_FLAG, "HASBINARY");
        tmp.put(NodeDocument.DELETED_ONCE, "DELETEDONCE");
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
        SUPPORTED_OPS = Collections.unmodifiableSet(tmp);
    }

    @Nonnull
    public List<RDBRow> query(Connection connection, RDBTableMetaData tmd, String minId, String maxId,
            List<String> excludeKeyPatterns, List<QueryCondition> conditions, int limit) throws SQLException {
        long start = System.currentTimeMillis();
        StringBuilder selectClause = new StringBuilder();
        StringBuilder whereClause = new StringBuilder();
        if (limit != Integer.MAX_VALUE && this.dbInfo.getFetchFirstSyntax() == FETCHFIRSTSYNTAX.TOP) {
            selectClause.append("TOP " + limit + " ");
        }
        selectClause.append("ID, MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, DATA, BDATA from ").append(tmd.getName());

        // dynamically build where clause
        String whereSep = "";
        if (minId != null) {
            whereClause.append("ID > ?");
            whereSep = " and ";
        }
        if (maxId != null) {
            whereClause.append(whereSep).append("ID < ?");
            whereSep = " and ";
        }
        if (!excludeKeyPatterns.isEmpty()) {
            whereClause.append(whereSep);
            whereSep = " and ";
            whereClause.append("not (");
            for (int i = 0; i < excludeKeyPatterns.size(); i++) {
                whereClause.append(i == 0 ? "" : " or ");
                whereClause.append("ID like ?");
            }
            whereClause.append(")");
        }
        for (QueryCondition cond : conditions) {
            String op = cond.getOperator();
            if (!SUPPORTED_OPS.contains(op)) {
                throw new DocumentStoreException("unsupported operator: " + op);
            }
            String indexedProperty = cond.getPropertyName();
            String column = INDEXED_PROP_MAPPING.get(indexedProperty);
            if (column != null) {
                whereClause.append(whereSep).append(column).append(" ").append(op).append(" ?");
                whereSep = " and ";
            } else {
                throw new DocumentStoreException("unsupported indexed property: " + indexedProperty);
            }
        }

        StringBuilder query = new StringBuilder();
        query.append("select ").append(selectClause);
        if (whereClause.length() != 0) {
            query.append(" where ").append(whereClause);
        }

        query.append(" order by ID");

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
        List<RDBRow> result = new ArrayList<RDBRow>();
        long dataTotal = 0, bdataTotal = 0;
        try {
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
                stmt.setLong(si++, cond.getValue());
            }
            if (limit != Integer.MAX_VALUE) {
                stmt.setFetchSize(limit);
            }
            ResultSet rs = stmt.executeQuery();
            while (rs.next() && result.size() < limit) {
                String id = getIdFromRS(tmd, rs, 1);

                if ((minId != null && id.compareTo(minId) < 0) || (maxId != null && id.compareTo(maxId) > 0)) {
                    throw new DocumentStoreException(
                            "unexpected query result: '" + minId + "' < '" + id + "' < '" + maxId + "' - broken DB collation?");
                }
                long modified = rs.getLong(2);
                long modcount = rs.getLong(3);
                long cmodcount = rs.getLong(4);
                long hasBinary = rs.getLong(5);
                long deletedOnce = rs.getLong(6);
                String data = rs.getString(7);
                byte[] bdata = rs.getBytes(8);
                result.add(new RDBRow(id, hasBinary == 1, deletedOnce == 1, modified, modcount, cmodcount, data, bdata));
                dataTotal += data.length();
                bdataTotal += bdata == null ? 0 : bdata.length;
            }
        } finally {
            stmt.close();
        }

        long elapsed = System.currentTimeMillis() - start;
        if (this.queryHitsLimit != 0 && result.size() > this.queryHitsLimit) {
            String message = String.format(
                    "Potentially excessive query with %d hits (limited to %d, configured QUERYHITSLIMIT %d), elapsed time %dms, params minid '%s' maxid '%s' excludeKeyPatterns %s condition %s limit %d. Check calling method.",
                    result.size(), limit, this.queryHitsLimit, elapsed, minId, maxId, excludeKeyPatterns, conditions, limit);
            LOG.info(message, new Exception("call stack"));
        } else if (this.queryTimeLimit != 0 && elapsed > this.queryTimeLimit) {
            String message = String.format(
                    "Long running query with %d hits (limited to %d), elapsed time %dms (configured QUERYTIMELIMIT %d), params minid '%s' maxid '%s' excludeKeyPatterns %s conditions %s limit %d. Read %d chars from DATA and %d bytes from BDATA. Check calling method.",
                    result.size(), limit, elapsed, this.queryTimeLimit, minId, maxId, excludeKeyPatterns, conditions, limit,
                    dataTotal, bdataTotal);
            LOG.info(message, new Exception("call stack"));
        }

        return result;
    }

    public List<RDBRow> read(Connection connection, RDBTableMetaData tmd, Collection<String> allKeys) throws SQLException {

        List<RDBRow> rows = new ArrayList<RDBRow>();

        for (List<String> keys : Iterables.partition(allKeys, RDBJDBCTools.MAX_IN_CLAUSE)) {
            PreparedStatementComponent inClause = RDBJDBCTools.createInStatement("ID", keys, tmd.isIdBinary());
            StringBuilder query = new StringBuilder();
            query.append("select ID, MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, DATA, BDATA from ");
            query.append(tmd.getName());
            query.append(" where ").append(inClause.getStatementComponent());

            PreparedStatement stmt = connection.prepareStatement(query.toString());
            stmt.setPoolable(false);
            try {
                inClause.setParameters(stmt,  1);
                ResultSet rs = stmt.executeQuery();

                while (rs.next()) {
                    int col = 1;
                    String id = getIdFromRS(tmd, rs, col++);
                    long modified = rs.getLong(col++);
                    long modcount = rs.getLong(col++);
                    long cmodcount = rs.getLong(col++);
                    long hasBinary = rs.getLong(col++);
                    long deletedOnce = rs.getLong(col++);
                    String data = rs.getString(col++);
                    byte[] bdata = rs.getBytes(col++);
                    RDBRow row = new RDBRow(id, hasBinary == 1, deletedOnce == 1, modified, modcount, cmodcount, data, bdata);
                    rows.add(row);
                }
            } catch (SQLException ex) {
                LOG.error("attempting to read " + keys, ex);
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
                stmt.close();
            }
        }
        return rows;
    }

    @CheckForNull
    public RDBRow read(Connection connection, RDBTableMetaData tmd, String id, long lastmodcount) throws SQLException {
        PreparedStatement stmt;

        boolean useCaseStatement = lastmodcount != -1 && this.dbInfo.allowsCaseInSelect();
        if (useCaseStatement) {
            // the case statement causes the actual row data not to be
            // sent in case we already have it
            stmt = connection.prepareStatement(
                    "select MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, case MODCOUNT when ? then null else DATA end as DATA, "
                            + "case MODCOUNT when ? then null else BDATA end as BDATA from " + tmd.getName() + " where ID = ?");
        } else {
            // either we don't have a previous version of the document
            // or the database does not support CASE in SELECT
            stmt = connection.prepareStatement("select MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, DATA, BDATA from "
                    + tmd.getName() + " where ID = ?");
        }

        try {
            int si = 1;
            if (useCaseStatement) {
                stmt.setLong(si++, lastmodcount);
                stmt.setLong(si++, lastmodcount);
            }
            setIdInStatement(tmd, stmt, si, id);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                long modified = rs.getLong(1);
                long modcount = rs.getLong(2);
                long cmodcount = rs.getLong(3);
                long hasBinary = rs.getLong(4);
                long deletedOnce = rs.getLong(5);
                String data = rs.getString(6);
                byte[] bdata = rs.getBytes(7);
                return new RDBRow(id, hasBinary == 1, deletedOnce == 1, modified, modcount, cmodcount, data, bdata);
            } else {
                return null;
            }
        } catch (SQLException ex) {
            LOG.error("attempting to read " + id + " (id length is " + id.length() + ")", ex);
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
            stmt.close();
        }
    }

    public boolean update(Connection connection, RDBTableMetaData tmd, String id, Long modified, Boolean hasBinary,
            Boolean deletedOnce, Long modcount, Long cmodcount, Long oldmodcount, String data) throws SQLException {
        String t = "update " + tmd.getName()
                + " set MODIFIED = ?, HASBINARY = ?, DELETEDONCE = ?, MODCOUNT = ?, CMODCOUNT = ?, DSIZE = ?, DATA = ?, BDATA = ? where ID = ?";
        if (oldmodcount != null) {
            t += " and MODCOUNT = ?";
        }
        PreparedStatement stmt = connection.prepareStatement(t);
        try {
            int si = 1;
            stmt.setObject(si++, modified, Types.BIGINT);
            stmt.setObject(si++, hasBinary ? 1 : 0, Types.SMALLINT);
            stmt.setObject(si++, deletedOnce ? 1 : 0, Types.SMALLINT);
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

    private static final Function<Document, String> idExtractor = new Function<Document, String>() {
        @Override
        public String apply(Document input) {
            return input.getId();
        }
    };
}
