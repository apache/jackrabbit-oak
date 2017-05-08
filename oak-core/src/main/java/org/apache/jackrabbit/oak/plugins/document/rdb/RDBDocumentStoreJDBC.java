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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
            boolean setModifiedConditionally, Number hasBinary, Boolean deletedOnce, Long modcount, Long cmodcount,
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

    public <T extends Document> boolean insert(Connection connection, RDBTableMetaData tmd, List<T> documents) throws SQLException {
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
                stmt.setObject(si++, hasBinaryAsNullOrInteger(hasBinary), Types.SMALLINT);
                stmt.setObject(si++, deletedOnceAsNullOrInteger(deletedOnce), Types.SMALLINT);
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
            boolean success = true;
            for (int i = 0; i < documents.size(); i++) {
                int result = results[i];
                if (result != 1 && result != Statement.SUCCESS_NO_INFO) {
                    LOG.error("DB insert failed for {}: {}", tmd.getName(), documents.get(i).getId());
                    success = false;
                }
            }
            return success;
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
        ResultSet rs = null;
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
            rs = stmt.executeQuery();
            while (rs.next() && result.size() < limit) {
                String id = getIdFromRS(tmd, rs, 1);

                if ((minId != null && id.compareTo(minId) < 0) || (maxId != null && id.compareTo(maxId) > 0)) {
                    throw new DocumentStoreException(
                            "unexpected query result: '" + minId + "' < '" + id + "' < '" + maxId + "' - broken DB collation?");
                }
                long modified = readLongFromResultSet(rs, 2);
                long modcount = readLongFromResultSet(rs, 3);
                long cmodcount = readLongFromResultSet(rs, 4);
                Long hasBinary = readLongOrNullFromResultSet(rs, 5);
                Boolean deletedOnce = readBooleanOrNullFromResultSet(rs, 6);
                String data = rs.getString(7);
                byte[] bdata = rs.getBytes(8);
                result.add(new RDBRow(id, hasBinary, deletedOnce, modified, modcount, cmodcount, data, bdata));
                dataTotal += data.length();
                bdataTotal += bdata == null ? 0 : bdata.length;
            }
        } finally {
            closeResultSet(rs);
            closeStatement(stmt);
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

    @CheckForNull
    public RDBRow read(Connection connection, RDBTableMetaData tmd, String id, long lastmodcount, long lastmodified) throws SQLException {

        boolean useCaseStatement = lastmodcount != -1 && lastmodified >= 1;
        StringBuffer sql = new StringBuffer();
        sql.append("select MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, ");
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
                long modified = readLongFromResultSet(rs, 1);
                long modcount = readLongFromResultSet(rs, 2);
                long cmodcount = readLongFromResultSet(rs, 3);
                Long hasBinary = readLongOrNullFromResultSet(rs, 4);
                Boolean deletedOnce = readBooleanOrNullFromResultSet(rs, 5);
                String data = rs.getString(6);
                byte[] bdata = rs.getBytes(7);
                return new RDBRow(id, hasBinary, deletedOnce, modified, modcount, cmodcount, data, bdata);
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
        t.append("MODIFIED = ?, HASBINARY = ?, DELETEDONCE = ?, MODCOUNT = ?, CMODCOUNT = ?, DSIZE = ?, DATA = ?, BDATA = ? ");
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
}
