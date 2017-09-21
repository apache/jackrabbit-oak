/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getModuleVersion;

import java.io.Closeable;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.plugins.blob.CachingBlobStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.PreparedStatementComponent;
import org.apache.jackrabbit.oak.spi.blob.AbstractBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

public class RDBBlobStore extends CachingBlobStore implements Closeable {

    /**
     * Creates a {@linkplain RDBBlobStore} instance using the provided
     * {@link DataSource} using the given {@link RDBOptions}.
     */
    public RDBBlobStore(DataSource ds, RDBOptions options) {
        try {
            initialize(ds, options);
        } catch (Exception ex) {
            throw new DocumentStoreException("initializing RDB blob store", ex);
        }
    }

    /**
     * Creates a {@linkplain RDBBlobStore} instance using the provided
     * {@link DataSource} using default {@link RDBOptions}.
     */
    public RDBBlobStore(DataSource ds) {
        this(ds, new RDBOptions());
    }

    @Override
    public void close() {
        String dropped = "";
        if (!this.tablesToBeDropped.isEmpty()) {
            LOG.debug("attempting to drop: " + this.tablesToBeDropped);
            for (String tname : this.tablesToBeDropped) {
                Connection con = null;
                try {
                    con = this.ch.getRWConnection();
                    Statement stmt = null;
                    try {
                        stmt = con.createStatement();
                        stmt.execute("drop table " + tname);
                        stmt.close();
                        con.commit();
                        dropped += tname + " ";
                    } catch (SQLException ex) {
                        LOG.debug("attempting to drop: " + tname, ex);
                    } finally {
                        closeStatement(stmt);
                    }
                } catch (SQLException ex) {
                    LOG.debug("attempting to drop: " + tname, ex);
                } finally {
                    this.ch.closeConnection(con);
                }
            }
            dropped = dropped.trim();
        }
        try {
            this.ch.close();
        } catch (IOException ex) {
            LOG.error("closing connection handler", ex);
        }
        LOG.info("RDBBlobStore (" + getModuleVersion() + ") closed"
                + (dropped.isEmpty() ? "" : " (tables dropped: " + dropped + ")"));
    }

    @Override
    protected void finalize() throws Throwable {
        if (!this.ch.isClosed() && this.callStack != null) {
            LOG.debug("finalizing RDBDocumentStore that was not disposed", this.callStack);
        }
        super.finalize();
    }

    private static final Logger LOG = LoggerFactory.getLogger(RDBBlobStore.class);

    // ID size we need to support; is 2 * (hex) size of digest length
    protected static final int IDSIZE;
    static {
        try {
            MessageDigest md = MessageDigest.getInstance(AbstractBlobStore.HASH_ALGORITHM);
            IDSIZE = md.getDigestLength() * 2;
        } catch (NoSuchAlgorithmException ex) {
            LOG.error ("can't determine digest length for blob store", ex);
            throw new RuntimeException(ex);
        }
    }


    private Exception callStack;

    protected RDBConnectionHandler ch;

    // from options
    protected String tnData;
    protected String tnMeta;
    private Set<String> tablesToBeDropped = new HashSet<String>();


    private void initialize(DataSource ds, RDBOptions options) throws Exception {

        this.tnData = RDBJDBCTools.createTableName(options.getTablePrefix(), "DATASTORE_DATA");
        this.tnMeta = RDBJDBCTools.createTableName(options.getTablePrefix(), "DATASTORE_META");

        this.ch = new RDBConnectionHandler(ds);
        Connection con = this.ch.getRWConnection();

        int isolation = con.getTransactionIsolation();
        String isolationDiags = RDBJDBCTools.isolationLevelToString(isolation);
        if (isolation != Connection.TRANSACTION_READ_COMMITTED) {
            LOG.info("Detected transaction isolation level " + isolationDiags + " is "
                    + (isolation < Connection.TRANSACTION_READ_COMMITTED ? "lower" : "higher") + " than expected "
                    + RDBJDBCTools.isolationLevelToString(Connection.TRANSACTION_READ_COMMITTED)
                    + " - check datasource configuration");
        }

        DatabaseMetaData md = con.getMetaData();
        RDBBlobStoreDB db = RDBBlobStoreDB.getValue(md.getDatabaseProductName());
        String versionDiags = db.checkVersion(md);
        if (!versionDiags.isEmpty()) {
            LOG.error(versionDiags);
        }

        String dbDesc = String.format("%s %s (%d.%d)", md.getDatabaseProductName(), md.getDatabaseProductVersion(),
                md.getDatabaseMajorVersion(), md.getDatabaseMinorVersion()).replaceAll("[\r\n\t]", " ").trim();
        String driverDesc = String.format("%s %s (%d.%d)", md.getDriverName(), md.getDriverVersion(), md.getDriverMajorVersion(),
                md.getDriverMinorVersion()).replaceAll("[\r\n\t]", " ").trim();
        String dbUrl = md.getURL();

        List<String> tablesCreated = new ArrayList<String>();
        List<String> tablesPresent = new ArrayList<String>();

        Statement createStatement = null;

        try {
            for (String tableName : new String[] { this.tnData, this.tnMeta }) {
                PreparedStatement checkStatement = null;
                try {
                    checkStatement = con.prepareStatement("select ID from " + tableName + " where ID = ?");
                    checkStatement.setString(1, "0");
                    checkStatement.executeQuery().close();
                    checkStatement.close();
                    checkStatement = null;
                    con.commit();
                    tablesPresent.add(tableName);
                } catch (SQLException ex) {
                    closeStatement(checkStatement);
 
                    // table does not appear to exist
                    con.rollback();

                    createStatement = con.createStatement();

                    if (this.tnMeta.equals(tableName)) {
                        String ct = db.getMetaTableCreationStatement(tableName);
                        createStatement.execute(ct);
                    } else {
                        String ct = db.getDataTableCreationStatement(tableName);
                        createStatement.execute(ct);
                    }

                    createStatement.close();
                    createStatement = null;

                    con.commit();

                    tablesCreated.add(tableName);
                }
            }

            if (options.isDropTablesOnClose()) {
                tablesToBeDropped.addAll(tablesCreated);
            }

            LOG.info("RDBBlobStore (" + getModuleVersion() + ") instantiated for database " + dbDesc + ", using driver: "
                    + driverDesc + ", connecting to: " + dbUrl + ", transaction isolation level: " + isolationDiags);
            if (!tablesPresent.isEmpty()) {
                LOG.info("Tables present upon startup: " + tablesPresent);
            }
            if (!tablesCreated.isEmpty()) {
                LOG.info("Tables created upon startup: " + tablesCreated
                        + (options.isDropTablesOnClose() ? " (will be dropped on exit)" : ""));
            }

            this.callStack = LOG.isDebugEnabled() ? new Exception("call stack of RDBBlobStore creation") : null;
        } finally {
            closeStatement(createStatement);
            this.ch.closeConnection(con);
        }
    }

    private long minLastModified;

    @Override
    protected void storeBlock(byte[] digest, int level, byte[] data) throws IOException {
        try {
            storeBlockInDatabase(digest, level, data);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private void storeBlockInDatabase(byte[] digest, int level, byte[] data) throws SQLException {

        String id = StringUtils.convertBytesToHex(digest);
        cache.put(id, data);
        Connection con = this.ch.getRWConnection();

        try {
            long now = System.currentTimeMillis();
            PreparedStatement prep = con.prepareStatement("update " + this.tnMeta + " set LASTMOD = ? where ID = ?");
            int count;
            try {
                prep.setLong(1, now);
                prep.setString(2, id);
                count = prep.executeUpdate();
            }
            catch (SQLException ex) {
                LOG.error("trying to update metadata", ex);
                throw new RuntimeException("trying to update metadata", ex);
            }
            finally {
                prep.close();
            }
            if (count == 0) {
                try {
                    prep = con.prepareStatement("insert into " + this.tnData + " (ID, DATA) values(?, ?)");
                    try {
                        prep.setString(1, id);
                        prep.setBytes(2, data);
                        int rows = prep.executeUpdate();
                        LOG.trace("insert-data id={} rows={}", id, rows);
                        if (rows != 1) {
                            throw new SQLException("Insert of id " + id + " into " + this.tnData + " failed with result " + rows);
                        }
                    } finally {
                        prep.close();
                    }
                } catch (SQLException ex) {
                    this.ch.rollbackConnection(con);
                    // the insert failed although it should have succeeded; see whether the blob already exists
                    prep = con.prepareStatement("select DATA from " + this.tnData + " where ID = ?");
                    ResultSet rs = null;
                    byte[] dbdata = null;
                    try {
                        prep.setString(1, id);
                        rs = prep.executeQuery();
                        if (rs.next()) {
                            dbdata = rs.getBytes(1);
                        }
                    } finally {
                        closeResultSet(rs);
                        closeStatement(prep);
                    }

                    if (dbdata == null) {
                        // insert failed although record isn't there
                        String message = "insert document failed for id " + id + " with length " + data.length + " (check max size of datastore_data.data)";
                        LOG.error(message, ex);
                        throw new RuntimeException(message, ex);
                    }
                    else if (!Arrays.equals(data, dbdata)) {
                        // record is there but contains different data
                        String message = "DATA table already contains blob for id " + id + ", but the actual data differs (lengths: " + data.length + ", " + dbdata.length + ")";
                        LOG.error(message, ex);
                        throw new RuntimeException(message, ex);
                    }
                    else {
                        // just recover
                        LOG.info("recovered from DB inconsistency for id " + id + ": meta record was missing (impact will be minor performance degradation)");
                    }
                }
                try {
                    prep = con.prepareStatement("insert into " + this.tnMeta + " (ID, LVL, LASTMOD) values(?, ?, ?)");
                    try {
                        prep.setString(1, id);
                        prep.setInt(2, level);
                        prep.setLong(3, now);
                        int rows = prep.executeUpdate();
                        LOG.trace("insert-meta id={} rows={}", id, rows);
                        if (rows != 1) {
                            throw new SQLException("Insert of id " + id + " into " + this.tnMeta + " failed with result " + rows);
                        }
                    } finally {
                        prep.close();
                    }
                } catch (SQLException e) {
                    // already exists - ok
                    LOG.debug("inserting meta record for id " + id, e);
                }
            }
        } finally {
            con.commit();
            this.ch.closeConnection(con);
        }
    }

    // needed in test
    protected byte[] readBlockFromBackend(byte[] digest) throws Exception {
        String id = StringUtils.convertBytesToHex(digest);
        Connection con = this.ch.getROConnection();
        byte[] data;

        try {
            PreparedStatement prep = con.prepareStatement("select DATA from " + this.tnData + " where ID = ?");
            ResultSet rs = null;
            try {
                prep.setString(1, id);
                rs = prep.executeQuery();
                if (!rs.next()) {
                    throw new IOException("Datastore block " + id + " not found");
                }
                data = rs.getBytes(1);
            } finally {
                closeResultSet(rs);
                closeStatement(prep);
            }
        } finally {
            con.commit();
            this.ch.closeConnection(con);
        }
        return data;
    }

    @Override
    protected byte[] readBlockFromBackend(BlockId blockId) throws Exception {

        String id = StringUtils.convertBytesToHex(blockId.getDigest());
        byte[] data = cache.get(id);

        if (data == null) {
            Connection con = this.ch.getROConnection();
            long start = System.nanoTime();
            try {
                PreparedStatement prep = con.prepareStatement("select DATA from " + this.tnData + " where ID = ?");
                ResultSet rs = null;
                try {
                    prep.setString(1, id);
                    rs = prep.executeQuery();
                    if (!rs.next()) {
                        throw new IOException("Datastore block " + id + " not found");
                    }
                    data = rs.getBytes(1);
                } finally {
                    closeResultSet(rs);
                    closeStatement(prep);
                }

                getStatsCollector().downloaded(id, System.nanoTime() - start, TimeUnit.NANOSECONDS, data.length);
                cache.put(id, data);
            } finally {
                con.commit();
                this.ch.closeConnection(con);
            }
        }
        // System.out.println("    read block " + id + " blockLen: " +
        // data.length + " [0]: " + data[0]);
        if (blockId.getPos() == 0) {
            return data;
        }
        int len = (int) (data.length - blockId.getPos());
        if (len < 0) {
            return new byte[0];
        }
        byte[] d2 = new byte[len];
        System.arraycopy(data, (int) blockId.getPos(), d2, 0, len);
        return d2;
    }

    @Override
    public void startMark() throws IOException {
        minLastModified = System.currentTimeMillis();
        markInUse();
    }

    @Override
    protected boolean isMarkEnabled() {
        return minLastModified != 0;
    }

    @Override
    protected void mark(BlockId blockId) throws Exception {
        Connection con = this.ch.getRWConnection();
        PreparedStatement prep = null;
        try {
            if (minLastModified == 0) {
                return;
            }
            String id = StringUtils.convertBytesToHex(blockId.getDigest());
            prep = con.prepareStatement("update " + this.tnMeta + " set LASTMOD = ? where ID = ? and LASTMOD < ?");
            prep.setLong(1, System.currentTimeMillis());
            prep.setString(2, id);
            prep.setLong(3, minLastModified);
            prep.executeUpdate();
            prep.close();
        } finally {
            closeStatement(prep);
            con.commit();
            this.ch.closeConnection(con);
        }
    }

    @Override
    public int sweep() throws IOException {
        try {
            return sweepFromDatabase();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private int sweepFromDatabase() throws SQLException {
        Connection con = this.ch.getRWConnection();
        PreparedStatement prepCheck = null, prepDelMeta = null, prepDelData = null;
        ResultSet rs = null;
        try {
            int count = 0;
            prepCheck = con.prepareStatement("select ID from " + this.tnMeta + " where LASTMOD < ?");
            prepCheck.setLong(1, minLastModified);
            rs = prepCheck.executeQuery();
            ArrayList<String> ids = new ArrayList<String>();
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
            rs.close();
            rs = null;
            prepCheck.close();
            prepCheck = null;

            prepDelMeta = con.prepareStatement("delete from " + this.tnMeta + " where ID = ?");
            prepDelData = con.prepareStatement("delete from " + this.tnData + " where ID = ?");

            for (String id : ids) {
                prepDelMeta.setString(1, id);
                int mrows = prepDelMeta.executeUpdate();
                LOG.trace("delete-meta id={} rows={}", id, mrows);
                prepDelData.setString(1, id);
                int drows = prepDelData.executeUpdate();
                LOG.trace("delete-data id={} rows={}", id, drows);
                count++;
            }
            prepDelMeta.close();
            prepDelMeta = null;
            prepDelData.close();
            prepDelData = null;
            minLastModified = 0;
            return count;
        } finally {
            closeResultSet(rs);
            closeStatement(prepCheck);
            closeStatement(prepDelMeta);
            closeStatement(prepDelData);
            con.commit();
            this.ch.closeConnection(con);
        }
    }

    @Override
    public long countDeleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
        long count = 0;

        for (List<String> chunk : Lists.partition(chunkIds, RDBJDBCTools.MAX_IN_CLAUSE)) {
            Connection con = this.ch.getRWConnection();
            PreparedStatement prepMeta = null;
            PreparedStatement prepData = null;

            try {
                PreparedStatementComponent inClause = RDBJDBCTools.createInStatement("ID", chunk, false);

                StringBuilder metaStatement = new StringBuilder("delete from " + this.tnMeta + " where ")
                        .append(inClause.getStatementComponent());
                StringBuilder dataStatement = new StringBuilder("delete from " + this.tnData + " where ")
                        .append(inClause.getStatementComponent());

                if (maxLastModifiedTime > 0) {
                    // delete only if the last modified is OLDER than x
                    metaStatement.append(" and LASTMOD <= ?");
                    // delete if there is NO entry where the last modified of
                    // the meta is YOUNGER than x
                    dataStatement.append(" and not exists(select * from " + this.tnMeta + " where " + this.tnMeta + ".ID = "
                            + this.tnData + ".ID and LASTMOD > ?)");
                }

                prepMeta = con.prepareStatement(metaStatement.toString());
                prepData = con.prepareStatement(dataStatement.toString());

                int mindex = 1, dindex = 1;
                mindex = inClause.setParameters(prepMeta, mindex);
                dindex = inClause.setParameters(prepData, dindex);

                if (maxLastModifiedTime > 0) {
                    prepMeta.setLong(mindex, maxLastModifiedTime);
                    prepData.setLong(dindex, maxLastModifiedTime);
                }

                int deletedMeta = prepMeta.executeUpdate();
                LOG.trace("delete-meta rows={}", deletedMeta);
                int deletedData = prepData.executeUpdate();
                LOG.trace("delete-data rows={}", deletedData);

                if (deletedMeta != deletedData) {
                    String message = String.format(
                            "chunk deletion affected different numbers of DATA records (%s) and META records (%s)", deletedData,
                            deletedMeta);
                    LOG.info(message);
                }

                count += deletedMeta;
            } finally {
                closeStatement(prepMeta);
                closeStatement(prepData);
                con.commit();
                this.ch.closeConnection(con);
            }
        }

        return count;
    }

    @Override
    public Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception {
        return new ChunkIdIterator(this.ch, maxLastModifiedTime, this.tnMeta);
    }

    /**
     * Reads chunk IDs in batches.
     */
    private static class ChunkIdIterator extends AbstractIterator<String> {

        private long maxLastModifiedTime;
        private RDBConnectionHandler ch;
        private static int BATCHSIZE = 1024 * 64;
        private List<String> results = new LinkedList<String>();
        private String lastId = null;
        private String metaTable;

        public ChunkIdIterator(RDBConnectionHandler ch, long maxLastModifiedTime, String metaTable) {
            this.maxLastModifiedTime = maxLastModifiedTime;
            this.ch = ch;
            this.metaTable = metaTable;
        }

        @Override
        protected String computeNext() {
            if (!results.isEmpty()) {
                return results.remove(0);
            } else {
                // need to refill
                if (refill()) {
                    return computeNext();
                } else {
                    return endOfData();
                }
            }
        }

        private boolean refill() {
            StringBuffer query = new StringBuffer();
            query.append("select ID from " + metaTable);
            if (maxLastModifiedTime > 0) {
                query.append(" where LASTMOD <= ?");
                if (lastId != null) {
                    query.append(" and ID > ?");
                }
            } else {
                if (lastId != null) {
                    query.append(" where ID > ?");
                }
            }
            query.append(" order by ID");

            Connection connection = null;
            try {
                connection = this.ch.getROConnection();
                PreparedStatement prep = null;
                ResultSet rs = null;
                try {
                    prep = connection.prepareStatement(query.toString());
                    int idx = 1;
                    if (maxLastModifiedTime > 0) {
                        prep.setLong(idx++, maxLastModifiedTime);
                    }
                    if (lastId != null) {
                        prep.setString(idx, lastId);
                    }
                    prep.setFetchSize(BATCHSIZE);
                    rs = prep.executeQuery();
                    while (rs.next()) {
                        lastId = rs.getString(1);
                        results.add(lastId);
                    }
                    rs.close();
                    rs = null;
                    return !results.isEmpty();
                } finally {
                    closeResultSet(rs);
                    closeStatement(prep);
                    connection.commit();
                    this.ch.closeConnection(connection);
                }
            } catch (SQLException ex) {
                LOG.debug("error executing ID lookup", ex);
                this.ch.rollbackConnection(connection);
                this.ch.closeConnection(connection);
                return false;
            }
        }
    }
}
