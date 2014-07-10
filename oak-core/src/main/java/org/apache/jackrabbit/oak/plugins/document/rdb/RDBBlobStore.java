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

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.plugins.blob.CachingBlobStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;

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
        if (!this.tablesToBeDropped.isEmpty()) {
            LOG.debug("attempting to drop: " + this.tablesToBeDropped);
            for (String tname : this.tablesToBeDropped) {
                Connection con = null;
                try {
                    con = getConnection();
                    try {
                        Statement stmt = con.createStatement();
                        stmt.execute("drop table " + tname);
                        stmt.close();
                        con.commit();
                    } catch (SQLException ex) {
                        LOG.debug("attempting to drop: " + tname);
                    }
                } catch (SQLException ex) {
                    LOG.debug("attempting to drop: " + tname);
                } finally {
                    try {
                        if (con != null) {
                            con.close();
                        }
                    } catch (SQLException ex) {
                        LOG.debug("on close ", ex);
                    }
                }
            }
        }
        this.ds = null;
    }

    @Override
    public void finalize() {
        if (this.ds != null && this.callStack != null) {
            LOG.debug("finalizing RDBDocumentStore that was not disposed", this.callStack);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(RDBBlobStore.class);

    // blob size we need to support
    private static final int MINBLOB = 2 * 1024 * 1024;

    private Exception callStack;

    private DataSource ds;

    // from options
    private String dataTable;
    private String metaTable;
    private Set<String> tablesToBeDropped = new HashSet<String>();

    private void initialize(DataSource ds, RDBOptions options) throws Exception {

        String tablePrefix = options.getTablePrefix();
        if (tablePrefix.length() > 0 && !tablePrefix.endsWith("_")) {
            tablePrefix += "_";
        }
        this.dataTable = tablePrefix + "DATASTORE_DATA";
        this.metaTable = tablePrefix + "DATASTORE_META";

        this.ds = ds;
        Connection con = getConnection();

        try {
            con.setAutoCommit(false);

            for (String baseName : new String[] { "DATASTORE_META", "DATASTORE_DATA" }) {
                String tableName = tablePrefix + baseName;
                try {
                    PreparedStatement stmt = con.prepareStatement("select ID from " + tableName + " where ID = ?");
                    stmt.setString(1, "0");
                    stmt.executeQuery();
                } catch (SQLException ex) {
                    // table does not appear to exist
                    con.rollback();

                    String dbtype = con.getMetaData().getDatabaseProductName();
                    LOG.info("Attempting to create table " + tableName + " in " + dbtype);

                    Statement stmt = con.createStatement();

                    if (baseName.equals("DATASTORE_META")) {
                        if ("MySQL".equals(dbtype)) {
                            stmt.execute("create table " + tableName
                                    + " (ID varchar(767) not null primary key, LVL int, LASTMOD bigint)");
                        } else if ("Oracle".equals(dbtype)) {
                            stmt.execute("create table " + tableName
                                    + " (ID varchar(1000) not null primary key, LVL number, LASTMOD number)");
                        } else {
                            stmt.execute("create table " + tableName
                                    + " (ID varchar(1000) not null primary key, LVL int, LASTMOD bigint)");
                        }
                    } else {
                        // the code below likely will need to be extended for
                        // new database types
                        if ("PostgreSQL".equals(dbtype)) {
                            stmt.execute("create table " + tableName + " (ID varchar(1000) not null primary key, DATA bytea)");
                        } else if ("DB2".equals(dbtype) || (dbtype != null && dbtype.startsWith("DB2/"))) {
                            stmt.execute("create table " + tableName + " (ID varchar(1000) not null primary key, DATA blob("
                                    + MINBLOB + "))");
                        } else if ("MySQL".equals(dbtype)) {
                            stmt.execute("create table " + tableName + " (ID varchar(767) not null primary key, DATA mediumblob)");
                        } else {
                            stmt.execute("create table " + tableName + " (ID varchar(1000) not null primary key, DATA blob)");
                        }
                    }

                    stmt.close();

                    con.commit();

                    if (options.isDropTablesOnClose()) {
                        tablesToBeDropped.add(tableName);
                    }
                }
            }
        } finally {
            // con.close();
        }

        this.callStack = LOG.isDebugEnabled() ? new Exception("call stack of RDBBlobStore creation") : null;
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
        Connection con = getConnection();

        try {
            long now = System.currentTimeMillis();
            PreparedStatement prep = con.prepareStatement("update " + metaTable + " set lastMod = ? where id = ?");
            int count;
            try {
                prep.setLong(1, now);
                prep.setString(2, id);
                count = prep.executeUpdate();
            } finally {
                prep.close();
            }
            if (count == 0) {
                try {
                    prep = con.prepareStatement("insert into " + dataTable + "(id, data) values(?, ?)");
                    try {
                        prep.setString(1, id);
                        prep.setBytes(2, data);
                        prep.execute();
                    } finally {
                        prep.close();
                    }
                } catch (SQLException ex) {
                    // TODO: this code used to ignore exceptions here, assuming that it might be a case where the blob is already in the database (maybe this requires inspecting the exception code)
                    String message = "insert document failed for id " + id + " with length " + data.length + " (check max size of datastore_data.data)";
                    LOG.error(message, ex);
                    throw new RuntimeException(message, ex);
                }
                try {
                    prep = con.prepareStatement("insert into " + metaTable + "(id, lvl, lastMod) values(?, ?, ?)");
                    try {
                        prep.setString(1, id);
                        prep.setInt(2, level);
                        prep.setLong(3, now);
                        prep.execute();
                    } finally {
                        prep.close();
                    }
                } catch (SQLException e) {
                    // already exists - ok
                }
            }
        } finally {
            con.commit();
            con.close();
        }
    }

    // needed in test
    protected byte[] readBlockFromBackend(byte[] digest) throws Exception {
        String id = StringUtils.convertBytesToHex(digest);
        Connection con = getConnection();
        byte[] data;

        try {
            PreparedStatement prep = con.prepareStatement("select data from " + dataTable + " where id = ?");
            try {
                prep.setString(1, id);
                ResultSet rs = prep.executeQuery();
                if (!rs.next()) {
                    throw new IOException("Datastore block " + id + " not found");
                }
                data = rs.getBytes(1);
            } finally {
                prep.close();
            }
        } finally {
            con.commit();
            con.close();
        }
        return data;
    }

    @Override
    protected byte[] readBlockFromBackend(BlockId blockId) throws Exception {

        String id = StringUtils.convertBytesToHex(blockId.getDigest());
        byte[] data = cache.get(id);

        if (data == null) {
            Connection con = getConnection();

            try {
                PreparedStatement prep = con.prepareStatement("select data from " + dataTable + " where id = ?");
                try {
                    prep.setString(1, id);
                    ResultSet rs = prep.executeQuery();
                    if (!rs.next()) {
                        throw new IOException("Datastore block " + id + " not found");
                    }
                    data = rs.getBytes(1);
                } finally {
                    prep.close();
                }
                cache.put(id, data);
            } finally {
                con.commit();
                con.close();
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
        Connection con = getConnection();
        try {
            if (minLastModified == 0) {
                return;
            }
            String id = StringUtils.convertBytesToHex(blockId.getDigest());
            PreparedStatement prep = con.prepareStatement("update " + metaTable + " set lastMod = ? where id = ? and lastMod < ?");
            prep.setLong(1, System.currentTimeMillis());
            prep.setString(2, id);
            prep.setLong(3, minLastModified);
            prep.executeUpdate();
            prep.close();
        } finally {
            con.commit();
            con.close();
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
        Connection con = getConnection();
        try {
            int count = 0;
            PreparedStatement prep = con.prepareStatement("select id from " + metaTable + " where lastMod < ?");
           prep.setLong(1, minLastModified);
            ResultSet rs = prep.executeQuery();
            ArrayList<String> ids = new ArrayList<String>();
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
            prep = con.prepareStatement("delete from " + metaTable + " where id = ?");
            PreparedStatement prepData = con.prepareStatement("delete from " + dataTable + " where id = ?");
            for (String id : ids) {
                prep.setString(1, id);
                prep.execute();
                prepData.setString(1, id);
                prepData.execute();
                count++;
            }
            prepData.close();
            prep.close();
            minLastModified = 0;
            return count;
        } finally {
            con.commit();
            con.close();
        }
    }

    @Override
    public boolean deleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {

        // sanity check
        if (chunkIds.isEmpty()) {
            // sanity check, nothing to do
            return true;
        }

        Connection con = getConnection();
        try {
            PreparedStatement prepMeta = null;
            PreparedStatement prepData = null;

            StringBuilder inClause = new StringBuilder();
            int batch = chunkIds.size();
            for (int i = 0; i < batch; i++) {
                inClause.append('?');
                if (i != batch - 1) {
                    inClause.append(',');
                }
            }

            if (maxLastModifiedTime > 0) {
                prepMeta = con.prepareStatement("delete from " + metaTable + " where id in (" + inClause.toString()
                        + ") and lastMod <= ?");
                prepMeta.setLong(batch + 1, maxLastModifiedTime);

                prepData = con.prepareStatement("delete from " + dataTable + " where id in (" + inClause.toString()
                        + ") and lastMod <= ?");
                prepData.setLong(batch + 1, maxLastModifiedTime);
            } else {
                prepMeta = con.prepareStatement("delete from " + metaTable + " where id in (" + inClause.toString() + ")");

                prepData = con.prepareStatement("delete from " + dataTable + " where id in (" + inClause.toString() + ")");
            }

            for (int idx = 0; idx < batch; idx++) {
                prepMeta.setString(idx + 1, chunkIds.get(idx));
                prepData.setString(idx + 1, chunkIds.get(idx));
            }

            prepMeta.execute();
            prepData.execute();
            prepMeta.close();
            prepData.close();
        } finally {
            con.commit();
            con.close();
        }

        return true;
    }

    @Override
    public Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception {
        return new ChunkIdIterator(this.ds, maxLastModifiedTime, metaTable);
    }

    /**
     * Reads chunk IDs in batches.
     */
    private static class ChunkIdIterator extends AbstractIterator<String> {

        private long maxLastModifiedTime;
        private DataSource ds;
        private static int BATCHSIZE = 1024 * 64;
        private List<String> results = new LinkedList<String>();
        private String lastId = null;
        private String metaTable;

        public ChunkIdIterator(DataSource ds, long maxLastModifiedTime, String metaTable) {
            this.maxLastModifiedTime = maxLastModifiedTime;
            this.ds = ds;
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
            query.append("select id from " + metaTable);
            if (maxLastModifiedTime > 0) {
                query.append(" where lastMod <= ?");
                if (lastId != null) {
                    query.append(" and id > ?");
                }
            } else {
                if (lastId != null) {
                    query.append(" where id > ?");
                }
            }
            query.append(" order by id");

            Connection connection = null;
            try {
                connection = ds.getConnection();
                connection.setAutoCommit(false);
                try {
                    PreparedStatement prep = connection.prepareStatement(query.toString());
                    int idx = 1;
                    if (maxLastModifiedTime > 0) {
                        prep.setLong(idx++, maxLastModifiedTime);
                    }
                    if (lastId != null) {
                        prep.setString(idx++, lastId);
                    }
                    prep.setFetchSize(BATCHSIZE);
                    ResultSet rs = prep.executeQuery();
                    while (rs.next()) {
                        lastId = rs.getString(1);
                        results.add(lastId);
                    }
                    return !results.isEmpty();
                } finally {
                    connection.commit();
                    connection.close();
                }
            } catch (SQLException ex) {
                LOG.debug("error executing ID lookup", ex);
                try {
                    if (connection != null) {
                        connection.rollback();
                        connection.close();
                    }
                } catch (SQLException e) {
                }
                return false;
            }
        }
    }

    private Connection getConnection() throws SQLException {
        DataSource ds = this.ds;
        if (ds == null) {
            throw new DocumentStoreException("This instance of the RDBBlobStore has already been closed.");
        }
        Connection c = ds.getConnection();
        c.setAutoCommit(false);
        return c;
    }
}
