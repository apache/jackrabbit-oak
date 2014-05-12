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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.plugins.blob.CachingBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;

public class RDBBlobStore extends CachingBlobStore implements Closeable {

    /**
     * Creates a {@linkplain RDBBlobStore} instance using the provided
     * {@link DataSource}.
     */
    public RDBBlobStore(DataSource ds) {
        try {
            initialize(ds);
        } catch (Exception ex) {
            throw new MicroKernelException("initializing RDB blob store", ex);
        }
    }

    @Override
    public void close() {
        this.ds = null;
    }

    @Override
    public void finalize() {
        if (this.ds != null && this.callStack != null) {
            LOG.debug("finalizing RDBDocumentStore that was not disposed", this.callStack);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(RDBBlobStore.class);

    private Exception callStack;

    private DataSource ds;

    private void initialize(DataSource ds) throws Exception {

        this.ds = ds;
        Connection con = ds.getConnection();

        try {
            con.setAutoCommit(false);

            for (String tableName : new String[] { "DATASTORE_META", "DATASTORE_DATA" }) {
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

                    if (tableName.equals("DATASTORE_META")) {
                        stmt.execute("create table " + tableName
                                + " (ID varchar(1000) not null primary key, LEVEL int, LASTMOD bigint)");
                    } else {
                        // the code below likely will need to be extended for
                        // new
                        // database types
                        if ("PostgreSQL".equals(dbtype)) {
                            stmt.execute("create table " + tableName + " (ID varchar(1000) not null primary key, DATA bytea)");
                        } else if ("DB2".equals(dbtype) || (dbtype != null && dbtype.startsWith("DB2/"))) {
                            stmt.execute("create table " + tableName + " (ID varchar(1000) not null primary key, DATA blob)");
                        } else {
                            stmt.execute("create table " + tableName + " (ID varchar(1000) not null primary key, DATA blob)");
                        }
                    }

                    stmt.close();

                    con.commit();
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
        Connection con = ds.getConnection();

        try {
            long now = System.currentTimeMillis();
            PreparedStatement prep = con.prepareStatement("update datastore_meta set lastMod = ? where id = ?");
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
                    prep = con.prepareStatement("insert into datastore_data(id, data) values(?, ?)");
                    try {
                        prep.setString(1, id);
                        prep.setBytes(2, data);
                        prep.execute();
                    } finally {
                        prep.close();
                    }
                } catch (SQLException e) {
                    // already exists - ok
                }
                try {
                    prep = con.prepareStatement("insert into datastore_meta(id, level, lastMod) values(?, ?, ?)");
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

    @Override
    protected byte[] readBlockFromBackend(BlockId blockId) throws Exception {

        String id = StringUtils.convertBytesToHex(blockId.getDigest());
        byte[] data = cache.get(id);
        Connection con = ds.getConnection();

        try {
            PreparedStatement prep = con.prepareStatement("select data from datastore_data where id = ?");
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
        Connection con = ds.getConnection();
        try {
            if (minLastModified == 0) {
                return;
            }
            String id = StringUtils.convertBytesToHex(blockId.getDigest());
            PreparedStatement prep = con.prepareStatement("update datastore_meta set lastMod = ? where id = ? and lastMod < ?");
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
        Connection con = ds.getConnection();
        try {
            int count = 0;
            PreparedStatement prep = con.prepareStatement("select id from datastore_meta where lastMod < ?");
            prep.setLong(1, minLastModified);
            ResultSet rs = prep.executeQuery();
            ArrayList<String> ids = new ArrayList<String>();
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
            prep = con.prepareStatement("delete from datastore_meta where id = ?");
            PreparedStatement prepData = con.prepareStatement("delete from datastore_data where id = ?");
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

        Connection con = ds.getConnection();
        try {
            PreparedStatement prep = null;
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
                prep = con.prepareStatement(
                        "delete from datastore_meta where id in (" 
                                + inClause.toString() + ") and lastMod <= ?");
                prep.setLong(batch + 1, maxLastModifiedTime);

                prepData = con.prepareStatement(
                        "delete from datastore_data where id in (" 
                                + inClause.toString() + ") and lastMod <= ?");
                prepData.setLong(batch + 1, maxLastModifiedTime);
            } else {
                prep = con.prepareStatement(
                        "delete from datastore_meta where id in (" 
                                + inClause.toString() + ")");

                prepData = con.prepareStatement(
                        "delete from datastore_data where id in (" 
                                + inClause.toString() + ")");
            }

            for (int idx = 0; idx < batch; idx++) {
                prep.setString(idx + 1, chunkIds.get(idx));
                prepData.setString(idx + 1, chunkIds.get(idx));
            }

            prep.execute();
            prepData.execute();
            prep.close();
            prepData.close();
        } finally {
            con.commit();
            con.close();
        }

        return true;
    }

    @Override
    public Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception {
        return new ChunkIdIterator(this.ds, maxLastModifiedTime);
    }


    /**
     * Reads chunk IDs in batches.
     */
    private static class ChunkIdIterator extends AbstractIterator<String> {

        private long maxLastModifiedTime;
        private DataSource ds;
        private static int BATCHSIZE = 1024 * 256;
        private List<String> results = new LinkedList<String>();
        private String lastId = null;

        public ChunkIdIterator(DataSource ds, long maxLastModifiedTime) {
            this.maxLastModifiedTime = maxLastModifiedTime;
            this.ds = ds;
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
            query.append("select id from datastore_meta");
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
            query.append(" order by id limit " + BATCHSIZE);

            Connection connection = null;
            try {
                connection = ds.getConnection();
                try {
                    PreparedStatement prep = connection.prepareStatement(query.toString());
                    int idx = 1;
                    if (maxLastModifiedTime > 0) {
                        prep.setLong(idx++, maxLastModifiedTime);
                    }
                    if (lastId != null) {
                        prep.setString(idx++, lastId);
                    }

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
}
