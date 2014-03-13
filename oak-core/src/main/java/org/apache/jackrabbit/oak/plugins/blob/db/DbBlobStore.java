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
package org.apache.jackrabbit.oak.plugins.blob.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.plugins.blob.CachingBlobStore;
import org.h2.jdbcx.JdbcConnectionPool;

import com.google.common.collect.AbstractIterator;

/**
 * A database blob store.
 */
public class DbBlobStore extends CachingBlobStore {

    private JdbcConnectionPool cp;
    private long minLastModified;

    public void setConnectionPool(JdbcConnectionPool cp) throws SQLException {
        this.cp = cp;
        Connection conn = cp.getConnection();
        Statement stat = conn.createStatement();
        stat.execute("create table if not exists datastore_meta" +
                "(id varchar primary key, level int, lastMod bigint)");
        stat.execute("create table if not exists datastore_data" +
                "(id varchar primary key, data binary)");
        stat.close();
        conn.close();
    }

    @Override
    protected synchronized void storeBlock(byte[] digest, int level, byte[] data) throws IOException {
        try {
            storeBlockToDatabase(digest, level, data);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
    
    private void storeBlockToDatabase(byte[] digest, int level, byte[] data) throws SQLException {
        String id = StringUtils.convertBytesToHex(digest);
        cache.put(id, data);
        Connection conn = cp.getConnection();
        try {
            long now = System.currentTimeMillis();
            PreparedStatement prep = conn.prepareStatement(
                    "update datastore_meta set lastMod = ? where id = ?");
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
                    prep = conn.prepareStatement(
                            "insert into datastore_data(id, data) values(?, ?)");
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
                    prep = conn.prepareStatement(
                            "insert into datastore_meta(id, level, lastMod) values(?, ?, ?)");
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
            conn.close();
        }
    }

    @Override
    protected byte[] readBlockFromBackend(BlockId blockId) throws Exception {
        String id = StringUtils.convertBytesToHex(blockId.getDigest());
        byte[] data = cache.get(id);
        if (data == null) {        
            Connection conn = cp.getConnection();
            try {
                PreparedStatement prep = conn.prepareStatement(
                        "select data from datastore_data where id = ?");
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
                conn.close();
            }
        }
        // System.out.println("    read block " + id + " blockLen: " + data.length + " [0]: " + data[0]);
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
        if (minLastModified == 0) {
            return;
        }
        Connection conn = cp.getConnection();
        try {
            String id = StringUtils.convertBytesToHex(blockId.getDigest());
            PreparedStatement prep = conn.prepareStatement(
                    "update datastore_meta set lastMod = ? where id = ? and lastMod < ?");
            prep.setLong(1, System.currentTimeMillis());
            prep.setString(2, id);
            prep.setLong(3, minLastModified);
            prep.executeUpdate();
            prep.close();
        } finally {
            conn.close();
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
        int count = 0;
        Connection conn = cp.getConnection();
        try {
            PreparedStatement prep = conn.prepareStatement(
                    "select id from datastore_meta where lastMod < ?");
            prep.setLong(1, minLastModified);
            ResultSet rs = prep.executeQuery();
            ArrayList<String> ids = new ArrayList<String>();
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
            prep = conn.prepareStatement(
                "delete from datastore_meta where id = ?");
            PreparedStatement prepData = conn.prepareStatement(
                "delete from datastore_data where id = ?");
            for (String id : ids) {
                prep.setString(1, id);
                prep.execute();
                prepData.setString(1, id);
                prepData.execute();
                count++;
            }
            prepData.close();
            prep.close();
        } finally {
            conn.close();
        }
        minLastModified = 0;
        return count;
    }

    @Override
    public boolean deleteChunk(String chunkId, long maxLastModifiedTime) throws Exception {
        Connection conn = cp.getConnection();
        try {
            PreparedStatement prep = null;
            PreparedStatement prepData = null;

            if (maxLastModifiedTime > 0) {
                prep = conn.prepareStatement(
                        "delete from datastore_meta where id = ? and lastMod <= ?");
                prep.setLong(2, maxLastModifiedTime);

                prepData = conn.prepareStatement(
                        "delete from datastore_data where id = ? and lastMod <= ?");
                prepData.setLong(2, maxLastModifiedTime);
            } else {
                prep = conn.prepareStatement(
                        "delete from datastore_meta where id = ?");

                prepData = conn.prepareStatement(
                        "delete from datastore_data where id = ?");
            }
            prep.setString(1, chunkId);
            prep.execute();
            prepData.setString(1, chunkId);
            prepData.execute();

            prep.close();
            prepData.close();
        } finally {
            conn.commit();
            conn.close();
        }

        return true;
    }

    @Override
    public Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception {
        final Connection conn = cp.getConnection();
        PreparedStatement prep = null;

        if (maxLastModifiedTime > 0) {
            prep = conn.prepareStatement(
                    "select id from datastore_meta where lastMod <= ?");
            prep.setLong(1, maxLastModifiedTime);
        } else {
            prep = conn.prepareStatement(
                    "select id from datastore_meta");
        }

        final ResultSet rs = prep.executeQuery();

        return new AbstractIterator<String>() {
            @Override
            protected String computeNext() {
                try {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                    conn.close();
                } catch (SQLException e) {
                    try {
                        if ((conn != null) && !conn.isClosed()) {
                            conn.close();
                        }
                    } catch (Exception e2) {
                        // ignore
                    }
                    throw new RuntimeException(e);
                }
                return endOfData();
            }
        };
    }
    
}
