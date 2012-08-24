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
package org.apache.jackrabbit.mk.persistence;

import org.apache.jackrabbit.mk.model.ChildNodeEntries;
import org.apache.jackrabbit.mk.model.ChildNodeEntriesMap;
import org.apache.jackrabbit.mk.model.Commit;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.Node;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.model.StoredNode;
import org.apache.jackrabbit.mk.store.BinaryBinding;
import org.apache.jackrabbit.mk.store.IdFactory;
import org.apache.jackrabbit.mk.store.NotFoundException;
import org.h2.Driver;
import org.h2.jdbcx.JdbcConnectionPool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

/**
 *
 */
public class H2Persistence implements GCPersistence {

    private static final boolean FAST = Boolean.getBoolean("mk.fastDb");

    private JdbcConnectionPool cp;
    private long gcStart;
    
    // TODO: make this configurable
    private IdFactory idFactory = IdFactory.getDigestFactory();

    //---------------------------------------------------< Persistence >

    public void initialize(File homeDir) throws Exception {
        File dbDir = new File(homeDir, "db");
        if (!dbDir.exists()) {
            dbDir.mkdirs();
        }

        Driver.load();
        String url = "jdbc:h2:" + dbDir.getCanonicalPath() + "/revs";
        if (FAST) {
            url += ";log=0;undo_log=0";
        }
        cp = JdbcConnectionPool.create(url, "sa", "");
        cp.setMaxConnections(40);
        Connection con = cp.getConnection();
        try {
            Statement stmt = con.createStatement();
            stmt.execute("create table if not exists REVS(ID binary primary key, DATA binary, TIME timestamp)");
            stmt.execute("create table if not exists NODES(ID binary primary key, DATA binary, TIME timestamp)");
            stmt.execute("create table if not exists HEAD(ID binary) as select null");
            stmt.execute("create sequence if not exists DATASTORE_ID");
/*
            DbBlobStore store = new DbBlobStore();
            store.setConnectionPool(cp);
            blobStore = store;
*/
        } finally {
            con.close();
        }
    }

    public void close() {
        cp.dispose();
    }

    public Id[] readIds() throws Exception {
        Id lastCommitId = null;
        Id headId = readHead();
        if (headId != null) {
            lastCommitId = readLastCommitId();
        }
        return new Id[] { headId, lastCommitId };
    }
    
    private Id readHead() throws Exception {
        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con.prepareStatement("select * from HEAD");
            ResultSet rs = stmt.executeQuery();
            byte[] rawId = null;
            if (rs.next()) {
                rawId = rs.getBytes(1);
            }
            stmt.close();
            return rawId == null ? null : new Id(rawId); 
        } finally {
            con.close();
        }
    }

    private Id readLastCommitId() throws Exception {
        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con.prepareStatement("select MAX(ID) from REVS");
            ResultSet rs = stmt.executeQuery();
            byte[] rawId = null;
            if (rs.next()) {
                rawId = rs.getBytes(1);
            }
            stmt.close();
            return rawId == null ? null : new Id(rawId); 
        } finally {
            con.close();
        }
    }

    public void writeHead(Id id) throws Exception {
        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con.prepareStatement("update HEAD set ID=?");
            stmt.setBytes(1, id.getBytes());
            stmt.execute();
            stmt.close();
        } finally {
            con.close();
        }
    }

    public void readNode(StoredNode node) throws NotFoundException, Exception {
        Id id = node.getId();
        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con.prepareStatement("select DATA from NODES where ID = ?");
            try {
                stmt.setBytes(1, id.getBytes());
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    ByteArrayInputStream in = new ByteArrayInputStream(rs.getBytes(1));
                    node.deserialize(new BinaryBinding(in));
                } else {
                    throw new NotFoundException(id.toString());
                }
            } finally {
                stmt.close();
            }
        } finally {
            con.close();
        }
    }

    public Id writeNode(Node node) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        node.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();
        byte[] rawId = idFactory.createContentId(bytes);
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        //String id = StringUtils.convertBytesToHex(rawId);

        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con
                    .prepareStatement(
                            "insert into NODES (ID, DATA, TIME) select ?, ?, ? where not exists (select 1 from NODES where ID = ?)");
            try {
                stmt.setBytes(1, rawId);
                stmt.setBytes(2, bytes);
                stmt.setTimestamp(3, ts);
                stmt.setBytes(4, rawId);
                stmt.executeUpdate();
            } finally {
                stmt.close();
            }
        } finally {
            con.close();
        }
        return new Id(rawId);
    }

    public StoredCommit readCommit(Id id) throws NotFoundException, Exception {
        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con.prepareStatement("select DATA from REVS where ID = ?");
            try {
                stmt.setBytes(1, id.getBytes());
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    ByteArrayInputStream in = new ByteArrayInputStream(rs.getBytes(1));
                    return StoredCommit.deserialize(id, new BinaryBinding(in));
                } else {
                    throw new NotFoundException(id.toString());
                }
            } finally {
                stmt.close();
            }
        } finally {
            con.close();
        }
    }
    
    public void writeCommit(Id id, Commit commit) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        commit.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();
        Timestamp ts = new Timestamp(System.currentTimeMillis());

        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con
                    .prepareStatement(
                            "insert into REVS (ID, DATA, TIME) select ?, ?, ?");
            try {
                stmt.setBytes(1, id.getBytes());
                stmt.setBytes(2, bytes);
                stmt.setTimestamp(3, ts);
                stmt.executeUpdate();
            } finally {
                stmt.close();
            }
        } finally {
            con.close();
        }
    }

    public ChildNodeEntriesMap readCNEMap(Id id) throws NotFoundException, Exception {
        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con.prepareStatement("select DATA from NODES where ID = ?");
            try {
                stmt.setBytes(1, id.getBytes());
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    ByteArrayInputStream in = new ByteArrayInputStream(rs.getBytes(1));
                    return ChildNodeEntriesMap.deserialize(new BinaryBinding(in));
                } else {
                    throw new NotFoundException(id.toString());
                }
            } finally {
                stmt.close();
            }
        } finally {
            con.close();
        }
    }

    public Id writeCNEMap(ChildNodeEntries map) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        map.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();
        byte[] rawId = idFactory.createContentId(bytes);
        Timestamp ts = new Timestamp(System.currentTimeMillis());

        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con
                    .prepareStatement(
                            "insert into NODES (ID, DATA, TIME) select ?, ?, ? where not exists (select 1 from NODES where ID = ?)");
            try {
                stmt.setBytes(1, rawId);
                stmt.setBytes(2, bytes);
                stmt.setTimestamp(3, ts);
                stmt.setBytes(4, rawId);
                stmt.executeUpdate();
            } finally {
                stmt.close();
            }
        } finally {
            con.close();
        }
        return new Id(rawId);
    }

    @Override
    public void start() {
        gcStart = System.currentTimeMillis();
    }
    
    @Override
    public boolean markCommit(Id id) throws Exception {
        return touch("REVS", id, gcStart);
    }

    @Override
    public void replaceCommit(Id id, Commit commit) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        commit.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();

        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con
                    .prepareStatement(
                            "update REVS set DATA = ?, TIME = CURRENT_TIMESTAMP() where ID = ?");
            try {
                stmt.setBytes(1, bytes);
                stmt.setBytes(2, id.getBytes());
                stmt.executeUpdate();
            } finally {
                stmt.close();
            }
        } finally {
            con.close();
        }
    }
    
    @Override
    public boolean markNode(Id id) throws Exception {
        return touch("NODES", id, gcStart);
    }

    @Override
    public boolean markCNEMap(Id id) throws Exception {
        return touch("NODES", id, gcStart);
    }
    
    private boolean touch(String table, Id id, long timeMillis) throws Exception {
        Timestamp ts = new Timestamp(timeMillis);

        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con.prepareStatement(
                    String.format("update %s set TIME = ? where ID = ? and TIME < ?",
                            table));
                                    
            try {
                stmt.setTimestamp(1, ts);
                stmt.setBytes(2, id.getBytes());
                stmt.setTimestamp(3, ts);
                return stmt.executeUpdate() == 1;
            } finally {
                stmt.close();
            }
        } finally {
            con.close();
        }
    }
    
    @Override
    public int sweep() throws Exception {
        Timestamp ts = new Timestamp(gcStart);
        int swept = 0;

        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con.prepareStatement("delete REVS where TIME < ?");
            try {
                stmt.setTimestamp(1, ts);
                swept += stmt.executeUpdate();
            } finally {
                stmt.close();
            }

            stmt = con.prepareStatement("delete NODES where TIME < ?");
            
            try {
                stmt.setTimestamp(1, ts);
                swept += stmt.executeUpdate();
            } finally {
                stmt.close();
            }
        } finally {
            con.close();
        }
        return swept;
     }
}