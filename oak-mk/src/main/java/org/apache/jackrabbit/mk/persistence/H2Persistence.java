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

import org.apache.jackrabbit.mk.model.ChildNodeEntriesMap;
import org.apache.jackrabbit.mk.model.Commit;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.Node;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.store.BinaryBinding;
import org.apache.jackrabbit.mk.store.Binding;
import org.apache.jackrabbit.mk.store.IdFactory;
import org.apache.jackrabbit.mk.store.NotFoundException;
import org.h2.jdbcx.JdbcConnectionPool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 *
 */
public class H2Persistence implements Persistence {

    private static final boolean FAST = Boolean.getBoolean("mk.fastDb");

    private JdbcConnectionPool cp;
    
    // TODO: make this configurable
    private IdFactory idFactory = IdFactory.getDigestFactory();

    //---------------------------------------------------< Persistence >

    public void initialize(File homeDir) throws Exception {
        File dbDir = new File(homeDir, "db");
        if (!dbDir.exists()) {
            dbDir.mkdirs();
        }

        Class.forName("org.h2.Driver");
        String url = "jdbc:h2:" + dbDir.getCanonicalPath() + "/revs";
        if (FAST) {
            url += ";log=0;undo_log=0";
        }
        cp = JdbcConnectionPool.create(url, "sa", "");
        cp.setMaxConnections(40);
        Connection con = cp.getConnection();
        try {
            Statement stmt = con.createStatement();
            stmt.execute("create table if not exists REVS(ID binary primary key, DATA binary)");
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

    public Id readHead() throws Exception {
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

    public Binding readNodeBinding(Id id) throws NotFoundException, Exception {
        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con.prepareStatement("select DATA from REVS where ID = ?");
            try {
                stmt.setBytes(1, id.getBytes());
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    ByteArrayInputStream in = new ByteArrayInputStream(rs.getBytes(1));
                    return new BinaryBinding(in);
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
        //String id = StringUtils.convertBytesToHex(rawId);

        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con
                    .prepareStatement(
                            "insert into REVS (ID, DATA) select ?, ? where not exists (select 1 from REVS where ID = ?)");
            try {
                stmt.setBytes(1, rawId);
                stmt.setBytes(2, bytes);
                stmt.setBytes(3, rawId);
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

        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con
                    .prepareStatement(
                            "insert into REVS (ID, DATA) select ?, ? where not exists (select 1 from REVS where ID = ?)");
            try {
                stmt.setBytes(1, id.getBytes());
                stmt.setBytes(2, bytes);
                stmt.setBytes(3, id.getBytes());
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
            PreparedStatement stmt = con.prepareStatement("select DATA from REVS where ID = ?");
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

    public Id writeCNEMap(ChildNodeEntriesMap map) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        map.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();
        byte[] rawId = idFactory.createContentId(bytes);

        Connection con = cp.getConnection();
        try {
            PreparedStatement stmt = con
                    .prepareStatement(
                            "insert into REVS (ID, DATA) select ?, ? where not exists (select 1 from REVS where ID = ?)");
            try {
                stmt.setBytes(1, rawId);
                stmt.setBytes(2, bytes);
                stmt.setBytes(3, rawId);
                stmt.executeUpdate();
            } finally {
                stmt.close();
            }
        } finally {
            con.close();
        }
        return new Id(rawId);
    }
}