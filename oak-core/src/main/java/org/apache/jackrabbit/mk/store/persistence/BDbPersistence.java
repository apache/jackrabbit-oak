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
package org.apache.jackrabbit.mk.store.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;

import org.apache.jackrabbit.mk.model.ChildNodeEntriesMap;
import org.apache.jackrabbit.mk.model.Commit;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.Node;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.store.BinaryBinding;
import org.apache.jackrabbit.mk.store.Binding;
import org.apache.jackrabbit.mk.store.IdFactory;
import org.apache.jackrabbit.mk.store.NotFoundException;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 *
 */
public class BDbPersistence implements Persistence {

    private final static byte[] HEAD_ID = new byte[]{0};
    private Environment dbEnv;
    private Database db;
    private Database head;

    // TODO: make this configurable
    private IdFactory idFactory = IdFactory.getDigestFactory();
    
    public void initialize(File homeDir) throws Exception {
        File dbDir = new File(homeDir, "db");
        if (!dbDir.exists()) {
            dbDir.mkdir();
        }

        EnvironmentConfig envConfig = new EnvironmentConfig();
        //envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        dbEnv = new Environment(dbDir, envConfig);

        EnvironmentMutableConfig envMutableConfig = new EnvironmentMutableConfig();
        //envMutableConfig.setDurability(Durability.COMMIT_SYNC);
        //envMutableConfig.setDurability(Durability.COMMIT_NO_SYNC);
        envMutableConfig.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
        dbEnv.setMutableConfig(envMutableConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        //dbConfig.setDeferredWrite(true);
        db = dbEnv.openDatabase(null, "revs", dbConfig);

        head = dbEnv.openDatabase(null, "head", dbConfig);

        // TODO FIXME workaround in case we're not closed properly
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try { close(); } catch (Throwable ignore) {}
            }
        });
    }

    public void close() {
        try {
            if (db.getConfig().getDeferredWrite()) {
                db.sync();
            }
            db.close();
            head.close();
            dbEnv.close();

            db = null;
            dbEnv = null;
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public Id readHead() throws Exception {
        DatabaseEntry key = new DatabaseEntry(HEAD_ID);
        DatabaseEntry data = new DatabaseEntry();

        if (head.get(null, key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            return new Id(data.getData());
        } else {
            return null;
        }
    }

    public void writeHead(Id id) throws Exception {
        DatabaseEntry key = new DatabaseEntry(HEAD_ID);
        DatabaseEntry data = new DatabaseEntry(id.getBytes());

        head.put(null, key, data);
    }

    public Binding readNodeBinding(Id id) throws NotFoundException, Exception {
        DatabaseEntry key = new DatabaseEntry(id.getBytes());
        DatabaseEntry data = new DatabaseEntry();

        if (db.get(null, key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            ByteArrayInputStream in = new ByteArrayInputStream(data.getData());
            return new BinaryBinding(in);
        } else {
            throw new NotFoundException(id.toString());
        }
    }

    public Id writeNode(Node node) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        node.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();
        Id id = new Id(idFactory.createContentId(bytes));
        persist(id.getBytes(), bytes);
        return id;
    }

    public StoredCommit readCommit(Id id) throws NotFoundException, Exception {
        DatabaseEntry key = new DatabaseEntry(id.getBytes());
        DatabaseEntry data = new DatabaseEntry();

        if (db.get(null, key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            ByteArrayInputStream in = new ByteArrayInputStream(data.getData());
            return StoredCommit.deserialize(id.toString(), new BinaryBinding(in));
        } else {
            throw new NotFoundException(id.toString());
        }
    }

    public void writeCommit(Id id, Commit commit) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        commit.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();
        persist(id.getBytes(), bytes);
    }

    public ChildNodeEntriesMap readCNEMap(Id id) throws NotFoundException, Exception {
        DatabaseEntry key = new DatabaseEntry(id.getBytes());
        DatabaseEntry data = new DatabaseEntry();

        if (db.get(null, key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            ByteArrayInputStream in = new ByteArrayInputStream(data.getData());
            return ChildNodeEntriesMap.deserialize(new BinaryBinding(in));
        } else {
            throw new NotFoundException(id.toString());
        }
    }

    public Id writeCNEMap(ChildNodeEntriesMap map) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        map.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();
        Id id = new Id(idFactory.createContentId(bytes));
        persist(id.getBytes(), bytes);
        return id;
    }

    //-------------------------------------------------------< implementation >

    protected void persist(byte[] rawId, byte[] bytes) throws Exception {
        DatabaseEntry key = new DatabaseEntry(rawId);
        DatabaseEntry data = new DatabaseEntry(bytes);

        db.put(null, key, data);
    }
}
