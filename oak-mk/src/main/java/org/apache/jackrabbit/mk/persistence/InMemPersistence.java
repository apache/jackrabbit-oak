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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class InMemPersistence implements GCPersistence {

    private final Map<Id, byte[]> objects = Collections.synchronizedMap(new HashMap<Id, byte[]>());
    private final Map<Id, byte[]> marked = Collections.synchronizedMap(new HashMap<Id, byte[]>());

    private Id head;
    private long gcStart;

    // TODO: make this configurable
    private IdFactory idFactory = IdFactory.getDigestFactory();
    
    @Override
    public void initialize(File homeDir) {
        // nothing to initialize
    }
    
    public Id readHead() {
        return head;
    }

    public void writeHead(Id id) {
        head = id;
    }

    public void readNode(StoredNode node) throws NotFoundException, Exception {
        Id id = node.getId();
        byte[] bytes = objects.get(id);
        if (bytes != null) {
            node.deserialize(new BinaryBinding(new ByteArrayInputStream(bytes)));
            return;
        }
        throw new NotFoundException(id.toString());
    }

    public Id writeNode(Node node) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        node.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();
        Id id = new Id(idFactory.createContentId(bytes));

        if (!objects.containsKey(id)) {
            objects.put(id, bytes);
        }
        if (gcStart != 0) {
            marked.put(id, bytes);
        }
        return id;
    }

    public StoredCommit readCommit(Id id) throws NotFoundException, Exception {
        byte[] bytes = objects.get(id);
        if (bytes != null) {
            return StoredCommit.deserialize(id, new BinaryBinding(new ByteArrayInputStream(bytes)));
        }
        throw new NotFoundException(id.toString());
    }

    public void writeCommit(Id id, Commit commit) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        commit.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();

        if (!objects.containsKey(id)) {
            objects.put(id, bytes);
        }
        if (gcStart != 0) {
            marked.put(id, bytes);
        }
    }

    public ChildNodeEntriesMap readCNEMap(Id id) throws NotFoundException, Exception {
        byte[] bytes = objects.get(id);
        if (bytes != null) {
            return ChildNodeEntriesMap.deserialize(new BinaryBinding(new ByteArrayInputStream(bytes)));
        }
        throw new NotFoundException(id.toString());
    }

    public Id writeCNEMap(ChildNodeEntries map) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        map.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();
        Id id = new Id(idFactory.createContentId(bytes));

        if (!objects.containsKey(id)) {
            objects.put(id, bytes);
        }
        if (gcStart != 0) {
            marked.put(id, bytes);
        }
        return id;
    }

    @Override
    public void close() {
        // nothing to do here
    }
    
    @Override
    public void start() {
        gcStart = System.currentTimeMillis();
        marked.clear();
    }

    @Override
    public boolean markCommit(Id id) throws NotFoundException {
        return markObject(id);
    }

    @Override
    public boolean markNode(Id id) throws NotFoundException {
        return markObject(id);
    }

    @Override
    public boolean markCNEMap(Id id) throws NotFoundException {
        return markObject(id);
    }
    
    @Override
    public void replaceCommit(Id id, Commit commit) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        commit.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();

        objects.put(id, bytes);
        marked.put(id, bytes);
    }
    
    private boolean markObject(Id id) throws NotFoundException {
        byte[] data = objects.get(id);
        if (data != null) {
            return marked.put(id, data) == null;
        }
        throw new NotFoundException(id.toString());
    }

    @Override
    public int sweep() {
        int count = objects.size();
        
        objects.clear();
        objects.putAll(marked);
        
        gcStart = 0;
        return count;
    }
}
