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
package org.apache.jackrabbit.mongomk.prototype;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.jackrabbit.mongomk.prototype.UpdateOp.Operation;

/**
 * Emulates a MongoDB store (possibly consisting of multiple shards and
 * replicas).
 */
public class MemoryDocumentStore implements DocumentStore {
    
    /**
     * The 'nodes' collection. It contains all the node data, with one document
     * per node, and the path as the primary key. Each document possibly
     * contains multiple revisions.
     * <p>
     * Key: the path, value: the node data (possibly multiple revisions)
     * <p>
     * Old revisions are removed after some time, either by the process that
     * removed or updated the node, lazily when reading, or in a background
     * process.
     */
    private ConcurrentSkipListMap<String, Map<String, Object>> nodes = 
            new ConcurrentSkipListMap<String, Map<String, Object>>();
    
    /**
     * Get a document. The returned map is a clone (the caller
     * can modify it without affecting the stored version).
     * 
     * @param collection the collection
     * @param path the path
     * @return the map, or null if not found
     */
    public Map<String, Object> find(Collection collection, String path) {
        ConcurrentSkipListMap<String, Map<String, Object>> map = getMap(collection);
        Map<String, Object> n = map.get(path);
        if (n == null) {
            return null;
        }
        Map<String, Object> copy = Utils.newMap();
        synchronized (n) {
            copy.putAll(n);
            return copy;
        }
    }
    
    /**
     * Remove a document.
     * 
     * @param collection the collection
     * @param path the path
     */
    public void remove(Collection collection, String path) {
        getMap(collection).remove(path);
    }
    
    /**
     * Get the in-memory map for this collection.
     * 
     * @param collection the collection
     * @return the map
     */
    private ConcurrentSkipListMap<String, Map<String, Object>> getMap(Collection collection) {
        switch (collection) {
        case NODES:
            return nodes;
        default:
            throw new IllegalArgumentException(collection.name());
        }
    }
    
    /**
     * Create or update a document. For MongoDb, this is using "findAndModify" with
     * the "upsert" flag (insert or update).
     * 
     * @param collection the collection
     * @param update the update operation
     * @return the old document, or null if there was no
     */
    public Map<String, Object> createOrUpdate(Collection collection, UpdateOp update) {
        ConcurrentSkipListMap<String, Map<String, Object>> map = getMap(collection);
        Map<String, Object> n;
        Map<String, Object> oldNode;
        
        // get the node if it's there
        oldNode = n = map.get(update.key);

        if (n == null) {
            // for a new node, add it (without synchronization)
            n = Utils.newMap();
            oldNode = map.putIfAbsent(update.key, n);
            if (oldNode != null) {
                // somebody else added it at the same time
                n = oldNode;
            }
        }
        if (oldNode != null) {
            // clone the old node
            // (document level operations are synchronized)
            Map<String, Object> old = Utils.newMap();
            synchronized (oldNode) {
                old.putAll(oldNode);
            }
            oldNode = old;
        }
        // update the document 
        // (document level operations are synchronized)
        synchronized (n) {
            for (Entry<String, Operation> e : update.changes.entrySet()) {
                String k = e.getKey();
                Object old = n.get(k);
                Operation op = e.getValue();
                switch (op.type) {
                case SET: {
                    n.put(k, op.value);
                    break;
                }
                case INCREMENT: {
                    Long x = (Long) op.value;
                    if (old == null) {
                        old = 0L;
                    }
                    n.put(k, ((Long) old) + x);
                    break;
                }
                case ADD_MAP_ENTRY: {
                    @SuppressWarnings("unchecked")
                    Map<String, String> m = (Map<String, String>) old;
                    if (m == null) {
                        m = Utils.newMap();
                        n.put(k, m);
                    }
                    m.put(op.subKey.toString(), op.value.toString());
                    break;
                }
                case REMOVE_MAP_ENTRY: {
                    @SuppressWarnings("unchecked")
                    Map<String, String> m = (Map<String, String>) old;
                    if (m != null) {
                        m.remove(op.subKey.toString());
                    }
                    break;
                }
                }
            }
        }
        return oldNode;
    }
    
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("Nodes:\n");
        for(String p : nodes.keySet()) {
            buff.append("Path: ").append(p).append('\n');
            Map<String, Object> e = nodes.get(p);
            for(String prop : e.keySet()) {
                buff.append(prop).append('=').append(e.get(prop)).append('\n');
            }
            buff.append("\n");
        }
        return buff.toString();
    }
    
}
