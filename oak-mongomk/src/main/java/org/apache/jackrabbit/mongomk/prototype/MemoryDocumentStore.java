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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
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

    public Map<String, Object> find(Collection collection, String path) {
        ConcurrentSkipListMap<String, Map<String, Object>> map = getMap(collection);
        Map<String, Object> n = map.get(path);
        if (n == null) {
            return null;
        }
        Map<String, Object> copy = Utils.newMap();
        synchronized (n) {
            copy.putAll(n);
        }
        return copy;
    }
    
    public List<Map<String, Object>> query(Collection collection, String fromKey, String toKey, int limit) {
        ConcurrentSkipListMap<String, Map<String, Object>> map = getMap(collection);
        ConcurrentNavigableMap<String, Map<String, Object>> sub = map.subMap(fromKey, toKey);
        ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        for (Map<String, Object> n : sub.values()) {
            Map<String, Object> copy = Utils.newMap();
            synchronized (n) {
                copy.putAll(n);
            }
            list.add(copy);
            if (list.size() > limit) {
                break;
            }
        }
        return list;
    }

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
            applyChanges(n, update);
        }
        return n;
    }
    
    public static void applyChanges(Map<String, Object> target, UpdateOp update) {
        for (Entry<String, Operation> e : update.changes.entrySet()) {
            String k = e.getKey();
            String[] kv = k.split("\\.");
            Operation op = e.getValue();
            switch (op.type) {
            case SET: {
                target.put(k, op.value);
                break;
            }
            case INCREMENT: {
                Object old = target.get(k);
                Long x = (Long) op.value;
                if (old == null) {
                    old = 0L;
                }
                target.put(k, ((Long) old) + x);
                break;
            }
            case ADD_MAP_ENTRY: {
                Object old = target.get(kv[0]);
                @SuppressWarnings("unchecked")
                Map<String, String> m = (Map<String, String>) old;
                if (m == null) {
                    m = Utils.newMap();
                    target.put(kv[0], m);
                }
                m.put(kv[1], op.value.toString());
                break;
            }
            case REMOVE_MAP_ENTRY: {
                Object old = target.get(kv[0]);
                @SuppressWarnings("unchecked")
                Map<String, String> m = (Map<String, String>) old;
                if (m != null) {
                    m.remove(kv[1]);
                }
                break;
            }
            case SET_MAP_ENTRY: {
                Map<String, String> m = Utils.newMap();
                target.put(k, m);
                break;
            }
            }
        }
    }

    @Override
    public void create(Collection collection, List<UpdateOp> updateOps) {
        for (UpdateOp op : updateOps) {
            createOrUpdate(collection, op);
        }
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("Nodes:\n");
        for (String p : nodes.keySet()) {
            buff.append("Path: ").append(p).append('\n');
            Map<String, Object> e = nodes.get(p);
            for (String prop : e.keySet()) {
                buff.append(prop).append('=').append(e.get(prop)).append('\n');
            }
            buff.append("\n");
        }
        return buff.toString();
    }

    @Override
    public void dispose() {
        // ignore
    }

}