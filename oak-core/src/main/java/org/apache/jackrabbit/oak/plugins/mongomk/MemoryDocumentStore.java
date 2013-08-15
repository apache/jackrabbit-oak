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
package org.apache.jackrabbit.oak.plugins.mongomk;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.plugins.mongomk.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.mongomk.util.Utils;

/**
 * Emulates a MongoDB store (possibly consisting of multiple shards and
 * replicas).
 */
public class MemoryDocumentStore implements DocumentStore {

    /**
     * The 'nodes' collection.
     */
    private ConcurrentSkipListMap<String, NodeDocument> nodes =
            new ConcurrentSkipListMap<String, NodeDocument>();
    
    /**
     * The 'clusterNodes' collection.
     */
    private ConcurrentSkipListMap<String, Document> clusterNodes =
            new ConcurrentSkipListMap<String, Document>();

    @Override
    public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {
        return find(collection, key);
    }
    
    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        ConcurrentSkipListMap<String, T> map = getMap(collection);
        Document doc = map.get(key);
        if (doc == null) {
            return null;
        }
        T copy = collection.newDocument();
        synchronized (doc) {
            Utils.deepCopyMap(doc, copy);
        }
        return copy;
    }
    
    @Override
    @Nonnull
    public <T extends Document> List<T> query(Collection<T> collection,
                                String fromKey,
                                String toKey,
                                int limit) {
        return query(collection, fromKey, toKey, null, 0, limit);
    }
    
    @Override
    @Nonnull
    public <T extends Document> List<T> query(Collection<T> collection,
                                String fromKey,
                                String toKey,
                                String indexedProperty,
                                long startValue,
                                int limit) {
        ConcurrentSkipListMap<String, T> map = getMap(collection);
        ConcurrentNavigableMap<String, T> sub = map.subMap(fromKey, toKey);
        ArrayList<T> list = new ArrayList<T>();
        for (Document doc : sub.values()) {
            if (indexedProperty != null) {
                Long value = (Long) doc.get(indexedProperty);
                if (value < startValue) {
                    continue;
                }
            }
            T copy = collection.newDocument();
            synchronized (doc) {
                Utils.deepCopyMap(doc, copy);
            }
            list.add(copy);
            if (list.size() >= limit) {
                break;
            }
        }
        return list;
    }

    @Override
    public void remove(Collection collection, String path) {
        getMap(collection).remove(path);
    }

    /**
     * Get the in-memory map for this collection.
     *
     * @param collection the collection
     * @return the map
     */
    @SuppressWarnings("unchecked")
    private <T extends Document> ConcurrentSkipListMap<String, T> getMap(Collection<T> collection) {
        if (collection == Collection.NODES) {
            return (ConcurrentSkipListMap<String, T>) nodes;
        } else if (collection == Collection.CLUSTER_NODES) {
            return (ConcurrentSkipListMap<String, T>) clusterNodes;
        } else {
            throw new IllegalArgumentException(
                    "Unknown collection: " + collection.toString());
        }
    }

    @CheckForNull
    private <T extends Document> T internalCreateOrUpdate(Collection<T> collection,
                                                          UpdateOp update,
                                                          boolean checkConditions) {
        ConcurrentSkipListMap<String, T> map = getMap(collection);
        T doc;
        T oldDoc;

        // get the node if it's there
        oldDoc = doc = map.get(update.key);

        if (doc == null) {
            if (!update.isNew) {
                throw new MicroKernelException("Document does not exist: " + update.key);
            }
            // for a new node, add it (without synchronization)
            doc = collection.newDocument();
            oldDoc = map.putIfAbsent(update.key, doc);
            if (oldDoc != null) {
                // somebody else added it at the same time
                doc = oldDoc;
            }
        }
        synchronized (doc) {
            if (checkConditions && !checkConditions(doc, update)) {
                return null;
            }
            if (oldDoc != null) {
                // clone the old node
                // (document level operations are synchronized)
                T oldDoc2 = collection.newDocument();
                Utils.deepCopyMap(oldDoc, oldDoc2);
                oldDoc = oldDoc2;
            }
            // to return the new document:
            // update the document
            // (document level operations are synchronized)
            applyChanges(doc, update);
        }
        return oldDoc;
    }

    @CheckForNull
    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update)
            throws MicroKernelException {
        return internalCreateOrUpdate(collection, update, false);
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update)
            throws MicroKernelException {
        return internalCreateOrUpdate(collection, update, true);
    }

    private static boolean checkConditions(Map<String, Object> target,
                                           UpdateOp update) {
        for (Map.Entry<String, Operation> change : update.changes.entrySet()) {
            Operation op = change.getValue();
            if (op.type == Operation.Type.CONTAINS_MAP_ENTRY) {
                String k = change.getKey();
                String[] kv = k.split("\\.");
                Object value = target.get(kv[0]);
                if (value == null) {
                    if (Boolean.TRUE.equals(op.value)) {
                        return false;
                    }
                } else {
                    if (value instanceof Map) {
                        Map<?, ?> map = (Map<?, ?>) value;
                        if (Boolean.TRUE.equals(op.value)) {
                            if (!map.containsKey(kv[1])) {
                                return false;
                            }
                        } else {
                            if (map.containsKey(kv[1])) {
                                return false;
                            }
                        }
                    } else {
                        return false;
                    }
                }
            }
        }
        return true;
    }


    /**
     * Apply the changes to the in-memory map.
     * 
     * @param target the target map
     * @param update the changes to apply
     */
    public static void applyChanges(Map<String, Object> target, UpdateOp update) {
        for (Entry<String, Operation> e : update.changes.entrySet()) {
            String k = e.getKey();
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
            case SET_MAP_ENTRY: {
                String[] kv = splitInTwo(k, '.');
                Object old = target.get(kv[0]);
                @SuppressWarnings("unchecked")
                Map<String, Object> m = (Map<String, Object>) old;
                if (m == null) {
                    m = Utils.newMap();
                    target.put(kv[0], m);
                }
                m.put(kv[1], op.value);
                break;
            }
            case REMOVE_MAP_ENTRY: {
                String[] kv = splitInTwo(k, '.');
                Object old = target.get(kv[0]);
                @SuppressWarnings("unchecked")
                Map<String, Object> m = (Map<String, Object>) old;
                if (m != null) {
                    m.remove(kv[1]);
                }
                break;
            }
            case SET_MAP: {
                String[] kv = splitInTwo(k, '.');
                Object old = target.get(kv[0]);
                @SuppressWarnings("unchecked")
                Map<String, Object> m = (Map<String, Object>) old;
                if (m == null) {
                    m = Utils.newMap();
                    target.put(kv[0], m);
                }
                m.put(kv[1], op.value);
                break;
            }
            case CONTAINS_MAP_ENTRY:
                // no effect
                break;
            }
        }
    }
    
    private static String[] splitInTwo(String s, char separator) {
        int index = s.indexOf(separator);
        if (index < 0) {
            return new String[] { s };
        }
        return new String[] { s.substring(0, index), s.substring(index + 1) };
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {
        ConcurrentSkipListMap<String, T> map = getMap(collection);
        for (UpdateOp op : updateOps) {
            if (map.containsKey(op.key)) {
                return false;
            }
        }
        for (UpdateOp op : updateOps) {
            createOrUpdate(collection, op);
        }
        return true;
    }

    @Override
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
    public void invalidateCache() {
        // there is no cache, so nothing to invalidate
    }

    @Override
    public void dispose() {
        // ignore
    }

    @Override
    public boolean isCached(Collection collection, String key) {
        return false;
    }

    @Override
    public void invalidateCache(Collection collection, String key) {
        // ignore
    }

}