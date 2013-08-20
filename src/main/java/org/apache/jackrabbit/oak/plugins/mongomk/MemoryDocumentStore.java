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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    @Override
    public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {
        return find(collection, key);
    }
    
    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            ConcurrentSkipListMap<String, T> map = getMap(collection);
            return map.get(key);
        } finally {
            lock.unlock();
        }
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
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            ConcurrentSkipListMap<String, T> map = getMap(collection);
            ConcurrentNavigableMap<String, T> sub = map.subMap(fromKey, toKey);
            ArrayList<T> list = new ArrayList<T>();
            for (T doc : sub.values()) {
                if (indexedProperty != null) {
                    Long value = (Long) doc.get(indexedProperty);
                    if (value < startValue) {
                        continue;
                    }
                }
                list.add(doc);
                if (list.size() >= limit) {
                    break;
                }
            }
            return list;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void remove(Collection collection, String path) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            getMap(collection).remove(path);
        } finally {
            lock.unlock();
        }
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
        T oldDoc;

        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            // get the node if it's there
            oldDoc = map.get(update.key);

            T doc = collection.newDocument();
            if (oldDoc == null) {
                if (!update.isNew) {
                    throw new MicroKernelException("Document does not exist: " + update.key);
                }
            } else {
                oldDoc.deepCopy(doc);
            }
            if (checkConditions && !checkConditions(doc, update)) {
                return null;
            }
            // update the document
            applyChanges(doc, update);
            doc.seal();
            map.put(update.key, doc);
            return oldDoc;
        } finally {
            lock.unlock();
        }
    }

    private static boolean checkConditions(Document doc,
                                           UpdateOp update) {
        for (Map.Entry<String, Operation> change : update.changes.entrySet()) {
            Operation op = change.getValue();
            if (op.type == Operation.Type.CONTAINS_MAP_ENTRY) {
                String k = change.getKey();
                String[] kv = k.split("\\.");
                Object value = doc.get(kv[0]);
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
     * Apply the changes to the in-memory document.
     * 
     * @param doc the target document.
     * @param update the changes to apply
     */
    public static void applyChanges(Document doc, UpdateOp update) {
        for (Entry<String, Operation> e : update.changes.entrySet()) {
            String k = e.getKey();
            Operation op = e.getValue();
            switch (op.type) {
            case SET: {
                doc.put(k, op.value);
                break;
            }
            case INCREMENT: {
                Object old = doc.get(k);
                Long x = (Long) op.value;
                if (old == null) {
                    old = 0L;
                }
                doc.put(k, ((Long) old) + x);
                break;
            }
            case SET_MAP_ENTRY: {
                String[] kv = splitInTwo(k, '.');
                Object old = doc.get(kv[0]);
                @SuppressWarnings("unchecked")
                Map<String, Object> m = (Map<String, Object>) old;
                if (m == null) {
                    m = Utils.newMap();
                    doc.put(kv[0], m);
                }
                m.put(kv[1], op.value);
                break;
            }
            case REMOVE_MAP_ENTRY: {
                String[] kv = splitInTwo(k, '.');
                Object old = doc.get(kv[0]);
                @SuppressWarnings("unchecked")
                Map<String, Object> m = (Map<String, Object>) old;
                if (m != null) {
                    m.remove(kv[1]);
                }
                break;
            }
            case SET_MAP: {
                String[] kv = splitInTwo(k, '.');
                Object old = doc.get(kv[0]);
                @SuppressWarnings("unchecked")
                Map<String, Object> m = (Map<String, Object>) old;
                if (m == null) {
                    m = Utils.newMap();
                    doc.put(kv[0], m);
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
    public <T extends Document> boolean create(Collection<T> collection,
                                               List<UpdateOp> updateOps) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            ConcurrentSkipListMap<String, T> map = getMap(collection);
            for (UpdateOp op : updateOps) {
                if (map.containsKey(op.key)) {
                    return false;
                }
            }
            for (UpdateOp op : updateOps) {
                internalCreateOrUpdate(collection, op, false);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("Nodes:\n");
        for (String p : nodes.keySet()) {
            buff.append("Path: ").append(p).append('\n');
            NodeDocument doc = nodes.get(p);
            for (String prop : doc.keySet()) {
                buff.append(prop).append('=').append(doc.get(prop)).append('\n');
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