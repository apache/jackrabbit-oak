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
package org.apache.jackrabbit.oak.plugins.document.memory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.JournalEntry;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateUtils;

import com.google.common.base.Splitter;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;

import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition.newEqualsCondition;
import static org.apache.jackrabbit.oak.plugins.document.UpdateUtils.assertUnconditional;
import static org.apache.jackrabbit.oak.plugins.document.UpdateUtils.checkConditions;

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

    /**
     * The 'settings' collection.
     */
    private ConcurrentSkipListMap<String, Document> settings =
            new ConcurrentSkipListMap<String, Document>();

    /**
     * The 'externalChanges' collection.
     */
    private ConcurrentSkipListMap<String, JournalEntry> externalChanges =
            new ConcurrentSkipListMap<String, JournalEntry>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private ReadPreference readPreference;

    private WriteConcern writeConcern;

    private Object lastReadWriteMode;

    private final Map<String, String> metadata;

    private final boolean maintainModCount;

    private static final Key KEY_MODIFIED = new Key(MODIFIED_IN_SECS, null);

    public MemoryDocumentStore() {
        this(false);
    }

    public MemoryDocumentStore(boolean maintainModCount) {
        metadata = ImmutableMap.<String,String>builder()
                        .put("type", "memory")
                        .build();
        this.maintainModCount = maintainModCount;
    }

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
            ConcurrentNavigableMap<String, T> sub = map.subMap(fromKey + "\0", toKey);
            ArrayList<T> list = new ArrayList<T>();
            for (T doc : sub.values()) {
                if (indexedProperty != null) {
                    Object value = doc.get(indexedProperty);
                    if (value instanceof Boolean) {
                        long test = ((Boolean) value) ? 1 : 0;
                        if (test < startValue) {
                            continue;
                        }
                    } else if (value instanceof Long) {
                        if ((Long) value < startValue) {
                            continue;
                        }
                    } else if (value != null) {
                        throw new DocumentStoreException("unexpected type for property " + indexedProperty + ": "
                                + value.getClass());
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
    public <T extends Document> void remove(Collection<T> collection, String key) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            getMap(collection).remove(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        for(String key : keys){
            remove(collection, key);
        }
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection, Map<String, Long> toRemove) {
        int num = 0;
        ConcurrentSkipListMap<String, T> map = getMap(collection);
        for (Map.Entry<String, Long> entry : toRemove.entrySet()) {
            Lock lock = rwLock.writeLock();
            lock.lock();
            try {
                T doc = map.get(entry.getKey());
                Condition c = newEqualsCondition(entry.getValue());
                if (doc != null && checkConditions(doc, Collections.singletonMap(KEY_MODIFIED, c))) {
                    if (map.remove(entry.getKey()) != null) {
                        num++;
                    }
                }
            } finally {
                lock.unlock();
            }
        }
        return num;
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection,
                                    final String indexedProperty, final long startValue, final long endValue)
            throws DocumentStoreException {
        ConcurrentSkipListMap<String, T> map = getMap(collection);
        int num = map.size();

        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            Maps.filterValues(map, new Predicate<T>() {
                @Override
                public boolean apply(@Nullable T doc) {
                    Long modified = Utils.asLong((Number) doc.get(indexedProperty));
                    return startValue < modified && modified < endValue;
                }
            }).clear();
        } finally {
            lock.unlock();
        }

        num -= map.size();
        return num;
    }

    @CheckForNull
    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update) {
        assertUnconditional(update);
        return internalCreateOrUpdate(collection, update, false);
    }

    @Override
    public <T extends Document> List<T> createOrUpdate(Collection<T> collection, List<UpdateOp> updateOps) {
        List<T> result = new ArrayList<T>(updateOps.size());
        for (UpdateOp update : updateOps) {
            result.add(createOrUpdate(collection, update));
        }
        return result;
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) {
        return internalCreateOrUpdate(collection, update, true);
    }

    /**
     * @return a copy of this document store.
     */
    @Nonnull
    public MemoryDocumentStore copy() {
        MemoryDocumentStore copy = new MemoryDocumentStore();
        copyDocuments(Collection.NODES, copy);
        copyDocuments(Collection.CLUSTER_NODES, copy);
        copyDocuments(Collection.SETTINGS, copy);
        copyDocuments(Collection.JOURNAL, copy);
        return copy;
    }

    private <T extends Document> void copyDocuments(Collection<T> collection,
                                                    MemoryDocumentStore target) {
        ConcurrentSkipListMap<String, T> from = getMap(collection);
        ConcurrentSkipListMap<String, T> to = target.getMap(collection);

        for (Map.Entry<String, T> entry : from.entrySet()) {
            T doc = collection.newDocument(target);
            entry.getValue().deepCopy(doc);
            doc.seal();
            to.put(entry.getKey(), doc);
        }
    }

    /**
     * Get the in-memory map for this collection.
     *
     * @param collection the collection
     * @return the map
     */
    @SuppressWarnings("unchecked")
    protected <T extends Document> ConcurrentSkipListMap<String, T> getMap(Collection<T> collection) {
        if (collection == Collection.NODES) {
            return (ConcurrentSkipListMap<String, T>) nodes;
        } else if (collection == Collection.CLUSTER_NODES) {
            return (ConcurrentSkipListMap<String, T>) clusterNodes;
        } else if (collection == Collection.SETTINGS) {
            return (ConcurrentSkipListMap<String, T>) settings;
        } else if (collection == Collection.JOURNAL) {
            return (ConcurrentSkipListMap<String, T>) externalChanges;
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
            oldDoc = map.get(update.getId());

            T doc = collection.newDocument(this);
            if (oldDoc == null) {
                if (!update.isNew()) {
                    throw new DocumentStoreException("Document does not exist: " + update.getId());
                }
            } else {
                oldDoc.deepCopy(doc);
            }
            if (checkConditions && !checkConditions(doc, update.getConditions())) {
                return null;
            }
            // update the document
            UpdateUtils.applyChanges(doc, update);
            maintainModCount(doc);
            doc.seal();
            map.put(update.getId(), doc);
            return oldDoc;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection,
                                               List<UpdateOp> updateOps) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            ConcurrentSkipListMap<String, T> map = getMap(collection);
            for (UpdateOp op : updateOps) {
                if (map.containsKey(op.getId())) {
                    return false;
                }
            }
            for (UpdateOp op : updateOps) {
                assertUnconditional(op);
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
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                buff.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
            }
            buff.append("\n");
        }
        return buff.toString();
    }

    @Override
    public CacheInvalidationStats invalidateCache() {
        return null;
    }

    @Override
    public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        return null;
    }
    
    @Override
    public void dispose() {
        // ignore
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String key) {
        return find(collection, key);
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String key) {
        // ignore
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {
        if (readWriteMode == null || readWriteMode.equals(lastReadWriteMode)) {
            return;
        }
        lastReadWriteMode = readWriteMode;
        try {
            Map<String, String> map = Splitter.on(", ").withKeyValueSeparator(":").split(readWriteMode);
            String read = map.get("read");
            if (read != null) {
                ReadPreference readPref = ReadPreference.valueOf(read);
                if (!readPref.equals(this.readPreference)) {
                    this.readPreference = readPref;
                }
            }
            String write = map.get("write");
            if (write != null) {
                WriteConcern writeConcern = WriteConcern.valueOf(write);
                if (!writeConcern.equals(this.writeConcern)) {
                    this.writeConcern = writeConcern;
                }
            }
        } catch (Exception e) {
            // unsupported or parse error - ignore
        }
    }

    public ReadPreference getReadPreference() {
        return readPreference;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public Iterable<CacheStats> getCacheStats() {
        return null;
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
    }

    @Nonnull
    @Override
    public Map<String, String> getStats() {
        return ImmutableMap.<String, String>builder()
                .put(Collection.NODES.toString(), String.valueOf(nodes.size()))
                .put(Collection.CLUSTER_NODES.toString(), String.valueOf(clusterNodes.size()))
                .put(Collection.SETTINGS.toString(), String.valueOf(settings.size()))
                .put(Collection.JOURNAL.toString(), String.valueOf(externalChanges.size()))
                .build();
    }

    @Override
    public long determineServerTimeDifferenceMillis() {
        // the MemoryDocumentStore has no delays, thus return 0
        return 0;
    }

    private void maintainModCount(Document doc) {
        if (!maintainModCount) {
            return;
        }
        Long modCount = doc.getModCount();
        if (modCount == null) {
            modCount = 0L;
        }
        doc.put(Document.MOD_COUNT, modCount + 1);
    }
}