/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document.cache;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.ForwardingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.MemoryManagerServiceImpl;
import org.apache.directmemory.memory.Pointer;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.CachedNodeDocument;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.MongoMK;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.cache.AbstractCache.SimpleStatsCounter;
import static com.google.common.cache.AbstractCache.StatsCounter;

public class NodeDocOffHeapCache
        extends ForwardingCache.SimpleForwardingCache<String, NodeDocument>
        implements Closeable, OffHeapCache {
    private final StatsCounter statsCounter = new SimpleStatsCounter();

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Cache<String, NodeDocReference> offHeapCache;
    private final CacheStats offHeapCacheStats;

    private final MemoryManagerService<NodeDocument> memoryManager;

    private final KryoSerializer serializer;

    public NodeDocOffHeapCache(Cache<String, NodeDocument> delegate,
                               ForwardingListener<String, NodeDocument> forwardingListener,
                               MongoMK.Builder builder,
                               DocumentStore documentStore) {
        super(delegate);
        forwardingListener.setDelegate(new PrimaryRemovalListener());

        final long maxMemory = builder.getOffHeapCacheSize();

        //TODO We may also expire the entries from cache if not accessed for some time
        offHeapCache = CacheBuilder.newBuilder()
                .weigher(builder.getWeigher())
                .maximumWeight(maxMemory)
                .removalListener(new SecondaryRemovalListener())
                .recordStats()
                .build();

        offHeapCacheStats = new CacheStats(offHeapCache, "MongoMk-Documents-L2", builder.getWeigher(),
                builder.getOffHeapCacheSize());

        final long bufferSize = Ram.Gb(1);
        int noOfBuffers = Math.max(1, (int) (maxMemory / bufferSize));
        int buffSize = (int) Math.min(maxMemory, bufferSize);

        //TODO Check if UnsafeMemoryManagerServiceImpl should be preferred
        //on Sun/Oracle JDK
        memoryManager = new MemoryManagerServiceImpl<NodeDocument>();
        memoryManager.init(noOfBuffers, buffSize);

        serializer = new KryoSerializer(new OakKryoPool(documentStore));
    }

    @Override
    public NodeDocument getIfPresent(Object key) {
        NodeDocument result = super.getIfPresent(key);
        if (result == null) {
            result = retrieve(key, false);
        }
        return result;
    }


    @Override
    public NodeDocument get(final String key, final Callable<? extends NodeDocument> valueLoader)
            throws ExecutionException {
        return super.get(key, new Callable<NodeDocument>() {
            @Override
            public NodeDocument call()
                    throws Exception {
                //Check in offHeap first
                NodeDocument result = retrieve(key, true);

                //Not found in L2 then load
                if (result == null) {
                    result = valueLoader.call();
                }
                return result;
            }
        });
    }

    @Override
    public ImmutableMap<String, NodeDocument> getAllPresent(Iterable<?> keys) {
        @SuppressWarnings("unchecked") List<String> list = Lists.newArrayList((Iterable<String>) keys);
        ImmutableMap<String, NodeDocument> result = super.getAllPresent(list);

        //All the requested keys found then no
        //need to check L2
        if (result.size() == list.size()) {
            return result;
        }

        //Look up value from L2
        Map<String, NodeDocument> r2 = Maps.newHashMap(result);
        for (String key : list) {
            if (!result.containsKey(key)) {
                NodeDocument val = retrieve(key, false);
                if (val != null) {
                    r2.put(key, val);
                }
            }
        }
        return ImmutableMap.copyOf(r2);
    }

    @Override
    public void invalidate(Object key) {
        super.invalidate(key);
        offHeapCache.invalidate(key);
    }

    @Override
    public void invalidateAll(Iterable<?> keys) {
        super.invalidateAll(keys);
        offHeapCache.invalidateAll(keys);
    }

    @Override
    public void invalidateAll() {
        super.invalidateAll();
        offHeapCache.invalidateAll();
    }

    @Override
    public void close() throws IOException {
        memoryManager.close();
        serializer.close();
    }

    @Override
    public Map<String, ? extends CachedNodeDocument> offHeapEntriesMap() {
        return Collections.unmodifiableMap(offHeapCache.asMap());
    }

    @Override
    public CacheStats getCacheStats() {
        return offHeapCacheStats;
    }

    @Nullable
    @Override
    public CachedNodeDocument getCachedDocument(String id) {
        NodeDocument doc = super.getIfPresent(id);
        if (doc != null) {
            return doc;
        }
        return offHeapCache.getIfPresent(id);
    }

    /**
     * Retrieves the value from the off heap cache.
     *
     * @param key                     cache entry key to retrieve
     * @param invalidateAfterRetrieve set it to true if the entry from off heap cache has
     *                                to be invalidated. This would be the case when value loaded is
     *                                made part of L1 cache
     */
    private NodeDocument retrieve(Object key, boolean invalidateAfterRetrieve) {
        Stopwatch watch = Stopwatch.createStarted();

        NodeDocReference value = offHeapCache.getIfPresent(key);
        if (value == null) {
            statsCounter.recordMisses(1);
            return null;
        }

        NodeDocument result = value.getDocument();
        if (result != null) {
            statsCounter.recordLoadSuccess(watch.elapsed(TimeUnit.NANOSECONDS));
        } else {
            statsCounter.recordMisses(1);
        }

        if (invalidateAfterRetrieve) {
            //The value would be made part of L1 cache so no need to keep it
            //in backend
            offHeapCache.invalidate(key);
        }

        return result;
    }

    private class PrimaryRemovalListener implements RemovalListener<String, NodeDocument> {

        @Override
        public void onRemoval(RemovalNotification<String, NodeDocument> n) {
            //If removed explicitly then we clear from L2
            if (n.getCause() == RemovalCause.EXPLICIT
                    || n.getCause() == RemovalCause.REPLACED) {
                offHeapCache.invalidate(n.getKey());
            }

            //If removed because of size then we move it to
            //L2
            if (n.getCause() == RemovalCause.SIZE) {
                NodeDocument doc = n.getValue();
                if (doc != NodeDocument.NULL) {
                    offHeapCache.put(n.getKey(), new NodeDocReference(n.getKey(), doc));
                }
            }
        }
    }

    private class SecondaryRemovalListener implements RemovalListener<String, NodeDocReference> {
        @Override
        public void onRemoval(RemovalNotification<String, NodeDocReference> notification) {
            NodeDocReference doc = notification.getValue();
            if (doc != null && doc.getPointer() != null) {
                memoryManager.free(doc.getPointer());
            }
        }
    }

    private class NodeDocReference implements CachedNodeDocument, CacheValue {
        private final Number modCount;
        private final long created;
        private final AtomicLong lastCheckTime;
        private final Pointer<NodeDocument> documentPointer;
        private final String key;

        public NodeDocReference(String key, NodeDocument doc) {
            this.modCount = doc.getModCount();
            this.created = doc.getCreated();
            this.lastCheckTime = new AtomicLong(doc.getLastCheckTime());
            this.documentPointer = serialize(doc);
            this.key = key;
        }

        @Override
        public Number getModCount() {
            return modCount;
        }

        @Override
        public long getCreated() {
            return created;
        }

        @Override
        public long getLastCheckTime() {
            return lastCheckTime.get();
        }

        @Override
        public void markUpToDate(long checkTime) {
            lastCheckTime.set(checkTime);
        }

        @Override
        public boolean isUpToDate(long lastCheckTime) {
            return lastCheckTime <= this.lastCheckTime.get();
        }

        @CheckForNull
        public NodeDocument getDocument() {
            return deserialize(documentPointer);
        }

        @CheckForNull
        public Pointer<NodeDocument> getPointer() {
            return documentPointer;
        }

        @CheckForNull
        private Pointer<NodeDocument> serialize(NodeDocument doc) {
            try {
                byte[] payload = serializer.serialize(doc);
                Pointer<NodeDocument> ptr = memoryManager.store(payload, 0);
                ptr.setClazz(NodeDocument.class);
                return ptr;
            } catch (IOException e) {
                log.warn("Not able to serialize doc {}", doc.getId(), e);
                return null;
            }
        }

        @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
        @CheckForNull
        private NodeDocument deserialize(@CheckForNull Pointer<NodeDocument> pointer) {
            try {
                //If there was some error in serializing then pointer
                // would be null
                if (pointer == null) {
                    return null;
                }

                //TODO Look for a way to have a direct access to MemoryManager buffer
                //for Kryo so that no copying is involved

                final byte[] value;

                //Workaround for DIRECTMEMORY-137 Concurrent access via same pointer
                //can lead to issues. For now synchronizing on the pointer
                synchronized (pointer) {
                    value = memoryManager.retrieve(pointer);
                }

                NodeDocument doc = serializer.deserialize(value, pointer.getClazz());
                doc.markUpToDate(getLastCheckTime());
                return doc;
            } catch (Exception e) {
                log.warn("Not able to deserialize doc {} with pointer {}", new Object[]{key, pointer, e});
            }
            return null;
        }


        @Override
        public int getMemory() {
            int result = 168;

            if (documentPointer != null) {
                result += (int) documentPointer.getSize();
            }
            return result;
        }
    }

    private static class OakKryoPool extends KryoSerializer.KryoPool {
        private final DocumentStore documentStore;

        public OakKryoPool(DocumentStore documentStore) {
            this.documentStore = documentStore;
        }

        @Override
        protected Kryo createInstance() {
            return KryoFactory.createInstance(documentStore);
        }
    }

}
