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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.EvictionCause;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.commons.collections.ListUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.MemoryDiffCache;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.async.CacheWriteQueue;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.h2.mvstore.WriteBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AsyncQueueTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private static final StringValue VAL = new StringValue("xyz");

    private static final StringValue NEW_VAL = new StringValue("abc");

    private PersistentCache pCache;

    private List<PathRev> putActions;

    private List<PathRev> invalidateActions;

    private NodeCache<PathRev, StringValue> nodeCache;

    private int id;

    @Before
    public void setup() throws IOException {
        FileUtils.deleteDirectory(new File("target/cacheTest"));
        pCache = new PersistentCache("target/cacheTest,+asyncDiff");
        final AtomicReference<NodeCache<PathRev, StringValue>> nodeCacheRef = new AtomicReference<NodeCache<PathRev, StringValue>>();
        CacheLIRS<PathRev, StringValue> lirs = new CacheLIRS.Builder<PathRev, StringValue>().maximumSize(1).evictionCallback((key, value, cause) -> {
            if (nodeCacheRef.get() != null) {
                nodeCacheRef.get().evicted(key, value, EvictionCause.valueOf(cause.name()));
            }
        }).build();
        nodeCache = (NodeCache<PathRev, StringValue>) pCache.wrap(builderProvider.newBuilder().getNodeStore(),
                null, lirs.asOakCache(), CacheType.NODE);
        nodeCacheRef.set(nodeCache);

        CacheWriteQueueWrapper writeQueue = new CacheWriteQueueWrapper(nodeCache.writeQueue);
        nodeCache.writeQueue = writeQueue;

        this.putActions = writeQueue.putActions;
        this.invalidateActions = writeQueue.invalidateActions;
        this.id = 0;
    }

    @After
    public void teardown() {
        if (pCache != null) {
            pCache.close();
        }
    }
    
    @Test
    public void unusedItemsShouldntBePersisted() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        flush();
        assertEquals(emptyList(), putActions);
    }

    @Test
    public void readItemsShouldntBePersistedAgain() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        nodeCache.getIfPresent(k);
        flush();
        assertEquals(asList(k), putActions);

        putActions.clear();
        nodeCache.getIfPresent(k); // k should be loaded from persisted cache
        flush();
        assertEquals(emptyList(), putActions); // k is not persisted again
    }

    @Test
    public void usedItemsShouldBePersisted() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        nodeCache.getIfPresent(k);
        flush();
        assertEquals(asList(k), putActions);
    }

    @Test
    public void receivedBroadcastReplacesMetadataWithoutPersistingStaleValue() {
        NodeCache<MemoryDiffCache.Key, StringValue> cache = newDiffCache(
                CacheBuilder.<MemoryDiffCache.Key, StringValue>newBuilder().maximumSize(100).build());
        DiffCacheWriteQueueWrapper writeQueue = (DiffCacheWriteQueueWrapper) cache.writeQueue;
        MemoryDiffCache.Key key = new MemoryDiffCache.Key(Path.fromString("/broadcast"),
                RevisionVector.fromString("r1-0-1"), RevisionVector.fromString("r2-0-1"));
        StringValue receivedValue = new StringValue("received");
        cache.put(key, VAL);
        cache.getIfPresent(key);

        WriteBuffer broadcast = new WriteBuffer(1024);
        CacheType.DIFF.writeKey(broadcast, key);
        broadcast.put((byte) 1);
        CacheType.DIFF.writeValue(broadcast, receivedValue);
        ByteBuffer buffer = broadcast.getBuffer();
        buffer.rewind();
        cache.receive(buffer);

        cache.evicted(key, VAL, EvictionCause.SIZE);
        assertEquals(emptyList(), writeQueue.putActions);

        StringValue cachedValue = cache.getIfPresent(key);
        assertEquals(receivedValue, cachedValue);
        cache.evicted(key, cachedValue, EvictionCause.SIZE);
        assertEquals(asList(key), writeQueue.putActions);
        assertEquals(Collections.singletonList("received"), writeQueue.putValues);
    }

    @Test
    public void staleEvictionShouldNotPersistReinsertedValue() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        nodeCache.getIfPresent(k);
        nodeCache.invalidate(k);
        nodeCache.put(k, NEW_VAL);
        nodeCache.getIfPresent(k);

        nodeCache.evicted(k, VAL, EvictionCause.SIZE);

        assertEquals(emptyList(), putActions);
        assertEquals(NEW_VAL, nodeCache.getIfPresent(k));
    }

    @Test
    public void loadedItemEvictedBeforeReturnShouldBePersisted() {
        PathRev k = generatePathRev();
        AtomicReference<NodeCache<PathRev, StringValue>> cacheRef = new AtomicReference<>();
        Cache<PathRev, StringValue> memCache = Mockito.spy(
                new CacheLIRS.Builder<PathRev, StringValue>().maximumSize(10).build().asOakCache());
        Mockito.doAnswer(invocation -> {
            StringValue loaded = (StringValue) invocation.callRealMethod();
            // simulate an async eviction callback landing before get() returns
            cacheRef.get().evicted(k, loaded, EvictionCause.SIZE);
            return loaded;
        }).when(memCache).get(Mockito.eq(k), Mockito.any());
        NodeCache<PathRev, StringValue> cache = (NodeCache<PathRev, StringValue>) pCache.wrap(
                builderProvider.newBuilder().getNodeStore(), null, memCache, CacheType.NODE);
        cacheRef.set(cache);
        CacheWriteQueueWrapper writeQueue = new CacheWriteQueueWrapper(cache.writeQueue);
        cache.writeQueue = writeQueue;

        assertEquals(VAL, cache.get(k, key -> VAL));

        assertEquals(asList(k), writeQueue.putActions);
    }

    @Test
    public void delayedEvictionOfInvalidatedValueShouldNotBePersisted() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        nodeCache.getIfPresent(k);
        nodeCache.invalidate(k);

        nodeCache.evicted(k, VAL, EvictionCause.SIZE);

        assertEquals(emptyList(), putActions);
        assertEquals(asList(k), invalidateActions);
    }

    @Test
    public void reinsertedValueShouldBePersistedAfterStaleEviction() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        nodeCache.getIfPresent(k);
        nodeCache.invalidate(k);
        nodeCache.put(k, NEW_VAL);
        nodeCache.getIfPresent(k);
        nodeCache.evicted(k, VAL, EvictionCause.SIZE);

        flush();

        assertEquals(asList(k), putActions);
    }

    @Test
    public void staleEvictionShouldNotConsumeMetadataOfReplacedValue() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        nodeCache.getIfPresent(k);
        nodeCache.put(k, NEW_VAL);
        nodeCache.getIfPresent(k);

        nodeCache.evicted(k, VAL, EvictionCause.SIZE);
        assertEquals(emptyList(), putActions);

        flush();
        assertEquals(asList(k), putActions);
    }

    @Test
    public void staleEvictionShouldNotConsumeMetadataOfBulkReadValue() {
        NodeCache<PathRev, StringValue> cache = newNodeCache(
                CacheBuilder.<PathRev, StringValue>newBuilder().maximumSize(100).build());
        List<PathRev> cachePutActions = ((CacheWriteQueueWrapper) cache.writeQueue).putActions;
        PathRev k = generatePathRev();
        cache.put(k, VAL);
        cache.put(k, NEW_VAL);
        assertEquals(NEW_VAL, cache.getAllPresent(asList(k)).get(k));

        cache.evicted(k, VAL, EvictionCause.SIZE);
        assertEquals(emptyList(), cachePutActions);

        cache.evicted(k, NEW_VAL, EvictionCause.SIZE);
        assertEquals(asList(k), cachePutActions);
    }

    @Test
    public void staleEvictionOfPersistedValueShouldNotSuppressNewValue() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        nodeCache.getIfPresent(k);
        flush();
        assertEquals(asList(k), putActions);
        putActions.clear();

        StringValue persisted = nodeCache.getIfPresent(k); // loaded from persistent cache
        assertEquals(VAL, persisted);
        nodeCache.put(k, NEW_VAL);
        assertEquals(NEW_VAL, nodeCache.getIfPresent(k));
        nodeCache.evicted(k, persisted, EvictionCause.SIZE);
        assertEquals(emptyList(), putActions);

        // k is hot in LIRS, so evict it explicitly instead of flushing
        nodeCache.evicted(k, NEW_VAL, EvictionCause.SIZE);
        assertEquals(asList(k), putActions);
    }

    @Test
    public void caffeineEvictionShouldPersistUsedValues() throws Exception {
        AtomicReference<NodeCache<PathRev, StringValue>> cacheRef = new AtomicReference<>();
        Cache<PathRev, StringValue> memCache = CacheBuilder.<PathRev, StringValue>newBuilder()
                .maximumSize(10)
                .evictionListener((key, value, cause) -> cacheRef.get().evicted(key, value, cause))
                .build();
        NodeCache<PathRev, StringValue> cache = newNodeCache(memCache);
        cacheRef.set(cache);
        List<PathRev> cachePutActions = ((CacheWriteQueueWrapper) cache.writeQueue).putActions;

        List<PathRev> unused = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            PathRev used = generatePathRev();
            cache.put(used, new StringValue("used-" + i));
            cache.getIfPresent(used);
            PathRev notUsed = generatePathRev();
            cache.put(notUsed, new StringValue("unused-" + i));
            unused.add(notUsed);
        }
        memCache.cleanUp();

        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        while (cachePutActions.size() < 50 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertTrue("expected used values to be persisted, got " + cachePutActions.size(),
                cachePutActions.size() >= 50);
        synchronized (cachePutActions) {
            for (PathRev k : unused) {
                assertFalse("unused value persisted: " + k, cachePutActions.contains(k));
            }
        }
    }

    @Test
    public void getAllPresentShouldTrackOnlyPresentKeys() {
        NodeCache<PathRev, StringValue> cache = newNodeCache(
                CacheBuilder.<PathRev, StringValue>newBuilder().maximumSize(100).build());
        List<PathRev> cachePutActions = ((CacheWriteQueueWrapper) cache.writeQueue).putActions;
        PathRev present = generatePathRev();
        PathRev absent = generatePathRev();
        cache.put(present, VAL);

        assertEquals(1, cache.getAllPresent(asList(present, absent)).size());

        cache.evicted(absent, NEW_VAL, EvictionCause.SIZE);
        assertEquals(emptyList(), cachePutActions);
        cache.evicted(present, VAL, EvictionCause.SIZE);
        assertEquals(asList(present), cachePutActions);
    }

    @Test
    public void concurrentStaleEvictionAndReinsertShouldPersistOnlyNewValue() throws Exception {
        assertConcurrentStaleEviction((cache, k, newValue) -> {
            cache.put(k, newValue);
            cache.getIfPresent(k);
        });
    }

    @Test
    public void concurrentStaleEvictionAndInvalidateReinsertShouldPersistOnlyNewValue() throws Exception {
        assertConcurrentStaleEviction((cache, k, newValue) -> {
            cache.invalidate(k);
            cache.put(k, newValue);
            cache.getIfPresent(k);
        });
    }

    @Test
    public void concurrentStaleEvictionAndReloadShouldPersistOnlyNewValue() throws Exception {
        assertConcurrentStaleEviction((cache, k, newValue) -> {
            cache.invalidate(k);
            cache.get(k, key -> newValue);
        });
    }

    /**
     * Races a delayed eviction callback of an unused old value against a
     * replacement of the same key, then evicts the new value and verifies
     * that only new values reach the persistent cache.
     */
    private void assertConcurrentStaleEviction(Replacement replacement) throws Exception {
        NodeCache<PathRev, StringValue> cache = newNodeCache(
                CacheBuilder.<PathRev, StringValue>newBuilder().maximumSize(10_000).build());
        CacheWriteQueueWrapper writeQueue = (CacheWriteQueueWrapper) cache.writeQueue;
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            int iterations = 500;
            for (int i = 0; i < iterations; i++) {
                PathRev k = generatePathRev();
                StringValue oldValue = new StringValue("old-" + i);
                StringValue newValue = new StringValue("new-" + i);
                cache.put(k, oldValue);
                CyclicBarrier barrier = new CyclicBarrier(2);
                Future<?> eviction = executor.submit(() -> {
                    barrier.await();
                    cache.evicted(k, oldValue, EvictionCause.SIZE);
                    return null;
                });
                Future<?> replace = executor.submit(() -> {
                    barrier.await();
                    replacement.apply(cache, k, newValue);
                    return null;
                });
                eviction.get(10, TimeUnit.SECONDS);
                replace.get(10, TimeUnit.SECONDS);
                cache.evicted(k, newValue, EvictionCause.SIZE);
            }
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < iterations; i++) {
                expected.add("new-" + i);
            }
            assertEquals(expected, writeQueue.putValues);
        } finally {
            executor.shutdownNow();
        }
    }

    private NodeCache<PathRev, StringValue> newNodeCache(Cache<PathRev, StringValue> memCache) {
        NodeCache<PathRev, StringValue> cache = (NodeCache<PathRev, StringValue>) pCache.wrap(
                builderProvider.newBuilder().getNodeStore(), null, memCache, CacheType.NODE);
        cache.writeQueue = new CacheWriteQueueWrapper(cache.writeQueue);
        return cache;
    }

    private NodeCache<MemoryDiffCache.Key, StringValue> newDiffCache(
            Cache<MemoryDiffCache.Key, StringValue> memCache) {
        NodeCache<MemoryDiffCache.Key, StringValue> cache =
                (NodeCache<MemoryDiffCache.Key, StringValue>) pCache.wrap(
                        builderProvider.newBuilder().getNodeStore(), null, memCache, CacheType.DIFF);
        cache.writeQueue = new DiffCacheWriteQueueWrapper(cache.writeQueue);
        return cache;
    }

    private interface Replacement {
        void apply(NodeCache<PathRev, StringValue> cache, PathRev key, StringValue newValue);
    }

    private PathRev generatePathRev() {
        return new PathRev(Path.fromString("/" + id++), new RevisionVector(new Revision(0, 0, 0)));
    }

    private void flush() {
        for (int i = 0; i < 1024; i++) {
            nodeCache.put(generatePathRev(), VAL); // cause eviction of k
        }
    }

    private static class CacheWriteQueueWrapper extends CacheWriteQueue<PathRev, StringValue> {

        private final CacheWriteQueue<PathRev, StringValue>  wrapped;

        private final List<PathRev> putActions = Collections.synchronizedList(new ArrayList<>());

        private final List<String> putValues = Collections.synchronizedList(new ArrayList<>());

        private final List<PathRev> invalidateActions = Collections.synchronizedList(new ArrayList<>());

        public CacheWriteQueueWrapper(CacheWriteQueue<PathRev, StringValue>  wrapped) {
            super(null, null, null);
            this.wrapped = wrapped;
        }

        @Override
        public boolean addPut(PathRev key, StringValue value) {
            putActions.add(key);
            putValues.add(value.asString());
            return wrapped.addPut(key, value);
        }

        public boolean addInvalidate(Iterable<PathRev> keys) {
            invalidateActions.addAll(ListUtils.toList(keys));
            return wrapped.addInvalidate(keys);
        }
    }

    private static class DiffCacheWriteQueueWrapper extends CacheWriteQueue<MemoryDiffCache.Key, StringValue> {

        private final CacheWriteQueue<MemoryDiffCache.Key, StringValue> wrapped;

        private final List<MemoryDiffCache.Key> putActions = new ArrayList<>();

        private final List<String> putValues = new ArrayList<>();

        public DiffCacheWriteQueueWrapper(CacheWriteQueue<MemoryDiffCache.Key, StringValue> wrapped) {
            super(null, null, null);
            this.wrapped = wrapped;
        }

        @Override
        public boolean addPut(MemoryDiffCache.Key key, StringValue value) {
            putActions.add(key);
            putValues.add(value.asString());
            return wrapped.addPut(key, value);
        }
    }

}
