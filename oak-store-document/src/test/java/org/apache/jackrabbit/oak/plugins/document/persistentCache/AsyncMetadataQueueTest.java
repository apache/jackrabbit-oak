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

import org.junit.Assert;

import java.util.Arrays;

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
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.function.BooleanSupplier;

/** Exercises queued persistence with entry-owned ASYNC metadata. */
public class AsyncMetadataQueueTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private static final StringValue VAL = new StringValue("xyz");

    private static final StringValue NEW_VAL = new StringValue("abc");

    private PersistentCache pCache;

    private List<PathRev> putActions;

    private List<PathRev> invalidateActions;

    private AsyncNodeCache<PathRev, StringValue> nodeCache;

    private int id;

    @Before
    public void setup() throws IOException {
        pCache = new PersistentCache(folder.newFolder().getAbsolutePath() + ",+asyncDiff");
        final AtomicReference<AsyncNodeCache<PathRev, StringValue>> nodeCacheRef = new AtomicReference<AsyncNodeCache<PathRev, StringValue>>();
        CacheLIRS<PathRev, CacheEntry<StringValue>> lirs = new CacheLIRS.Builder<PathRev, CacheEntry<StringValue>>().maximumSize(1).evictionCallback((key, value, cause) -> {
            if (nodeCacheRef.get() != null) {
                nodeCacheRef.get().evicted(key, value, EvictionCause.valueOf(cause.name()));
            }
        }).build();
        nodeCache = (AsyncNodeCache<PathRev, StringValue>) pCache.wrapAsyncMaintenance(builderProvider.newBuilder().getNodeStore(),
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

    /** Eviction skips values that were never read. */
    @Test
    public void unusedItemsShouldntBePersisted() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        flush();
        Assert.assertEquals(Collections.emptyList(), putActions);
    }

    /** Values loaded from persistence are not written again. */
    @Test
    public void readItemsShouldntBePersistedAgain() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        nodeCache.getIfPresent(k);
        flush();
        Assert.assertEquals(Arrays.asList(k), putActions);

        putActions.clear();
        nodeCache.getIfPresent(k); // k should be loaded from persisted cache
        flush();
        Assert.assertEquals(Collections.emptyList(), putActions); // k is not persisted again
    }

    /** Eviction queues values that were read from memory. */
    @Test
    public void usedItemsShouldBePersisted() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        nodeCache.getIfPresent(k);
        flush();
        Assert.assertEquals(Arrays.asList(k), putActions);
    }

    /** Racing puts persist only the surviving entry. */
    @Test
    public void concurrentPutsShouldPersistSurvivingValue() throws Exception {
        PathRev key = generatePathRev();
        AsyncNodeCache<PathRev, StringValue> cache = raceSameKeyPuts(key);
        CacheWriteQueueWrapper queue = (CacheWriteQueueWrapper) cache.writeQueue;
        Assert.assertEquals(NEW_VAL, cache.getIfPresent(key));
        cache.evicted(key, cache.memCache.asMap().get(key), EvictionCause.SIZE);
        Assert.assertEquals(Collections.singletonList(NEW_VAL.asString()), queue.putValues);
    }

    /** Unused racing entries leave no metadata after removal. */
    @Test
    public void concurrentUnusedPutsShouldNotRetainMetadataAfterEviction() throws Exception {
        PathRev key = generatePathRev();
        AsyncNodeCache<PathRev, StringValue> cache = raceSameKeyPuts(key);
        cache.asMap().remove(key);
        cache.evicted(key, cache.memCache.asMap().get(key), EvictionCause.SIZE);
        Assert.assertEquals(Collections.emptyList(), ((CacheWriteQueueWrapper) cache.writeQueue).putValues);
    }

    /** A late lookup must not recreate an evicted entry. */
    @Test
    public void evictionBeforeLookupReturnsShouldNotRecreateMetadata() throws Exception {
        PathRev key = generatePathRev();
        Cache<PathRev, CacheEntry<StringValue>> memory = Mockito.spy(
                CacheBuilder.<PathRev, CacheEntry<StringValue>>newBuilder().maximumSize(100).build());
        AsyncNodeCache<PathRev, StringValue> cache = newAsyncNodeCache(memory);
        cache.put(key, VAL);
        Mockito.doAnswer(invocation -> {
            CacheEntry<StringValue> value = (CacheEntry<StringValue>) invocation.callRealMethod();
            memory.asMap().remove(key);
            cache.evicted(key, value, EvictionCause.SIZE);
            return value;
        }).when(memory).getIfPresent(key);

        Assert.assertEquals(VAL, cache.getIfPresent(key));
        Assert.assertEquals(Collections.singletonList(VAL.asString()),
                ((CacheWriteQueueWrapper) cache.writeQueue).putValues);
    }

    /** A null load leaves no entry metadata. */
    @Test
    public void nullCaffeineLoadShouldNotLeaveMetadata() throws Exception {
        PathRev key = generatePathRev();
        AsyncNodeCache<PathRev, StringValue> cache = newAsyncNodeCache(
                CacheBuilder.<PathRev, CacheEntry<StringValue>>newBuilder().maximumSize(100).build());
        Assert.assertEquals(null, cache.get(key, k -> null));
        Assert.assertEquals(null, cache.asMap().get(key));
        Assert.assertEquals(Collections.emptyList(), ((CacheWriteQueueWrapper) cache.writeQueue).putValues);

        Assert.assertEquals(VAL, cache.get(key, k -> VAL));
        cache.evicted(key, cache.memCache.asMap().get(key), EvictionCause.SIZE);
        Assert.assertEquals(Collections.singletonList(VAL.asString()),
                ((CacheWriteQueueWrapper) cache.writeQueue).putValues);
    }

    /**
     * Completes the second insertion while the first is paused after publishing its metadata.
     */
    private AsyncNodeCache<PathRev, StringValue> raceSameKeyPuts(PathRev key) throws Exception {
        Cache<PathRev, CacheEntry<StringValue>> memory = Mockito.spy(
                CacheBuilder.<PathRev, CacheEntry<StringValue>>newBuilder().maximumSize(100).build());
        CountDownLatch firstPutStarted = new CountDownLatch(1);
        CountDownLatch resumeFirstPut = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            firstPutStarted.countDown();
            Assert.assertTrue(resumeFirstPut.await(10, TimeUnit.SECONDS));
            return invocation.callRealMethod();
        }).when(memory).put(Mockito.eq(key), Mockito.argThat(entry -> entry.getValue() == VAL));
        AsyncNodeCache<PathRev, StringValue> cache = newAsyncNodeCache(memory);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> firstPut = executor.submit(() -> cache.put(key, VAL));
            Assert.assertTrue(firstPutStarted.await(10, TimeUnit.SECONDS));
            Future<?> secondPut = executor.submit(() -> cache.put(key, NEW_VAL));
            resumeFirstPut.countDown();
            firstPut.get(10, TimeUnit.SECONDS);
            secondPut.get(10, TimeUnit.SECONDS);

            return cache;
        } finally {
            resumeFirstPut.countDown();
            executor.shutdownNow();
        }
    }

    // A received broadcast replaces metadata without persisting the old value.
    @Test
    public void receivedBroadcastReplacesMetadataWithoutPersistingStaleValue() {
        AsyncNodeCache<MemoryDiffCache.Key, StringValue> cache = newDiffCache(
                CacheBuilder.<MemoryDiffCache.Key, CacheEntry<StringValue>>newBuilder().maximumSize(100).build());
        DiffCacheWriteQueueWrapper writeQueue = (DiffCacheWriteQueueWrapper) cache.writeQueue;
        MemoryDiffCache.Key key = new MemoryDiffCache.Key(Path.fromString("/broadcast"),
                RevisionVector.fromString("r1-0-1"), RevisionVector.fromString("r2-0-1"));
        StringValue receivedValue = new StringValue("received");
        cache.put(key, VAL);
        CacheEntry<StringValue> oldEntry = cache.memCache.asMap().get(key);
        cache.getIfPresent(key);

        WriteBuffer broadcast = new WriteBuffer(1024);
        CacheType.DIFF.writeKey(broadcast, key);
        broadcast.put((byte) 1);
        CacheType.DIFF.writeValue(broadcast, receivedValue);
        ByteBuffer buffer = broadcast.getBuffer();
        buffer.rewind();
        cache.receive(buffer);

        cache.evicted(key, oldEntry, EvictionCause.SIZE);
        Assert.assertEquals(Collections.emptyList(), writeQueue.putActions);

        StringValue cachedValue = cache.getIfPresent(key);
        Assert.assertEquals(receivedValue, cachedValue);
        cache.evicted(key, cache.memCache.asMap().get(key), EvictionCause.SIZE);
        Assert.assertEquals(Arrays.asList(key), writeQueue.putActions);
        Assert.assertEquals(Collections.singletonList("received"), writeQueue.putValues);
    }

    // An old callback neither queues a write nor removes the replacement value.
    @Test
    public void staleEvictionShouldNotPersistReinsertedValue() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        CacheEntry<StringValue> oldEntry = nodeCache.memCache.asMap().get(k);
        nodeCache.getIfPresent(k);
        nodeCache.invalidate(k);
        nodeCache.put(k, NEW_VAL);
        nodeCache.getIfPresent(k);

        nodeCache.evicted(k, oldEntry, EvictionCause.SIZE);

        Assert.assertEquals(Collections.emptyList(), putActions);
        Assert.assertEquals(NEW_VAL, nodeCache.getIfPresent(k));
    }

    // A loaded value qualifies for persistence even if evicted before get returns.
    @Test
    public void loadedItemEvictedBeforeReturnShouldBePersisted() {
        PathRev k = generatePathRev();
        AtomicReference<AsyncNodeCache<PathRev, StringValue>> cacheRef = new AtomicReference<>();
        Cache<PathRev, CacheEntry<StringValue>> memCache = Mockito.spy(
                new CacheLIRS.Builder<PathRev, CacheEntry<StringValue>>().maximumSize(10).build().asOakCache());
        Mockito.doAnswer(invocation -> {
            CacheEntry<StringValue> loaded = (CacheEntry<StringValue>) invocation.callRealMethod();
            // simulate an async eviction callback landing before get() returns
            cacheRef.get().evicted(k, loaded, EvictionCause.SIZE);
            return loaded;
        }).when(memCache).get(Mockito.eq(k), Mockito.any());
        AsyncNodeCache<PathRev, StringValue> cache = (AsyncNodeCache<PathRev, StringValue>) pCache.wrapAsyncMaintenance(
                builderProvider.newBuilder().getNodeStore(), null, memCache, CacheType.NODE);
        cacheRef.set(cache);
        CacheWriteQueueWrapper writeQueue = new CacheWriteQueueWrapper(cache.writeQueue);
        cache.writeQueue = writeQueue;

        Assert.assertEquals(VAL, cache.get(k, key -> VAL));

        Assert.assertEquals(Arrays.asList(k), writeQueue.putActions);
    }

    // A delayed callback cannot persist an explicitly invalidated value.
    @Test
    public void delayedEvictionOfInvalidatedValueShouldNotBePersisted() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        CacheEntry<StringValue> oldEntry = nodeCache.memCache.asMap().get(k);
        nodeCache.getIfPresent(k);
        nodeCache.invalidate(k);

        nodeCache.evicted(k, oldEntry, EvictionCause.SIZE);

        Assert.assertEquals(Collections.emptyList(), putActions);
        Assert.assertEquals(Arrays.asList(k), invalidateActions);
    }

    // A reinserted value remains eligible after the old callback runs.
    @Test
    public void reinsertedValueShouldBePersistedAfterStaleEviction() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        CacheEntry<StringValue> oldEntry = nodeCache.memCache.asMap().get(k);
        nodeCache.getIfPresent(k);
        nodeCache.invalidate(k);
        nodeCache.put(k, NEW_VAL);
        nodeCache.getIfPresent(k);
        nodeCache.evicted(k, oldEntry, EvictionCause.SIZE);

        flush();

        Assert.assertEquals(Arrays.asList(k), putActions);
    }

    // An old callback preserves the replacement's access metadata.
    @Test
    public void staleEvictionShouldNotConsumeMetadataOfReplacedValue() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        CacheEntry<StringValue> oldEntry = nodeCache.memCache.asMap().get(k);
        nodeCache.getIfPresent(k);
        nodeCache.put(k, NEW_VAL);
        nodeCache.getIfPresent(k);

        nodeCache.evicted(k, oldEntry, EvictionCause.SIZE);
        Assert.assertEquals(Collections.emptyList(), putActions);

        flush();
        Assert.assertEquals(Arrays.asList(k), putActions);
    }

    // Bulk-read access metadata survives an old value's callback.
    @Test
    public void staleEvictionShouldNotConsumeMetadataOfBulkReadValue() {
        AsyncNodeCache<PathRev, StringValue> cache = newAsyncNodeCache(
                CacheBuilder.<PathRev, CacheEntry<StringValue>>newBuilder().maximumSize(100).build());
        List<PathRev> cachePutActions = ((CacheWriteQueueWrapper) cache.writeQueue).putActions;
        PathRev k = generatePathRev();
        cache.put(k, VAL);
        CacheEntry<StringValue> oldEntry = cache.memCache.asMap().get(k);
        cache.put(k, NEW_VAL);
        Assert.assertEquals(NEW_VAL, cache.getAllPresent(Arrays.asList(k)).get(k));

        cache.evicted(k, oldEntry, EvictionCause.SIZE);
        Assert.assertEquals(Collections.emptyList(), cachePutActions);

        cache.evicted(k, cache.memCache.asMap().get(k), EvictionCause.SIZE);
        Assert.assertEquals(Arrays.asList(k), cachePutActions);
    }

    // Old persistence-origin metadata cannot suppress a replacement's write.
    @Test
    public void staleEvictionOfPersistedValueShouldNotSuppressNewValue() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        nodeCache.getIfPresent(k);
        flush();
        Assert.assertEquals(Arrays.asList(k), putActions);
        putActions.clear();

        StringValue persisted = nodeCache.getIfPresent(k); // loaded from persistent cache
        Assert.assertEquals(VAL, persisted);
        CacheEntry<StringValue> persistedEntry = nodeCache.memCache.asMap().get(k);
        nodeCache.put(k, NEW_VAL);
        Assert.assertEquals(NEW_VAL, nodeCache.getIfPresent(k));
        nodeCache.evicted(k, persistedEntry, EvictionCause.SIZE);
        Assert.assertEquals(Collections.emptyList(), putActions);

        // k is hot in LIRS, so evict it explicitly instead of flushing
        nodeCache.evicted(k, nodeCache.memCache.asMap().get(k), EvictionCause.SIZE);
        Assert.assertEquals(Arrays.asList(k), putActions);
    }

    // Caffeine evictions persist accessed values but skip unused ones.
    @Test
    public void caffeineEvictionShouldPersistUsedValues() throws Exception {
        AtomicReference<AsyncNodeCache<PathRev, StringValue>> cacheRef = new AtomicReference<>();
        Cache<PathRev, CacheEntry<StringValue>> memCache = CacheBuilder.<PathRev, CacheEntry<StringValue>>newBuilder()
                .maximumSize(10)
                .evictionListener((key, value, cause) -> cacheRef.get().evicted(key, value, cause))
                .build();
        AsyncNodeCache<PathRev, StringValue> cache = newAsyncNodeCache(memCache);
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
        Assert.assertTrue("expected used values to be persisted, got " + cachePutActions.size(),
                cachePutActions.size() >= 50);
        synchronized (cachePutActions) {
            for (PathRev k : unused) {
                Assert.assertFalse("unused value persisted: " + k, cachePutActions.contains(k));
            }
        }
    }

    // Bulk lookup counts present entries without creating metadata for misses.
    @Test
    public void getAllPresentShouldTrackOnlyPresentKeys() {
        AsyncNodeCache<PathRev, StringValue> cache = newAsyncNodeCache(
                CacheBuilder.<PathRev, CacheEntry<StringValue>>newBuilder().maximumSize(100).build());
        List<PathRev> cachePutActions = ((CacheWriteQueueWrapper) cache.writeQueue).putActions;
        PathRev present = generatePathRev();
        PathRev absent = generatePathRev();
        cache.put(present, VAL);

        Assert.assertEquals(1, cache.getAllPresent(Arrays.asList(present, absent)).size());

        cache.evicted(absent, null, EvictionCause.SIZE);
        Assert.assertEquals(Collections.emptyList(), cachePutActions);
        cache.evicted(present, cache.memCache.asMap().get(present), EvictionCause.SIZE);
        Assert.assertEquals(Arrays.asList(present), cachePutActions);
    }

    // A replacement racing an old callback persists only the new value.
    @Test
    public void concurrentStaleEvictionAndReinsertShouldPersistOnlyNewValue() throws Exception {
        assertConcurrentStaleEviction((cache, k, newValue) -> {
            cache.put(k, newValue);
            cache.getIfPresent(k);
        });
    }

    // Invalidation and reinsertion racing an old callback persist only the new value.
    @Test
    public void concurrentStaleEvictionAndInvalidateReinsertShouldPersistOnlyNewValue() throws Exception {
        assertConcurrentStaleEviction((cache, k, newValue) -> {
            cache.invalidate(k);
            cache.put(k, newValue);
            cache.getIfPresent(k);
        });
    }

    // A reload racing an old callback persists only the newly loaded value.
    @Test
    public void concurrentStaleEvictionAndReloadShouldPersistOnlyNewValue() throws Exception {
        assertConcurrentStaleEviction((cache, k, newValue) -> {
            cache.invalidate(k);
            cache.get(k, key -> newValue);
        });
    }

    /** A delayed callback must persist only the surviving replacement. */
    private void assertConcurrentStaleEviction(Replacement replacement) throws Exception {
        AsyncNodeCache<PathRev, StringValue> cache = newAsyncNodeCache(
                CacheBuilder.<PathRev, CacheEntry<StringValue>>newBuilder().maximumSize(10_000).build());
        CacheWriteQueueWrapper writeQueue = (CacheWriteQueueWrapper) cache.writeQueue;
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            int iterations = 500;
            for (int i = 0; i < iterations; i++) {
                PathRev k = generatePathRev();
                StringValue oldValue = new StringValue("old-" + i);
                StringValue newValue = new StringValue("new-" + i);
                cache.put(k, oldValue);
                CacheEntry<StringValue> oldEntry = cache.memCache.asMap().get(k);
                CyclicBarrier barrier = new CyclicBarrier(2);
                Future<?> eviction = executor.submit(() -> {
                    barrier.await();
                    cache.evicted(k, oldEntry, EvictionCause.SIZE);
                    return null;
                });
                Future<?> replace = executor.submit(() -> {
                    barrier.await();
                    replacement.apply(cache, k, newValue);
                    return null;
                });
                eviction.get(10, TimeUnit.SECONDS);
                replace.get(10, TimeUnit.SECONDS);
                cache.evicted(k, cache.memCache.asMap().get(k), EvictionCause.SIZE);
            }
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < iterations; i++) {
                expected.add("new-" + i);
            }
            Assert.assertEquals(expected, writeQueue.putValues);
        } finally {
            executor.shutdownNow();
        }
    }

    private AsyncNodeCache<PathRev, StringValue> newAsyncNodeCache(Cache<PathRev, CacheEntry<StringValue>> memCache) {
        AsyncNodeCache<PathRev, StringValue> cache = (AsyncNodeCache<PathRev, StringValue>) pCache.wrapAsyncMaintenance(
                builderProvider.newBuilder().getNodeStore(), null, memCache, CacheType.NODE);
        cache.writeQueue = new CacheWriteQueueWrapper(cache.writeQueue);
        return cache;
    }

    private AsyncNodeCache<MemoryDiffCache.Key, StringValue> newDiffCache(
            Cache<MemoryDiffCache.Key, CacheEntry<StringValue>> memCache) {
        AsyncNodeCache<MemoryDiffCache.Key, StringValue> cache =
                (AsyncNodeCache<MemoryDiffCache.Key, StringValue>) pCache.wrapAsyncMaintenance(
                        builderProvider.newBuilder().getNodeStore(), null, memCache, CacheType.DIFF);
        cache.writeQueue = new DiffCacheWriteQueueWrapper(cache.writeQueue);
        return cache;
    }

    private interface Replacement {
        void apply(AsyncNodeCache<PathRev, StringValue> cache, PathRev key, StringValue newValue);
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
        public boolean addPut(PathRev key, StringValue value, BooleanSupplier valid, Lock writeOrder, Runnable written) {
            putActions.add(key);
            putValues.add(value.asString());
            return wrapped.addPut(key, value, valid, writeOrder, written);
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
        public boolean addPut(MemoryDiffCache.Key key, StringValue value, BooleanSupplier valid, Lock writeOrder, Runnable written) {
            putActions.add(key);
            putValues.add(value.asString());
            return wrapped.addPut(key, value, valid, writeOrder, written);
        }
    }

}
