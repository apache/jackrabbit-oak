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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.EvictionCause;
import org.apache.jackrabbit.oak.plugins.document.MemoryDiffCache;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.async.CacheActionDispatcher;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.async.TestCacheActionDispatcher;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.async.CacheWriteQueue;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.h2.mvstore.WriteBuffer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mockito;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Tests exact-entry persistence eligibility in {@link AsyncNodeCache} under both maintenance modes. */
@RunWith(Parameterized.class)
public class NodeCacheEntryTest {
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> modes() {
        return Arrays.asList(new Object[][] {{CacheBuilder.MaintenanceMode.SYNC}, {CacheBuilder.MaintenanceMode.ASYNC}});
    }

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));
    private final CacheBuilder.MaintenanceMode mode;
    private PersistentCache persistence;
    private Cache<CacheValue, CacheEntry<StringValue>> memory;
    private AsyncNodeCache<CacheValue, StringValue> cache;
    private final List<StringValue> writes = new ArrayList<>();
    private final List<BooleanSupplier> eligibility = new ArrayList<>();
    private final MemoryDiffCache.Key key = new MemoryDiffCache.Key(Path.fromString("/key"),
            new RevisionVector(new Revision(1, 0, 1)), new RevisionVector(new Revision(2, 0, 1)));
    private final StringValue value = new StringValue("value");

    public NodeCacheEntryTest(CacheBuilder.MaintenanceMode mode) { this.mode = mode; }

    @Before
    public void createCache() throws Exception {
        persistence = new PersistentCache(folder.newFolder().getAbsolutePath() + ",+asyncDiff");
        initialize(100);
    }

    private void initialize(int maximumSize) {
        AtomicReference<AsyncNodeCache<CacheValue, StringValue>> listener = new AtomicReference<>();
        memory = CacheBuilder.<CacheValue, CacheEntry<StringValue>>newBuilder().maximumSize(maximumSize)
                .maintenanceMode(mode).evictionListener((k, v, cause) -> listener.get().evicted(k, v, cause)).build();
        cache = (AsyncNodeCache<CacheValue, StringValue>) persistence.wrapAsyncMaintenance(null, null, memory, CacheType.DIFF);
        cache.writeQueue = new CacheWriteQueue<CacheValue, StringValue>(null, null, null) {
            @Override
            public boolean addPut(CacheValue key, StringValue value, BooleanSupplier valid, Lock lock, Runnable written) {
                writes.add(value);
                eligibility.add(valid);
                return true;
            }
            @Override
            public boolean addInvalidate(Iterable<CacheValue> keys) { return true; }
        };
        listener.set(cache);
    }

    @After
    public void close() { persistence.close(); }

    /** Loader failure publishes neither a value nor metadata. */
    @Test
    public void failedLoaderDoesNotPublishAnEntry() {
        IllegalStateException failure = new IllegalStateException("load failed");
        Assert.assertSame(failure, Assert.assertThrows(IllegalStateException.class,
                () -> cache.get(key, k -> { throw failure; })));
        Assert.assertNull(cache.getIfPresent(key));
        Assert.assertTrue(memory.asMap().isEmpty());
        Assert.assertTrue(writes.isEmpty());
    }

    /** A full write queue must not consume eviction eligibility. */
    @Test
    public void rejectedEvictionCanBeRetriedWithoutLosingItsEntry() {
        cache.get(key, k -> value);
        CacheEntry<StringValue> entry = memory.asMap().get(key);
        CacheWriteQueue<CacheValue, StringValue> accepted = cache.writeQueue;
        cache.writeQueue = new CacheWriteQueue<>(TestCacheActionDispatcher.rejectingDispatcher(), persistence, null);
        cache.evicted(key, entry, null);
        cache.evicted(key, entry, EvictionCause.SIZE);
        Assert.assertFalse(entry.evictionQueued);
        cache.writeQueue = accepted;
        cache.evicted(key, entry, EvictionCause.SIZE);
        Assert.assertEquals(Arrays.asList(value), writes);
    }

    /** Generation rewrites and rejected invalidations remain correct after reopen. */
    @Test
    public void olderGenerationIsRewrittenAndQueueFullInvalidationSurvivesReopen() throws Exception {
        String directory = folder.newFolder().getAbsolutePath();
        PersistentCache first = Mockito.spy(new PersistentCache(directory));
        try {
            Cache<CacheValue, StringValue> writer = first.wrap(null, null,
                    CacheBuilder.<CacheValue, StringValue>newBuilder().maximumSize(100)
                            .maintenanceMode(CacheBuilder.MaintenanceMode.SYNC).build(), CacheType.DIFF);
            writer.put(key, value);
            Mockito.doReturn(true).when(first).needSwitch();
            first.switchGenerationIfNeeded();
        } finally {
            first.close();
        }
        PersistentCache reopened = Mockito.spy(new PersistentCache(directory + ",+asyncDiff"));
        try {
            Cache<CacheValue, CacheEntry<StringValue>> entries = CacheBuilder.<CacheValue, CacheEntry<StringValue>>newBuilder()
                    .maximumSize(100).maintenanceMode(mode).build();
            AsyncNodeCache<CacheValue, StringValue> reader = (AsyncNodeCache<CacheValue, StringValue>)
                    reopened.wrapAsyncMaintenance(null, null, entries, CacheType.DIFF);
            Assert.assertEquals(value, reader.getIfPresent(key));
            CacheEntry<StringValue> loaded = entries.asMap().get(key);
            Assert.assertFalse("Old-generation entries must qualify for rewriting", loaded.isFromPersistence());
            TestCacheActionDispatcher dispatcher = new TestCacheActionDispatcher();
            Field mapField = AsyncNodeCache.class.getDeclaredField("map");
            mapField.setAccessible(true);
            Map<CacheValue, StringValue> generations = (Map<CacheValue, StringValue>) mapField.get(reader);
            reader.writeQueue = new CacheWriteQueue<>(dispatcher, reopened, generations);
            reader.evicted(key, loaded, EvictionCause.SIZE);
            Assert.assertEquals(1, dispatcher.pendingCount());
            Mockito.doReturn(true).when(reopened).needSwitch();
            dispatcher.executeAll();
            Mockito.doReturn(false).when(reopened).needSwitch();
            Assert.assertEquals(value, reader.getGenerationalMap().get(key));
            reader.writeQueue = new CacheWriteQueue<>(TestCacheActionDispatcher.rejectingDispatcher(), reopened, null);
            reader.invalidate(key);
            Assert.assertNull(reader.getIfPresent(key));
            Assert.assertFalse(reader.getGenerationalMap().containsKey(key));
        } finally {
            reopened.close();
        }
        PersistentCache last = new PersistentCache(directory);
        try {
            Cache<CacheValue, StringValue> reader = last.wrap(null, null,
                    CacheBuilder.<CacheValue, StringValue>newBuilder().maximumSize(100).build(), CacheType.DIFF);
            Assert.assertNull(reader.getIfPresent(key));
        } finally {
            last.close();
        }
    }

    /** Reinserting the same value still creates a distinct entry. */
    @Test
    public void sameObjectReinsertionHasIndependentMetadata() {
        cache.put(key, value);
        CacheEntry<StringValue> old = memory.asMap().get(key);
        cache.getIfPresent(key);
        cache.put(key, value);
        CacheEntry<StringValue> current = memory.asMap().get(key);
        cache.evicted(key, old, EvictionCause.SIZE);
        Assert.assertTrue(writes.isEmpty());
        cache.getIfPresent(key);
        cache.evicted(key, current, EvictionCause.SIZE);
        cache.evicted(key, current, EvictionCause.SIZE);
        Assert.assertEquals(Arrays.asList(value), writes);
        Assert.assertTrue(eligibility.get(0).getAsBoolean());
    }

    /** Eviction queues disk work without waiting for it. */
    @Test
    public void evictionCallbackDoesNotWaitForPersistentWrite() throws Exception {
        cache.put(key, value);
        cache.getIfPresent(key);
        CacheEntry<StringValue> entry = memory.asMap().get(key);
        Field field = AsyncNodeCache.class.getDeclaredField("writeOrder");
        field.setAccessible(true);
        ReentrantLock writeOrder = (ReentrantLock) field.get(cache);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        writeOrder.lock();
        try {
            Future<?> callback = executor.submit(() -> cache.evicted(key, entry, EvictionCause.SIZE));
            callback.get(1, TimeUnit.SECONDS);
        } finally {
            writeOrder.unlock();
            executor.shutdownNow();
        }

        Assert.assertEquals(Arrays.asList(value), writes);
    }

    /** Invalidation cancels queued and late eviction writes. */
    @Test
    public void invalidationCancelsClaimedAndDelayedEvictions() {
        cache.get(key, k -> value);
        CacheEntry<StringValue> entry = memory.asMap().get(key);
        cache.evicted(key, entry, EvictionCause.SIZE);
        Assert.assertEquals(1, writes.size());
        cache.invalidate(key);
        Assert.assertFalse(eligibility.get(0).getAsBoolean());
        cache.evicted(key, entry, EvictionCause.SIZE);
        Assert.assertEquals(1, writes.size());
        Assert.assertNull(cache.getIfPresent(key));
    }

    /** Clearing the cache cancels already queued writes. */
    @Test
    public void clearCancelsClaimedEvictions() {
        cache.get(key, k -> value);
        cache.evicted(key, memory.asMap().get(key), EvictionCause.SIZE);
        cache.invalidateAll();
        Assert.assertFalse(eligibility.get(0).getAsBoolean());
        Assert.assertTrue(cache.asMap().isEmpty());
    }

    /** A broadcast replaces the old entry without retiring the new one. */
    @Test
    public void broadcastReplacementRetiresOnlyTheOldEntry() {
        cache.get(key, k -> value);
        CacheEntry<StringValue> old = memory.asMap().get(key);
        StringValue replacement = new StringValue("replacement");
        WriteBuffer buffer = new WriteBuffer();
        CacheType.DIFF.writeKey(buffer, key);
        buffer.put((byte) 1);
        CacheType.DIFF.writeValue(buffer, replacement);
        ByteBuffer bytes = buffer.getBuffer();
        bytes.flip();
        cache.receive(bytes);
        Assert.assertEquals(replacement, cache.getIfPresent(key));
        cache.evicted(key, old, EvictionCause.SIZE);
        Assert.assertTrue(writes.isEmpty());
        cache.evicted(key, memory.asMap().get(key), EvictionCause.SIZE);
        Assert.assertEquals(Arrays.asList(replacement), writes);
    }

    /** A loaded value qualifies for persistence before immediate eviction. */
    @Test
    public void zeroCapacityLoadIsUsedBeforeInlineEviction() {
        initialize(0);
        Assert.assertSame(value, cache.get(key, k -> value));
        Assert.assertEquals(Arrays.asList(value), writes);
        Assert.assertTrue(cache.asMap().isEmpty());
        Assert.assertEquals(0, cache.estimatedSize());
    }

    /** Invalidation cannot be overtaken by a pending disk read. */
    @Test
    public void invalidationWaitsForPersistentReadPublication() throws Exception {
        Cache<CacheValue, CacheEntry<StringValue>> entries = CacheBuilder.<CacheValue, CacheEntry<StringValue>>newBuilder()
                .maximumSize(100).maintenanceMode(mode).build();
        CountDownLatch read = new CountDownLatch(1);
        CountDownLatch publish = new CountDownLatch(1);
        AtomicBoolean pause = new AtomicBoolean();
        AsyncNodeCache<CacheValue, StringValue> reading = new AsyncNodeCache<CacheValue, StringValue>(persistence, entries,
                null, null, CacheType.DIFF, new CacheActionDispatcher(), StatisticsProvider.NOOP, false) {
            @Override
            void putFromPersistence(CacheValue key, StringValue value, boolean persisted, CacheEntry.KeyState<CacheValue> state) {
                if (pause.get()) {
                    read.countDown();
                    try {
                        Assert.assertTrue(publish.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }
                super.putFromPersistence(key, value, persisted, state);
            }
        };
        reading.addGeneration(0, false);
        reading.put(key, value);
        reading.asMap().clear();
        pause.set(true);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<StringValue> load = executor.submit(() -> reading.getIfPresent(key));
            Assert.assertTrue(read.await(5, TimeUnit.SECONDS));
            CountDownLatch invalidating = new CountDownLatch(1);
            Future<?> invalidation = executor.submit(() -> {
                invalidating.countDown();
                reading.invalidate(key);
            });
            Assert.assertTrue(invalidating.await(5, TimeUnit.SECONDS));
            Assert.assertThrows(TimeoutException.class, () -> invalidation.get(100, TimeUnit.MILLISECONDS));
            publish.countDown();
            Assert.assertEquals(value, load.get(5, TimeUnit.SECONDS));
            invalidation.get(5, TimeUnit.SECONDS);
            Assert.assertTrue(reading.asMap().isEmpty());
            Assert.assertNull(reading.getIfPresent(key));
        } finally {
            publish.countDown();
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /** Synchronous persistence needs entry retirement but no access counters. */
    @Test
    public void synchronousPersistenceDoesNotTrackAccess() {
        AsyncNodeCache<CacheValue, StringValue> synchronous = new AsyncNodeCache<>(persistence, memory,
                null, null, CacheType.DIFF, new CacheActionDispatcher(), StatisticsProvider.NOOP, false);
        synchronous.addGeneration(0, false);
        synchronous.put(key, value);
        CacheEntry<StringValue> entry = memory.asMap().get(key);
        Assert.assertSame(value, synchronous.getIfPresent(key));
        Assert.assertSame(value, synchronous.get(key, k -> value));
        Assert.assertSame(value, synchronous.asMap().get(key));
        Assert.assertEquals(0, entry.getAccessCount());
        synchronous.invalidate(key);
        Assert.assertTrue(entry.isRetired());
    }

    /** Executes deferred writes against the persistent map after same-key mutations. */
    @Test
    public void staleUsedEntryCannotWriteAfterReplacementInvalidationOrReload() throws Exception {
        for (int mutation = 0; mutation < 3; mutation++) {
            for (boolean evictionFirst : new boolean[] {true, false}) {
                TestCacheActionDispatcher dispatcher = new TestCacheActionDispatcher();
                Cache<CacheValue, CacheEntry<StringValue>> entries = CacheBuilder
                        .<CacheValue, CacheEntry<StringValue>>newBuilder().maximumSize(100)
                        .maintenanceMode(mode).build();
                AsyncNodeCache<CacheValue, StringValue> guarded = new AsyncNodeCache<>(persistence, entries,
                        null, null, CacheType.DIFF, dispatcher, StatisticsProvider.NOOP, true);
                guarded.addGeneration(0, false);
                guarded.invalidateAll();
                guarded.get(key, k -> value);
                CacheEntry<StringValue> old = entries.asMap().get(key);
                Assert.assertTrue(old.getAccessCount() > 0);
                if (evictionFirst) {
                    guarded.evicted(key, old, EvictionCause.SIZE);
                    Assert.assertEquals(1, dispatcher.pendingCount());
                }
                StringValue replacement = new StringValue("replacement-" + mutation);
                final int operation = mutation;
                ExecutorService executor = Executors.newSingleThreadExecutor();
                try {
                    executor.submit(() -> {
                        if (operation != 0) {
                            guarded.invalidate(key);
                        }
                        if (operation == 2) {
                            guarded.get(key, k -> replacement);
                        } else {
                            guarded.put(key, replacement);
                            guarded.getIfPresent(key);
                        }
                    }).get(5, TimeUnit.SECONDS);
                } finally {
                    executor.shutdownNow();
                    Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
                }
                if (!evictionFirst) {
                    guarded.evicted(key, old, EvictionCause.SIZE);
                }
                dispatcher.executeAll();
                Assert.assertNull("Retired entry reached persistent storage", guarded.getGenerationalMap().get(key));
                guarded.evicted(key, entries.asMap().get(key), EvictionCause.SIZE);
                dispatcher.executeAll();
                Assert.assertEquals(replacement, guarded.getGenerationalMap().get(key));
            }
        }
    }

    /** Used entries racing a replacement, invalidation or reload cannot write after retirement. */
    @Test
    public void usedEntryCallbackRacingMutationCannotPersistRetiredValue() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int mutation = 0; mutation < 3; mutation++) {
                for (int iteration = 0; iteration < 50; iteration++) {
                    TestCacheActionDispatcher dispatcher = new TestCacheActionDispatcher();
                    Cache<CacheValue, CacheEntry<StringValue>> entries = CacheBuilder
                            .<CacheValue, CacheEntry<StringValue>>newBuilder().maximumSize(100)
                            .maintenanceMode(mode).build();
                    AsyncNodeCache<CacheValue, StringValue> guarded = new AsyncNodeCache<>(persistence, entries,
                            null, null, CacheType.DIFF, dispatcher, StatisticsProvider.NOOP, true);
                    guarded.addGeneration(0, false);
                    guarded.invalidateAll();
                    guarded.get(key, k -> value);
                    CacheEntry<StringValue> old = entries.asMap().get(key);
                    Assert.assertTrue(old.getAccessCount() > 0);
                    StringValue replacement = new StringValue("replacement-" + mutation);
                    CyclicBarrier start = new CyclicBarrier(2);
                    final int operation = mutation;
                    Future<?> eviction = executor.submit(() -> {
                        start.await();
                        guarded.evicted(key, old, EvictionCause.SIZE);
                        return null;
                    });
                    Future<?> replace = executor.submit(() -> {
                        start.await();
                        if (operation != 0) {
                            guarded.invalidate(key);
                        }
                        if (operation == 2) {
                            guarded.get(key, k -> replacement);
                        } else {
                            guarded.put(key, replacement);
                            guarded.getIfPresent(key);
                        }
                        return null;
                    });
                    eviction.get(5, TimeUnit.SECONDS);
                    replace.get(5, TimeUnit.SECONDS);
                    dispatcher.executeAll();
                    Assert.assertNull("Retired entry reached persistent storage", guarded.getGenerationalMap().get(key));
                    guarded.evicted(key, entries.asMap().get(key), EvictionCause.SIZE);
                    dispatcher.executeAll();
                    Assert.assertEquals(replacement, guarded.getGenerationalMap().get(key));
                }
            }
        } finally {
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /** Cancelled writes must leave both persistent write signals unchanged. */
    @Test
    public void cancelledWriteDoesNotCountAsPutOrDiskSpace() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            DefaultStatisticsProvider provider = new DefaultStatisticsProvider(executor);
            TestCacheActionDispatcher dispatcher = new TestCacheActionDispatcher();
            AsyncNodeCache<CacheValue, StringValue> guarded = new AsyncNodeCache<>(persistence, memory,
                    null, null, CacheType.DIFF, dispatcher, provider, true);
            guarded.addGeneration(0, false);
            guarded.get(key, k -> value);
            guarded.evicted(key, memory.asMap().get(key), EvictionCause.SIZE);
            Assert.assertEquals(0, putCount(provider));
            Assert.assertEquals(0, guarded.getPersistentCacheStats().estimateCurrentWeight());
            guarded.invalidate(key);
            dispatcher.executeAll();
            Assert.assertEquals(0, putCount(provider));
            Assert.assertEquals(0, guarded.getPersistentCacheStats().estimateCurrentWeight());
            guarded.get(key, k -> value);
            guarded.evicted(key, memory.asMap().get(key), EvictionCause.SIZE);
            dispatcher.executeAll();
            Assert.assertEquals(1, putCount(provider));
            Assert.assertEquals((long) key.getMemory() + value.getMemory(),
                    guarded.getPersistentCacheStats().estimateCurrentWeight());
        } finally {
            executor.shutdownNow();
        }
    }

    private long putCount(StatisticsProvider provider) {
        return provider.getMeter("PersistentCache.NodeCache.diff.CACHE_PUT", StatsOptions.DEFAULT).getCount();
    }

    /** Skip unused entries and values already stored on disk. */
    @Test
    public void unusedAndPersistentEntriesAreNotWritten() {
        cache.put(key, value);
        cache.evicted(key, memory.asMap().get(key), EvictionCause.SIZE);
        Assert.assertTrue(writes.isEmpty());
        cache.putFromPersistence(key, value, true);
        cache.evicted(key, memory.asMap().get(key), EvictionCause.SIZE);
        Assert.assertTrue(writes.isEmpty());
    }
}
