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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.File;
import java.util.HashMap;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.impl.caffeine.CaffeineCacheAdapter;
import org.apache.jackrabbit.oak.cache.impl.lirs.LirsLoadingCacheAdapter;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.locks.StripedNodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheType;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCacheStats;
import org.apache.jackrabbit.oak.plugins.document.util.RevisionsKey;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link DocumentNodeStoreBuilder} cache configuration.
 * Apart from the cache implementation selection tests, these assertions
 * intentionally avoid third-party cache types so the same tests can run
 * across cache implementation changes.
 */
public class DocumentNodeStoreBuilderTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder(new File("target"));

    @After
    public void resetCaffeineCacheFeature() {
        DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(true);
    }

    @Test
    public void usesCaffeineCacheByDefault() {
        Cache<CacheValue, NodeDocument> cache = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .buildDocumentCache(new MemoryDocumentStore());

        Assert.assertTrue(cache instanceof CaffeineCacheAdapter);
    }

    @Test
    public void usesLirsCacheWhenCaffeineFeatureIsDisabled() {
        DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(false);

        Cache<CacheValue, NodeDocument> cache = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .buildDocumentCache(new MemoryDocumentStore());

        Assert.assertTrue(cache instanceof LirsLoadingCacheAdapter);
    }

    @Test
    public void buildNodeDocumentCacheReturnsNonNull() {
        // Verify the builder can construct the node-document cache with the default
        // in-memory configuration and a plain in-memory document store.
        DocumentStore store = new MemoryDocumentStore();
        NodeDocumentCache cache = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .buildNodeDocumentCache(store, new StripedNodeDocumentLocks());
        Assert.assertNotNull(cache);
    }

    @Test
    public void buildNodeDocumentCacheStatsAreNonEmpty() {
        // The builder wires cache stats as part of construction, so the returned
        // cache should already expose at least one stats entry.
        DocumentStore store = new MemoryDocumentStore();
        NodeDocumentCache cache = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .buildNodeDocumentCache(store, new StripedNodeDocumentLocks());
        Iterable<AbstractCacheStats> stats = cache.getCacheStats();
        Assert.assertNotNull(stats);
        Assert.assertTrue(stats.iterator().hasNext());
    }

    @Test
    public void buildNodeDocumentCacheIsUsable() throws Exception {
        // Round-trip a document through the built cache so this test checks
        // observable put/get behavior instead of just construction.
        DocumentStore docStore = new MemoryDocumentStore();
        NodeDocumentCache cache = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .buildNodeDocumentCache(docStore, new StripedNodeDocumentLocks());
        // put a document and verify it can be retrieved
        NodeDocument doc = new NodeDocument(docStore, 1L);
        doc.put(Document.ID, "test-id");
        doc.put(Document.MOD_COUNT, 1L);
        cache.put(doc);
        NodeDocument result = cache.getIfPresent("test-id");
        Assert.assertNotNull(result);
        Assert.assertEquals(doc.getModCount(), result.getModCount());
    }

    @Test
    public void buildNodeDocumentCacheWithZeroMemoryDistributionStillReturnsUsableCache() throws Exception {
        DocumentStore docStore = new MemoryDocumentStore();
        // This verifies builder behavior when all memory cache buckets are disabled.
        // It does not assert cache-capacity semantics.
        NodeDocumentCache cache = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .memoryCacheDistribution(0, 0, 0, 0, 0)
                .buildNodeDocumentCache(docStore, new StripedNodeDocumentLocks());
        NodeDocument doc = new NodeDocument(docStore, 2L);
        doc.put(Document.ID, "zero-distribution-id");
        doc.put(Document.MOD_COUNT, 2L);
        cache.put(doc);

        NodeDocument result = cache.getIfPresent("zero-distribution-id");
        Assert.assertNotNull(result);
        Assert.assertEquals(doc.getModCount(), result.getModCount());
    }

    @Test
    public void buildDocumentCacheStoresAndRetrievesDocuments() throws Exception {
        // buildDocumentCache() currently returns an implementation-specific cache type,
        // so this test uses reflection and checks only the observable put/get contract.
        DocumentStore store = new MemoryDocumentStore();
        Object cache = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder().buildDocumentCache(store);
        NodeDocument document = new NodeDocument(store, 1L);
        StringValue key = StringValue.fromString("document-cache-id");
        document.put(Document.ID, key.toString());
        document.put(Document.MOD_COUNT, 7L);

        invoke(cache, "put", Object.class, Object.class, key, document);
        Object cached = invoke(cache, "getIfPresent", Object.class, key);

        Assert.assertNotNull(cached);
        Assert.assertTrue(cached instanceof NodeDocument);
        Assert.assertEquals(document.getModCount(), ((NodeDocument) cached).getModCount());
    }

    @Test
    public void buildMemoryDiffCacheCreatesPersistentCacheStats() throws Exception {
        String cacheDir = temp.newFolder().getAbsolutePath() + ",-async";
        MemoryDiffCache.Key key = new MemoryDiffCache.Key(
                Path.fromString("/diff-memory"),
                new RevisionVector(new Revision(1, 0, 1)),
                new RevisionVector(new Revision(2, 0, 1)));

        DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .setPersistentCache(cacheDir);
        try {
            Cache<CacheValue, StringValue> cache = builder.buildMemoryDiffCache();
            PersistentCacheStats persistentStats = PersistentCache.getPersistentCacheStats(cache);

            Assert.assertNotNull("Expected persistent wrapper for " + CacheType.DIFF, persistentStats);
            Assert.assertTrue("Expected persistent cache stats map entry for " + CacheType.DIFF,
                    builder.getPersistenceCacheStats().containsKey(CacheType.DIFF.name()));

            cache.put(key, StringValue.fromString("memory-diff-value"));
        } finally {
            closeCaches(builder);
        }

        DocumentNodeStoreBuilder<?> reopenedBuilder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .setPersistentCache(cacheDir);
        try {
            Cache<CacheValue, StringValue> reopened = reopenedBuilder.buildMemoryDiffCache();
            StringValue value = reopened.getIfPresent(key);
            Assert.assertNotNull("Expected persisted DIFF entry to be readable after reopen", value);
            Assert.assertEquals("memory-diff-value", value.asString());
        } finally {
            closeCaches(reopenedBuilder);
        }
    }

    @Test
    public void buildLocalDiffCacheCreatesPersistentCacheStats() throws Exception {
        String cacheDir = temp.newFolder().getAbsolutePath() + ",-async";
        RevisionsKey key = new RevisionsKey(
                new RevisionVector(new Revision(3, 0, 1)),
                new RevisionVector(new Revision(4, 0, 1)));
        Map<Path, String> changes = new HashMap<>();
        changes.put(Path.fromString("/diff-local"), "+\"child\":{}");
        LocalDiffCache.Diff expected = new LocalDiffCache.Diff(changes, 0);

        DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .setPersistentCache(cacheDir);
        try {
            Cache<RevisionsKey, LocalDiffCache.Diff> cache = builder.buildLocalDiffCache();
            PersistentCacheStats persistentStats = PersistentCache.getPersistentCacheStats(cache);

            Assert.assertNotNull("Expected persistent wrapper for " + CacheType.LOCAL_DIFF, persistentStats);
            Assert.assertTrue("Expected persistent cache stats map entry for " + CacheType.LOCAL_DIFF,
                    builder.getPersistenceCacheStats().containsKey(CacheType.LOCAL_DIFF.name()));

            cache.put(key, expected);
        } finally {
            closeCaches(builder);
        }

        DocumentNodeStoreBuilder<?> reopenedBuilder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .setPersistentCache(cacheDir);
        try {
            Cache<RevisionsKey, LocalDiffCache.Diff> reopened = reopenedBuilder.buildLocalDiffCache();
            LocalDiffCache.Diff value = reopened.getIfPresent(key);
            Assert.assertNotNull("Expected persisted LOCAL_DIFF entry to be readable after reopen", value);
            Assert.assertEquals(expected, value);
        } finally {
            closeCaches(reopenedBuilder);
        }
    }

    @Test
    public void sizeEvictionReachesAsyncPersistentCache() throws Exception {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        StatisticsProvider statsProvider = new DefaultStatisticsProvider(executor);
        DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .memoryCacheSize(1024 * 1024)
                .setStatisticsProvider(statsProvider)
                .setPersistentCache(temp.newFolder().getAbsolutePath() + ",+asyncDiff");
        try {
            Cache<CacheValue, StringValue> cache = builder.buildMemoryDiffCache();
            MeterStats persistedPuts = statsProvider.getMeter(
                    "PersistentCache.NodeCache.diff.CACHE_PUT", StatsOptions.DEFAULT);
            String payload = "x".repeat(4096);
            for (int i = 0; i < 100; i++) {
                MemoryDiffCache.Key key = new MemoryDiffCache.Key(
                        Path.fromString("/evict-" + i),
                        new RevisionVector(new Revision(1, 0, 1)),
                        new RevisionVector(new Revision(2, 0, 1)));
                cache.put(key, StringValue.fromString(payload));
                cache.getIfPresent(key);
            }
            // removal listeners run asynchronously on the cache maintenance executor
            long deadline = System.currentTimeMillis() + 10_000;
            while (persistedPuts.getCount() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }

            Assert.assertTrue("Expected evicted entries to be handed to the persistent cache",
                    persistedPuts.getCount() > 0);
        } finally {
            closeCaches(builder);
            new ExecutorCloser(executor).close();
        }
    }

    @Test
    public void zeroWeightEvictionReachesAsyncPersistentCache() throws Exception {
        DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .memoryCacheSize(0)
                .setPersistentCache(temp.newFolder().getAbsolutePath() + ",+asyncDiff");
        try {
            Cache<CacheValue, StringValue> cache = builder.buildMemoryDiffCache();
            for (int i = 0; i < 100; i++) {
                cache.put(new MemoryDiffCache.Key(
                        Path.fromString("/evict-" + i),
                        new RevisionVector(new Revision(1, 0, 1)),
                        new RevisionVector(new Revision(2, 0, 1))), StringValue.fromString("x"));
            }
            // zero-capacity caches evict synchronously; each eviction must clear the NodeCache metadata
            Object metadata = readField(cache, "memCacheMetadata");
            Map<?, ?> metadataMap = (Map<?, ?>) readField(metadata, "metadataMap");
            Assert.assertTrue("Expected evictions to reach NodeCache, leaked metadata: " + metadataMap.size(),
                    metadataMap.isEmpty());
        } finally {
            closeCaches(builder);
        }
    }

    private static Object readField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void closeCaches(DocumentNodeStoreBuilder<?> builder) {
        PersistentCache cache = builder.getPersistentCache();
        if (cache != null) {
            cache.close();
        }
        PersistentCache journalCache = builder.getJournalCache();
        if (journalCache != null) {
            journalCache.close();
        }
    }

    private static Object invoke(Object target, String methodName, Class<?> parameterType, Object argument)
            throws Exception {
        Method method = target.getClass().getMethod(methodName, parameterType);
        return method.invoke(target, argument);
    }

    private static Object invoke(Object target,
                                 String methodName,
                                 Class<?> firstType,
                                 Class<?> secondType,
                                 Object firstArgument,
                                 Object secondArgument) throws Exception {
        Method method = target.getClass().getMethod(methodName, firstType, secondType);
        return method.invoke(target, firstArgument, secondArgument);
    }
}
