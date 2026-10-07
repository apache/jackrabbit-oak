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
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.impl.caffeine.CaffeineCacheAdapter;
import org.apache.jackrabbit.oak.cache.impl.lirs.LirsLoadingCacheAdapter;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.locks.StripedNodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheType;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheMetadata;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;

/**
 * Tests for {@link DocumentNodeStoreBuilder} cache configuration.
 * Apart from the cache implementation selection tests, these assertions
 * intentionally avoid third-party cache types so the same tests can run
 * across cache implementation changes.
 */
public class DocumentNodeStoreBuilderTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder(new File("target"));

    private boolean caffeineCacheEnabled;

    @Before
    public void captureCaffeineCacheFeature() {
        caffeineCacheEnabled = DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.get();
    }

    @After
    public void resetCaffeineCacheFeature() {
        DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(caffeineCacheEnabled);
    }

    // Enabling the feature selects Caffeine with synchronous maintenance.
    @Test
    public void usesCaffeineCacheWhenFeatureIsEnabled() {
        DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(true);
        Cache<CacheValue, NodeDocument> cache = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .buildDocumentCache(new MemoryDocumentStore());

        Assert.assertTrue(cache instanceof CaffeineCacheAdapter);
    }

    // Disabling the Caffeine toggle restores the LIRS cache.
    @Test
    public void usesLirsCacheWhenCaffeineFeatureIsDisabled() {
        DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(false);

        Cache<CacheValue, NodeDocument> cache = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .buildDocumentCache(new MemoryDocumentStore());

        Assert.assertTrue(cache instanceof LirsLoadingCacheAdapter);
    }

    /** Local diffs follow the bundle's implementation selection with their original weights. */
    @Test
    public void localDiffUsesTheSelectedCacheImplementation() {
        for (boolean enabled : new boolean[] {false, true}) {
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(enabled);
            DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                    .memoryCacheSize(16 * 1024 * 1024).setPersistentCache(null)
                    .withWeigher((key, value) -> value.getMemory());
            Cache<RevisionsKey, LocalDiffCache.Diff> cache = builder.buildLocalDiffCache();
            Assert.assertEquals(enabled ? CaffeineCacheAdapter.class : LirsLoadingCacheAdapter.class, cache.getClass());
            RevisionsKey key = new RevisionsKey(new RevisionVector(new Revision(1, 0, 1)),
                    new RevisionVector(new Revision(2, 0, 1)));
            LocalDiffCache.Diff value = new LocalDiffCache.Diff(new HashMap<>(), 1200);
            cache.put(key, value);
            Assert.assertSame(value, cache.getIfPresent(key));
            Assert.assertEquals(1200, cache.getUsedWeight());
        }
    }

    /** Caffeine admission may discard entries under pressure but must respect the local-diff budget. */
    @Test
    public void localDiffCaffeineRespectsItsMemoryBudget() {
        DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(true);
        DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .memoryCacheSize(16 * 1024 * 1024).setPersistentCache(null)
                .withWeigher((key, value) -> value.getMemory());
        Cache<RevisionsKey, LocalDiffCache.Diff> cache = builder.buildLocalDiffCache();
        for (int i = 0; i < 4015; i++) {
            RevisionsKey key = new RevisionsKey(new RevisionVector(new Revision(i, 0, 1)),
                    new RevisionVector(new Revision(i + 1, 0, 1)));
            cache.put(key, new LocalDiffCache.Diff(new HashMap<>(), i < 15 ? 178_000 : 1200));
        }
        Assert.assertTrue("Workload must exceed the local diff memory budget", cache.stats().evictionCount() > 0);
        Assert.assertTrue(cache.getUsedWeight() <= builder.getLocalDiffCacheSize());
    }

    /** A zero local-diff budget retains the existing no-cache behavior. */
    @Test
    public void zeroWeightLocalDiffCacheDoesNotUseLirs() {
        for (boolean enabled : new boolean[] {false, true}) {
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(enabled);
            DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                    .memoryCacheSize(0).setPersistentCache(null);
            Cache<RevisionsKey, LocalDiffCache.Diff> cache = builder.buildLocalDiffCache();
            Assert.assertTrue(cache instanceof CaffeineCacheAdapter);
            cache.put(new RevisionsKey(new RevisionVector(new Revision(1, 0, 1)),
                    new RevisionVector(new Revision(2, 0, 1))), new LocalDiffCache.Diff(new HashMap<>(), 1200));
            Assert.assertEquals(0, cache.estimatedSize());
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(!enabled);
            Cache<CacheValue, StringValue> later = builder.memoryCacheSize(1024 * 1024).buildMemoryDiffCache();
            Assert.assertEquals(enabled ? CaffeineCacheAdapter.class : LirsLoadingCacheAdapter.class, later.getClass());
        }
    }

    /** The selection test restores the incoming feature state. */
    @Test
    public void cacheSelectionTestShouldRestoreIncomingToggle() {
        boolean incoming = DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.get();
        try {
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(false);
            Result result = new JUnitCore().run(Request.method(DocumentNodeStoreBuilderTest.class,
                    "usesLirsCacheWhenCaffeineFeatureIsDisabled"));
            Assert.assertTrue(result.getFailures().toString(), result.wasSuccessful());
            Assert.assertFalse("Selection test leaked its toggle into subsequent tests",
                    DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.get());
        } finally {
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(incoming);
        }
    }

    /** A builder keeps its cache implementation after a toggle change. */
    @Test
    public void implementationChangesApplyOnlyToNewBuilders() {
        MemoryDocumentStore documents = new MemoryDocumentStore();
        for (boolean enabled : new boolean[] {false, true}) {
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(enabled);
            DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder();
            Cache<CacheValue, NodeDocument> first = builder.buildDocumentCache(documents);
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(!enabled);
            Cache<StringValue, NodeDocument> second = builder.buildPrevDocumentsCache(documents);
            Assert.assertEquals(first.getClass(), second.getClass());
            Cache<CacheValue, NodeDocument> replacement = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                    .buildDocumentCache(documents);
            Assert.assertNotEquals(first.getClass(), replacement.getClass());
        }
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

    /** Opted-in document caches evict on the caller thread. */
    @Test
    public void documentCacheUsesSynchronousMaintenanceWhenEnabled() {
        DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(true);
        DocumentStore store = new MemoryDocumentStore();
        Cache<CacheValue, NodeDocument> cache = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .memoryCacheSize(100)
                .memoryCacheDistribution(0, 0, 0, 0, 0)
                .withWeigher((key, value) -> 100)
                .buildDocumentCache(store);

        NodeDocument first = new NodeDocument(store, 1L);
        NodeDocument second = new NodeDocument(store, 2L);
        CacheValue firstKey = StringValue.fromString("first");
        CacheValue secondKey = StringValue.fromString("second");
        cache.put(firstKey, first);
        cache.put(secondKey, second);

        Assert.assertEquals("synchronous maintenance should enforce the selected cache's limit inline",
                1, cache.estimatedSize());
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

    // Size evictions reach the asynchronous persistent-cache write queue.
    @Test
    public void sizeEvictionReachesAsyncPersistentCache() throws Exception {
        DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(true);
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
            // Persistent writes run asynchronously even with synchronous Caffeine maintenance.
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

    // Zero-capacity evictions remove metadata instead of leaking it.
    @Test
    public void zeroWeightEvictionReachesAsyncPersistentCache() throws Exception {
        DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .memoryCacheSize(0)
                .setPersistentCache(temp.newFolder().getAbsolutePath() + ",+asyncDiff");
        try {
            Cache<CacheValue, StringValue> cache = builder.buildMemoryDiffCache();
            Field metadataField = cache.getClass().getDeclaredField("memCacheMetadata");
            metadataField.setAccessible(true);
            CacheMetadata<?> metadata = (CacheMetadata<?>) metadataField.get(cache);
            Field entriesField = CacheMetadata.class.getDeclaredField("metadataMap");
            entriesField.setAccessible(true);
            Map<?, ?> entries = (Map<?, ?>) entriesField.get(metadata);
            for (int i = 0; i < 100; i++) {
                cache.put(new MemoryDiffCache.Key(
                        Path.fromString("/evict-" + i),
                        new RevisionVector(new Revision(1, 0, 1)),
                        new RevisionVector(new Revision(2, 0, 1))), StringValue.fromString("x"));
                Assert.assertTrue("Eviction must remove metadata before put returns", entries.isEmpty());
            }
            Assert.assertEquals(0, cache.estimatedSize());
            Assert.assertTrue(cache.asMap().isEmpty());
        } finally {
            closeCaches(builder);
        }
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
