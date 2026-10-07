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
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheEntry;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheType;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.EvictionListener;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;
import org.apache.jackrabbit.oak.plugins.document.util.RevisionsKey;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.mockito.stubbing.Answer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

/** Tests per-cache construction policies in {@link DocumentNodeStoreBuilder}. */
public class DocumentCacheMaintenanceTest {
    private boolean previousCaffeine;
    private boolean previousAsync;

    @Before
    public void captureFeatures() {
        previousCaffeine = DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.getAndSet(true);
        previousAsync = DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.get();
    }

    @After
    public void restoreFeatures() {
        DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(previousCaffeine);
        DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(previousAsync);
    }

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    /** Document diff callbacks run inline by default. */
    @Test
    public void documentDiffCacheUsesSynchronousMaintenance() throws Exception {
        checkCallbackThread(CacheType.DIFF, true, false);
    }

    /** An opted-in diff cache dispatches eviction off the caller. */
    @Test
    public void explicitDiffAsyncRunsItsEvictionCallbackOffTheCaller() throws Exception {
        checkCallbackThread(CacheType.DIFF, true, true);
    }

    /** Disabling ASYNC restores inline callbacks despite an override. */
    @Test
    public void disablingFeatureIgnoresAsyncOverridesForAllCaches() throws Exception {
        checkCallbackThread(CacheType.DIFF, false, true);
    }

    /** Previous-document callbacks follow the effective feature state. */
    @Test
    public void previousDocumentCacheHonorsAsyncOptInAndFeatureOff() throws Exception {
        checkCallbackThread(CacheType.PREV_DOCUMENT, true, true);
        checkCallbackThread(CacheType.PREV_DOCUMENT, false, true);
    }

    /** Local diff callbacks follow the effective feature state. */
    @Test
    public void localDiffCacheHonorsAsyncOptInAndFeatureOff() throws Exception {
        checkCallbackThread(CacheType.LOCAL_DIFF, true, true);
        checkCallbackThread(CacheType.LOCAL_DIFF, false, true);
    }

    /** Local diffs use entry metadata only with both opt-ins, ASYNC selection and persistence. */
    @Test
    public void localDiffUsesTheSelectedModeAndMetadata() throws Exception {
        for (boolean caffeine : new boolean[] {false, true}) {
            for (boolean async : new boolean[] {false, true}) {
                for (boolean persistent : new boolean[] {false, true}) {
                    for (long memory : new long[] {0, 1_000_000}) {
                        for (CacheBuilder.MaintenanceMode requested : CacheBuilder.MaintenanceMode.values()) {
                            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(caffeine);
                            DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(async);
                            DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                                    .memoryCacheSize(memory).setCacheSegmentCount(1)
                                    .setPersistentCache(persistent ? folder.newFolder().getAbsolutePath() + ",+asyncDiff" : null)
                                    .setCacheMaintenanceMode(CacheType.LOCAL_DIFF, requested)
                                    .setCacheMaintenanceMode(CacheType.NODE, CacheBuilder.MaintenanceMode.ASYNC)
                                    .withWeigher((key, value) -> {
                                        Assert.assertTrue(value instanceof LocalDiffCache.Diff);
                                        return 100;
                                    });
                            try {
                                boolean usesAsync = caffeine && async && requested == CacheBuilder.MaintenanceMode.ASYNC;
                                boolean usesEntries = usesAsync && persistent;
                                Assert.assertEquals(usesAsync ? CacheBuilder.MaintenanceMode.ASYNC : CacheBuilder.MaintenanceMode.SYNC,
                                        builder.getCacheMaintenanceMode(CacheType.LOCAL_DIFF));
                                Assert.assertEquals(caffeine && async ? CacheBuilder.MaintenanceMode.ASYNC
                                        : CacheBuilder.MaintenanceMode.SYNC, builder.getCacheMaintenanceMode(CacheType.NODE));
                                Cache<RevisionsKey, LocalDiffCache.Diff> cache = builder.buildLocalDiffCache();
                                String memoryType = caffeine || memory == 0 ? "CaffeineCacheAdapter" : "LirsLoadingCacheAdapter";
                                Assert.assertEquals(persistent ? usesEntries ? "AsyncNodeCache" : "NodeCache" : memoryType,
                                        cache.getClass().getSimpleName());
                                RevisionsKey key = new RevisionsKey(new RevisionVector(new Revision(1, 0, 1)),
                                        new RevisionVector(new Revision(2, 0, 1)));
                                LocalDiffCache.Diff value = new LocalDiffCache.Diff(Collections.emptyMap(), 100);
                                cache.put(key, value);
                                cache.cleanUp();
                                Assert.assertEquals(memory == 0 ? 0 : usesEntries ? 100 + CacheEntry.MEMORY_OVERHEAD : 100,
                                        cache.getUsedWeight());
                                if (memory > 0) {
                                    Assert.assertSame(value, cache.asMap().get(key));
                                }
                                if (persistent) {
                                    Assert.assertNotNull(PersistentCache.getPersistentCacheStats(cache));
                                    Assert.assertSame(PersistentCache.getPersistentCacheStats(cache),
                                            builder.getPersistenceCacheStats().get(CacheType.LOCAL_DIFF.name()));
                                }
                            } finally {
                                if (builder.getPersistentCache() != null) { builder.getPersistentCache().close(); }
                            }
                        }
                    }
                }
            }
        }
    }

    /** Mutable documents use raw Caffeine entries even when persistence and ASYNC are enabled. */
    @Test
    public void asyncDocumentCacheDoesNotWrapMutableDocumentsInMetadata() throws Exception {
        DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(true);
        MemoryDocumentStore documents = new MemoryDocumentStore();
        DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .setPersistentCache(folder.newFolder().getAbsolutePath())
                .setCacheMaintenanceMode(CacheType.DOCUMENT, CacheBuilder.MaintenanceMode.ASYNC)
                .withWeigher((key, value) -> {
                    Assert.assertTrue(value instanceof NodeDocument);
                    return 100;
                });
        try {
            Cache<CacheValue, NodeDocument> cache = builder.buildDocumentCache(documents);
            Assert.assertEquals(CacheBuilder.MaintenanceMode.ASYNC, builder.getCacheMaintenanceMode(CacheType.DOCUMENT));
            Assert.assertEquals("CaffeineCacheAdapter", cache.getClass().getSimpleName());
            Assert.assertNull(PersistentCache.getPersistentCacheStats(cache));
            Assert.assertNull(builder.getPersistenceCacheStats().get(CacheType.DOCUMENT.name()));
            StringValue key = new StringValue("document");
            NodeDocument value = new NodeDocument(documents);
            cache.put(key, value);
            cache.cleanUp();
            Assert.assertSame(value, cache.getIfPresent(key));
            Assert.assertSame(value, cache.asMap().get(key));
            Assert.assertEquals(100, cache.getUsedWeight());
        } finally {
            builder.getPersistentCache().close();
        }
    }

    /** Journal configuration must select the same disk cache in both maintenance modes. */
    @Test
    public void localDiffUsesJournalPersistenceAndSurvivesReopen() throws Exception {
        DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(true);
        for (CacheBuilder.MaintenanceMode mode : CacheBuilder.MaintenanceMode.values()) {
            String mainDirectory = folder.newFolder().getAbsolutePath() + ",-async";
            String journalDirectory = folder.newFolder().getAbsolutePath() + ",-async";
            RevisionsKey key = new RevisionsKey(new RevisionVector(new Revision(1, 0, 1)),
                    new RevisionVector(new Revision(2, 0, 1)));
            LocalDiffCache.Diff expected = new LocalDiffCache.Diff(
                    Collections.singletonMap(Path.ROOT, "^\"child\":{}"), 100);
            DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                    .setPersistentCache(mainDirectory).setJournalCache(journalDirectory)
                    .setCacheMaintenanceMode(CacheType.LOCAL_DIFF, mode);
            try {
                Cache<RevisionsKey, LocalDiffCache.Diff> cache = builder.buildLocalDiffCache();
                Assert.assertEquals(mode == CacheBuilder.MaintenanceMode.ASYNC ? "AsyncNodeCache" : "NodeCache",
                        cache.getClass().getSimpleName());
                cache.put(key, expected);
            } finally {
                builder.getJournalCache().close();
                builder.getPersistentCache().close();
            }
            for (String directory : new String[] {journalDirectory, mainDirectory}) {
                DocumentNodeStoreBuilder<?> reopened = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                        .setPersistentCache(directory).setCacheMaintenanceMode(CacheType.LOCAL_DIFF, mode);
                try {
                    LocalDiffCache.Diff stored = reopened.buildLocalDiffCache().getIfPresent(key);
                    if (directory.equals(journalDirectory)) { Assert.assertEquals(expected, stored); }
                    else { Assert.assertNull("LOCAL_DIFF must use journal storage when configured", stored); }
                } finally {
                    reopened.getPersistentCache().close();
                }
            }
        }
    }

    /** Both opt-ins are required; unconfigured caches stay SYNC. */
    @Test
    public void asyncMaintenanceRequiresFeatureAndIsSelectedPerCache() {
        DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder();
        for (CacheType cacheType : CacheType.values()) {
            builder.setCacheMaintenanceMode(cacheType, CacheBuilder.MaintenanceMode.ASYNC);
        }
        boolean previousAsync = DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED
                .getAndSet(false);
        try {
            for (CacheType cacheType : CacheType.values()) {
                Assert.assertEquals(cacheType.name(), CacheBuilder.MaintenanceMode.SYNC,
                        builder.getCacheMaintenanceMode(cacheType));
            }

            DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(true);
            DocumentNodeStoreBuilder<?> optedInBuilder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                    .setCacheMaintenanceMode(CacheType.NODE, CacheBuilder.MaintenanceMode.ASYNC);
            for (CacheType cacheType : CacheType.values()) {
                Assert.assertEquals(cacheType.name(),
                        cacheType == CacheType.NODE ? CacheBuilder.MaintenanceMode.ASYNC
                                : CacheBuilder.MaintenanceMode.SYNC,
                        optedInBuilder.getCacheMaintenanceMode(cacheType));
            }
            optedInBuilder.setCacheMaintenanceMode(CacheType.NODE, CacheBuilder.MaintenanceMode.SYNC);
            Assert.assertEquals(CacheBuilder.MaintenanceMode.SYNC,
                    optedInBuilder.getCacheMaintenanceMode(CacheType.NODE));
        } finally {
            DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(previousAsync);
        }
    }

    /** A failing SYNC eviction listener cannot fail cache reads or subsequent writes. */
    @Test
    public void synchronousEvictionListenerFailureDoesNotFailCacheOperations() throws Exception {
        AtomicReference<Thread> callbackThread = new AtomicReference<>();
        AtomicInteger callbacks = new AtomicInteger();
        PersistentCache persistence = Mockito.mock(PersistentCache.class);
        Mockito.doAnswer(invocation -> {
            Cache<?, ?> memory = invocation.getArgument(2);
            return Mockito.mock(Cache.class, Mockito.withSettings().extraInterfaces(EvictionListener.class)
                    .defaultAnswer(callback -> {
                        if (callback.getMethod().getName().equals("evicted")) {
                            callbackThread.set(Thread.currentThread());
                            callbacks.incrementAndGet();
                            throw new IllegalStateException("Simulated persistence callback failure");
                        }
                        return AdditionalAnswers.delegatesTo(memory).answer(callback);
                    }));
        }).when(persistence).wrap(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        DocumentNodeStoreBuilder<?> builder = new RecordingBuilder(persistence)
                .memoryCacheSize(1000).withWeigher((key, value) -> 1000);
        boolean previous = DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.getAndSet(true);
        try {
            Cache<CacheValue, StringValue> cache = builder.buildMemoryDiffCache();
            StringValue value = new StringValue("value");
            Assert.assertSame(value, cache.get(key(), key -> value));
            Assert.assertNull(cache.getIfPresent(key()));
            cache.put(key(), value);
            Assert.assertNull(cache.getIfPresent(key()));
            Assert.assertEquals(2, callbacks.get());
            Assert.assertSame(Thread.currentThread(), callbackThread.get());
        } finally {
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(previous);
        }
    }

    /** The legacy wrapper adds no entry overhead to custom weights. */
    @Test
    public void toggleChangesApplyOnlyToNewBuilders() {
        boolean previousAsync = DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.get();
        try {
            for (boolean enabled : new boolean[] {false, true}) {
                DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(enabled);
                DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder();
                for (CacheType type : CacheType.values()) {
                    builder.setCacheMaintenanceMode(type, CacheBuilder.MaintenanceMode.ASYNC);
                }
                Assert.assertEquals(enabled ? CacheBuilder.MaintenanceMode.ASYNC : CacheBuilder.MaintenanceMode.SYNC,
                        builder.getCacheMaintenanceMode(CacheType.DOCUMENT));

                DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(!enabled);
                for (CacheType type : CacheType.values()) {
                    Assert.assertEquals(type.name(),
                            enabled ? CacheBuilder.MaintenanceMode.ASYNC
                                    : CacheBuilder.MaintenanceMode.SYNC,
                            builder.getCacheMaintenanceMode(type));
                }
                DocumentNodeStoreBuilder<?> restarted = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                        .setCacheMaintenanceMode(CacheType.NODE, CacheBuilder.MaintenanceMode.ASYNC);
                Assert.assertEquals(enabled ? CacheBuilder.MaintenanceMode.SYNC : CacheBuilder.MaintenanceMode.ASYNC,
                        restarted.getCacheMaintenanceMode(CacheType.NODE));
            }
        } finally {
            DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(previousAsync);
        }
    }

    private void checkCallbackThread(CacheType cacheType, boolean featureEnabled, boolean cacheAsync) throws Exception {
        RecordingCallback callbackThread = new RecordingCallback();
        PersistentCache persistence = Mockito.spy(new PersistentCache(folder.newFolder().getAbsolutePath() + ",+asyncDiff"));
        Answer<Object> wrappingAnswer = invocation -> {
            Cache<?, ?> real = (Cache<?, ?>) invocation.callRealMethod();
            return Mockito.mock(Cache.class, Mockito.withSettings().extraInterfaces(EvictionListener.class)
                    .defaultAnswer(callback -> {
                        Object result = AdditionalAnswers.delegatesTo(real).answer(callback);
                        if (callback.getMethod().getName().equals("evicted")) {
                            callbackThread.record();
                        }
                        return result;
                    }));
        };
        Mockito.doAnswer(wrappingAnswer).when(persistence)
                .wrap(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doAnswer(wrappingAnswer).when(persistence)
                .wrapAsyncMaintenance(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        DocumentNodeStoreBuilder<?> builder = new RecordingBuilder(persistence).memoryCacheSize(1000)
                .withWeigher((key, value) -> 1000);
        if (cacheAsync) {
            builder.setCacheMaintenanceMode(cacheType, CacheBuilder.MaintenanceMode.ASYNC);
        }
        boolean previousCaffeine = DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.getAndSet(true);
        boolean previousAsync = DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED
                .getAndSet(featureEnabled);
        try {
            Thread caller = Thread.currentThread();
            Cache<?, ?> localDiff = null;
            switch (cacheType) {
                case DIFF:
                    loadAndFlipToggle(builder.buildMemoryDiffCache(), key(), new StringValue("value"), featureEnabled);
                    break;
                case LOCAL_DIFF:
                    Cache<RevisionsKey, LocalDiffCache.Diff> local = builder.memoryCacheSize(20_000)
                            .setCacheSegmentCount(1).buildLocalDiffCache();
                    for (int i = 0; i < 10; i++) {
                        loadAndFlipToggle(local,
                                new RevisionsKey(new RevisionVector(new Revision(i, 0, 1)),
                                        new RevisionVector(new Revision(i + 1, 0, 1))),
                                new LocalDiffCache.Diff(Collections.singletonMap(Path.ROOT, "^\"child\":{}"), 1000),
                                featureEnabled);
                    }
                    localDiff = local;
                    break;
                case PREV_DOCUMENT:
                    MemoryDocumentStore documents = new MemoryDocumentStore();
                    loadAndFlipToggle(builder.buildPrevDocumentsCache(documents), new StringValue("previous"),
                            new NodeDocument(documents), featureEnabled);
                    break;
                default:
                    throw new AssertionError("Unsupported cache type: " + cacheType);
            }
            if (featureEnabled && cacheAsync) {
                Assert.assertTrue("eviction callback did not arrive", callbackThread.called.await(5, TimeUnit.SECONDS));
                Assert.assertNotSame(caller, callbackThread.thread.get());
            } else {
                Assert.assertSame(caller, callbackThread.thread.get());
            }
            if (localDiff != null) { Assert.assertTrue(localDiff.stats().evictionCount() > 0); }
        } finally {
            builder.getPersistentCache().close();
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(previousCaffeine);
            DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(previousAsync);
        }
    }

    private <K extends CacheValue, V extends CacheValue> void loadAndFlipToggle(
            Cache<K, V> cache, K key, V value, boolean featureEnabled) throws Exception {
        DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(!featureEnabled);
        Assert.assertSame(value, cache.get(key, k -> value));
    }

    /** Reject incomplete per-cache configuration. */
    @Test
    public void configurationRejectsNullCacheTypeAndMode() {
        DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder();
        Assert.assertThrows(NullPointerException.class,
                () -> builder.setCacheMaintenanceMode(null, CacheBuilder.MaintenanceMode.SYNC));
        Assert.assertThrows(NullPointerException.class,
                () -> builder.setCacheMaintenanceMode(CacheType.DIFF, null));
        Assert.assertThrows(NullPointerException.class, () -> builder.getCacheMaintenanceMode(null));
    }

    /** Only persistent, opted-in ASYNC caches pay for entry metadata. */
    @Test
    public void onlyOptedInAsyncCachesUseEntryMetadataAndExtraWeight() throws Exception {
        for (boolean caffeine : new boolean[] {false, true}) {
            for (boolean asyncFeature : new boolean[] {false, true}) {
                for (CacheBuilder.MaintenanceMode requested : CacheBuilder.MaintenanceMode.values()) {
                    DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(caffeine);
                    DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(asyncFeature);
                    DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                            .memoryCacheSize(1_000_000).setCacheSegmentCount(1)
                            .setPersistentCache(folder.newFolder().getAbsolutePath() + ",+asyncDiff")
                            .setCacheMaintenanceMode(CacheType.DIFF, requested)
                            .withWeigher((key, value) -> {
                                Assert.assertTrue(value instanceof StringValue);
                                return 100;
                            });
                    try {
                        boolean usesEntries = caffeine && asyncFeature
                                && requested == CacheBuilder.MaintenanceMode.ASYNC;
                        Assert.assertEquals(usesEntries ? CacheBuilder.MaintenanceMode.ASYNC
                                : CacheBuilder.MaintenanceMode.SYNC, builder.getCacheMaintenanceMode(CacheType.DIFF));
                        Cache<CacheValue, StringValue> cache = builder.buildMemoryDiffCache();
                        Assert.assertEquals(usesEntries ? "AsyncNodeCache" : "NodeCache",
                                cache.getClass().getSimpleName());
                        Assert.assertNotNull(PersistentCache.getPersistentCacheStats(cache));
                        Assert.assertSame(PersistentCache.getPersistentCacheStats(cache),
                                builder.getPersistenceCacheStats().get(CacheType.DIFF.name()));
                        StringValue value = new StringValue("value");
                        cache.put(key(), value);
                        cache.cleanUp();
                        Assert.assertSame(value, cache.asMap().get(key()));
                        Assert.assertEquals(usesEntries ? 100 + CacheEntry.MEMORY_OVERHEAD : 100,
                                cache.getUsedWeight());
                    } finally {
                        builder.getPersistentCache().close();
                    }
                }
            }
        }
    }

    /** Caches without persistence need no entry metadata. */
    @Test
    public void asyncWithoutPersistenceUsesRawValuesWithoutEntryWeight() throws Exception {
        DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(true);
        for (boolean configuredPersistence : new boolean[] {false, true}) {
            DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                    .memoryCacheSize(100_000).setCacheMaintenanceMode(CacheType.DIFF, CacheBuilder.MaintenanceMode.ASYNC)
                    .setPersistentCache(configuredPersistence ? folder.newFolder().getAbsolutePath() + ",-diff" : null)
                    .withWeigher((key, value) -> {
                        Assert.assertTrue(value instanceof StringValue);
                        return 100;
                    });
            try {
                Cache<CacheValue, StringValue> cache = builder.buildMemoryDiffCache();
                Assert.assertEquals("CaffeineCacheAdapter", cache.getClass().getSimpleName());
                Assert.assertNull(PersistentCache.getPersistentCacheStats(cache));
                StringValue value = new StringValue("value");
                cache.put(key(), value);
                cache.cleanUp();
                Assert.assertSame(value, cache.asMap().get(key()));
                Assert.assertEquals(100, cache.getUsedWeight());
            } finally {
                if (builder.getPersistentCache() != null) {
                    builder.getPersistentCache().close();
                }
            }
        }
    }

    private static final class RecordingBuilder extends DocumentNodeStoreBuilder<RecordingBuilder> {
        private final PersistentCache persistence;

        private RecordingBuilder(PersistentCache persistence) {
            this.persistence = persistence;
        }

        @Override
        public PersistentCache getPersistentCache() { return persistence; }
    }

    private static MemoryDiffCache.Key key() {
        return new MemoryDiffCache.Key(Path.fromString("/key"), new RevisionVector(new Revision(1, 0, 1)),
                new RevisionVector(new Revision(2, 0, 1)));
    }

    private static final class RecordingCallback {
        private final CountDownLatch called = new CountDownLatch(1);
        private final AtomicReference<Thread> thread = new AtomicReference<>();

        private void record() {
            thread.set(Thread.currentThread());
            called.countDown();
        }
    }
}
