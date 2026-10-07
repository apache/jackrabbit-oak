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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.EvictionListener;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

/** Tests per-cache construction policies in {@link DocumentNodeStoreBuilder}. */
public class DocumentCacheMaintenanceTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    /** Document diff callbacks run inline by default. */
    @Test
    public void documentDiffCacheUsesSynchronousMaintenance() throws Exception {
        checkCallbackThread();
    }

    private void checkCallbackThread() throws Exception {
        RecordingCallback callbackThread = new RecordingCallback();
        PersistentCache persistence = Mockito.spy(new PersistentCache(folder.newFolder().getAbsolutePath() + ",+asyncDiff"));
        Mockito.doAnswer(invocation -> {
            Cache<?, ?> real = (Cache<?, ?>) invocation.callRealMethod();
            return Mockito.mock(Cache.class, Mockito.withSettings().extraInterfaces(EvictionListener.class)
                    .defaultAnswer(callback -> {
                        if (callback.getMethod().getName().equals("evicted")) {
                            callbackThread.record();
                        }
                        return AdditionalAnswers.delegatesTo(real).answer(callback);
                    }));
        }).when(persistence).wrap(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        DocumentNodeStoreBuilder<?> builder = new RecordingBuilder(persistence)
                .memoryCacheSize(1000).withWeigher((key, value) -> 1000);
        boolean previous = DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.getAndSet(true);
        try {
            Cache<CacheValue, StringValue> cache = builder.buildMemoryDiffCache();
            Thread caller = Thread.currentThread();
            cache.get(key(), k -> new StringValue("value"));
            Assert.assertSame(caller, callbackThread.thread.get());
        } finally {
            builder.getPersistentCache().close();
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(previous);
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
    public void customWeigherReceivesTheRawValueWithoutEntryOverhead() throws Exception {
        DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .memoryCacheSize(100_000)
                .setPersistentCache(folder.newFolder().getAbsolutePath() + ",+asyncDiff")
                .withWeigher((key, value) -> {
                    Assert.assertTrue(value instanceof StringValue);
                    return 100;
                });
        boolean previous = DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.getAndSet(true);
        try {
            Cache<CacheValue, StringValue> cache = builder.buildMemoryDiffCache();
            cache.put(key(), new StringValue("value"));
            cache.cleanUp();
            Assert.assertEquals(100, cache.getUsedWeight());
        } finally {
            builder.getPersistentCache().close();
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(previous);
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
        private final AtomicReference<Thread> thread = new AtomicReference<>();

        private void record() {
            thread.set(Thread.currentThread());
        }
    }
}
