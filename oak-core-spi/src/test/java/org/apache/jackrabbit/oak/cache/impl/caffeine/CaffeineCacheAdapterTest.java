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
package org.apache.jackrabbit.oak.cache.impl.caffeine;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.CacheStatsSnapshot;
import org.apache.jackrabbit.oak.cache.api.EvictionCause;
import org.junit.Assert;
import org.junit.Test;

public class CaffeineCacheAdapterTest {

    @Test
    public void toOakCauseMapsEveryCaffeineCause() {
        Assert.assertEquals(EvictionCause.EXPLICIT, CaffeineCacheAdapter.toOakCause(RemovalCause.EXPLICIT));
        Assert.assertEquals(EvictionCause.REPLACED, CaffeineCacheAdapter.toOakCause(RemovalCause.REPLACED));
        Assert.assertEquals(EvictionCause.SIZE, CaffeineCacheAdapter.toOakCause(RemovalCause.SIZE));
        Assert.assertEquals(EvictionCause.EXPIRED, CaffeineCacheAdapter.toOakCause(RemovalCause.EXPIRED));
        Assert.assertEquals(EvictionCause.COLLECTED, CaffeineCacheAdapter.toOakCause(RemovalCause.COLLECTED));
    }

    @Test
    public void statsSnapshotReflectsUnderlyingCacheStats() {
        CaffeineCacheAdapter<String, String> adapter =
                new CaffeineCacheAdapter<>(Caffeine.newBuilder().recordStats().build());

        adapter.put("hit", "value");
        adapter.getIfPresent("hit");
        adapter.getIfPresent("miss");

        CacheStatsSnapshot stats = adapter.stats();
        Assert.assertEquals(1, stats.hitCount());
        Assert.assertEquals(1, stats.missCount());
    }

    /**
     * Verifies that eviction listeners registered via {@link CacheBuilder#evictionListener} fire
     * synchronously during {@link CaffeineCacheAdapter#invalidateAll()}.
     *
     * <p>Oak {@link CacheBuilder} configures {@code executor(Runnable::run)} for caches without
     * {@code refreshAfterWrite}, so removal listeners usually run during {@code invalidateAll()}
     * even without an explicit {@code cleanUp()} in this adapter. The adapter still calls
     * {@code cleanUp()} to guarantee that contract for every backing Caffeine instance and to
     * drain any buffered maintenance work before returning.</p>
     */
    @Test
    public void evictionListenerFiresForAllEntriesDuringInvalidateAll() {
        AtomicInteger listenerCallCount = new AtomicInteger(0);
        org.apache.jackrabbit.oak.cache.api.Cache<String, String> cache =
                CacheBuilder.<String, String>newBuilder()
                        .maximumSize(100)
                        .evictionListener((k, v, cause) -> listenerCallCount.incrementAndGet())
                        .build();

        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");

        cache.invalidateAll();

        Assert.assertEquals("eviction listener must fire for every entry during invalidateAll()",
                3, listenerCallCount.get());
    }

    /**
     * Verifies the same guarantee for {@link CaffeineCacheAdapter#invalidateAll(Iterable)}.
     */
    @Test
    public void evictionListenerFiresForRequestedEntriesDuringInvalidateAllIterable() {
        AtomicInteger listenerCallCount = new AtomicInteger(0);
        org.apache.jackrabbit.oak.cache.api.Cache<String, String> cache =
                CacheBuilder.<String, String>newBuilder()
                        .maximumSize(100)
                        .evictionListener((k, v, cause) -> listenerCallCount.incrementAndGet())
                        .build();

        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");

        cache.invalidateAll(Arrays.asList("a", "c"));

        Assert.assertEquals("eviction listener must fire for each invalidated key",
                2, listenerCallCount.get());
        Assert.assertNotNull("non-invalidated entry must still be present", cache.getIfPresent("b"));
    }

    @Test
    public void invalidateAllIterableRemovesOnlyRequestedKeys() {
        CaffeineCacheAdapter<String, String> adapter =
                new CaffeineCacheAdapter<>(Caffeine.newBuilder().build());

        adapter.put("a", "1");
        adapter.put("b", "2");
        adapter.put("c", "3");

        Assert.assertEquals(2, adapter.getAllPresent(Arrays.asList("a", "b")).size());

        adapter.invalidateAll(Arrays.asList("a", "c"));

        Assert.assertNull(adapter.getIfPresent("a"));
        Assert.assertEquals("2", adapter.getIfPresent("b"));
        Assert.assertNull(adapter.getIfPresent("c"));
    }
}
