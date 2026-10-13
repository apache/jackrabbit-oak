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

package org.apache.jackrabbit.oak.segment;

import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.segment.WriterCacheManager.Default;
import org.apache.jackrabbit.oak.segment.WriterCacheManager.Empty;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.junit.Test;

public class WriteCacheManagerTest {

    @Test
    public void emptyGenerations() {
        WriterCacheManager cache = Empty.INSTANCE;
        assertEquals(
                cache.getTemplateCache(0),
                cache.getTemplateCache(1));
        assertEquals(
                cache.getStringCache(0),
                cache.getStringCache(1));
    }

    @Test
    public void nonEmptyGenerations() {
        WriterCacheManager cache = new Default();
        assertNotEquals(
                cache.getTemplateCache(0),
                cache.getTemplateCache(1));
        assertNotEquals(
                cache.getStringCache(0),
                cache.getStringCache(1));
    }

    @Test
    public void accessTrackingCacheForwardsToDelegate() {
        WriterCacheManager tracked = new Default().withAccessTracking("test", StatisticsProvider.NOOP);

        tracked.getStringCache(0).put("key", RecordId.NULL);
        assertEquals(RecordId.NULL, tracked.getStringCache(0).get("key"));

        tracked.getNodeCache(0).put("stableId", RecordId.NULL, (byte) 1);
        assertEquals(RecordId.NULL, tracked.getNodeCache(0).get("stableId"));
    }

    @Test
    public void accessTrackingCacheNeverGoesStaleAfterDelegateEviction() {
        Default delegate = new Default();
        WriterCacheManager tracked = delegate.withAccessTracking("test", StatisticsProvider.NOOP);

        tracked.getStringCache(0).put("key", RecordId.NULL);
        tracked.getNodeCache(0).put("stableId", RecordId.NULL, (byte) 1);

        // Simulate a GC monitor evicting a (failed) generation before it is retried
        // with the same generation number, as done by FileStoreBuilder's
        // EvictingWriteCacheManager#evictGeneration.
        delegate.evictCaches(generation -> generation == 0);

        // getStringCache/getTemplateCache/getNodeCache never cache the wrapped delegate
        // cache: every access resolves it afresh, so the entries written before the
        // eviction above must not resurface once the delegate has discarded them (or,
        // for the node cache, purged them in place).
        assertNull(tracked.getStringCache(0).get("key"));
        assertNull(tracked.getNodeCache(0).get("stableId"));
    }

    @Test
    public void accessTrackingCacheSharesStatisticsAcrossCalls() throws IOException {
        ScheduledExecutorService scheduler = newScheduledThreadPool(1);
        try {
            StatisticsProvider statistics = new DefaultStatisticsProvider(scheduler);
            WriterCacheManager tracked = new Default().withAccessTracking("test", statistics);

            // Each getStringCache() call returns a distinct, disposable instance: the
            // access/miss counts must accumulate across them rather than being scoped
            // to a single instance (which is what registering the counters once, in
            // AccessTrackingCacheManager's constructor, achieves).
            tracked.getStringCache(0).get("miss");
            tracked.getStringCache(0).put("key", RecordId.NULL);
            tracked.getStringCache(0).get("key");

            MeterStats accessCount = statistics.getMeter(
                    "oak.segment.string-deduplication-cache-test.access-count", StatsOptions.DEFAULT);
            MeterStats missCount = statistics.getMeter(
                    "oak.segment.string-deduplication-cache-test.miss-count", StatsOptions.DEFAULT);

            assertEquals(2, accessCount.getCount());
            assertEquals(1, missCount.getCount());
        } finally {
            new ExecutorCloser(scheduler).close();
        }
    }

}
