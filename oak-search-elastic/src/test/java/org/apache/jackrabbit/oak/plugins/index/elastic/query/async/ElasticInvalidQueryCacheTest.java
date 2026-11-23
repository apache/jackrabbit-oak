/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic.query.async;

import java.time.LocalDate;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.plugins.index.elastic.internal.ElasticFeatureToggles;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ElasticInvalidQueryCacheTest {

    @After
    public void reset() {
        ElasticFeatureToggles.FT_OAK_12403_ENABLE.set(false);
        ElasticInvalidQueryCache.clear();
    }

    @Test
    public void disabledByDefault() {
        assertFalse(ElasticFeatureToggles.FT_OAK_12403_ENABLE.get());
        ElasticFeatureToggles.FT_OAK_12403_ENABLE.set(true);
        ElasticInvalidQueryCache.markInvalid("some-query");
        ElasticFeatureToggles.FT_OAK_12403_ENABLE.set(false);

        // toggle is off: even a previously marked entry must not be reported as invalid
        assertFalse(ElasticInvalidQueryCache.isKnownInvalid("some-query"));
    }

    @Test
    public void marksAndReportsKnownInvalidQuery() {
        ElasticFeatureToggles.FT_OAK_12403_ENABLE.set(true);

        assertFalse(ElasticInvalidQueryCache.isKnownInvalid("my-index::query-dsl"));

        ElasticInvalidQueryCache.markInvalid("my-index::query-dsl");

        assertTrue(ElasticInvalidQueryCache.isKnownInvalid("my-index::query-dsl"));
        // a different key must not be affected
        assertFalse(ElasticInvalidQueryCache.isKnownInvalid("my-index::other-query-dsl"));
    }

    @Test
    public void nullKeyIsNeverKnownInvalid() {
        ElasticFeatureToggles.FT_OAK_12403_ENABLE.set(true);
        ElasticInvalidQueryCache.markInvalid(null);
        assertFalse(ElasticInvalidQueryCache.isKnownInvalid(null));
    }

    @Test
    public void disabledToggleDoesNotRecordQueries() {
        ElasticInvalidQueryCache.markInvalid("query");
        ElasticFeatureToggles.FT_OAK_12403_ENABLE.set(true);
        assertFalse(ElasticInvalidQueryCache.isKnownInvalid("query"));
    }

    @Test
    public void clearRemovesQueries() {
        ElasticFeatureToggles.FT_OAK_12403_ENABLE.set(true);
        ElasticInvalidQueryCache.markInvalid("query");
        ElasticInvalidQueryCache.clear();
        assertFalse(ElasticInvalidQueryCache.isKnownInvalid("query"));
    }

    @Test
    public void entriesExpireFiveMinutesAfterWriteDespiteReads() {
        AtomicLong nanos = new AtomicLong();
        Cache<String, Boolean> cache = ElasticInvalidQueryCache.setupCache(nanos::get);
        cache.put("query", Boolean.TRUE);

        nanos.set(TimeUnit.MINUTES.toNanos(5) - 1);
        assertEquals(Boolean.TRUE, cache.getIfPresent("query"));
        nanos.incrementAndGet();
        assertNull(cache.getIfPresent("query"));
    }

    @Test
    public void recordingAgainRenewsExpiry() {
        AtomicLong nanos = new AtomicLong();
        Cache<String, Boolean> cache = ElasticInvalidQueryCache.setupCache(nanos::get);
        cache.put("query", Boolean.TRUE);

        nanos.set(TimeUnit.MINUTES.toNanos(4));
        cache.put("query", Boolean.TRUE);
        nanos.set(TimeUnit.MINUTES.toNanos(5));
        assertEquals(Boolean.TRUE, cache.getIfPresent("query"));
        nanos.set(TimeUnit.MINUTES.toNanos(9));
        assertNull(cache.getIfPresent("query"));
    }

    @Test
    public void cacheIsBoundedToOneThousandEntries() {
        Cache<String, Boolean> cache = ElasticInvalidQueryCache.setupCache(() -> 0L);
        for (int i = 0; i < 1100; i++) {
            cache.put("query-" + i, Boolean.TRUE);
        }
        cache.cleanUp();
        assertEquals(1000, cache.asMap().size());
    }

    @Test
    public void ft_oak_12403_toggleShouldBeRemoved() {
        assertTrue("Feature toggle " + ElasticFeatureToggles.FT_OAK_12403 + " is overdue for removal",
                LocalDate.now().isBefore(LocalDate.of(2027, 10, 13)));
    }
}
