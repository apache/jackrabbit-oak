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
package org.apache.jackrabbit.oak.cache.api;

import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link CacheStats}. */
public class CacheStatsTest {

    private CacheStats stats(long hits, long misses, long loadSuccess, long loadFail,
                             long loadTime, long evictions) {
        return new CacheStats(hits, misses, loadSuccess, loadFail, loadTime, evictions);
    }

    /** Verifies that {@code requestCount()} returns the sum of hits and misses. */
    @Test
    public void requestCountIsHitsPlusMisses() {
        CacheStats s = stats(3, 7, 0, 0, 0, 0);
        Assert.assertEquals(10, s.requestCount());
    }

    /** Verifies that {@code hitRate()} returns hits divided by total requests. */
    @Test
    public void hitRateWithRequests() {
        CacheStats s = stats(3, 7, 0, 0, 0, 0);
        Assert.assertEquals(0.3, s.hitRate(), 0.001);
    }

    /** Verifies that {@code hitRate()} returns {@code 1.0} when no requests have been made. */
    @Test
    public void hitRateWithNoRequestsReturnsOne() {
        CacheStats s = stats(0, 0, 0, 0, 0, 0);
        Assert.assertEquals(1.0, s.hitRate(), 0.0);
    }

    /** Verifies that {@code missRate()} returns misses divided by total requests. */
    @Test
    public void missRateWithRequests() {
        CacheStats s = stats(3, 7, 0, 0, 0, 0);
        Assert.assertEquals(0.7, s.missRate(), 0.001);
    }

    /** Verifies that {@code missRate()} returns {@code 0.0} when no requests have been made. */
    @Test
    public void missRateWithNoRequestsReturnsZero() {
        CacheStats s = stats(0, 0, 0, 0, 0, 0);
        Assert.assertEquals(0.0, s.missRate(), 0.0);
    }

    /** Verifies that {@code minus()} produces the correct per-field delta between two snapshots. */
    @Test
    public void minusProducesDelta() {
        CacheStats later  = stats(10, 5, 4, 1, 1000, 3);
        CacheStats earlier = stats(6,  3, 2, 0,  400, 1);
        CacheStats delta  = later.minus(earlier);

        Assert.assertEquals(4,   delta.hitCount());
        Assert.assertEquals(2,   delta.missCount());
        Assert.assertEquals(2,   delta.loadSuccessCount());
        Assert.assertEquals(1,   delta.loadFailureCount());
        Assert.assertEquals(600, delta.totalLoadTime());
        Assert.assertEquals(2,   delta.evictionCount());
    }

    /** Verifies that {@code minus()} clamps negative deltas to zero when the earlier snapshot has larger values. */
    @Test
    public void minusClampsNegativeValuesToZero() {
        CacheStats later   = stats(5, 2, 1, 0, 100, 1);
        CacheStats earlier = stats(9, 3, 2, 1, 200, 2);
        CacheStats delta   = later.minus(earlier);

        Assert.assertEquals(0, delta.hitCount());
        Assert.assertEquals(0, delta.missCount());
        Assert.assertEquals(0, delta.loadSuccessCount());
        Assert.assertEquals(0, delta.loadFailureCount());
        Assert.assertEquals(0, delta.totalLoadTime());
        Assert.assertEquals(0, delta.evictionCount());
    }

    /** Verifies that all record accessors return the values supplied to the canonical constructor. */
    @Test
    public void accessorsReturnConstructorValues() {
        CacheStats s = stats(1, 2, 3, 4, 5, 6);
        Assert.assertEquals(1, s.hitCount());
        Assert.assertEquals(2, s.missCount());
        Assert.assertEquals(3, s.loadSuccessCount());
        Assert.assertEquals(4, s.loadFailureCount());
        Assert.assertEquals(5, s.totalLoadTime());
        Assert.assertEquals(6, s.evictionCount());
    }

    /** Verifies that {@code toString()} includes every field name and its value. */
    @Test
    public void toStringContainsAllFields() {
        CacheStats s = stats(1, 2, 3, 4, 5, 6);
        String str = s.toString();
        Assert.assertTrue(str.contains("hitCount=1"));
        Assert.assertTrue(str.contains("missCount=2"));
        Assert.assertTrue(str.contains("loadSuccessCount=3"));
        Assert.assertTrue(str.contains("loadFailureCount=4"));
        Assert.assertTrue(str.contains("totalLoadTime=5"));
        Assert.assertTrue(str.contains("evictionCount=6"));
    }
}
