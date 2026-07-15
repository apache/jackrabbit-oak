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
package org.apache.jackrabbit.oak.security.authorization.permission;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.junit.Test;

import static org.apache.jackrabbit.oak.security.authorization.permission.CacheStrategyImpl.EAGER_CACHE_MAXPATHS_PARAM;
import static org.apache.jackrabbit.oak.security.authorization.permission.CacheStrategyImpl.EAGER_CACHE_SIZE_PARAM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CacheStrategyImplTest {

    @Test
    public void testMaxSize() {
        long maxSize = 30L;
        assertEquals(maxSize, new CacheStrategyImpl(ConfigurationParameters.of(EAGER_CACHE_SIZE_PARAM, maxSize), false).maxSize());
        assertEquals(maxSize, new CacheStrategyImpl(ConfigurationParameters.of(EAGER_CACHE_SIZE_PARAM, maxSize), true).maxSize());
    }

    @Test
    public void testDefaultMaxSize() {
        long defaultMaxSize = 250L;
        assertEquals(defaultMaxSize, new CacheStrategyImpl(ConfigurationParameters.EMPTY, false).maxSize());
        assertEquals(defaultMaxSize, new CacheStrategyImpl(ConfigurationParameters.EMPTY, true).maxSize());
    }

    @Test
    public void testThreshold() {
        long maxPaths = 5;
        CacheStrategy cs = new CacheStrategyImpl(ConfigurationParameters.of(EAGER_CACHE_MAXPATHS_PARAM, maxPaths), false);

        long[] cnts = new long[] {Long.MIN_VALUE, 0L, 1L, 10L, 1000L, Long.MAX_VALUE-1};
        for (long cnt : cnts) {
            assertTrue(cs.loadFully(maxPaths-1, cnt));
            assertTrue(cs.loadFully(maxPaths, cnt));
            assertFalse(cs.loadFully(maxPaths+1, cnt));
        }
        assertFalse(cs.loadFully(maxPaths+1, Long.MAX_VALUE));

    }

    @Test
    public void testThresholdIsRefresh() {
        long maxSize = 10;
        long maxPaths = 5;
        CacheStrategy cs = new CacheStrategyImpl(ConfigurationParameters.of(
                EAGER_CACHE_SIZE_PARAM, maxSize,
                EAGER_CACHE_MAXPATHS_PARAM, maxPaths), true);

        for (long cnt : new long[] {Long.MIN_VALUE, 0L, 1L, 10L-1}) {
            assertTrue(cs.loadFully(maxPaths-1, cnt));
            assertTrue(cs.loadFully(maxPaths, cnt));
            assertFalse(cs.loadFully(maxPaths+1, cnt));
        }

        for (long cnt : new long[] {10L, 1000L, Long.MAX_VALUE}) {
            assertFalse(cs.loadFully(maxPaths-1, cnt));
            assertFalse(cs.loadFully(maxPaths, cnt));
            assertFalse(cs.loadFully(maxPaths+1, cnt));
        }
    }

    @Test
    public void testUseEntryMapCacheZeroCnt() {
        long maxSize = 100;
        long maxPaths = 1;
        CacheStrategy cs = new CacheStrategyImpl(ConfigurationParameters.of(
                EAGER_CACHE_SIZE_PARAM, maxSize,
                EAGER_CACHE_MAXPATHS_PARAM, maxPaths), true);

        long cnt = 0;
        assertFalse(cs.usePathEntryMap(cnt));
        assertTrue(cs.loadFully(1, cnt));
        assertFalse(cs.usePathEntryMap(cnt));
    }

    @Test
    public void testUseEntryMapCacheIsRefresh() {
        long maxSize = 100;
        long maxPaths = 1;
        CacheStrategy cs = new CacheStrategyImpl(ConfigurationParameters.of(
                EAGER_CACHE_SIZE_PARAM, maxSize,
                EAGER_CACHE_MAXPATHS_PARAM, maxPaths), true);

        long numentrysize = maxPaths;
        long cnt = 0;
        assertTrue(cs.loadFully(numentrysize, cnt));
        assertTrue(cs.usePathEntryMap(++cnt));

        cnt=10;
        assertTrue(cs.loadFully(numentrysize, cnt));
        assertTrue(cs.usePathEntryMap(++cnt));
        assertTrue(cs.usePathEntryMap(maxSize));

        numentrysize += 5;
        assertFalse(cs.loadFully(numentrysize, cnt));
        assertTrue(cs.usePathEntryMap(numentrysize+cnt));
        assertFalse(cs.usePathEntryMap(maxSize));
    }

    @Test
    public void testUseEntryMapCache() {
        long maxSize = 100;
        long maxPaths = 1;
        CacheStrategy cs = new CacheStrategyImpl(ConfigurationParameters.of(
                EAGER_CACHE_SIZE_PARAM, maxSize,
                EAGER_CACHE_MAXPATHS_PARAM, maxPaths), false);

        assertTrue(cs.loadFully(maxPaths, maxSize));
        assertTrue(cs.usePathEntryMap(maxSize));

        assertFalse(cs.loadFully(maxPaths+1, maxSize));
        assertTrue(cs.usePathEntryMap(maxSize-1));
        assertFalse(cs.usePathEntryMap(maxSize));
    }
}