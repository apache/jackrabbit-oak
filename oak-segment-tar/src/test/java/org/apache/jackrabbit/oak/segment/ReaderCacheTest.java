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

import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import org.junit.Test;

public class ReaderCacheTest {

    @Test
    public void empty() {
        final AtomicInteger counter = new AtomicInteger();
        IntFunction<String> loader = input -> {
                counter.incrementAndGet();
                return valueOf(input);
        };
        // A zero-weight cache disables the slow (LIRS) tier and serves everything from the
        // lock-free fast cache, populated on load. The 1000 small values fit the fast array, so
        // after the first pass lookups hit and only a handful of hash collisions reload.
        StringCache c = new StringCache(0);
        for (int repeat = 0; repeat < 10; repeat++) {
            for (int i = 0; i < 1000; i++) {
                assertEquals(valueOf(i), c.get(i, i, i, loader));
            }
        }
        // Each distinct value is loaded at least once; the fast cache keeps the total far below
        // the 10000 lookups a cache-less run would incur.
        assertTrue(valueOf(counter), counter.get() >= 1000);
        assertTrue(valueOf(counter), counter.get() < 5000);
    }

    @Test
    public void fastOnlyLargeValueReDecoded() {
        // With the slow cache disabled (weight 0), a value too large for the fast cache
        // (> MAX_STRING_SIZE) is not cached anywhere and is re-decoded on every access.
        final AtomicInteger counter = new AtomicInteger();
        final String large = new String(new char[1024]);
        IntFunction<String> loader = input -> {
            counter.incrementAndGet();
            return large + input;
        };
        StringCache c = new StringCache(0);
        for (int i = 0; i < 5; i++) {
            assertEquals(large + 7, c.get(7, 7, 7, loader));
        }
        assertEquals(5, counter.get());
    }

    @Test
    public void largeEntryServedFromSlowCache() {
        final AtomicInteger counter = new AtomicInteger();
        final String large = new String(new char[1024]);
        IntFunction<String> loader = input -> {
                counter.incrementAndGet();
                return large + input;
        };
        // A large value (> MAX_STRING_SIZE) bypasses the fast cache, but with an ample weight budget
        // it is retained by the slow cache. The second read is therefore a slow-cache hit rather than
        // a reload. (Contrast with fastOnlyLargeValueReDecoded(), where the slow cache is disabled and
        // the same large value is re-decoded on every access.)
        StringCache c = new StringCache(1024 * 1024);
        assertEquals(large + 7, c.get(7, 7, 7, loader)); // miss -> load, stored in the slow cache
        assertEquals(large + 7, c.get(7, 7, 7, loader)); // slow-cache hit -> no reload
        assertEquals(1, counter.get());
    }

    @Test
    public void clear() {
        final AtomicInteger counter = new AtomicInteger();
        IntFunction<String> uniqueLoader = input -> valueOf(counter.incrementAndGet());
        // Use a weight that actually retains, so a repeat read hits the slow cache and promotes
        // the entry to the fast cache; clear() must then empty both tiers.
        StringCache c = new StringCache(1024 * 1024);
        // load a new entry
        assertEquals("1", c.get(0, 0, 0, uniqueLoader));
        // a repeat read hits the (retained) slow cache and returns the same value
        assertEquals("1", c.get(0, 0, 0, uniqueLoader));
        c.clear();
        // after clearing the cache, load a new entry
        assertEquals("2", c.get(0, 0, 0, uniqueLoader));
        assertEquals("2", c.get(0, 0, 0, uniqueLoader));
    }

    @Test
    public void randomized() {
        ArrayList<IntFunction<String>> loaderList = new ArrayList<>();
        int segmentCount = 10;
        for (int i = 0; i < segmentCount; i++) {
            final int x = i;
            IntFunction<String> loader = input -> "loader #" + x + " offset " + input;
            loaderList.add(loader);
        }
        StringCache c = new StringCache(10);
        Random r = new Random(1);
        for (int i = 0; i < 1000; i++) {
            int segment = r.nextInt(segmentCount);
            int offset = r.nextInt(10);
            IntFunction<String> loader = loaderList.get(segment);
            String x = c.get(segment, segment, offset, loader);
            assertEquals(loader.apply(offset), x);
        }
    }

}
