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
        // A zero-weight cache retains next to nothing in the LIRS tier, and the fast cache is
        // only populated once a slow-cache hit proves reuse (GRANITE-69536). With almost no
        // retention there are almost no such hits, so nearly every lookup reloads - i.e. "0"
        // effectively means no caching (unlike before, when the fast cache was populated eagerly).
        StringCache c = new StringCache(0);
        for (int repeat = 0; repeat < 10; repeat++) {
            for (int i = 0; i < 1000; i++) {
                assertEquals(valueOf(i), c.get(i, i, i, loader));
            }
        }
        assertTrue(valueOf(counter), counter.get() > 9000);
    }

    @Test
    public void largeEntries() {
        final AtomicInteger counter = new AtomicInteger();
        final String large = new String(new char[1024]);
        IntFunction<String> loader = input -> {
                counter.incrementAndGet();
                return large + input;
        };
        StringCache c = new StringCache(1024);
        for (int repeat = 0; repeat < 10; repeat++) {
            for (int i = 0; i < 1000; i++) {
                assertEquals(large + i, c.get(i, i, i, loader));
                assertEquals(large + 0, c.get(0, 0, 0, loader));
            }
        }
        // the LIRS cache should be almost empty (low hit rate there)
        // and large strings are not kept in the fast cache, so hit rate should be bad
        assertTrue(valueOf(counter), counter.get() > 9000);
        assertTrue(valueOf(counter), counter.get() < 10000);
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
