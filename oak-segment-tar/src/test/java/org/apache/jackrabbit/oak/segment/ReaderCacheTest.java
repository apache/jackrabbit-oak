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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.Test;

import com.google.common.base.Function;

public class ReaderCacheTest {

    @Test
    public void empty() {
        final AtomicInteger counter = new AtomicInteger();
        Function<Integer, String> loader = new Function<Integer, String>() {
            @Override @Nonnull
            public String apply(@Nullable Integer input) {
                counter.incrementAndGet();
                return valueOf(input);
            }
        };
        StringCache c = new StringCache(0);
        for (int repeat = 0; repeat < 10; repeat++) {
            for (int i = 0; i < 1000; i++) {
                assertEquals(valueOf(i), c.get(i, i, i, loader));
            }
        }
        // the LIRS cache should be almost empty (low hit rate there)
        assertTrue(valueOf(counter), counter.get() > 1000);
        // but the fast cache should improve the total hit rate
        assertTrue(valueOf(counter), counter.get() < 5000);
    }
    
    @Test
    public void largeEntries() {
        final AtomicInteger counter = new AtomicInteger();
        final String large = new String(new char[1024]);
        Function<Integer, String> loader = new Function<Integer, String>() {
            @Override @Nullable
            public String apply(@Nullable Integer input) {
                counter.incrementAndGet();
                return large + input;
            }
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
        Function<Integer, String> uniqueLoader = new Function<Integer, String>() {
            @Override @Nullable
            public String apply(@Nullable Integer input) {
                return valueOf(counter.incrementAndGet());
            }
        };
        StringCache c = new StringCache(0);
        // load a new entry
        assertEquals("1", c.get(0, 0, 0, uniqueLoader));
        // but only once
        assertEquals("1", c.get(0, 0, 0, uniqueLoader));
        c.clear();
        // after clearing the cache, load a new entry
        assertEquals("2", c.get(0, 0, 0, uniqueLoader));
        assertEquals("2", c.get(0, 0, 0, uniqueLoader));
    }
    
    @Test
    public void randomized() {
        ArrayList<Function<Integer, String>> loaderList = new ArrayList<Function<Integer, String>>();
        int segmentCount = 10;
        for (int i = 0; i < segmentCount; i++) {
            final int x = i;
            Function<Integer, String> loader = new Function<Integer, String>() {
                @Override @Nullable
                public String apply(@Nullable Integer input) {
                    return "loader #" + x + " offset " + input;
                }
            };
            loaderList.add(loader);
        }
        StringCache c = new StringCache(10);
        Random r = new Random(1);
        for (int i = 0; i < 1000; i++) {
            int segment = r.nextInt(segmentCount);
            int offset = r.nextInt(10);
            Function<Integer, String> loader = loaderList.get(segment);
            String x = c.get(segment, segment, offset, loader);
            assertEquals(loader.apply(offset), x);
        }
    }
    
}
