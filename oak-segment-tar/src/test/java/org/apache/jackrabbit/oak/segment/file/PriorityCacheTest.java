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

package org.apache.jackrabbit.oak.segment.file;

import static java.lang.Integer.valueOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Random;

import com.google.common.base.Predicate;
import com.google.common.cache.Weigher;
import org.apache.jackrabbit.oak.segment.CacheWeights;
import org.junit.Test;

public class PriorityCacheTest {

    @Test(expected = IllegalArgumentException.class)
    public void illegalSize() {
        new PriorityCache<String, String>(42, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void zeroSize() {
        new PriorityCache<String, String>(0, 0);
    }

    @Test
    public void legalHash() {
        for (int k = 0; k < 8; k++) {
            new PriorityCache<String, String>(0x1000000, k);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalHash() {
        new PriorityCache<String, String>(0x1000000, 9);
    }

    @Test
    public void singletonCache() {
        PriorityCache<String, Integer> cache = new PriorityCache<String, Integer>(1, 0);
        assertTrue(cache.put("one", 1, 0, (byte) 0));

        // Cache is full -> cannot put another key of the same cost
        assertFalse(cache.put("two", 2, 0, (byte) 0));

        // Retrieving "one" leads to a cache hit increasing this key's cost to 1
        assertEquals(valueOf(1), cache.get("one", 0));
        assertNull(cache.get("one", 1));
        assertNull(cache.get("two", 0));

        // Inserting "two" only succeeds for cost 2, which is bigger than "one"'s cost of 1
        assertFalse(cache.put("two", 2, 0, (byte) 1));
        assertTrue(cache.put("two", 2, 0, (byte) 2));
        assertEquals(valueOf(2), cache.get("two", 0));
        assertNull(cache.get("two", 1));
        assertNull(cache.get("one", 0));
    }

    @Test
    public void readWrite() {
        PriorityCache<String, Integer> cache = new PriorityCache<String, Integer>(128, 0);
        for (int k = 0; k < 128; k++) {
            if (cache.put("key-" + k, k, 0, (byte) 0)) {
                assertEquals(Integer.valueOf(k), cache.get("key-" + k, 0));
                assertNull(cache.get("key-" + k, 1));
            } else {
                assertNull(cache.get("key-" + k, 0));
            }
        }

        for (int k = 0; k < 128; k++) {
            Integer value = cache.get("key-" + k, 0);
            if (value != null) {
                assertEquals(Integer.valueOf(k), value);
            }
        }
    }

    @Test
    public void updateKey() {
        PriorityCache<String, Integer> cache = new PriorityCache<String, Integer>(1, 0);

        assertTrue(cache.put("one", 1, 0, (byte) 0));

        // Cache is full -> cannot put another key of the same cost
        assertFalse(cache.put("two", 2, 0, (byte) 0));

        // But updating an existing key works and boosts its cost to 1
        assertTrue(cache.put("one", 1, 0, (byte) 0));

        // such that adding another key only works at cost 2 and greater
        assertFalse(cache.put("two", 2, 0, (byte) 1));
        assertTrue(cache.put("two", 2, 0, (byte) 2));
    }

    @Test
    public void updateWithNewGeneration() {
        PriorityCache<String, Integer> cache = new PriorityCache<String, Integer>(1, 0);
        assertTrue(cache.put("one", 1, 0, (byte) 0));

        // Cache is full but we can still put a key of a higher generation
        assertTrue(cache.put("two", 2, 1, (byte) 0));
        assertNull(cache.get("one", 0));

        // Cannot put a key of a lower generation
        assertFalse(cache.put("two", 2, 0, (byte) 0));

        // But one of the same generation
        assertTrue(cache.put("two", 2, 1, (byte) 0));
    }

    @Test
    public void generationPurge() {
        PriorityCache<String, Integer> cache = new PriorityCache<String, Integer>(65536);

        for (int gen = 4; gen >= 0; gen--) {
            // Backward iteration avoids earlier generations are replaced with later ones
            for (int k = 0; k < 100; k++) {
                if (!cache.put("key-" + gen + "-" + k, 0, gen, (byte) 0)) {
                    assumeTrue("All test keys are in the cache", false);
                }
            }
        }

        assertEquals(500, cache.size());
        cache.purgeGenerations(new Predicate<Integer>() {
            @Override
            public boolean apply(Integer generation) {
                return generation <= 2;
            }
        });
        assertEquals(200, cache.size());
    }

    @Test
    public void nextPowerOfTwo() {
        assertEquals(1, PriorityCache.nextPowerOfTwo(-1));
        assertEquals(1, PriorityCache.nextPowerOfTwo(-123));
        assertEquals(1, PriorityCache.nextPowerOfTwo(0));
        assertEquals(1, PriorityCache.nextPowerOfTwo(1));
        assertEquals(2, PriorityCache.nextPowerOfTwo(2));
        assertEquals(4, PriorityCache.nextPowerOfTwo(3));
        assertEquals(4, PriorityCache.nextPowerOfTwo(4));
        assertEquals(512, PriorityCache.nextPowerOfTwo(500));
        assertEquals(2147483648L, PriorityCache.nextPowerOfTwo(Integer.MAX_VALUE));
    }

    @Test
    public void evictionCount() {
        Random rnd = new Random();
        Weigher<String, Integer> weigher = CacheWeights.noopWeigher();
        PriorityCache<String, Integer> cache = new PriorityCache<>(128, 2, weigher);
        int count = 0;
        for (int b = Byte.MIN_VALUE; b <= Byte.MAX_VALUE; b++) {
            if (cache.put("k-" + b + "-" + rnd.nextInt(1000), b, 0, (byte) b)) {
                count++;
            }
        }

        assertEquals(count, cache.size() + cache.getStats().evictionCount());
    }

    @Test
    public void loadExceptionCount() {
        Random rnd = new Random();
        PriorityCache<String, Integer> cache = new PriorityCache<>(16);
        int success = 0;
        int failure = 0;
        for (int i = 0; i < 1000; i++) {
            if (cache.put("k-" + i + "-" + rnd.nextInt(1000), i, 0, (byte) 0)) {
                success++;
            } else {
                failure++;
            }
        }

        assertEquals(0, cache.getStats().evictionCount());
        assertEquals(success, cache.size());
        assertEquals(failure, cache.getStats().loadExceptionCount());
    }

}
