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
package org.apache.jackrabbit.oak.cache;

import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.jackrabbit.oak.cache.CacheLIRS.EvictionCallback;
import org.junit.Test;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.Weigher;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Tests the LIRS cache.
 */
public class CacheTest {

    @Test
    public void testRandomSmallCache() {
        Random r = new Random(1);
        for (int i = 0; i < 10000; i++) {
            int j = 0;
            StringBuilder buff = new StringBuilder();
            CacheLIRS<Integer, Integer> test = createCache(1 + r.nextInt(10));
            for (; j < 30; j++) {
                int key = r.nextInt(5);
                switch (r.nextInt(3)) {
                case 0:
                    int memory = r.nextInt(5) + 1;
                    buff.append("add ").append(key).append(' ').append(memory).append('\n');
                    test.put(key, j, memory);
                    break;
                case 1:
                    buff.append("remove ").append(key).append('\n');
                    test.invalidate(key);
                    break;
                case 2:
                    buff.append("get ").append(key).append('\n');
                    test.getIfPresent(key);
                }
            }
        }
    }

    @Test
    public void testEdgeCases() {
        CacheLIRS<Integer, Integer> test = createCache(1);
        test.put(1, 10, 100);
        assertEquals(10, test.getUnchecked(1).intValue());
        try {
            test.put(null,  10, 100);
            fail();
        } catch (NullPointerException e) {
            // expected
        }
        try {
            test.put(1,  null, 100);
            fail();
        } catch (NullPointerException e) {
            // expected
        }
        try {
            test.setMaxMemory(-1);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            test.setAverageMemory(0);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testSize() {
        verifyMapSize(7, 16);
        verifyMapSize(13, 32);
        verifyMapSize(25, 64);
        verifyMapSize(49, 128);
        verifyMapSize(97, 256);
        verifyMapSize(193, 512);
        verifyMapSize(385, 1024);
        verifyMapSize(769, 2048);

        CacheLIRS<Integer, Integer> test;
        test = createCache(3, 10);
        test.put(0, 0, 9);
        test.put(1, 10, 9);
        test.put(2, 20, 9);
        test.put(3, 30, 9);
        test.put(4, 40, 9);

        test = createCache(1, 1);
        test.put(1, 10);
        test.put(0, 0);
        test.getUnchecked(0);

        test = createCache(1000);
        for (int j = 0; j < 2000; j++) {
            test.put(j, j);
        }
        // for a cache of size 1000,
        // there are 62 cold entries (about 6.25%).
        assertEquals(62, test.size() - test.sizeHot());
        // at most as many non-resident elements
        // as there are entries in the stack
        assertEquals(968, test.sizeNonResident());
    }

    private static void verifyMapSize(int elements, int expectedMapSize) {
        CacheLIRS<Integer, Integer> test;
        test = createCache(elements - 1);
        assertTrue(test.sizeMapArray() < expectedMapSize);
        test = createCache(elements);
        assertEquals(expectedMapSize, test.sizeMapArray());
        test = createCache(elements * 100, 100);
        assertEquals(expectedMapSize, test.sizeMapArray());
    }

    @Test
    public void testGetPutPeekRemove() {
        CacheLIRS<Integer, Integer> test = createCache(4);
        test.put(1,  10);
        test.put(2,  20);
        test.put(3,  30);
        assertNull(test.peek(4));
        assertNull(test.getIfPresent(4));
        test.put(4,  40);
        verify(test, "mem: 4 stack: 4 3 2 1 cold: non-resident:");
        // move middle to front
        assertEquals(30, test.getUnchecked(3).intValue());
        assertEquals(20, test.getUnchecked(2).intValue());
        assertEquals(20, test.peek(2).intValue());
        // already on (an optimization)
        assertEquals(20, test.getUnchecked(2).intValue());
        assertEquals(10, test.peek(1).intValue());
        assertEquals(10, test.getUnchecked(1).intValue());
        verify(test, "mem: 4 stack: 1 2 3 4 cold: non-resident:");
        test.put(3,  30);
        verify(test, "mem: 4 stack: 3 1 2 4 cold: non-resident:");
        // 5 is cold; will make 4 non-resident
        test.put(5,  50);
        verify(test, "mem: 4 stack: 5 3 1 2 cold: 5 non-resident: 4");
        assertEquals(1, test.getMemory(1));
        assertEquals(1, test.getMemory(5));
        assertEquals(0, test.getMemory(4));
        assertEquals(0, test.getMemory(100));
        assertNull(test.peek(4));
        assertNull(test.getIfPresent(4));
        assertEquals(10, test.getUnchecked(1).intValue());
        assertEquals(20, test.getUnchecked(2).intValue());
        assertEquals(30, test.getUnchecked(3).intValue());
        verify(test, "mem: 4 stack: 3 2 1 cold: 5 non-resident: 4");
        assertEquals(50, test.getUnchecked(5).intValue());
        verify(test, "mem: 4 stack: 5 3 2 1 cold: 5 non-resident: 4");
        assertEquals(50, test.getUnchecked(5).intValue());
        verify(test, "mem: 4 stack: 5 3 2 cold: 1 non-resident: 4");

        // remove
        assertEquals(50, test.remove(5).intValue());
        assertNull(test.remove(5));
        verify(test, "mem: 3 stack: 3 2 1 cold: non-resident: 4");
        assertNull(test.remove(4));
        verify(test, "mem: 3 stack: 3 2 1 cold: non-resident:");
        assertNull(test.remove(4));
        verify(test, "mem: 3 stack: 3 2 1 cold: non-resident:");
        test.put(4,  40);
        test.put(5,  50);
        verify(test, "mem: 4 stack: 5 4 3 2 cold: 5 non-resident: 1");
        test.getUnchecked(5);
        test.getUnchecked(2);
        test.getUnchecked(3);
        test.getUnchecked(4);
        verify(test, "mem: 4 stack: 4 3 2 5 cold: 2 non-resident: 1");
        assertEquals(50, test.remove(5).intValue());
        verify(test, "mem: 3 stack: 4 3 2 cold: non-resident: 1");
        assertEquals(20, test.remove(2).intValue());
        assertFalse(test.containsKey(1));
        assertNull(test.remove(1));
        assertFalse(test.containsKey(1));
        verify(test, "mem: 2 stack: 4 3 cold: non-resident:");
        test.put(1,  10);
        test.put(2,  20);
        verify(test, "mem: 4 stack: 2 1 4 3 cold: non-resident:");
        test.getUnchecked(1);
        test.getUnchecked(3);
        test.getUnchecked(4);
        verify(test, "mem: 4 stack: 4 3 1 2 cold: non-resident:");
        assertEquals(10, test.remove(1).intValue());
        verify(test, "mem: 3 stack: 4 3 2 cold: non-resident:");
        test.remove(2);
        test.remove(3);
        test.remove(4);

        // test clear
        test.clear();
        verify(test, "mem: 0 stack: cold: non-resident:");

        // strange situation where there is only a non-resident entry
        test.put(1, 10);
        test.put(2, 20);
        test.put(3, 30);
        test.put(4, 40);
        test.put(5, 50);
        assertTrue(test.containsValue(50));
        verify(test, "mem: 4 stack: 5 4 3 2 cold: 5 non-resident: 1");
        test.put(1, 10);
        verify(test, "mem: 4 stack: 1 5 4 3 cold: 2 non-resident: 5");
        assertFalse(test.containsValue(50));
        test.remove(2);
        test.remove(3);
        test.remove(4);
        verify(test, "mem: 1 stack: 1 cold: non-resident: 5");
        assertTrue(test.containsKey(1));
        test.remove(1);
        assertFalse(test.containsKey(1));
        verify(test, "mem: 0 stack: cold: non-resident: 5");
        assertFalse(test.containsKey(5));
        assertTrue(test.isEmpty());

        // verify that converting a hot to cold entry will prune the stack
        test.clear();
        test.put(1, 10);
        test.put(2, 20);
        test.put(3, 30);
        test.put(4, 40);
        test.put(5, 50);
        test.getUnchecked(4);
        test.getUnchecked(3);
        verify(test, "mem: 4 stack: 3 4 5 2 cold: 5 non-resident: 1");
        test.put(6, 60);
        verify(test, "mem: 4 stack: 6 3 4 5 2 cold: 6 non-resident: 5 1");
        // this will prune the stack (remove entry 5 as entry 2 becomes cold)
        test.getUnchecked(6);
        verify(test, "mem: 4 stack: 6 3 4 cold: 2 non-resident: 5 1");
    }

    @Test
    public void testPruneStack() {
        CacheLIRS<Integer, Integer> test = createCache(5);
        for (int i = 0; i < 7; i++) {
            test.put(i, i * 10);
        }
        verify(test, "mem: 5 stack: 6 5 4 3 2 1 cold: 6 non-resident: 5 0");
        test.getUnchecked(4);
        test.getUnchecked(3);
        test.getUnchecked(2);
        verify(test, "mem: 5 stack: 2 3 4 6 5 1 cold: 6 non-resident: 5 0");
        // this call needs to prune the stack
        test.remove(1);
        verify(test, "mem: 4 stack: 2 3 4 6 cold: non-resident: 5 0");
        test.put(0,  0);
        test.put(1,  10);
        // the the stack was not pruned, the following will fail
        verify(test, "mem: 5 stack: 1 0 2 3 4 cold: 1 non-resident: 6 5");
    }

    @Test
    public void testClear() {
        CacheLIRS<Integer, Integer> test = createCache(40, 10);
        for (int i = 0; i < 5; i++) {
            test.put(i, 10 * i, 9);
        }
        verify(test, "mem: 36 stack: 4 3 2 1 cold: 4 non-resident: 0");
        for (Entry<Integer, Integer> e : test.entrySet()) {
            assertTrue(e.getKey() >= 1 && e.getKey() <= 4);
            assertTrue(e.getValue() >= 10 && e.getValue() <= 40);
        }
        for (int x : test.asMap().values()) {
            assertTrue(x >= 10 && x <= 40);
        }
        for (int x : test.keySet()) {
            assertTrue(x >= 1 && x <= 4);
        }
        assertEquals(40,  test.getMaxMemory());
        assertEquals(10, test.getAverageMemory());
        assertEquals(36,  test.getUsedMemory());
        assertEquals(4, test.size());
        assertEquals(3,  test.sizeHot());
        assertEquals(1,  test.sizeNonResident());
        assertFalse(test.isEmpty());

        // changing the limit is not supposed to modify the map
        test.setMaxMemory(10);
        assertEquals(10, test.getMaxMemory());
        test.setMaxMemory(40);
        test.setAverageMemory(1);
        assertEquals(1, test.getAverageMemory());
        test.setAverageMemory(10);
        verify(test, "mem: 36 stack: 4 3 2 1 cold: 4 non-resident: 0");

        // putAll uses the average memory
        test.asMap().putAll(test.asMap());
        verify(test, "mem: 40 stack: 4 3 2 1 cold: non-resident: 0");

        test.clear();
        verify(test, "mem: 0 stack: cold: non-resident:");

        assertEquals(40,  test.getMaxMemory());
        assertEquals(10, test.getAverageMemory());
        assertEquals(0,  test.getUsedMemory());
        assertEquals(0, test.size());
        assertEquals(0,  test.sizeHot());
        assertEquals(0,  test.sizeNonResident());
        assertTrue(test.isEmpty());
    }

    @Test
    public void testLimitHot() {
        CacheLIRS<Integer, Integer> test = createCache(100);
        for (int i = 0; i < 300; i++) {
            test.put(i, 10 * i);
        }
        assertEquals(100, test.size());
        assertEquals(99, test.sizeNonResident());
        assertEquals(93, test.sizeHot());
    }
    
    @Test
    public void testNonResidentBecomeHot() throws ExecutionException {
        CacheLIRS<Integer, Integer> test = createCache(4);
        for (int i = 0; i < 20; i++) {
            test.put(i, 1);
        }
        verify(test, "mem: 4 stack: 19 18 17 16 3 2 1 cold: 19 non-resident: 18 17 16");
        // 18 is a non-resident entry, so should become hot
        test.put(18, 1);
        verify(test, "mem: 4 stack: 18 19 17 16 3 2 cold: 1 non-resident: 19 17 16");
        // 28 was never seen before, so should become cold
        test.put(28, 1);
        verify(test, "mem: 4 stack: 28 18 19 3 2 cold: 28 non-resident: 1 19");
    }

    @Test
    public void testLimitNonResident() {
        CacheLIRS<Integer, Integer> test = createCache(4);
        for (int i = 0; i < 20; i++) {
            test.put(i, 10 * i);
        }
        verify(test, "mem: 4 stack: 19 18 17 16 3 2 1 cold: 19 non-resident: 18 17 16");
    }

    @Test
    public void testBadHashMethod() {
        // ensure an 2^n cache size
        final int size = 4;

        /**
         * A class with a bad hashCode implementation.
         */
        class BadHash {
            int x;

            BadHash(int x) {
                this.x = x;
            }

            @Override
            public int hashCode() {
                return (x & 1) * size * 2;
            }

            @Override
            public boolean equals(Object o) {
                return ((BadHash) o).x == x;
            }

            @Override
            public String toString() {
                return "" + x;
            }

        }

        CacheLIRS<BadHash, Integer> test = createCache(size * 2);
        for (int i = 0; i < size; i++) {
            test.put(new BadHash(i), i);
        }
        for (int i = 0; i < size; i++) {
            if (i % 3 == 0) {
                assertEquals(i, test.remove(new BadHash(i)).intValue());
                assertNull(test.remove(new BadHash(i)));
            }
        }
        for (int i = 0; i < size; i++) {
            if (i % 3 == 0) {
                assertNull(test.getIfPresent(new BadHash(i)));
            } else {
                assertEquals(i, test.getIfPresent(new BadHash(i)).intValue());
            }
        }
        for (int i = 0; i < size; i++) {
            test.put(new BadHash(i), i);
        }
        for (int i = 0; i < size; i++) {
            if (i % 3 == 0) {
                assertEquals(i, test.remove(new BadHash(i)).intValue());
                assertNull(test.remove(new BadHash(i)));
            }
        }
        for (int i = 0; i < size; i++) {
            if (i % 3 == 0) {
                assertNull(test.getIfPresent(new BadHash(i)));
            } else {
                assertEquals(i, test.getIfPresent(new BadHash(i)).intValue());
            }
        }
    }

    @Test
    public void testScanResistance() {
        boolean log = false;
        int size = 20;
        // cache size 11 (10 hot, 1 cold)
        CacheLIRS<Integer, Integer> test = createCache(size / 2 + 1);
        // init the cache with some dummy entries
        for (int i = 0; i < size; i++) {
            test.put(-i, -i * 10);
        }
        verify(test, null);
        // init with 0..9, ensure those are hot entries
        for (int i = 0; i < size / 2; i++) {
            test.put(i, i * 10);
            test.getUnchecked(i);
            if (log) {
                System.out.println("get " + i + " -> " + test);
            }
        }
        verify(test, null);
        // read 0..9, add 10..19 (cold)
        for (int i = 0; i < size; i++) {
            Integer x = test.getIfPresent(i);
            Integer y = test.peek(i);
            if (i < size / 2) {
                assertTrue("i: " + i, x != null);
                assertTrue("i: " + i, y != null);
                assertEquals(i * 10, x.intValue());
                assertEquals(i * 10, y.intValue());
            } else {
                assertNull(x);
                assertNull(y);
                test.put(i, i * 10);
                // peek should have no effect
                assertEquals(i * 10, test.peek(i).intValue());
            }
            if (log) {
                System.out.println("get " + i + " -> " + test);
            }
            verify(test, null);
        }
        // ensure 0..9 are hot, 10..18 are not resident, 19 is cold
        for (int i = 0; i < size; i++) {
            Integer x = test.getIfPresent(i);
            if (i < size / 2 || i == size - 1) {
                assertTrue("i: " + i, x != null);
                assertEquals(i * 10, x.intValue());
            } else {
                assertNull(x);
            }
            verify(test, null);
        }
    }

    @Test
    public void testRandomOperations() {
        boolean log = false;
        int size = 10;
        Random r = new Random(1);
        for (int j = 0; j < 100; j++) {
            CacheLIRS<Integer, Integer> test = createCache(size / 2);
            HashMap<Integer, Integer> good = new HashMap<Integer, Integer>();
            for (int i = 0; i < 10000; i++) {
                int key = r.nextInt(size);
                int value = r.nextInt();
                switch (r.nextInt(3)) {
                case 0:
                    if (log) {
                        System.out.println(i + " put " + key + " " + value);
                    }
                    good.put(key, value);
                    test.put(key, value);
                    break;
                case 1:
                    if (log) {
                        System.out.println(i + " get " + key);
                    }
                    Integer a = good.get(key);
                    Integer b = test.getIfPresent(key);
                    if (a == null) {
                        assertNull(b);
                    } else if (b != null) {
                        assertEquals(a, b);
                    }
                    break;
                case 2:
                    if (log) {
                        System.out.println(i + " remove " + key);
                    }
                    good.remove(key);
                    test.remove(key);
                    break;
                }
                if (log) {
                    System.out.println(" -> " + toString(test));
                }
            }
            verify(test, null);
        }
    }

    private static <K, V> String toString(CacheLIRS<K, V> cache) {
        StringBuilder buff = new StringBuilder();
        buff.append("mem: " + cache.getUsedMemory());
        buff.append(" stack:");
        for (K k : cache.keys(false,  false)) {
            buff.append(' ').append(k);
        }
        buff.append(" cold:");
        for (K k : cache.keys(true,  false)) {
            buff.append(' ').append(k);
        }
        buff.append(" non-resident:");
        for (K k : cache.keys(true,  true)) {
            buff.append(' ').append(k);
        }
        return buff.toString();
    }

    private static <K, V> void verify(CacheLIRS<K, V> cache, String expected) {
        if (expected != null) {
            String got = toString(cache);
            assertEquals(expected, got);
        }
        int mem = 0;
        for (K k : cache.keySet()) {
            mem += cache.getMemory(k);
        }
        assertEquals(mem, cache.getUsedMemory());
        List<K> stack = cache.keys(false, false);
        List<K> cold = cache.keys(true, false);
        List<K> nonResident = cache.keys(true, true);
        assertEquals(nonResident.size(), cache.sizeNonResident());
        HashSet<K> hot = new HashSet<K>(stack);
        hot.removeAll(cold);
        hot.removeAll(nonResident);
        assertEquals(hot.size(), cache.sizeHot());
        assertEquals(hot.size() + cold.size(), cache.size());
        if (stack.size() > 0) {
            K lastStack = stack.get(stack.size() - 1);
            assertTrue(hot.contains(lastStack));
        }
    }

    private static <K, V> CacheLIRS<K, V> createCache(int maxElements) {
        return createCache(maxElements, 1);
    }

    private static <K, V> CacheLIRS<K, V> createCache(int maxSize, int averageSize) {
        return new CacheLIRS<K, V>(null, maxSize, averageSize, 1, 0, null, null, null);
    }
    
    @Test
    public void testBadLoader() {
        CacheLIRS<Integer, String> cache = createCache(10, 1);
        try {
            cache.get(1, new Callable<String>() {

                @Override
                public String call() throws Exception {
                    return null;
                }
                
            });
            fail();
        } catch (Exception e) {
            // expected
        }
    }
    
    @Test
    public void testAsMap() {
        CacheLIRS<Integer, String> cache = createCache(10, 1);
        ConcurrentMap<Integer, String> map = cache.asMap();
        assertNull(map.putIfAbsent(1, "Hello"));
        assertEquals(1, map.size());
        assertEquals("Hello", cache.getIfPresent(1));
        assertEquals("Hello", map.putIfAbsent(1, "Hallo"));
        assertEquals("Hello", cache.getIfPresent(1));
        assertEquals("Hello", map.replace(1, "Hallo"));
        assertEquals("Hallo", cache.getIfPresent(1));
        assertFalse(map.replace(1, "x", "y"));
        assertTrue(map.replace(1, "Hallo", "Hi!"));
        assertFalse(map.remove(1, "Hello"));
        assertTrue(map.remove(1, "Hi!"));
        assertEquals(0, map.size());
        
        map.put(1, "Hello");
        assertEquals("[1]", map.keySet().toString());
        assertEquals("[1=Hello]", cache.entrySet().toString());
        assertEquals("Hello", map.get(1));
        assertTrue(map.containsKey(1));
        assertFalse(map.containsKey(2));
        assertTrue(map.containsValue("Hello"));
        assertFalse(map.containsValue("Hallo"));
        assertFalse(map.isEmpty());
        map.remove(1);
        assertTrue(map.isEmpty());
        
        map.put(1, "Hello");
        map.clear();
        assertTrue(map.isEmpty());
    }
    
    @Test
    public void testZeroSizeCache() {
        CacheLIRS<Integer, String> cache = createCache(0, 100);
        cache.put(1, "Hello", 100);
        cache.put(2, "World", 100);
        cache.put(3, "!", 100);
        assertFalse(cache.containsKey(1));
    }
    
    @Test
    public void testRefresh() throws ExecutionException {
        CacheLIRS<Integer, String> cache = new CacheLIRS.Builder<Integer, String>().
                maximumWeight(100).
                weigher(new Weigher<Integer, String>() {

                    @Override
                    public int weigh(Integer key, String value) {
                        return key + value.length();
                    }
                    
                }).
                build(new CacheLoader<Integer, String>() {

                    @Override
                    public String load(Integer key) throws Exception {
                        if (key < 0 || key >= 100) {
                            throw new Exception("Out of range");
                        }
                        return "n" + key;
                    }

                    @Override
                    public ListenableFuture<String> reload(Integer key, String oldValue) {
                        assertTrue(oldValue != null);
                        SettableFuture<String> f = SettableFuture.create();
                        f.set(oldValue);
                        return f;
                    }

                });
        assertEquals("n1", cache.get(1));
        cache.refresh(1);
        cache.refresh(2);
        try {
            cache.get(-1);
            fail();
        } catch (Exception e) {
            // expected
        }
        // expected to log a warning, but not fail
        cache.refresh(-1);
    }

    @Test
    public void evictionCallback() throws ExecutionException {
        final Set<String> evictedKeys = newHashSet();
        final Set<Integer> evictedValues = newHashSet();
        CacheLIRS<String, Integer> cache = CacheLIRS.<String, Integer>newBuilder()
                .maximumSize(100)
                .evictionCallback(new EvictionCallback<String, Integer>() {
                    @Override
                    public void evicted(String key, Integer value, RemovalCause cause) {
                        evictedKeys.add(key);
                        if (value != null) {
                            assertEquals(key, valueOf(value));
                            evictedValues.add(value);
                        }
                        assertTrue(cause == RemovalCause.SIZE || cause == RemovalCause.EXPLICIT);
                    }
                })
                .build();

        for (int k = 0; k < 200; k++) {
            cache.put(valueOf(k), k);
        }

        assertTrue(evictedKeys.size() <= evictedValues.size());
        for (String key : evictedKeys) {
            assertFalse(cache.containsKey(key));
        }

        cache.invalidateAll();
        assertEquals(200, evictedKeys.size());
        assertEquals(200, evictedValues.size());
    }
    
    @Test
    public void evictionCallbackCause() {
        final Map<String, RemovalCause> causes = new HashMap<String, RemovalCause>();

        CacheLIRS<String, Integer> cache = CacheLIRS.<String, Integer> newBuilder().maximumSize(100)
                .evictionCallback(new EvictionCallback<String, Integer>() {
                    @Override
                    public void evicted(String key, Integer value, RemovalCause cause) {
                        if (key.startsWith("ignore-")) {
                            return;
                        }
                        causes.put(key, cause);
                    }
                }).build();

        cache.put("k1", 1);
        cache.remove("k1");
        assertEquals(RemovalCause.EXPLICIT, causes.remove("k1"));

        cache.put("k6", 1);
        cache.remove("k6", 1);
        assertEquals(RemovalCause.EXPLICIT, causes.remove("k6"));

        cache.put("k2", 1);
        cache.invalidate("k2");
        assertEquals(RemovalCause.EXPLICIT, causes.remove("k2"));

        cache.put("k3", 1);
        cache.invalidateAll();
        assertEquals(RemovalCause.EXPLICIT, causes.remove("k3"));

        cache.put("k4", 1);
        cache.put("k5", 1);
        cache.invalidateAll(Arrays.asList("k4", "k5"));
        assertEquals(RemovalCause.EXPLICIT, causes.remove("k4"));
        assertEquals(RemovalCause.EXPLICIT, causes.remove("k5"));

        cache.put("k7", 1);
        cache.clear();
        assertEquals(RemovalCause.EXPLICIT, causes.remove("k7"));

        cache.put("k8", 1);
        cache.put("k8", 2);
        assertEquals(RemovalCause.REPLACED, causes.remove("k8"));

        for (int i = 0; i < 50; i++) {
            cache.put("kk" + i, 1);
        }
        for (int i = 0; i < 200; i++) {
            cache.put("ignore-" + i, Integer.MAX_VALUE);
        }

        int checkedCount = 0;
        for (int i = 0; i < 50; i++) {
            String key = "kk" + i;
            if (!cache.containsKey(key)) {
                assertEquals("Callback hasn't been called for " + key, RemovalCause.SIZE, causes.get(key));
                checkedCount++;
            }
        }
        assertTrue(checkedCount > 10);
    }

    @Test
    public void evictionCallbackRandomized() throws ExecutionException {
        final HashMap<Integer, Integer> evictedMap = new HashMap<Integer, Integer>();
        final HashSet<Integer> evictedNonResidentSet = new HashSet<Integer>();
        CacheLIRS<Integer, Integer> cache = CacheLIRS.<Integer, Integer>newBuilder()
                .maximumSize(10)
                .evictionCallback(new EvictionCallback<Integer, Integer>() {
                    @Override
                    public void evicted(Integer key, Integer value, RemovalCause cause) {
                        if (value == null) {
                            assertTrue(evictedNonResidentSet.add(key));
                        } else {
                            assertEquals(null, evictedMap.put(key, value));
                        }
                    }
                })
                .build();
        Random r = new Random(1);
        for (int k = 0; k < 10000; k++) {
            if (r.nextInt(20) == 0) {
                evictedMap.clear();
                evictedNonResidentSet.clear();
                long size = cache.size();
                long sizeNonResident = cache.sizeNonResident();
                cache.invalidateAll();
                assertEquals(evictedMap.size(), size);
                assertEquals(evictedNonResidentSet.size(), sizeNonResident);
            }
            evictedMap.clear();
            evictedNonResidentSet.clear();
            int key = r.nextInt(20);
            if (r.nextBoolean()) {
                cache.put(key, k);
            } else {
                cache.get(key);
            }
            for (Entry<Integer, Integer> ev : evictedMap.entrySet()) {
                int ek = ev.getKey();
                if (ek == key) {
                    // the same key was inserted just now
                } else {
                    assertFalse(cache.containsKey(ek));
                }
            }
            for (Entry<Integer, Integer> ev : evictedMap.entrySet()) {
                int ek = ev.getKey();
                Integer v = ev.getValue();
                // an old value
                assertTrue(v < k);
                if (ek == key) {
                    // the same key was inserted just now
                } else {
                    assertFalse(cache.containsKey(ek));
                }
            }
        }
    }

}
