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
package org.apache.jackrabbit.oak.cache;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link EmpiricalWeigher}.
 * These assertions intentionally avoid third-party cache types so the same
 * tests can run across cache implementation changes.
 */
public class EmpiricalWeigherTest {

    private static final int ENTRY_OVERHEAD = 168;

    private final EmpiricalWeigher weigher = new EmpiricalWeigher();

    @Test
    public void weighIncludesBaseOverheadForZeroMemoryValues() {
        CacheValue key   = () -> 0;
        CacheValue value = () -> 0;
        Assert.assertEquals(ENTRY_OVERHEAD, weigher.weigh(key, value));
    }

    @Test
    public void weighAddsKeyAndValueMemoryToOverhead() {
        CacheValue key   = () -> 100;
        CacheValue value = () -> 200;
        Assert.assertEquals(ENTRY_OVERHEAD + 100 + 200, weigher.weigh(key, value));
    }

    @Test
    public void weighWithOnlyKeyMemory() {
        CacheValue key   = () -> 50;
        CacheValue value = () -> 0;
        Assert.assertEquals(ENTRY_OVERHEAD + 50, weigher.weigh(key, value));
    }

    @Test
    public void weighWithOnlyValueMemory() {
        CacheValue key   = () -> 0;
        CacheValue value = () -> 300;
        Assert.assertEquals(ENTRY_OVERHEAD + 300, weigher.weigh(key, value));
    }

    @Test
    public void weighCapsAtIntegerMaxValue() {
        // key + value + overhead overflows int
        CacheValue key   = () -> Integer.MAX_VALUE;
        CacheValue value = () -> Integer.MAX_VALUE;
        Assert.assertEquals(Integer.MAX_VALUE, weigher.weigh(key, value));
    }

    @Test
    public void weighIsAlwaysPositive() {
        CacheValue key   = () -> 1;
        CacheValue value = () -> 1;
        Assert.assertTrue(weigher.weigh(key, value) > 0);
    }

    @Test
    public void weighReturnsConsistentResultsForSameInput() {
        CacheValue key   = () -> 42;
        CacheValue value = () -> 99;
        int first  = weigher.weigh(key, value);
        int second = weigher.weigh(key, value);
        Assert.assertEquals(first, second);
    }

    @Test
    public void weighJustBelowOverflow() {
        // total = 168 + (Integer.MAX_VALUE - 168 - 1) + 1 = Integer.MAX_VALUE
        int keyMem   = Integer.MAX_VALUE - ENTRY_OVERHEAD - 1;
        int valueMem = 1;
        CacheValue key   = () -> keyMem;
        CacheValue value = () -> valueMem;
        Assert.assertEquals(Integer.MAX_VALUE, weigher.weigh(key, value));
    }
}
