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
package org.apache.jackrabbit.oak.cache.impl.lirs;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import org.apache.jackrabbit.guava.common.cache.RemovalCause;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.api.CacheCounters;
import org.apache.jackrabbit.oak.cache.api.EvictionCause;
import org.junit.Assert;
import org.junit.Test;

public class LirsCacheAdapterTest {

    @Test
    public void getUsesKeyAwareMappingFunctionAndCachesResult() {
        CacheLIRS<String, String> cache = CacheLIRS.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        LirsCacheAdapter<String, String> adapter = new LirsCacheAdapter<>(cache);

        Assert.assertEquals("value-k", adapter.get("k", key -> "value-" + key));
        Assert.assertEquals("value-k", cache.getIfPresent("k"));
    }

    @Test
    public void statsSnapshotReflectsUnderlyingCacheStats() {
        CacheLIRS<String, String> cache = CacheLIRS.<String, String>newBuilder()
                .maximumSize(10)
                .recordStats()
                .build();
        LirsCacheAdapter<String, String> adapter = new LirsCacheAdapter<>(cache);

        adapter.put("hit", "value");
        adapter.getIfPresent("hit");
        adapter.getIfPresent("miss");

        CacheCounters stats = adapter.stats();
        Assert.assertEquals(1, stats.hitCount());
        Assert.assertEquals(1, stats.missCount());
    }

    @Test
    public void toOakCauseMapsEveryGuavaCause() {
        Assert.assertEquals(EvictionCause.EXPLICIT, LirsCacheAdapter.toOakCause(RemovalCause.EXPLICIT));
        Assert.assertEquals(EvictionCause.REPLACED, LirsCacheAdapter.toOakCause(RemovalCause.REPLACED));
        Assert.assertEquals(EvictionCause.SIZE, LirsCacheAdapter.toOakCause(RemovalCause.SIZE));
        Assert.assertEquals(EvictionCause.EXPIRED, LirsCacheAdapter.toOakCause(RemovalCause.EXPIRED));
        Assert.assertEquals(EvictionCause.COLLECTED, LirsCacheAdapter.toOakCause(RemovalCause.COLLECTED));
    }

    @Test
    public void toCaffeineExceptionWrapsCheckedCause() {
        Exception failure = new Exception("checked");

        RuntimeException exception = LirsCacheAdapter.toCaffeineException(new ExecutionException(failure));

        Assert.assertTrue(exception instanceof CompletionException);
        Assert.assertSame(failure, exception.getCause());
    }

    @Test
    public void toCaffeineExceptionReturnsRuntimeCause() {
        RuntimeException failure = new RuntimeException("runtime");

        Assert.assertSame(failure, LirsCacheAdapter.toCaffeineException(new ExecutionException(failure)));
    }

    @Test
    public void toCaffeineExceptionRethrowsErrorCause() {
        AssertionError failure = new AssertionError("error");

        try {
            LirsCacheAdapter.toCaffeineException(new ExecutionException(failure));
            Assert.fail("expected AssertionError");
        } catch (AssertionError e) {
            Assert.assertSame(failure, e);
        }
    }
}
