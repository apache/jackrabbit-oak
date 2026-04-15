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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.guava.common.cache.CacheLoader;
import org.apache.jackrabbit.guava.common.util.concurrent.Futures;
import org.apache.jackrabbit.guava.common.util.concurrent.ListenableFuture;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.junit.Assert;
import org.junit.Test;

public class LirsLoadingCacheAdapterTest {

    @Test
    public void getLoadsMissingValue() {
        CacheLIRS<String, String> cache = CacheLIRS.<String, String>newBuilder()
                .maximumSize(10)
                .build(new CacheLoader<>() {
                    @Override
                    public String load(String key) {
                        return "loaded-" + key;
                    }
                });
        LirsLoadingCacheAdapter<String, String> adapter = new LirsLoadingCacheAdapter<>(cache);

        Assert.assertEquals("loaded-k", adapter.get("k"));
    }

    @Test
    public void getWrapsCheckedLoaderFailureInCompletionException() {
        Exception failure = new Exception("checked");
        CacheLIRS<String, String> cache = CacheLIRS.<String, String>newBuilder()
                .maximumSize(10)
                .build(new CacheLoader<>() {
                    @Override
                    public String load(String key) throws Exception {
                        throw failure;
                    }
                });
        LirsLoadingCacheAdapter<String, String> adapter = new LirsLoadingCacheAdapter<>(cache);

        try {
            adapter.get("k");
            Assert.fail("expected CompletionException");
        } catch (CompletionException e) {
            Assert.assertSame(failure, e.getCause());
        }
    }

    @Test
    public void getPropagatesRuntimeLoaderFailure() {
        RuntimeException failure = new RuntimeException("runtime");
        CacheLIRS<String, String> cache = CacheLIRS.<String, String>newBuilder()
                .maximumSize(10)
                .build(new CacheLoader<>() {
                    @Override
                    public String load(String key) {
                        throw failure;
                    }
                });
        LirsLoadingCacheAdapter<String, String> adapter = new LirsLoadingCacheAdapter<>(cache);

        try {
            adapter.get("k");
            Assert.fail("expected RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertSame(failure, e);
        }
    }

    @Test
    public void refreshReturnsRefreshedValue() {
        AtomicInteger loads = new AtomicInteger();
        CacheLIRS<String, String> cache = CacheLIRS.<String, String>newBuilder()
                .maximumSize(10)
                .build(new CacheLoader<>() {
                    @Override
                    public String load(String key) {
                        return "value-" + loads.incrementAndGet();
                    }

                    @Override
                    public ListenableFuture<String> reload(String key, String oldValue) {
                        return Futures.immediateFuture("value-" + loads.incrementAndGet());
                    }
                });
        LirsLoadingCacheAdapter<String, String> adapter = new LirsLoadingCacheAdapter<>(cache);

        Assert.assertEquals("value-1", adapter.get("k"));
        Assert.assertEquals("value-2", adapter.refresh("k").join());
        Assert.assertEquals("value-2", cache.getIfPresent("k"));
    }
}
