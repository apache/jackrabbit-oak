/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Compatibility tests for the Oak-visible {@link CacheLIRS} API surface.
 * These assertions intentionally avoid third-party cache APIs so the same
 * tests can run before and after the cache implementation migration.
 */
public class CacheLIRSCompatibilityTest {

    @Test
    public void getWithCallableCachesLoadedValue() throws ExecutionException {
        // Load through the Oak-visible callable API, then repeat the same lookup
        // with a different loader to prove the cached value wins on the second call.
        CacheLIRS<String, String> cache = CacheLIRS.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        AtomicInteger loadCount = new AtomicInteger();

        assertEquals("loaded", cache.get("k", () -> {
            loadCount.incrementAndGet();
            return "loaded";
        }));
        assertEquals("loaded", cache.get("k", () -> {
            loadCount.incrementAndGet();
            return "other";
        }));

        assertEquals(1, loadCount.get());
        assertEquals("loaded", cache.getIfPresent("k"));
    }

    @Test
    public void getWithCallableWrapsCheckedLoaderFailureInExecutionException() {
        // Use a checked exception from the loader and verify the legacy
        // ExecutionException shape is preserved for callers.
        CacheLIRS<String, String> cache = CacheLIRS.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        Exception failure = new Exception("checked failure");

        try {
            cache.get("k", () -> {
                throw failure;
            });
            fail("expected ExecutionException");
        } catch (ExecutionException e) {
            assertEquals(failure, e.getCause());
            assertEquals("checked failure", e.getCause().getMessage());
            assertNull(cache.getIfPresent("k"));
        }
    }

    @Test
    public void invalidateAllClearsPreviouslyCachedEntries() throws ExecutionException {
        // Populate two keys first, then clear the cache and verify both the
        // size counters and direct lookups observe an empty cache.
        CacheLIRS<String, String> cache = CacheLIRS.<String, String>newBuilder()
                .maximumSize(10)
                .build();

        cache.get("k1", () -> "v1");
        cache.get("k2", () -> "v2");
        assertEquals(2, cache.size());

        cache.invalidateAll();

        assertEquals(0, cache.size());
        assertNull(cache.getIfPresent("k1"));
        assertNull(cache.getIfPresent("k2"));
        assertTrue(cache.asMap().isEmpty());
    }

    @Test
    public void evictionCallbackIsInvokedWhenEntryIsEvictedBySize() {
        // Push the cache past capacity and capture the first callback so the
        // test checks real size-based eviction instead of explicit invalidation.
        AtomicInteger evictions = new AtomicInteger();
        AtomicReference<String> firstEvictedKey = new AtomicReference<>();
        AtomicReference<String> firstEvictedValue = new AtomicReference<>();
        CacheLIRS<String, String> cache = CacheLIRS.<String, String>newBuilder()
                .maximumSize(10)
                .evictionCallback((key, value, cause) -> {
                    if (evictions.getAndIncrement() == 0) {
                        firstEvictedKey.set(key);
                        firstEvictedValue.set(value);
                    }
                })
                .build();

        // LIRS requires cold-queue warm-up before evicting; 100× the cache capacity
        // ensures at least one eviction on any conforming implementation.
        for (int i = 0; i < 1000 && evictions.get() == 0; i++) {
            cache.put("k" + i, "v" + i);
        }

        assertTrue("expected at least one eviction callback", evictions.get() > 0);
        assertTrue(firstEvictedKey.get().startsWith("k"));
        assertTrue(firstEvictedValue.get().startsWith("v"));
        assertNull(cache.getIfPresent(firstEvictedKey.get()));
    }
}
