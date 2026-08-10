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
package org.apache.jackrabbit.oak.cache.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.EvictionCause;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests that Caffeine cache maintenance (eviction, removal notification) is dispatched
 * off the calling thread. Running it inline lets a request, indexer or writer thread hold
 * Caffeine's eviction lock for the duration of the maintenance work, which caused the
 * lock contention of OAK-12290 and the wedged JVM of SKYOPS-149400.
 */
public class CacheBuilderMaintenanceTest {

    private static final long TIMEOUT_SECONDS = 10;

    /** Maintenance triggered by a write must not be executed by the writing thread. */
    @Test
    public void evictionNotificationRunsOffCallerThread() throws InterruptedException {
        AtomicReference<Thread> evictionThread = new AtomicReference<>();
        CountDownLatch evicted = new CountDownLatch(1);

        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(1)
                .evictionListener((k, v, cause) -> {
                    if (cause == EvictionCause.SIZE) {
                        evictionThread.set(Thread.currentThread());
                        evicted.countDown();
                    }
                })
                .build();

        cache.put("k1", "v1");
        cache.put("k2", "v2");

        Assert.assertTrue("size-based eviction was never notified",
                evicted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertNotSame("cache maintenance must not run on the calling thread",
                Thread.currentThread(), evictionThread.get());
    }

    /**
     * A slow maintenance callback must not stall the writer. With inline maintenance the
     * writer runs the callback itself while holding the eviction lock, so {@code put()}
     * cannot return until the callback finishes - the wedge seen in SKYOPS-149400.
     */
    @Test(timeout = TIMEOUT_SECONDS * 1000)
    public void slowMaintenanceDoesNotBlockCallerThread() throws InterruptedException {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch maintenanceDone = new CountDownLatch(1);

        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(1)
                .evictionListener((k, v, cause) -> {
                    if (cause == EvictionCause.SIZE) {
                        try {
                            release.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        maintenanceDone.countDown();
                    }
                })
                .build();

        cache.put("k1", "v1");
        // returns only if the blocked maintenance callback runs on another thread
        cache.put("k2", "v2");

        release.countDown();
        Assert.assertTrue("maintenance callback never completed",
                maintenanceDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    /** Disabling the toggle restores the previous inline-maintenance behaviour. */
    @Test
    public void toggleDisabledRunsMaintenanceInline() {
        AtomicReference<Thread> evictionThread = new AtomicReference<>();

        CacheBuilder.FT_OAK_12290_ASYNC_CACHE_MAINTENANCE_ENABLED.set(false);
        try {
            Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                    .maximumSize(1)
                    .evictionListener((k, v, cause) -> {
                        if (cause == EvictionCause.SIZE) {
                            evictionThread.set(Thread.currentThread());
                        }
                    })
                    .build();

            cache.put("k1", "v1");
            cache.put("k2", "v2");
        } finally {
            CacheBuilder.FT_OAK_12290_ASYNC_CACHE_MAINTENANCE_ENABLED.set(true);
        }

        Assert.assertSame("maintenance should run inline when the toggle is off",
                Thread.currentThread(), evictionThread.get());
    }
}
