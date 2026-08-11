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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.EvictionCause;
import org.apache.jackrabbit.oak.cache.api.LoadingCache;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that Caffeine cache maintenance (eviction, removal notification) is dispatched
 * off the calling thread, per OAK-12290.
 */
public class CacheBuilderMaintenanceTest {

    private static final long TIMEOUT_SECONDS = 10;

    /**
     * The toggle is process-wide static state, so reset it around every test - a test that leaked
     * inline maintenance would silently change the behaviour asserted by every later test in the
     * same JVM.
     */
    @Before
    public void enableOak12290Toggle() {
        CacheBuilder.FT_OAK_12290_ASYNC_CACHE_MAINTENANCE_ENABLED.set(true);
    }

    @After
    public void resetOak12290Toggle() {
        CacheBuilder.FT_OAK_12290_ASYNC_CACHE_MAINTENANCE_ENABLED.set(true);
    }

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
     * cannot return until the callback finishes.
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

        Assert.assertSame("maintenance should run inline when the toggle is off",
                Thread.currentThread(), evictionThread.get());
    }

    /**
     * Maintenance must run on Oak's own named pool, not on {@code ForkJoinPool.commonPool()} -
     * the common pool is shared with the hosting application and can be configured with zero
     * workers, in which case submitted tasks are queued and never run.
     */
    @Test
    public void maintenanceRunsOnOakOwnedThread() throws InterruptedException {
        AtomicReference<String> threadName = new AtomicReference<>();
        CountDownLatch evicted = new CountDownLatch(1);

        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(1)
                .evictionListener((k, v, cause) -> {
                    if (cause == EvictionCause.SIZE) {
                        threadName.set(Thread.currentThread().getName());
                        evicted.countDown();
                    }
                })
                .build();

        cache.put("k1", "v1");
        cache.put("k2", "v2");

        Assert.assertTrue("size-based eviction was never notified",
                evicted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertTrue("maintenance ran on an unexpected thread: " + threadName.get(),
                threadName.get().startsWith("oak-cache-maintenance-"));
    }

    /** Refresh stays inline regardless of the toggle (see {@link CacheBuilder}). */
    @Test
    public void refreshRunsOnCallerThreadRegardlessOfToggle() throws InterruptedException {
        AtomicReference<Thread> reloadThread = new AtomicReference<>();
        CountDownLatch reloaded = new CountDownLatch(1);
        CountDownLatch firstLoadDone = new CountDownLatch(1);

        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .refreshAfterWrite(Duration.ofMillis(1))
                .build(key -> {
                    if (firstLoadDone.getCount() == 0) {
                        reloadThread.set(Thread.currentThread());
                        reloaded.countDown();
                    }
                    return "v";
                });

        cache.get("k");
        firstLoadDone.countDown();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        Thread callingThread = null;
        while (reloaded.getCount() > 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
            callingThread = Thread.currentThread();
            cache.get("k");
        }

        Assert.assertTrue("refresh never ran", reloaded.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertSame("refresh must run on the thread that triggered it, never the shared pool",
                callingThread, reloadThread.get());
    }

    /** Same guarantee with the toggle explicitly off, since refresh ignores it either way. */
    @Test
    public void refreshRunsOnCallerThreadWithToggleDisabled() throws InterruptedException {
        CacheBuilder.FT_OAK_12290_ASYNC_CACHE_MAINTENANCE_ENABLED.set(false);

        AtomicReference<Thread> reloadThread = new AtomicReference<>();
        CountDownLatch reloaded = new CountDownLatch(1);
        CountDownLatch firstLoadDone = new CountDownLatch(1);

        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .refreshAfterWrite(Duration.ofMillis(1))
                .build(key -> {
                    if (firstLoadDone.getCount() == 0) {
                        reloadThread.set(Thread.currentThread());
                        reloaded.countDown();
                    }
                    return "v";
                });

        cache.get("k");
        firstLoadDone.countDown();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        Thread callingThread = null;
        while (reloaded.getCount() > 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
            callingThread = Thread.currentThread();
            cache.get("k");
        }

        Assert.assertTrue("refresh never ran", reloaded.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertSame("refresh must run on the thread that triggered it, never the shared pool",
                callingThread, reloadThread.get());
    }

    /**
     * A refreshing cache's own eviction/removal notification must also stay inline - it shares the
     * same executor setting as refresh, and there is no separate knob for the two.
     */
    @Test
    public void refreshingCacheEvictionAlsoRunsOnCallerThread() {
        AtomicReference<Thread> evictionThread = new AtomicReference<>();

        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(1)
                .refreshAfterWrite(Duration.ofHours(1))
                .evictionListener((k, v, cause) -> {
                    if (cause == EvictionCause.SIZE) {
                        evictionThread.set(Thread.currentThread());
                    }
                })
                .build(key -> "v");

        cache.get("k1");
        cache.get("k2");

        Assert.assertSame("eviction on a refreshing cache must run inline, not on the shared pool",
                Thread.currentThread(), evictionThread.get());
    }

    /**
     * A zero-capacity cache (built with {@code maximumSize(0)}) is used elsewhere as a
     * "disable caching" idiom: callers write a value and immediately expect a read to miss.
     * With async maintenance that guarantee would depend on a background thread having already
     * run, so eviction must stay inline regardless of the toggle.
     */
    @Test
    public void zeroMaximumSizeEvictsSynchronously() {
        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(0)
                .build();

        cache.put("k1", "v1");

        Assert.assertNull("a zero-capacity cache must not retain the entry past the put() call",
                cache.getIfPresent("k1"));
    }

    /** Same guarantee for {@code maximumWeight(0)}, the weight-based equivalent of {@link #zeroMaximumSizeEvictsSynchronously()}. */
    @Test
    public void zeroMaximumWeightEvictsSynchronously() {
        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumWeight(0)
                .weigher((k, v) -> 1)
                .build();

        cache.put("k1", "v1");

        Assert.assertNull("a zero-weight cache must not retain the entry past the put() call",
                cache.getIfPresent("k1"));
    }

    /** The eviction listener of a zero-capacity cache must also run inline, for the same reason. */
    @Test
    public void zeroMaximumSizeEvictionNotificationRunsOnCallerThread() {
        AtomicReference<Thread> evictionThread = new AtomicReference<>();

        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(0)
                .evictionListener((k, v, cause) -> evictionThread.set(Thread.currentThread()))
                .build();

        cache.put("k1", "v1");

        Assert.assertSame("eviction on a zero-capacity cache must run inline, not on the shared pool",
                Thread.currentThread(), evictionThread.get());
    }

    /** Overwriting an existing key must notify the listener with {@link EvictionCause#REPLACED}, asynchronously. */
    @Test
    public void replacingAnEntryNotifiesListenerOffCallerThread() throws InterruptedException {
        AtomicReference<Thread> notificationThread = new AtomicReference<>();
        AtomicReference<EvictionCause> notifiedCause = new AtomicReference<>();
        CountDownLatch notified = new CountDownLatch(1);

        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .evictionListener((k, v, cause) -> {
                    notificationThread.set(Thread.currentThread());
                    notifiedCause.set(cause);
                    notified.countDown();
                })
                .build();

        cache.put("k1", "v1");
        cache.put("k1", "v2");

        Assert.assertTrue("replacement was never notified", notified.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertEquals(EvictionCause.REPLACED, notifiedCause.get());
        Assert.assertNotSame("replacement notification must not run on the calling thread",
                Thread.currentThread(), notificationThread.get());
    }
}
