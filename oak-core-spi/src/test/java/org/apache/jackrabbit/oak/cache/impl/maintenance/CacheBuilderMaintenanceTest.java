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
package org.apache.jackrabbit.oak.cache.impl.maintenance;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.EvictionCause;
import org.apache.jackrabbit.oak.cache.api.LoadingCache;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests Caffeine maintenance modes, default execution, and refresh/zero-capacity exceptions.
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

    /** Slow eviction callbacks must not block writes that exceed the cache budget. */
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

    /** Oak's pool keeps maintenance working when the application's common pool has no workers. */
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
                threadName.get().startsWith(CacheMaintenanceExecutor.THREAD_PREFIX));
    }

    /** Refresh runs off the caller so slow reloads cannot block reads. */
    @Test
    public void refreshRunsOffCallerThreadByDefault() throws InterruptedException {
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
        while (reloaded.getCount() > 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
            cache.get("k");
        }

        Assert.assertTrue("refresh never ran", reloaded.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertNotSame("refresh must not run on the thread that triggered it",
                Thread.currentThread(), reloadThread.get());
        Assert.assertTrue("refresh ran on an unexpected thread: " + reloadThread.get().getName(),
                reloadThread.get().getName().startsWith(CacheMaintenanceExecutor.THREAD_PREFIX));
    }

    /** Refresh and eviction share the asynchronous executor. */
    @Test
    public void refreshingCacheEvictionRunsOffCallerThread() throws InterruptedException {
        AtomicReference<Thread> evictionThread = new AtomicReference<>();
        CountDownLatch evicted = new CountDownLatch(1);

        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(1)
                .refreshAfterWrite(Duration.ofHours(1))
                .evictionListener((k, v, cause) -> {
                    if (cause == EvictionCause.SIZE) {
                        evictionThread.set(Thread.currentThread());
                        evicted.countDown();
                    }
                })
                .build(key -> "v");

        cache.get("k1");
        cache.get("k2");

        Assert.assertTrue("size-based eviction was never notified",
                evicted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertNotSame("eviction on a refreshing cache must not run on the calling thread",
                Thread.currentThread(), evictionThread.get());
    }

    /** Zero size disables caching, so put must not leave a readable entry. */
    @Test
    public void zeroMaximumSizeEvictsSynchronously() {
        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(0)
                .build();

        cache.put("k1", "v1");

        Assert.assertNull("a zero-capacity cache must not retain the entry past the put() call",
                cache.getIfPresent("k1"));
    }

    /** A zero weight budget also disables caching immediately. */
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

    /** Zero capacity requires immediate eviction even when refresh is configured. */
    @Test
    public void zeroMaximumSizeEvictsSynchronouslyEvenWithRefreshAfterWrite() {
        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(0)
                .refreshAfterWrite(Duration.ofHours(1))
                .build(key -> "v");

        cache.get("k1");

        Assert.assertNull("a zero-capacity refreshing cache must not retain the entry past the get() call",
                cache.getIfPresent("k1"));
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


    /** Reject null rather than silently choosing a mode. */
    @Test(expected = NullPointerException.class)
    public void nullMaintenanceModeIsRejected() {
        CacheBuilder.newBuilder().maintenanceMode(null);
    }

    /** The last mode selection determines which executor the cache uses. */
    @Test
    public void laterAsyncSelectionOverridesSynchronousMode() throws InterruptedException {
        AtomicReference<Thread> callbackThread = new AtomicReference<>();
        CountDownLatch removed = new CountDownLatch(1);
        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(1)
                .maintenanceMode(CacheBuilder.MaintenanceMode.SYNC)
                .maintenanceMode(CacheBuilder.MaintenanceMode.ASYNC)
                .evictionListener((key, value, cause) -> {
                    callbackThread.set(Thread.currentThread());
                    removed.countDown();
                }).build();
        cache.put("key", "value");
        cache.invalidate("key");
        Assert.assertTrue(removed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertNotSame(Thread.currentThread(), callbackThread.get());
        Assert.assertTrue("explicit ASYNC must use Oak's maintenance pool",
                callbackThread.get().getName().startsWith(CacheMaintenanceExecutor.THREAD_PREFIX));
    }

    /** Explicit SYNC keeps eviction callbacks on the caller. */
    @Test
    public void synchronousMaintenanceRunsInline() {
        AtomicReference<Thread> evictionThread = new AtomicReference<>();

        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(1)
                .maintenanceMode(CacheBuilder.MaintenanceMode.ASYNC)
                .maintenanceMode(CacheBuilder.MaintenanceMode.SYNC)
                .evictionListener((k, v, cause) -> {
                    if (cause == EvictionCause.SIZE) {
                        evictionThread.set(Thread.currentThread());
                    }
                })
                .build();

        cache.put("k1", "v1");
        cache.put("k2", "v2");

        Assert.assertSame("maintenance should run inline in synchronous mode",
                Thread.currentThread(), evictionThread.get());
    }

    /** Refresh must not block callers even when SYNC is requested. */
    @Test
    public void refreshRunsOffCallerThreadWithSynchronousMaintenance() throws InterruptedException {
        AtomicReference<Thread> reloadThread = new AtomicReference<>();
        CountDownLatch reloaded = new CountDownLatch(1);
        CountDownLatch firstLoadDone = new CountDownLatch(1);

        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .maintenanceMode(CacheBuilder.MaintenanceMode.SYNC)
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
        while (reloaded.getCount() > 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
            cache.get("k");
        }

        Assert.assertTrue("refresh never ran", reloaded.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertNotSame("refresh must not run on the thread that triggered it",
                Thread.currentThread(), reloadThread.get());
        Assert.assertTrue("refresh must use Oak's maintenance pool even when SYNC is requested",
                reloadThread.get().getName().startsWith(CacheMaintenanceExecutor.THREAD_PREFIX));
    }

    /** A disabled cache cannot expose entries while eviction is pending. */
    @Test
    public void zeroMaximumSizeEvictsSynchronouslyWithAsyncMode() {
        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(0)
                .maintenanceMode(CacheBuilder.MaintenanceMode.ASYNC)
                .build();

        cache.put("k1", "v1");

        Assert.assertNull("a zero-capacity cache must not retain the entry past the put() call",
                cache.getIfPresent("k1"));
    }

    /** Weighted caches honor explicit SYNC maintenance. */
    @Test
    public void synchronousMaintenanceRunsInlineWithMaximumWeight() {
        AtomicReference<Thread> evictionThread = new AtomicReference<>();
        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumWeight(1)
                .weigher((key, value) -> 1)
                .maintenanceMode(CacheBuilder.MaintenanceMode.SYNC)
                .evictionListener((key, value, cause) -> {
                    if (cause == EvictionCause.SIZE) {
                        evictionThread.set(Thread.currentThread());
                    }
                })
                .build();

        cache.put("k1", "v1");
        cache.put("k2", "v2");

        Assert.assertSame("weighted eviction must finish on the writing thread",
                Thread.currentThread(), evictionThread.get());
        Assert.assertEquals(1L, cache.getUsedWeight());
    }

    /** Refresh uses the shared executor in ASYNC mode. */
    @Test
    public void refreshRunsOffCallerThreadWithAsyncMaintenance() throws InterruptedException {
        AtomicLong ticker = new AtomicLong();
        AtomicReference<Thread> reloadThread = new AtomicReference<>();
        CountDownLatch reloaded = new CountDownLatch(1);
        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .maintenanceMode(CacheBuilder.MaintenanceMode.ASYNC)
                .refreshAfterWrite(Duration.ofMinutes(1))
                .ticker(ticker::get)
                .build(key -> {
                    reloadThread.set(Thread.currentThread());
                    reloaded.countDown();
                    return "refreshed";
                });

        cache.put("k", "initial");
        ticker.set(TimeUnit.MINUTES.toNanos(2));
        cache.get("k");

        Assert.assertTrue("refresh never ran", reloaded.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertNotSame("refresh must not run on the thread that triggered it",
                Thread.currentThread(), reloadThread.get());
        Assert.assertTrue("explicit ASYNC refresh must use Oak's maintenance pool",
                reloadThread.get().getName().startsWith(CacheMaintenanceExecutor.THREAD_PREFIX));
    }

    /** A zero weight budget requires immediate eviction. */
    @Test
    public void zeroMaximumWeightEvictsSynchronouslyWithAsyncMode() {
        AtomicReference<Thread> evictionThread = new AtomicReference<>();
        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumWeight(0)
                .weigher((key, value) -> 1)
                .maintenanceMode(CacheBuilder.MaintenanceMode.ASYNC)
                .evictionListener((key, value, cause) -> evictionThread.set(Thread.currentThread()))
                .build();

        cache.put("k", "v");

        Assert.assertSame("zero weight must override asynchronous maintenance",
                Thread.currentThread(), evictionThread.get());
        Assert.assertNull("zero weight must evict before put returns", cache.getIfPresent("k"));
    }

    /** Zero capacity takes precedence over asynchronous refresh. */
    @Test
    public void zeroMaximumSizeEvictsSynchronouslyEvenWithRefreshAfterWriteAndSyncMode() {
        AtomicReference<Thread> evictionThread = new AtomicReference<>();
        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(0)
                .refreshAfterWrite(Duration.ofHours(1))
                .maintenanceMode(CacheBuilder.MaintenanceMode.SYNC)
                .evictionListener((key, value, cause) -> evictionThread.set(Thread.currentThread()))
                .build(key -> "v");

        Assert.assertEquals("v", cache.get("k"));

        Assert.assertSame("zero capacity must override the refresh executor",
                Thread.currentThread(), evictionThread.get());
        Assert.assertNull("zero capacity must evict before get returns", cache.getIfPresent("k"));
    }

    /** Zero capacity overrides both explicit ASYNC maintenance and asynchronous refresh. */
    @Test
    public void zeroMaximumSizeEvictsSynchronouslyEvenWithRefreshAfterWriteAndAsyncMode() {
        AtomicReference<Thread> evictionThread = new AtomicReference<>();
        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(0)
                .refreshAfterWrite(Duration.ofHours(1))
                .maintenanceMode(CacheBuilder.MaintenanceMode.ASYNC)
                .evictionListener((key, value, cause) -> evictionThread.set(Thread.currentThread()))
                .build(key -> "v");

        Assert.assertEquals("v", cache.get("k"));

        Assert.assertSame("zero capacity must override explicit ASYNC and the refresh executor",
                Thread.currentThread(), evictionThread.get());
        Assert.assertNull("zero capacity must evict before get returns", cache.getIfPresent("k"));
    }

    /** A zero weight budget overrides asynchronous refresh and maintenance. */
    @Test
    public void zeroMaximumWeightEvictsSynchronouslyEvenWithRefreshAfterWriteAndAsyncMode() {
        AtomicReference<Thread> evictionThread = new AtomicReference<>();
        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumWeight(0)
                .weigher((key, value) -> 1)
                .refreshAfterWrite(Duration.ofHours(1))
                .maintenanceMode(CacheBuilder.MaintenanceMode.ASYNC)
                .evictionListener((key, value, cause) -> evictionThread.set(Thread.currentThread()))
                .build(key -> "v");

        Assert.assertEquals("v", cache.get("k"));

        Assert.assertSame("zero weight must override explicit ASYNC and the refresh executor",
                Thread.currentThread(), evictionThread.get());
        Assert.assertNull("zero weight must evict before get returns", cache.getIfPresent("k"));
    }

    /** Zero weight disables caching even when refresh would otherwise override SYNC. */
    @Test
    public void zeroMaximumWeightEvictsSynchronouslyEvenWithRefreshAfterWriteAndSyncMode() {
        AtomicReference<Thread> evictionThread = new AtomicReference<>();
        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumWeight(0)
                .weigher((key, value) -> 1)
                .refreshAfterWrite(Duration.ofHours(1))
                .maintenanceMode(CacheBuilder.MaintenanceMode.SYNC)
                .evictionListener((key, value, cause) -> evictionThread.set(Thread.currentThread()))
                .build(key -> "v");

        Assert.assertEquals("v", cache.get("k"));

        Assert.assertSame("zero weight must override the refresh executor even when SYNC is selected",
                Thread.currentThread(), evictionThread.get());
        Assert.assertNull("zero weight must evict before get returns", cache.getIfPresent("k"));
    }

    /** Refresh forces size eviction onto Oak's pool even when SYNC is requested. */
    @Test
    public void refreshingCacheEvictionRunsOffCallerThreadWithSyncMode() throws InterruptedException {
        AtomicReference<Thread> evictionThread = new AtomicReference<>();
        CountDownLatch evicted = new CountDownLatch(1);
        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(1)
                .maintenanceMode(CacheBuilder.MaintenanceMode.SYNC)
                .refreshAfterWrite(Duration.ofHours(1))
                .evictionListener((key, value, cause) -> {
                    if (cause == EvictionCause.SIZE) {
                        evictionThread.set(Thread.currentThread());
                        evicted.countDown();
                    }
                })
                .build(key -> "v");

        cache.get("k1");
        cache.get("k2");

        Assert.assertTrue("size-based eviction was never notified",
                evicted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertNotSame("refresh must override SYNC for eviction callbacks",
                Thread.currentThread(), evictionThread.get());
        Assert.assertTrue("refreshing cache eviction must use Oak's maintenance pool",
                evictionThread.get().getName().startsWith(CacheMaintenanceExecutor.THREAD_PREFIX));
    }
}
