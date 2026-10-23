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
package org.apache.jackrabbit.oak.segment;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.EvictionCause;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.mockito.Mockito;

/** Tests for {@link SegmentCacheMaintenance} and its bundle-scoped override. */
public class SegmentCacheMaintenanceTest {

    private static final long TIMEOUT_SECONDS = 10;
    private boolean incomingAsync;

    @Before
    public void enableAsyncMaintenance() {
        incomingAsync = SegmentCacheMaintenance.ASYNC_ENABLED.get();
        SegmentCacheMaintenance.ASYNC_ENABLED.set(true);
    }

    @After
    public void resetAsyncMaintenance() {
        SegmentCacheMaintenance.ASYNC_ENABLED.set(incomingAsync);
    }

    /** Segment policy changes leave independent caches on ASYNC. */
    @Test
    public void featureToggleOnlyChangesSegmentCacheMode() throws InterruptedException {
        SegmentCacheMaintenance.ASYNC_ENABLED.set(false);
        Assert.assertEquals(CacheBuilder.MaintenanceMode.SYNC, SegmentCacheMaintenance.mode());

        assertIndependentCacheRemainsAsync();
    }

    static void assertIndependentCacheRemainsAsync() throws InterruptedException {
        AtomicReference<Thread> callbackThread = new AtomicReference<>();
        CountDownLatch evicted = new CountDownLatch(1);
        Cache<String, String> independentCache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(1)
                .evictionListener((key, value, cause) -> {
                    if (cause == EvictionCause.SIZE) {
                        callbackThread.set(Thread.currentThread());
                        evicted.countDown();
                    }
                })
                .build();

        independentCache.put("one", "1");
        independentCache.put("two", "2");

        Assert.assertTrue("default cache maintenance should remain asynchronous",
                evicted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertNotSame(Thread.currentThread(), callbackThread.get());
    }

    /** The test restores the incoming Segment policy. */
    @Test
    public void incomingSyncPolicyIsRestored() {
        SegmentCacheMaintenance.ASYNC_ENABLED.set(false);
        Result result = new JUnitCore().run(Request.method(getClass(), "featureToggleOnlyChangesSegmentCacheMode"));
        Assert.assertTrue(result.getFailures().toString(), result.wasSuccessful());
        Assert.assertFalse(SegmentCacheMaintenance.ASYNC_ENABLED.get());
    }

    /** Segment eviction callbacks run on the selected thread. */
    @Test
    public void segmentCacheUsesSelectedMode() throws InterruptedException {
        for (boolean async : new boolean[] {false, true}) {
            SegmentCacheMaintenance.ASYNC_ENABLED.set(async);
            assertSegmentCacheCallbackMode(async);
        }
    }

    /** Changing the policy does not alter an existing cache. */
    @Test
    public void existingSegmentCacheRetainsModeAfterPolicyChange() throws InterruptedException {
        SegmentId id = Mockito.spy(new SegmentId(SegmentStore.EMPTY_STORE, 1L, 0xa000000000000001L));
        Segment segment = Mockito.mock(Segment.class);
        Mockito.when(segment.getSegmentId()).thenReturn(id);
        Mockito.when(segment.estimateMemoryUsage()).thenReturn(1);
        AtomicReference<Thread> callbackThread = new AtomicReference<>();
        CountDownLatch removed = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            callbackThread.set(Thread.currentThread());
            removed.countDown();
            return invocation.callRealMethod();
        }).when(id).compareAndUnload(segment);
        SegmentCache cache = SegmentCache.newSegmentCache(1);
        cache.putSegment(segment);

        SegmentCacheMaintenance.ASYNC_ENABLED.set(false);
        cache.clear();

        assertCallbackMode(true, removed, callbackThread);
    }

    static void assertSegmentCacheCallbackMode(boolean async) throws InterruptedException {
        SegmentId id = Mockito.spy(new SegmentId(SegmentStore.EMPTY_STORE, 1L, 0xa000000000000001L));
        Segment segment = Mockito.mock(Segment.class);
        Mockito.when(segment.getSegmentId()).thenReturn(id);
        Mockito.when(segment.estimateMemoryUsage()).thenReturn(1);
        AtomicReference<Thread> callbackThread = new AtomicReference<>();
        CountDownLatch removed = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            callbackThread.set(Thread.currentThread());
            removed.countDown();
            return invocation.callRealMethod();
        }).when(id).compareAndUnload(segment);
        SegmentCache cache = SegmentCache.newSegmentCache(1);
        cache.putSegment(segment);
        cache.clear();
        assertCallbackMode(async, removed, callbackThread);
    }

    /** Record eviction callbacks run on the selected thread. */
    @Test
    public void recordCacheUsesSelectedMode() throws InterruptedException {
        for (boolean async : new boolean[] {false, true}) {
            SegmentCacheMaintenance.ASYNC_ENABLED.set(async);
            assertRecordCacheCallbackMode(async);
        }
    }

    static void assertRecordCacheCallbackMode(boolean async) throws InterruptedException {
        RecordId first = new RecordId(SegmentId.NULL, 0);
        RecordId second = new RecordId(SegmentId.NULL, 4);
        AtomicBoolean observeRemoval = new AtomicBoolean();
        AtomicReference<Thread> callbackThread = new AtomicReference<>();
        CountDownLatch removed = new CountDownLatch(1);
        RecordCache<String> cache = RecordCache.<String>factory(10, (key, value) -> {
            if (value == first && observeRemoval.get()) {
                callbackThread.set(Thread.currentThread());
                removed.countDown();
            }
            return 1;
        }).get();
        cache.put("key", first);
        observeRemoval.set(true);
        cache.put("key", second);
        assertCallbackMode(async, removed, callbackThread);
    }

    /** Each Record cache generation samples the current policy. */
    @Test
    public void recordCacheFactorySamplesModeForEachGeneration() throws InterruptedException {
        RecordId first = new RecordId(SegmentId.NULL, 0);
        RecordId second = new RecordId(SegmentId.NULL, 4);
        AtomicBoolean observeRemoval = new AtomicBoolean();
        AtomicReference<Thread> asyncCallbackThread = new AtomicReference<>();
        AtomicReference<Thread> syncCallbackThread = new AtomicReference<>();
        CountDownLatch asyncRemoved = new CountDownLatch(1);
        CountDownLatch syncRemoved = new CountDownLatch(1);
        Supplier<RecordCache<String>> factory = RecordCache.factory(10, (key, value) -> {
            if (value == first && observeRemoval.get()) {
                if ("async".equals(key)) {
                    asyncCallbackThread.set(Thread.currentThread());
                    asyncRemoved.countDown();
                } else {
                    syncCallbackThread.set(Thread.currentThread());
                    syncRemoved.countDown();
                }
            }
            return 1;
        });
        RecordCache<String> asyncCache = factory.get();
        SegmentCacheMaintenance.ASYNC_ENABLED.set(false);
        RecordCache<String> syncCache = factory.get();
        asyncCache.put("async", first);
        syncCache.put("sync", first);
        observeRemoval.set(true);

        asyncCache.put("async", second);
        syncCache.put("sync", second);

        assertCallbackMode(true, asyncRemoved, asyncCallbackThread);
        assertCallbackMode(false, syncRemoved, syncCallbackThread);
        Assert.assertSame(second, asyncCache.get("async"));
        Assert.assertSame(second, syncCache.get("sync"));
    }

    private static void assertCallbackMode(boolean async, CountDownLatch removed,
                                    AtomicReference<Thread> callbackThread) throws InterruptedException {
        if (!async) {
            Assert.assertEquals("SYNC callback completes inline", 0L, removed.getCount());
        }
        Assert.assertTrue(removed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertEquals(!async, Thread.currentThread() == callbackThread.get());
    }
}
