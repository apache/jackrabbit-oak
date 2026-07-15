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
package org.apache.jackrabbit.oak.index.indexer.document;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.AheadOfTimeBlobDownloaderThrottler;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class AheadOfTimeBlobDownloaderThrottlerTest {

    private static final Logger LOG = LoggerFactory.getLogger(AheadOfTimeBlobDownloaderThrottlerTest.class);

    @Test
    public void blockOnWindowFullByteSize() throws ExecutionException, InterruptedException, TimeoutException {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        int maxWindow = 10;
        int maxWindowSizeBytes = 500;
        try {
            AheadOfTimeBlobDownloaderThrottler throttler = new AheadOfTimeBlobDownloaderThrottler(maxWindow, maxWindowSizeBytes);
            assertTrue(throttler.reserveSpaceForBlob(0, 100));
            assertEquals(maxWindowSizeBytes - 100, throttler.getAvailableWindowBytes());
            assertEquals(maxWindow - 1, throttler.getAvailableWindowSize());

            assertTrue(throttler.reserveSpaceForBlob(4, 300));
            assertEquals(maxWindowSizeBytes - 400, throttler.getAvailableWindowBytes());
            assertEquals(maxWindow - 2, throttler.getAvailableWindowSize());

            assertTrue(throttler.reserveSpaceForBlob(5, 100));
            assertEquals(0, throttler.getAvailableWindowBytes());
            assertEquals(maxWindow - 3, throttler.getAvailableWindowSize());

            // The prefetch window is full, so the next call to reserveSpaceForBlob should block. Do the call in a separate thread.
            AtomicBoolean spaceWasReservedAfterWait = new AtomicBoolean(false);
            CountDownLatch latch = new CountDownLatch(1);
            Future<?> f = executorService.submit((() -> {
                latch.countDown();
                try {
                    // This should block until the prefetch window is advanced
                    assertTrue(throttler.reserveSpaceForBlob(6, 10));
                    spaceWasReservedAfterWait.set(true);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));
            latch.await();
            // The thread above has started. Wait for a bit to make sure it's blocked
            Thread.sleep(50);
            // The worker thread cannot reserve space until the main thread advances the indexer
            assertFalse(spaceWasReservedAfterWait.get());
            // This should release the thread above
            throttler.advanceIndexer(0);
            f.get(100, TimeUnit.MILLISECONDS);
            // The thread should have advanced
            assertTrue(spaceWasReservedAfterWait.get());
            // Advance to the end
            throttler.advanceIndexer(6);
            // Check that the AOT download window is empty
            assertEquals(maxWindowSizeBytes, throttler.getAvailableWindowBytes());
            assertEquals(maxWindow, throttler.getAvailableWindowSize());
        } finally {
            new ExecutorCloser(executorService).close();
        }
    }

    @Test
    public void blockOnWindowFullCapacity() throws ExecutionException, InterruptedException, TimeoutException {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        int maxWindow = 10;
        int maxWindowSizeBytes = 500;
        try {
            AheadOfTimeBlobDownloaderThrottler throttler = new AheadOfTimeBlobDownloaderThrottler(maxWindow, maxWindowSizeBytes);
            // Fill the prefetch window
            for (int i = 0; i < maxWindow; i++) {
                assertTrue(throttler.reserveSpaceForBlob(i, 10));
            }
            assertEquals(maxWindowSizeBytes - 100, throttler.getAvailableWindowBytes());
            assertEquals(0, throttler.getAvailableWindowSize());
            AtomicBoolean spaceWasReservedAfterWait = new AtomicBoolean(false);
            CountDownLatch latch = new CountDownLatch(1);
            Future<?> f = executorService.submit((() -> {
                latch.countDown();
                try {
                    // This should block until the prefetch window is advanced
                    assertTrue(throttler.reserveSpaceForBlob(11, 10));
                    spaceWasReservedAfterWait.set(true);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));
            latch.await();
            // The thread above has started. Wait for a bit to make sure it's blocked
            Thread.sleep(50);
            assertFalse(spaceWasReservedAfterWait.get());
            // Advance one element, this should allow the worker thread to reserve space
            throttler.advanceIndexer(1);
            f.get(100, TimeUnit.MILLISECONDS);
            assertTrue(spaceWasReservedAfterWait.get());
            // Advance to the end
            throttler.advanceIndexer(11);
            // Check that the AOT download window is empty
            assertEquals(maxWindowSizeBytes, throttler.getAvailableWindowBytes());
            assertEquals(maxWindow, throttler.getAvailableWindowSize());
        } finally {
            new ExecutorCloser(executorService).close();
        }
    }

    @Test
    public void spaceReservationForPositionBehindIndexerIsIgnored() throws InterruptedException {
        AheadOfTimeBlobDownloaderThrottler throttler = new AheadOfTimeBlobDownloaderThrottler(10, 100);
        throttler.advanceIndexer(5);
        assertFalse(throttler.reserveSpaceForBlob(0, 10));
        assertFalse(throttler.reserveSpaceForBlob(1, 10));
        assertFalse(throttler.reserveSpaceForBlob(5, 10));
        assertEquals(100, throttler.getAvailableWindowBytes());
        assertEquals(10, throttler.getAvailableWindowSize());
        assertTrue(throttler.reserveSpaceForBlob(6, 10));
        assertEquals(90, throttler.getAvailableWindowBytes());
        assertEquals(9, throttler.getAvailableWindowSize());
    }

    @Test
    public void manyReservations() throws InterruptedException, ExecutionException {
        AheadOfTimeBlobDownloaderThrottler throttler = new AheadOfTimeBlobDownloaderThrottler(1024, 64 * 1024);
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        try {
            CountDownLatch latch = new CountDownLatch(1);
            Future<?> future = executorService.submit(() -> {
                Random random = new Random();
                latch.countDown();
                for (int i = 0; i < 500; i++) {
                    try {
                        throttler.reserveSpaceForBlob(i, 512 + random.nextInt(512));
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        fail("Should not have thrown an exception");
                    }
                }
            });

            latch.await();
            for (int i = 0; i < 500; i++) {
                throttler.advanceIndexer(i);
                Thread.sleep(1);
            }

            future.get();
            LOG.info("Stats: {}", throttler.formatStats());
        } finally {
            new ExecutorCloser(executorService).close();
        }
    }
}
