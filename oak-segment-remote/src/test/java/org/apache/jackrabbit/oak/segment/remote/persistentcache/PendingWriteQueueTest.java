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
package org.apache.jackrabbit.oak.segment.remote.persistentcache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.jackrabbit.oak.segment.remote.persistentcache.PersistentDiskCacheV2Async.PendingSegment;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PendingWriteQueueTest {
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @After
    public void tearDown() throws InterruptedException {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    public void zeroDelayPreservesImmediateFifoIncludingCapacityOne() throws Exception {
        PendingWriteQueue queue = new PendingWriteQueue(1, 1, 0);
        queue.put(segment(1));
        assertEquals(id(1), queue.take().segmentId);
        assertTrue(queue.offer(segment(2), 0, TimeUnit.SECONDS));
        assertEquals(id(2), queue.take().segmentId);
        assertEquals(0, queue.size());
    }

    @Test
    public void deadlineReleasesWithoutFurtherEnqueues() throws Exception {
        PendingWriteQueue queue = new PendingWriteQueue(4, 3, 200);
        long before = System.nanoTime();
        queue.put(segment(1));
        assertEquals(id(1), executor.submit(queue::take).get(5, TimeUnit.SECONDS).segmentId);
        assertTrue(System.nanoTime() - before >= TimeUnit.MILLISECONDS.toNanos(200));
    }

    @Test
    public void thresholdWakesWriterAndLeavesHeadroom() throws Exception {
        PendingWriteQueue queue = new PendingWriteQueue(4, 3, 60_000);
        Future<PendingSegment> first = executor.submit(queue::take);
        queue.put(segment(1));
        queue.put(segment(2));
        assertWaiting(first);
        queue.put(segment(3));
        assertEquals(id(1), first.get(5, TimeUnit.SECONDS).segmentId);
        assertEquals(2, queue.size());

        Future<PendingSegment> second = executor.submit(queue::take);
        assertWaiting(second); // Below threshold, the next entry must wait again.
        queue.put(segment(4));
        assertEquals(id(2), second.get(5, TimeUnit.SECONDS).segmentId);
        queue.startDraining();
        assertEquals(id(3), queue.take().segmentId);
        assertEquals(id(4), queue.take().segmentId);
    }

    @Test
    public void fullQueueBlocksProducerUntilSpaceIsAvailable() throws Exception {
        PendingWriteQueue queue = new PendingWriteQueue(3, 2, 60_000);
        queue.put(segment(1));
        queue.put(segment(2));
        queue.put(segment(3));
        Future<?> producer = executor.submit(() -> {
            queue.put(segment(4));
            return null;
        });
        assertWaiting(producer);
        assertEquals(3, queue.size());
        assertEquals(id(1), queue.take().segmentId);
        producer.get(5, TimeUnit.SECONDS);
        assertEquals(3, queue.size());
        queue.startDraining();
        for (int i = 2; i <= 4; i++) {
            assertEquals(id(i), queue.take().segmentId);
        }
    }

    @Test
    public void timedOfferHonorsCapacityAndWakesWhenSpaceIsAvailable() throws Exception {
        PendingWriteQueue queue = new PendingWriteQueue(2, 1, 60_000);
        queue.put(segment(1));
        queue.put(segment(2));
        assertFalse(queue.offer(segment(3), 0, TimeUnit.SECONDS));
        assertFalse(queue.offer(segment(3), 10, TimeUnit.MILLISECONDS));
        Future<Boolean> producer = executor.submit(() -> queue.offer(segment(3), 5, TimeUnit.SECONDS));
        assertWaiting(producer);
        assertEquals(id(1), queue.take().segmentId);
        assertTrue(producer.get(5, TimeUnit.SECONDS));
        assertEquals(id(2), queue.take().segmentId);
        assertEquals(id(3), queue.take().segmentId);
    }

    @Test
    public void drainingWakesWaitingWriterAndRejectsNewWrites() throws Exception {
        PendingWriteQueue queue = new PendingWriteQueue(4, 3, 60_000);
        queue.put(segment(1));
        Future<PendingSegment> writer = executor.submit(queue::take);
        assertWaiting(writer);
        queue.startDraining();
        assertEquals(id(1), writer.get(5, TimeUnit.SECONDS).segmentId);
        assertThrows(IllegalStateException.class, () -> queue.put(segment(2)));
    }

    @Test
    public void consumersDoNotDuplicateEntries() throws Exception {
        PendingWriteQueue queue = new PendingWriteQueue(8, 6, 0);
        List<Future<PendingSegment>> consumers = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            consumers.add(executor.submit(queue::take));
        }
        for (int i = 0; i < 20; i++) {
            queue.put(segment(i));
        }
        HashSet<UUID> received = new HashSet<>();
        for (Future<PendingSegment> consumer : consumers) {
            assertTrue(received.add(consumer.get(5, TimeUnit.SECONDS).segmentId));
        }
        assertEquals(20, received.size());
        assertEquals(0, queue.size());
    }

    @Test
    public void blockedOperationsAreInterruptible() throws Exception {
        PendingWriteQueue empty = new PendingWriteQueue(2, 1, 60_000);
        PendingWriteQueue delayed = new PendingWriteQueue(3, 2, 60_000);
        delayed.put(segment(1));
        PendingWriteQueue full = new PendingWriteQueue(1, 1, 0);
        full.put(segment(1));
        assertInterruptible(empty::take);
        assertInterruptible(delayed::take);
        assertInterruptible(() -> { full.put(segment(2)); return null; });
        assertInterruptible(() -> full.offer(segment(2), 1, TimeUnit.MINUTES));
    }

    @Test
    public void validatesConfigurationAndRejectsNullEntries() {
        assertThrows(IllegalArgumentException.class, () -> new PendingWriteQueue(0, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new PendingWriteQueue(4, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new PendingWriteQueue(4, 5, 0));
        assertThrows(IllegalArgumentException.class, () -> new PendingWriteQueue(4, 4, 1));
        assertThrows(IllegalArgumentException.class, () -> new PendingWriteQueue(4, 3, -1));
        PendingWriteQueue queue = new PendingWriteQueue(4, 3, 0);
        assertThrows(NullPointerException.class, () -> queue.put(null));
        assertThrows(NullPointerException.class, () -> queue.offer(null, 0, TimeUnit.SECONDS));
    }

    private static UUID id(int value) {
        return new UUID(0, value);
    }

    private static PendingSegment segment(int value) {
        return new PendingSegment(id(value), ByteBuffer.allocate(1));
    }

    private void assertInterruptible(java.util.concurrent.Callable<?> operation) throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        Future<?> future = executor.submit(() -> {
            started.countDown();
            try {
                operation.call();
                throw new AssertionError("Expected interruption");
            } catch (InterruptedException expected) {
                interrupted.countDown();
            }
            return null;
        });
        assertTrue(started.await(5, TimeUnit.SECONDS));
        assertWaiting(future);
        future.cancel(true);
        assertTrue(interrupted.await(5, TimeUnit.SECONDS));
    }

    private static void assertWaiting(Future<?> future) throws Exception {
        try {
            future.get(100, TimeUnit.MILLISECONDS);
            fail("Operation should still be waiting");
        } catch (TimeoutException expected) {
            // Still waiting for age, pressure, or capacity.
        }
    }
}
