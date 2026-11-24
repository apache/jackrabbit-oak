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
package org.apache.jackrabbit.oak.segment.remote.queue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.jackrabbit.oak.segment.remote.RemoteSegmentArchiveEntry;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.InstanceField;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;

public class SegmentWriteQueueTest {

    private static final byte[] EMPTY_DATA = new byte[0];

    private SegmentWriteQueue queue;

    private SegmentWriteQueue queueBlocked;

    @After
    public void shutdown() throws IOException {
        if (queue != null) {
            queue.close();
        }

        if (queueBlocked != null) {
            queueBlocked.close();
        }
    }

    @Test
    public void testThreadInterruptedWhileAddigToQueue() throws InterruptedException, NoSuchFieldException {

        Set<UUID> added = Collections.synchronizedSet(new HashSet<>());
        Semaphore semaphore = new Semaphore(0);


        BlockingDeque<SegmentWriteAction> queue = Mockito.mock(BlockingDeque.class);

        queueBlocked = new SegmentWriteQueue((tarEntry, data, offset, size) -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
            }
            added.add(new UUID(tarEntry.getMsb(), tarEntry.getLsb()));
        });

        Mockito.when(queue.offer(any(SegmentWriteAction.class), anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException());
        new InstanceField(queueBlocked.getClass().getDeclaredField("queue"), queueBlocked).set(queue);

        try {
            queueBlocked.addToQueue(tarEntry(0), EMPTY_DATA, 0, 0);
            fail("IOException should have been thrown");
        } catch (IOException e) {
            assertEquals(e.getCause().getClass(), InterruptedException.class);
        }

        semaphore.release(Integer.MAX_VALUE);

        AtomicBoolean flushFinished = new AtomicBoolean(false);
        Thread flusher = runInThread(() -> {
            try {
                queueBlocked.flush();
                flushFinished.set(true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        Thread.sleep(1000);

        assertEquals("Flush thread should have been completed till now", Thread.State.TERMINATED, flusher.getState());
        assertTrue("Segment queue is empty", flushFinished.get());
    }

    @Test
    public void testQueue() throws IOException, InterruptedException {
        Set<UUID> added = Collections.synchronizedSet(new HashSet<>());
        Semaphore semaphore = new Semaphore(0);
        queue = new SegmentWriteQueue((tarEntry, data, offset, size) -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
            }
            added.add(new UUID(tarEntry.getMsb(), tarEntry.getLsb()));
        });

        for (int i = 0; i < 10; i++) {
            queue.addToQueue(tarEntry(i), EMPTY_DATA, 0, 0);
        }

        for (int i = 0; i < 10; i++) {
            assertNotNull("Segments should be available for read", queue.read(uuid(i)));
        }
        assertFalse("Queue shouldn't be empty", queue.isEmpty());

        semaphore.release(Integer.MAX_VALUE);
        awaitUntil(() -> queue.isEmpty());

        assertEquals("There should be 10 segments consumed", 10, added.size());
        for (int i = 0; i < 10; i++) {
            assertTrue("Missing consumed segment", added.contains(uuid(i)));
        }
    }

    @Test(timeout = 1000)
    public void testFlush() throws IOException, InterruptedException {
        Set<UUID> added = new CopyOnWriteArraySet<>();
        Semaphore semaphore = new Semaphore(0);
        queue = new SegmentWriteQueue((tarEntry, data, offset, size) -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
            }
            added.add(new UUID(tarEntry.getMsb(), tarEntry.getLsb()));
        });

        for (int i = 0; i < 3; i++) {
            queue.addToQueue(tarEntry(i), EMPTY_DATA, 0, 0);
        }

        AtomicBoolean flushFinished = new AtomicBoolean(false);
        Set<UUID> addedAfterFlush = new HashSet<>();
        runInThread(() -> {
            try {
                queue.flush();
                addedAfterFlush.addAll(added);
                flushFinished.set(true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        Thread.sleep(100);
        assertFalse("Flush should be blocked", flushFinished.get());

        AtomicBoolean addFinished = new AtomicBoolean(false);
        runInThread(() -> {
            try {
                queue.addToQueue(tarEntry(10), EMPTY_DATA, 0, 0);
                addFinished.set(true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        Thread.sleep(100);
        assertFalse("Adding segments should be blocked until the flush is finished", addFinished.get());

        semaphore.release(Integer.MAX_VALUE);

        awaitUntil(addFinished);
        awaitUntil(flushFinished);
        assertTrue("Flush should be finished", flushFinished.get());
        assertTrue("Adding segments should be blocked until the flush is finished", addFinished.get());

        Set<UUID> expectedAddedAfterFlush = IntStream.range(0, 3)
            .mapToObj(SegmentWriteQueueTest::uuid)
            .collect(toSet());

        assertTrue("Expected all values of " + expectedAddedAfterFlush + " to be in " + addedAfterFlush,
            addedAfterFlush.containsAll(expectedAddedAfterFlush));
    }

    @Test(expected = IllegalStateException.class)
    public void testClose() throws IOException {
        queue = new SegmentWriteQueue((tarEntry, data, offset, size) -> {});
        queue.close();
        queue.addToQueue(tarEntry(10), EMPTY_DATA, 0, 0);
    }

    @Test
    public void testRecoveryMode() throws IOException, InterruptedException {
        Set<UUID> added = Collections.synchronizedSet(new HashSet<>());
        Semaphore semaphore = new Semaphore(0);
        AtomicBoolean doBreak = new AtomicBoolean(true);
        List<Long> writeAttempts = Collections.synchronizedList(new ArrayList<>());
        queue = new SegmentWriteQueue((tarEntry, data, offset, size) -> {
            writeAttempts.add(System.currentTimeMillis());
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
            }
            if (doBreak.get()) {
                throw new IOException();
            }
            added.add(new UUID(tarEntry.getMsb(), tarEntry.getLsb()));
        });

        for (int i = 0; i < 10; i++) {
            queue.addToQueue(tarEntry(i), EMPTY_DATA, 0, 0);
        }

        semaphore.release(Integer.MAX_VALUE);
        assertTrue("All failed writes should be in the retry queue", awaitCondition(() -> queue.getSize() == 10));
        assertTrue(queue.isBroken());
        assertEquals(10, queue.getSize()); // the retrying segment remains in the retry queue

        writeAttempts.clear();
        awaitWhile(() -> writeAttempts.size() < 5);
        long lastAttempt = writeAttempts.get(0);
        for (int i = 1; i < 5; i++) {
            long delay = writeAttempts.get(i) - lastAttempt;
            assertTrue("The delay between retry attempts should be at least 1000ms. Actual: " + delay, delay >= 1000);
            lastAttempt = writeAttempts.get(i);
        }

        AtomicBoolean addFinished = new AtomicBoolean(false);
        runInThread(() -> {
            try {
                queue.addToQueue(tarEntry(10), EMPTY_DATA, 0, 0);
                addFinished.set(true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        Thread.sleep(100);
        assertFalse("Adding segments should wait while the retry queue is non-empty", addFinished.get());

        doBreak.set(false);
        awaitWhile(queue::isBroken);
        assertFalse("The retry queue should be empty after recovery", queue.isBroken());

        awaitWhile(() -> added.size() < 11);
        assertEquals("All segments should be consumed", 11, added.size());
        for (int i = 0; i < 11; i++) {
            assertTrue("All segments should be consumed", added.contains(uuid(i)));
        }

        int i = writeAttempts.size() - 10;
        lastAttempt = writeAttempts.get(i);
        for (; i < writeAttempts.size(); i++) {
            long delay = writeAttempts.get(i) - lastAttempt;
            assertTrue("Segments should be persisted immediately", delay < 1000);
            lastAttempt = writeAttempts.get(i);
        }
    }

    @Test
    public void testRuntimeExceptionInSegmentConsumer() throws InterruptedException, IOException {

        Set<UUID> added = Collections.synchronizedSet(new HashSet<>());
        AtomicBoolean doBreak = new AtomicBoolean(true);
        queue = new SegmentWriteQueue((tarEntry, data, offset, size) -> {
            //simulate runtime exception that can happen while writing to the remote repository
            if (doBreak.get()) {
                throw new RuntimeException();
            }

            added.add(new UUID(tarEntry.getMsb(), tarEntry.getLsb()));
        });

        queue.addToQueue(tarEntry(0), EMPTY_DATA, 0, 0);
        queue.addToQueue(tarEntry(1), EMPTY_DATA, 0, 0);
        queue.addToQueue(tarEntry(2), EMPTY_DATA, 0, 0);

        AtomicBoolean flushFinished = new AtomicBoolean(false);
        runInThread(() -> {
            try {
                queue.flush();
                flushFinished.set(true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        Thread.sleep(100);

        assertFalse("Flush thread should not be finished", flushFinished.get());
        assertEquals(0, added.size());

        //Stop throwing runtime exception
        doBreak.set(false);

        //give enough time to emergency thread to wake up
        Thread.sleep(1200);

        assertTrue("Segment queue should be empty", flushFinished.get());
        assertEquals(3, added.size());
    }

    @Test(timeout = 10000)
    public void testFlushTimesOutWithConcurrentFailuresAndSmallQueues() throws Exception {
        int parallelConsumers = 2;
        AtomicBoolean failWrites = new AtomicBoolean(true);
        Set<UUID> initialWrites = ConcurrentHashMap.newKeySet();
        Set<UUID> persisted = ConcurrentHashMap.newKeySet();
        CountDownLatch initialWritesStarted = new CountDownLatch(parallelConsumers);
        CountDownLatch releaseInitialWrites = new CountDownLatch(1);

        queue = new SegmentWriteQueue((tarEntry, data, offset, size) -> {
            UUID id = new UUID(tarEntry.getMsb(), tarEntry.getLsb());
            if (failWrites.get()) {
                if (id.getLeastSignificantBits() < parallelConsumers && initialWrites.add(id)) {
                    initialWritesStarted.countDown();
                    awaitLatch(releaseInitialWrites);
                }
                throw new IOException("Simulated concurrent persistence failure");
            }
            persisted.add(id);
        }, 1, parallelConsumers, 250);

        try {
            for (int i = 0; i < parallelConsumers; i++) {
                queue.addToQueue(tarEntry(i), EMPTY_DATA, 0, 0);
            }
            assertTrue("Both main workers should be writing concurrently",
                    initialWritesStarted.await(5, TimeUnit.SECONDS));

            queue.addToQueue(tarEntry(parallelConsumers), EMPTY_DATA, 0, 0);
            assertEquals("The normal queue should be full", 1, queue.getSize());

            FutureTask<Void> flushTask = new FutureTask<>(() -> {
                queue.flush();
                return null;
            });
            Thread flusher = new Thread(flushTask, "segment-write-queue-flush-test");
            flusher.setDaemon(true);
            flusher.start();
            releaseInitialWrites.countDown();

            try {
                flushTask.get(2, TimeUnit.SECONDS);
                fail("flush should time out while every write is failing");
            } catch (ExecutionException e) {
                assertTrue("flush should report an IOException", e.getCause() instanceof IOException);
                assertTrue(e.getCause().getMessage().contains("No segment write completed"));
            } catch (TimeoutException e) {
                fail("flush should enforce its no-progress deadline");
            }
        } finally {
            failWrites.set(false);
            releaseInitialWrites.countDown();
        }

        assertTrue("Pending retries should drain once the writer recovers", awaitCondition(queue::isEmpty));
        queue.flush();
        assertEquals(parallelConsumers + 1, persisted.size());
    }

    private static void awaitLatch(CountDownLatch latch) throws IOException {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting for test latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private boolean awaitCondition(Supplier<Boolean> condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.get() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        return condition.get();
    }

    private static RemoteSegmentArchiveEntry tarEntry(long i) {
        return new RemoteSegmentArchiveEntry(0, i, 0, 0, 0, 0, false);
    }

    private static UUID uuid(long i) {
        return new UUID(0, i);
    }

    private void awaitUntil(AtomicBoolean condition) throws InterruptedException {
        awaitUntil(condition::get);
    }

    private void awaitUntil(Supplier<Boolean> condition) throws InterruptedException {
        awaitWhile(() -> !condition.get());
    }

    private void awaitWhile(Supplier<Boolean> condition) throws InterruptedException {
        int maxAttempts = 5000;
        int attempts = 0;
        while (condition.get() && attempts < maxAttempts) {
            attempts++;
            Thread.sleep(10);
        }
    }

    private static Thread runInThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.start();
        return thread;
    }
}
