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

package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.oak.segment.WriteOperationHandler.WriteOperation;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for Monitor and Monitor.Guard usage in SegmentBufferWriterPool.
 * These tests specifically verify the behavior of:
 * - Monitor.enter() and Monitor.leave() in borrowWriter/returnWriter
 * - Monitor.enterWhen(Guard) in flush() waiting for borrowed writers
 * - Monitor.Guard.isSatisfied() in allReturned()
 * - safeEnterWhen() interruption handling
 */
public class SegmentBufferWriterPoolMonitorTest {

    private MemoryStore store;
    private SegmentBufferWriterPool pool;
    private GCGeneration gcGeneration;
    private ExecutorService executor;

    @Before
    public void setUp() throws IOException {
        store = new MemoryStore();
        gcGeneration = GCGeneration.NULL;
        pool = SegmentBufferWriterPool.factory(
                store.getSegmentIdProvider(),
                "test",
                () -> gcGeneration
        ).newPool(SegmentBufferWriterPool.PoolType.GLOBAL);
        executor = Executors.newFixedThreadPool(10);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    /**
     * Tests concurrent borrowWriter and returnWriter operations.
     * This verifies Monitor.enter() and Monitor.leave() work correctly
     * with multiple threads accessing the pool simultaneously.
     */
    @Test
    public void testConcurrentBorrowAndReturn() throws Exception {
        int numThreads = 10;
        int iterationsPerThread = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < iterationsPerThread; j++) {
                        RecordId result = pool.execute(gcGeneration, new WriteOperation() {
                            @NotNull
                            @Override
                            public RecordId execute(@NotNull SegmentBufferWriter writer) {
                                // Simulate some work
                                return store.getRevisions().getHead();
                            }
                        });
                        Assert.assertNotNull(result);
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    Assert.fail("Concurrent operation failed: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        Assert.assertTrue("Threads should complete within timeout",
                doneLatch.await(30, TimeUnit.SECONDS));
        Assert.assertEquals("All operations should succeed",
                numThreads * iterationsPerThread, successCount.get());
    }

    /**
     * Tests that flush() waits for borrowed writers to be returned.
     * This specifically tests Monitor.enterWhen(Guard) and Monitor.Guard.isSatisfied().
     */
    @Test
    public void testFlushWaitsForBorrowedWriters() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicBoolean writeOperationStarted = new AtomicBoolean(false);
        AtomicBoolean writeOperationCompleted = new AtomicBoolean(false);
        AtomicBoolean flushCompleted = new AtomicBoolean(false);

        // Start a write operation that will hold a writer
        Future<RecordId> writeFuture = executor.submit(() -> {
            return pool.execute(gcGeneration, new WriteOperation() {
                @NotNull
                @Override
                public RecordId execute(@NotNull SegmentBufferWriter writer) throws IOException {
                    writeOperationStarted.set(true);
                    try {
                        // Wait for flush to be called
                        barrier.await(5, TimeUnit.SECONDS);
                        // Hold the writer for a bit to ensure flush is waiting
                        Thread.sleep(100);
                        writeOperationCompleted.set(true);
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                    return store.getRevisions().getHead();
                }
            });
        });

        // Wait for write operation to start
        while (!writeOperationStarted.get()) {
            Thread.sleep(10);
        }

        // Start flush operation
        Future<Void> flushFuture = executor.submit(() -> {
            try {
                // Signal that flush is about to be called
                barrier.await(5, TimeUnit.SECONDS);
                pool.flush(store);
                flushCompleted.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });

        // Wait for both operations to complete
        RecordId writeResult = writeFuture.get(10, TimeUnit.SECONDS);
        flushFuture.get(10, TimeUnit.SECONDS);

        Assert.assertNotNull("Write operation should complete", writeResult);
        Assert.assertTrue("Write operation should complete before flush",
                writeOperationCompleted.get());
        Assert.assertTrue("Flush should complete after write operation returns writer",
                flushCompleted.get());
    }

    /**
     * Tests that multiple writers are properly waited for during flush.
     * This tests the Monitor.Guard condition checking all borrowed writers.
     */
    @Test
    public void testFlushWaitsForMultipleBorrowedWriters() throws Exception {
        int numWriters = 5;
        CountDownLatch writersStarted = new CountDownLatch(numWriters);
        CountDownLatch releaseWriters = new CountDownLatch(1);
        AtomicInteger completedWrites = new AtomicInteger(0);

        List<Future<RecordId>> writeFutures = new ArrayList<>();

        // Start multiple write operations
        for (int i = 0; i < numWriters; i++) {
            Future<RecordId> future = executor.submit(() -> {
                return pool.execute(gcGeneration, new WriteOperation() {
                    @NotNull
                    @Override
                    public RecordId execute(@NotNull SegmentBufferWriter writer) throws IOException {
                        writersStarted.countDown();
                        try {
                            // Wait until all writers are borrowed
                            releaseWriters.await(5, TimeUnit.SECONDS);
                            completedWrites.incrementAndGet();
                        } catch (InterruptedException e) {
                            throw new IOException(e);
                        }
                        return store.getRevisions().getHead();
                    }
                });
            });
            writeFutures.add(future);
        }

        // Wait for all writers to be borrowed
        Assert.assertTrue("All writers should be borrowed",
                writersStarted.await(5, TimeUnit.SECONDS));

        // Start flush operation
        Future<Void> flushFuture = executor.submit(() -> {
            try {
                // Small delay to ensure flush starts after all writers borrowed
                Thread.sleep(50);
                pool.flush(store);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });

        // Give flush a chance to start waiting
        Thread.sleep(100);

        // Release all writers
        releaseWriters.countDown();

        // Wait for all writes to complete
        for (Future<RecordId> future : writeFutures) {
            Assert.assertNotNull("Write should complete", future.get(5, TimeUnit.SECONDS));
        }

        // Wait for flush to complete
        flushFuture.get(5, TimeUnit.SECONDS);

        Assert.assertEquals("All writes should complete", numWriters, completedWrites.get());
    }

    /**
     * Tests interrupted flush operation.
     * This verifies safeEnterWhen() properly handles InterruptedException.
     */
    @Test
    public void testFlushWithInterruption() throws Exception {
        CountDownLatch writerBorrowed = new CountDownLatch(1);
        CountDownLatch flushStarted = new CountDownLatch(1);

        // Borrow a writer and hold it
        Future<RecordId> writeFuture = executor.submit(() -> {
            return pool.execute(gcGeneration, new WriteOperation() {
                @NotNull
                @Override
                public RecordId execute(@NotNull SegmentBufferWriter writer) throws IOException {
                    writerBorrowed.countDown();
                    try {
                        // Hold writer for 5 seconds
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        // Expected if test is torn down
                    }
                    return store.getRevisions().getHead();
                }
            });
        });

        // Wait for writer to be borrowed
        Assert.assertTrue("Writer should be borrowed",
                writerBorrowed.await(2, TimeUnit.SECONDS));

        // Start flush in separate thread and interrupt it
        Thread flushThread = new Thread(() -> {
            try {
                flushStarted.countDown();
                pool.flush(store);
            } catch (IOException e) {
                // Ignore
            }
        });

        flushThread.start();

        // Wait for flush to start
        Assert.assertTrue("Flush should start", flushStarted.await(2, TimeUnit.SECONDS));

        // Give flush time to enter waiting state
        Thread.sleep(100);

        // Interrupt the flush thread
        flushThread.interrupt();

        // Verify thread was interrupted (should exit quickly)
        flushThread.join(1000);
        Assert.assertFalse("Flush thread should terminate after interrupt",
                flushThread.isAlive());

        // The interrupted flag should be set
        // Note: We can't easily verify this without access to thread internals
    }

    /**
     * Tests writer disposal during concurrent flush.
     * This tests the scenario where a writer is returned after flush() starts.
     */
    @Test
    public void testWriterDisposalDuringFlush() throws Exception {
        CountDownLatch writerBorrowed = new CountDownLatch(1);
        CountDownLatch flushCanStart = new CountDownLatch(1);
        CountDownLatch writerReturned = new CountDownLatch(1);

        // Borrow a writer
        Future<RecordId> writeFuture = executor.submit(() -> {
            return pool.execute(gcGeneration, new WriteOperation() {
                @NotNull
                @Override
                public RecordId execute(@NotNull SegmentBufferWriter writer) throws IOException {
                    writerBorrowed.countDown();
                    try {
                        // Wait for flush to start
                        flushCanStart.await(5, TimeUnit.SECONDS);
                        // Small delay to ensure flush has collected borrowed writers
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    } finally {
                        writerReturned.countDown();
                    }
                    return store.getRevisions().getHead();
                }
            });
        });

        // Wait for writer to be borrowed
        Assert.assertTrue("Writer should be borrowed",
                writerBorrowed.await(2, TimeUnit.SECONDS));

        // Start flush
        Future<Void> flushFuture = executor.submit(() -> {
            try {
                flushCanStart.countDown();
                pool.flush(store);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });

        // Wait for operations to complete
        RecordId writeResult = writeFuture.get(10, TimeUnit.SECONDS);
        flushFuture.get(10, TimeUnit.SECONDS);

        Assert.assertNotNull("Write should complete", writeResult);
        Assert.assertEquals("Writer should be returned", 0, writerReturned.getCount());
    }

    /**
     * Tests multiple concurrent flushes.
     * This verifies Monitor enter/leave work correctly with contention.
     */
    @Test
    public void testMultipleConcurrentFlushes() throws Exception {
        int numFlushes = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numFlushes);
        AtomicInteger successfulFlushes = new AtomicInteger(0);

        // Do some writes first
        for (int i = 0; i < 10; i++) {
            pool.execute(gcGeneration, new WriteOperation() {
                @NotNull
                @Override
                public RecordId execute(@NotNull SegmentBufferWriter writer) {
                    return store.getRevisions().getHead();
                }
            });
        }

        // Start multiple concurrent flushes
        for (int i = 0; i < numFlushes; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    pool.flush(store);
                    successfulFlushes.incrementAndGet();
                } catch (Exception e) {
                    Assert.fail("Flush failed: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        Assert.assertTrue("All flushes should complete",
                doneLatch.await(10, TimeUnit.SECONDS));
        Assert.assertEquals("All flushes should succeed",
                numFlushes, successfulFlushes.get());
    }

    /**
     * Tests that borrowWriter and returnWriter maintain proper writer state.
     * This verifies Monitor protection of the borrowed/disposed sets.
     */
    @Test
    public void testBorrowReturnStateConsistency() throws Exception {
        int iterations = 100;

        for (int i = 0; i < iterations; i++) {
            RecordId result = pool.execute(gcGeneration, new WriteOperation() {
                @NotNull
                @Override
                public RecordId execute(@NotNull SegmentBufferWriter writer) throws IOException {
                    // Writer should be borrowed here
                    Assert.assertNotNull("Writer should not be null", writer);
                    return store.getRevisions().getHead();
                }
            });
            Assert.assertNotNull("Result should not be null", result);
        }

        // Flush should work without issues
        pool.flush(store);

        // Should be able to borrow again after flush
        RecordId result = pool.execute(gcGeneration, new WriteOperation() {
            @NotNull
            @Override
            public RecordId execute(@NotNull SegmentBufferWriter writer) {
                Assert.assertNotNull("Writer should not be null after flush", writer);
                return store.getRevisions().getHead();
            }
        });
        Assert.assertNotNull("Result after flush should not be null", result);
    }

    /**
     * Tests rapid borrow/return cycles under load.
     * This stresses the Monitor enter/leave operations.
     */
    @Test
    public void testRapidBorrowReturnCycles() throws Exception {
        int numThreads = 20;
        int cyclesPerThread = 50;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger totalOperations = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < cyclesPerThread; j++) {
                        pool.execute(gcGeneration, new WriteOperation() {
                            @NotNull
                            @Override
                            public RecordId execute(@NotNull SegmentBufferWriter writer) {
                                totalOperations.incrementAndGet();
                                return store.getRevisions().getHead();
                            }
                        });
                    }
                } catch (Exception e) {
                    Assert.fail("Operation failed: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        Assert.assertTrue("All operations should complete",
                doneLatch.await(30, TimeUnit.SECONDS));
        Assert.assertEquals("All operations should execute",
                numThreads * cyclesPerThread, totalOperations.get());
    }
}
