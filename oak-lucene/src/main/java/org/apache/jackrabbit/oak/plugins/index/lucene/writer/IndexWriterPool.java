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
package org.apache.jackrabbit.oak.plugins.index.lucene.writer;

import org.apache.jackrabbit.guava.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.index.ConfigHelper;
import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.apache.lucene.index.IndexableField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class IndexWriterPool {
    private final static Logger LOG = LoggerFactory.getLogger(IndexWriterPool.class);

    public static final String OAK_INDEXER_PARALLEL_WRITER_MAX_BATCH_SIZE = "oak.indexer.parallelWriter.maxBatchSize";
    public static final int DEFAULT_OAK_INDEXER_PARALLEL_WRITER_MAX_BATCH_SIZE = 256;

    public static final String OAK_INDEXER_PARALLEL_WRITER_QUEUE_SIZE = "oak.indexer.parallelWriter.queueSize";
    public static final int DEFAULT_OAK_INDEXER_PARALLEL_WRITER_QUEUE_SIZE = 64;

    public static final String OAK_INDEXER_PARALLEL_WRITER_NUMBER_THREADS = "oak.indexer.parallelWriter.numberThreads";
    public static final int DEFAULT_OAK_INDEXER_PARALLEL_WRITER_NUMBER_THREADS = 4;

    private final int maxBatchSize = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PARALLEL_WRITER_MAX_BATCH_SIZE, DEFAULT_OAK_INDEXER_PARALLEL_WRITER_MAX_BATCH_SIZE);
    private final int queueSize = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PARALLEL_WRITER_QUEUE_SIZE, DEFAULT_OAK_INDEXER_PARALLEL_WRITER_QUEUE_SIZE);
    private final int numberOfThreads = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PARALLEL_WRITER_NUMBER_THREADS, DEFAULT_OAK_INDEXER_PARALLEL_WRITER_NUMBER_THREADS);

    // The batch of operations that will be sent to the workers.
    // Batching individual operations reduces the overhead of synchronization and context switching.
    private final ArrayList<Operation> batch = new ArrayList<>(maxBatchSize);
    // Shared queue between producer and workers
    private final BlockingQueue<OperationBatch> queue = new ArrayBlockingQueue<>(queueSize);
    private final List<Worker> workers;
    private final List<Future<?>> workerFutures;
    private final ExecutorService writersPool;
    // Used to schedule a task that periodically prints statistics
    private final ScheduledExecutorService monitorTaskExecutor;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Used to keep track of the sequence number of the batches that are currently being processed.
    // This is used to wait until all operations for a writer are processed before closing it.
    private final Object pendingBatchesLock = new Object();
    private final HashSet<Long> pendingBatches = new HashSet<>();
    private long batchSequenceNumber = 0;

    // Statistics
    private final long startTimeNanos = System.nanoTime();
    private long updateCount = 0;
    private long deleteCount = 0;
    private long totalEnqueueTimeNanos = 0;

    private static class OperationBatch {
        final long sequenceNumber;
        final Operation[] operations;

        public OperationBatch(long sequenceNumber, Operation[] operations) {
            Objects.requireNonNull(operations);
            this.sequenceNumber = sequenceNumber;
            this.operations = operations;
        }
    }

    private static abstract class Operation {
        final LuceneIndexWriter delegate;

        public Operation(LuceneIndexWriter delegate) {
            this.delegate = delegate;
        }

        abstract void execute() throws IOException;
    }

    private static class UpdateOperation extends Operation {
        private final String path;
        private final Iterable<? extends IndexableField> doc;

        UpdateOperation(LuceneIndexWriter delegate, String path, Iterable<? extends IndexableField> doc) {
            super(delegate);
            this.path = path;
            this.doc = doc;
        }

        @Override
        public void execute() throws IOException {
            delegate.updateDocument(path, doc);
        }
    }

    private static class DeleteOperation extends Operation {
        private final String path;

        DeleteOperation(LuceneIndexWriter delegate, String path) {
            super(delegate);
            this.path = path;
        }

        @Override
        public void execute() throws IOException {
            delegate.deleteDocuments(path);
        }
    }

    private static class CloseResult {
        // Either result or error is non-null. The two constructors enforce this invariant.
        final Boolean result;
        final Throwable error;

        CloseResult(boolean result) {
            this.result = result;
            this.error = null;
        }

        CloseResult(Throwable error) {
            this.result = null;
            this.error = error;
        }

        @Override
        public String toString() {
            return "CloseResult{" +
                    "result=" + result +
                    ", error=" + error +
                    '}';
        }
    }

    private static class CloseWriterOperation extends Operation {
        private final long timestamp;
        private final SynchronousQueue<CloseResult> sync;

        /**
         * The close operation should be synchronous and applied only after all the write operations for this writer
         * are processed.
         *
         * @param sync A synchronous queue used to wait for the result of the close operation.
         */
        CloseWriterOperation(LuceneIndexWriter delegate, long timestamp, SynchronousQueue<CloseResult> sync) {
            super(delegate);
            this.timestamp = timestamp;
            this.sync = sync;
        }

        @Override
        public void execute() {
            try {
                try {
                    boolean closeResult = delegate.close(timestamp);
                    sync.put(new CloseResult(closeResult));
                } catch (IOException e) {
                    sync.put(new CloseResult(e));
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // Sentinel used to terminate worker threads
    final static OperationBatch SHUTDOWN = new OperationBatch(-1, new Operation[0]);

    private class Worker implements Runnable {
        private final long startTime = System.nanoTime();
        private final int id;
        private long opCount = 0;
        private long batchesProcessed = 0;
        private long totalBusyTime = 0;

        public Worker(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            String oldName = Thread.currentThread().getName();
            Thread.currentThread().setName("index-writer-" + id);
            try {
                LOG.info("[{}] Worker started", id);
                while (true) {
                    OperationBatch op = queue.take();
                    if (op == SHUTDOWN) {
                        queue.add(SHUTDOWN);
                        LOG.info("[{}] Shutting down worker", id);
                        return;
                    }
                    long start = System.nanoTime();
                    for (Operation operation : op.operations) {
                        operation.execute();
                        opCount++;
                    }
                    batchesProcessed++;
                    long durationNanos = System.nanoTime() - start;
                    totalBusyTime += durationNanos;
                    long durationMillis = durationNanos / 1_000_000;
                    if (durationMillis > 1000) {
                        LOG.info("[{}] Processing batch {} of size {} took {} millis.",
                                id, op.sequenceNumber, op.operations.length, durationMillis);
                    }
                    synchronized (pendingBatchesLock) {
                        pendingBatches.remove(op.sequenceNumber);
                        pendingBatchesLock.notifyAll();
                    }
                }
            } catch (InterruptedException e) {
                LOG.warn("[{}] Interrupted while waiting for an index write operation", id, e);
                throw new RuntimeException(e);
            } catch (Throwable t) {
                LOG.error("[{}] Error while processing an index write operation", id, t);
                throw new RuntimeException(t);
            } finally {
                Thread.currentThread().setName(oldName);
            }
        }

        void printStatistics() {
            double busyTimePercentage = FormattingUtils.safeComputePercentage(totalBusyTime, System.nanoTime() - startTime);
            String busyTimePercentageStr = String.format("%.2f", busyTimePercentage);
            LOG.info("[{}] operationsProcessed: {}, batchesProcessed: {}, busyTime: {} ms ({}%)",
                    id, opCount, batchesProcessed, totalBusyTime / 1_000_000, busyTimePercentageStr);
        }
    }

    /**
     * Creates and starts a pool of writer threads.
     * <p>
     * WARN: This is not thread safe.
     */
    public IndexWriterPool() {
        this.writersPool = Executors.newFixedThreadPool(numberOfThreads, new ThreadFactoryBuilder()
                .setDaemon(true)
                .build());
        this.monitorTaskExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("index-writer-monitor")
                .build());
        this.workers = IntStream.range(0, numberOfThreads)
                .mapToObj(Worker::new)
                .collect(Collectors.toList());
        this.workerFutures = workers.stream()
                .map(writersPool::submit)
                .collect(Collectors.toList());
        monitorTaskExecutor.scheduleAtFixedRate(this::printStatistics, 30, 30, TimeUnit.SECONDS);
        LOG.info("Writing thread started");
    }

    public void updateDocument(LuceneIndexWriter writer, String path, Iterable<? extends IndexableField> doc) throws IOException {
        checkOpen();
        this.updateCount++;
        enqueueOperation(new UpdateOperation(writer, path, doc));
    }

    public void deleteDocuments(LuceneIndexWriter writer, String path) throws IOException {
        checkOpen();
        this.deleteCount++;
        enqueueOperation(new DeleteOperation(writer, path));
    }

    public boolean closeWriter(LuceneIndexWriter writer, long timestamp) throws IOException {
        checkOpen();
        try {
            LOG.debug("Closing writer: {}", writer);
            // Before closing the writer, we must wait until all previously submitted operations for
            // this writer are processed. For simplicity, we wait instead until ALL operations currently
            // in the queue are processed, because otherwise it would be more complex to distinguish which
            // operations are for which writer.
            long seqNumber = flushBatch();
            LOG.debug("All pending operations enqueued. Waiting until all batches up to {} are processed", seqNumber);
            synchronized (pendingBatchesLock) {
                while (true) {
                    Long earliestPending = pendingBatches.isEmpty() ? null : pendingBatches.stream().min(Long::compareTo).get();
                    LOG.debug("Earliest pending batch: {}. Waiting until all batches up to {} are processed", earliestPending, seqNumber);
                    if (earliestPending == null || earliestPending > seqNumber) {
                        break;
                    }
                    pendingBatchesLock.wait();
                }
            }
            LOG.debug("All batches up to {} processed. Enqueuing close operation for writer {}", seqNumber, writer);
            SynchronousQueue<CloseResult> closeOpSync = new SynchronousQueue<>();
            batch.add(new CloseWriterOperation(writer, timestamp, closeOpSync));
            flushBatch();
            CloseResult res = closeOpSync.take();
            LOG.debug("Writer {} closed. Result: {}", writer, res);
            if (res.error == null) {
                return res.result;
            } else {
                throw new IOException("Error while closing writer", res.error);
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for the worker to finish", e);
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            flushBatch();
            queue.add(SHUTDOWN);
            LOG.info("Shutting down PipelinedIndexWriter. Total enqueue time: {} ms", totalEnqueueTimeNanos / 1_000_000);
            for (Future<?> f : workerFutures) {
                LOG.info("Waiting for future: {}", f);
                try {
                    f.get();
                } catch (InterruptedException | ExecutionException e) {
                    LOG.info("Error while waiting for future", e);
                }
            }
            printStatistics();
            new ExecutorCloser(writersPool, 1, TimeUnit.SECONDS).close();
            new ExecutorCloser(monitorTaskExecutor, 1, TimeUnit.SECONDS).close();
        } else {
            LOG.warn("PipelinedIndexWriter already closed");
        }
    }

    private void enqueueOperation(Operation op) {
        batch.add(op);
        if (batch.size() == maxBatchSize) {
            flushBatch();
        }
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateException("PipelinedIndexWriter is closed");
        }
    }

    private long flushBatch() {
        // Empty batches are also enqueued. This is necessary for the close writer operation, which requires all previous
        // operations for the writer to be processed before the writer is closed. This means that all enqueued batches
        // must be processed and the current partially built batch must also be enqueued and processed. To ensure that,
        // the close operation will always enqueue the current batch and wait for all the batches older or equal to the
        // newly enqueued batch to be processed. If there are no operations in the currently pending batch, we enqueue
        // it anyway just to generate a new sequence number.
        try {
            long seqNumber;
            synchronized (pendingBatchesLock) {
                // Shared between producer and workers
                seqNumber = batchSequenceNumber;
                batchSequenceNumber++;
                pendingBatches.add(seqNumber);
            }
            if (seqNumber % 1000 == 0) {
                LOG.info("Enqueuing batch {}, size: {}", seqNumber, batch.size());
            }
            long start = System.nanoTime();
            queue.put(new OperationBatch(seqNumber, batch.toArray(new Operation[0])));
            long durationNanos = System.nanoTime() - start;
            long durationMillis = durationNanos / 1_000_000;
            totalEnqueueTimeNanos += durationNanos;
            if (durationMillis > 1) {
                LOG.info("Enqueuing batch delayed. Seq number: {}, size: {}. Delay: {} ms",
                        seqNumber, batch.size(), durationMillis);
            }
            batch.clear();
            return seqNumber;
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting to put an update operation in the queue", e);
            throw new RuntimeException(e);
        }
    }

    private void printStatistics() {
        double percentageEnqueueTime = FormattingUtils.safeComputePercentage(totalEnqueueTimeNanos, System.nanoTime() - startTimeNanos);
        String percentageEnqueueTimeStr = String.format("%.2f", percentageEnqueueTime);
        LOG.info("updateCount: {}, deleteCount: {}, batchesEnqueuedCount: {}, pendingBatchesCount: {},  enqueueTime: {} ms ({}%)",
                updateCount, deleteCount, batchSequenceNumber, pendingBatches.size(), totalEnqueueTimeNanos / 1_000_000, percentageEnqueueTimeStr);
        workers.forEach(Worker::printStatistics);
    }
}
