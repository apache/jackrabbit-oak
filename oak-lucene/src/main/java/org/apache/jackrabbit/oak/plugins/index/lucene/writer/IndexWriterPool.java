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
import org.apache.jackrabbit.oak.plugins.index.ThreadMonitor;
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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class IndexWriterPool {
    private final static Logger LOG = LoggerFactory.getLogger(IndexWriterPool.class);
    private final static ThreadMonitor threadMonitor = new ThreadMonitor();

    public static final String OAK_INDEXER_PARALLEL_WRITER_MAX_BATCH_SIZE = "oak.indexer.parallelWriter.maxBatchSize";
    public static final int DEFAULT_OAK_INDEXER_PARALLEL_WRITER_MAX_BATCH_SIZE = 256;

    public static final String OAK_INDEXER_PARALLEL_WRITER_QUEUE_SIZE = "oak.indexer.parallelWriter.queueSize";
    public static final int DEFAULT_OAK_INDEXER_PARALLEL_WRITER_QUEUE_SIZE = 64;

    public static final String OAK_INDEXER_PARALLEL_WRITER_NUMBER_THREADS = "oak.indexer.parallelWriter.numberThreads";
    public static final int DEFAULT_OAK_INDEXER_PARALLEL_WRITER_NUMBER_THREADS = 4;

    private final int maxBatchSize = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PARALLEL_WRITER_MAX_BATCH_SIZE, DEFAULT_OAK_INDEXER_PARALLEL_WRITER_MAX_BATCH_SIZE);
    private final int queueSize = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PARALLEL_WRITER_QUEUE_SIZE, DEFAULT_OAK_INDEXER_PARALLEL_WRITER_QUEUE_SIZE);
    private final int numberOfThreads = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PARALLEL_WRITER_NUMBER_THREADS, DEFAULT_OAK_INDEXER_PARALLEL_WRITER_NUMBER_THREADS);

    private final ArrayList<Operation> batch = new ArrayList<>(maxBatchSize);
    private final BlockingQueue<OperationBatch> queue = new ArrayBlockingQueue<>(queueSize);
    private final List<Future<?>> futures;
    private final ExecutorService writerPool;
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private long updateCount = 0;
    private long deleteCount = 0;
    private long totalEnqueueTimeNanos = 0;

    private final Object lock = new Object();
    // Used to keep track of the sequence number of the batches that are currently being processed.
    // This is used to wait until all operations for a writer are processed before closing it.
    private final HashSet<Long> pendingBatches = new HashSet<>();
    private long batchSequenceNumber = 0;

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

    private static class CloseWriterOperation extends Operation {
        private final long timestamp;
        private final SynchronousQueue<Boolean> sync;

        CloseWriterOperation(LuceneIndexWriter delegate, long timestamp, SynchronousQueue<Boolean> sync) {
            super(delegate);
            this.timestamp = timestamp;
            this.sync = sync;
        }

        @Override
        public void execute() throws IOException {
            boolean closeResult = delegate.close(timestamp);
            try {
                sync.put(closeResult);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    final static OperationBatch SHUTDOWN = new OperationBatch(-1, new Operation[0]);

    private class Worker implements Runnable {
        private long opCount = 0;

        public Worker() {
        }

        @Override
        public void run() {
            LOG.info("Worker started");
            threadMonitor.registerThread(Thread.currentThread());
            try {
                while (true) {
                    OperationBatch op = queue.take();
                    if (op == SHUTDOWN) {
                        queue.add(SHUTDOWN);
                        LOG.info("Shutting down worker");
                        return;
                    }
                    long sumSize = 0;
                    for (Operation operation : op.operations) {
                        operation.execute();
                        opCount++;
                        if (opCount % 100_000 == 0) {
                            LOG.info("Operations: {}. Queue size: {}, maxBatchSize: {}", opCount, queue.size(), -1);
                        }
                    }
                    LOG.info("Batch processed: {}", op.sequenceNumber);
                    synchronized (lock) {
                        pendingBatches.remove(op.sequenceNumber);
                        lock.notifyAll();
                    }
//                    maxSize = Math.max(maxSize, sumSize);
//                    LOG.info("Executed batch of size: {}. Total size: {}, Max: {}", op.length, sumSize, maxSize);
                }
            } catch (InterruptedException | IOException e) {
                LOG.warn("Interrupted while waiting to take an update operation from the queue", e);
                throw new RuntimeException(e);
            } catch (Throwable t) {
                LOG.error("Error while processing update operation", t);
                throw new RuntimeException(t);
            }
        }
    }

    /**
     * Creates and starts a pool of writer threads.
     * <p>
     * WARN: This is not thread safe.
     */
    public IndexWriterPool() {
        ThreadFactory delegateThreadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("index-writer-%d")
                .build();

        this.writerPool = Executors.newFixedThreadPool(numberOfThreads, delegateThreadFactory);
        this.futures = IntStream.range(0, numberOfThreads)
                .mapToObj(i -> writerPool.submit(new Worker()))
                .collect(Collectors.toList());
        threadMonitor.start();
        scheduledExecutor.scheduleAtFixedRate(this::printStatistics, 0, 30, TimeUnit.SECONDS);
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

    public boolean closeWriter(LuceneIndexWriter writer, long timestamp) {
        checkOpen();
        try {
            LOG.info("Closing writer: {}", writer);
            // Before closing the writer, we must wait until all previously submitted operations for
            // this writer are processed. For simplicity, we wait instead until ALL operations currently
            // in the queue are processed, because otherwise it would be more complex to distinguish which
            // operations are for which writer.
            long seqNumber = flushBatch();
            LOG.info("All operations for writer: {} enqueued (highest batch sequence number: {}). Waiting for them to be processed", writer, seqNumber);
            synchronized (lock) {
                while (true) {
                    Long earliestPending = pendingBatches.isEmpty() ? null : pendingBatches.stream().min(Long::compareTo).get();
                    LOG.info("Earliest pending batch: {}. Waiting for seqNumber: {}", earliestPending, seqNumber);
                    if (earliestPending == null || earliestPending > seqNumber) {
                        break;
                    }
                    lock.wait();
                }
            }
            LOG.info("All operations for writer: {} processed. Enqueuing close operation", writer);
            SynchronousQueue<Boolean> syncClosed = new SynchronousQueue<>();
            batch.add(new CloseWriterOperation(writer, timestamp, syncClosed));
            flushBatch();
            Boolean res = syncClosed.take();
            LOG.info("Writer closed: {}. Result: {}", writer, res);
            return res;
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
            for (Future<?> f : futures) {
                LOG.info("Waiting for future: {}", f);
                try {
                    f.get();
                } catch (InterruptedException | ExecutionException e) {
                    LOG.info("Error while waiting for future", e);
                }
            }
            new ExecutorCloser(writerPool, 1, TimeUnit.SECONDS).close();
            new ExecutorCloser(scheduledExecutor, 1, TimeUnit.SECONDS).close();
            threadMonitor.printStatistics();
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
        // Batches may be empty. This is necessary
        try {
            long seqNumber;
            synchronized (lock) {
                // Shared between producer and workers
                seqNumber = batchSequenceNumber;
                batchSequenceNumber++;
                pendingBatches.add(seqNumber);
            }
            LOG.info("Enqueuing batch {}, size: {}", seqNumber, batch.size());
            long start = System.nanoTime();
            queue.put(new OperationBatch(seqNumber, batch.toArray(new Operation[0])));
            long durationNanos = System.nanoTime() - start;
            long durationMS = durationNanos / 1_000_000;
            totalEnqueueTimeNanos += durationNanos;
            if (durationMS > 0) {
                LOG.info("Enqueued batch {}, size: {}. Duration: {} ms", seqNumber, batch.size(), durationMS);
            }
            batch.clear();
            return seqNumber;
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting to put an update operation in the queue", e);
            throw new RuntimeException(e);
        }
    }

    private void printStatistics() {
        LOG.info("updateCount: {}, deleteCount: {}, batchesEnqueued: {}, pendingBatchesCount: {},  totalEnqueueTimeMillis: {}",
                updateCount, deleteCount, batchSequenceNumber, pendingBatches.size(), totalEnqueueTimeNanos / 1_000_000);
        threadMonitor.printStatistics();
    }
}
