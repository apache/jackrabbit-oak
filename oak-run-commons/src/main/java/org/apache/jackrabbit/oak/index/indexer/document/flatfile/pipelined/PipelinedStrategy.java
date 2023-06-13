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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.BasicDBObject;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.guava.common.base.Preconditions;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.SortStrategy;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.NotificationEmitter;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.jackrabbit.guava.common.base.Charsets.UTF_8;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createWriter;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.sizeOf;

public class PipelinedStrategy implements SortStrategy {
    public static final BasicDBObject[] SENTINEL_MONGO_DOCUMENT = new BasicDBObject[0];
    public static final NodeStateEntryBatch SENTINEL_NSE_BUFFER = new NodeStateEntryBatch(ByteBuffer.allocate(0), 0);

    public static final String PIPELINED_MONGO_DOC_QUEUE_SIZE = "PIPELINED_MONGO_DOC_QUEUE_SIZE";
    private static final String PIPELINED_MONGO_BLOCK_SIZE = "PIPELINED_MONGO_DOC_BATCH_SIZE";
    private static final String PIPELINED_TRANSFORM_THREADS = "PIPELINED_TRANSFORM_THREADS";
    private static final String PIPELINED_WORKING_MEMORY_MB = "PIPELINED_WORKING_MEMORY_MB";

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedStrategy.class);

    private static class MonitorTask implements Runnable {
        private final ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue;
        private final ArrayBlockingQueue<NodeStateEntryBatch> emptyBuffersQueue;
        private final ArrayBlockingQueue<NodeStateEntryBatch> nonEmptyBuffersQueue;
        private final ArrayBlockingQueue<File> sortedFilesQueue;

        public MonitorTask(ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue,
                           ArrayBlockingQueue<NodeStateEntryBatch> emptyBuffersQueue,
                           ArrayBlockingQueue<NodeStateEntryBatch> nonEmptyBuffersQueue,
                           ArrayBlockingQueue<File> sortedFilesQueue) {
            this.mongoDocQueue = mongoDocQueue;
            this.emptyBuffersQueue = emptyBuffersQueue;
            this.nonEmptyBuffersQueue = nonEmptyBuffersQueue;
            this.sortedFilesQueue = sortedFilesQueue;
        }

        @Override
        public void run() {
            LOG.info("Queue sizes: mongoDocQueue: {}, emptyBuffersQueue: {}, nonEmptyBuffersQueue: {}, sortedFilesQueue: {}",
                    mongoDocQueue.size(), emptyBuffersQueue.size(), nonEmptyBuffersQueue.size(), sortedFilesQueue.size());
        }
    }

    private final MongoDocumentStore docStore;
    private final DocumentNodeStore documentNodeStore;
    private final RevisionVector rootRevision;
    private final BlobStore blobStore;
    private final File storeDir;
    private final Charset charset = UTF_8;
    private final Comparator<PipelinedNodeStateHolder> comparator;
    private final PathElementComparatorStringArray pathComparator;
    private NotificationEmitter emitter;
    private final Compression algorithm;
    private long entryCount;
    private final List<File> sortedFiles = new ArrayList<>();
    private final Predicate<String> pathPredicate;

    public PipelinedStrategy(MongoDocumentStore documentStore,
                             DocumentNodeStore documentNodeStore,
                             RevisionVector rootRevision,
                             Set<String> preferredPathElements,
                             BlobStore blobStore,
                             File storeDir,
                             Compression algorithm,
                             Predicate<String> pathPredicate) {
        this.docStore = documentStore;
        this.documentNodeStore = documentNodeStore;
        this.rootRevision = rootRevision;
        this.blobStore = blobStore;
        this.storeDir = storeDir;
        this.pathComparator = new PathElementComparatorStringArray(preferredPathElements);
        this.comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        this.pathPredicate = pathPredicate;
        this.algorithm = algorithm;

        Preconditions.checkState(documentStore.isReadOnly(), "Traverser can only be used with readOnly store");
    }

    private int getEnvVariableAsInt(String name, int defaultValue) {
        String value = System.getenv(name);
        int result;
        if (value == null) {
            result = defaultValue;
        } else {
            result = Integer.parseInt(value);
        }
        LOG.info("Config {}={}", name, result);
        return result;
    }

    @Override
    public File createSortedStoreFile() throws IOException {
        int mongoDocBlockQueueSize = getEnvVariableAsInt(PIPELINED_MONGO_DOC_QUEUE_SIZE, 1000);
        int mongoBatchSize = getEnvVariableAsInt(PIPELINED_MONGO_BLOCK_SIZE, 100);
        int transformThreads = getEnvVariableAsInt(PIPELINED_TRANSFORM_THREADS, 1);
        int workingMemoryMB = getEnvVariableAsInt(PIPELINED_WORKING_MEMORY_MB, 3000);

        int numberOfThreads = 1 + transformThreads + 1; // dump, transform, sort threads.
        ExecutorService threadPool = Executors.newFixedThreadPool(numberOfThreads,
                new ThreadFactoryBuilder().setNameFormat("mongo-dump").setDaemon(true).build()
        );
        @SuppressWarnings("unchecked")
        ExecutorCompletionService ecs = new ExecutorCompletionService(threadPool);
        ScheduledExecutorService monitorThreadPool = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("monitor").setDaemon(true).build()
        );
        try {
            int numberOfBuffers = 1 + transformThreads;
            int memoryArenas = numberOfBuffers + 1;
            int memoryForEntriesMB = workingMemoryMB / memoryArenas;
            int maxNumberOfEntries = memoryForEntriesMB * 1024 * 4; // Assuming that 1KB is enough for 4 entries.
            int maxNumberOfEntriesPerBuffer = maxNumberOfEntries / numberOfBuffers;

            long tmp = ((long) workingMemoryMB * FileUtils.ONE_MB) / memoryArenas;
            int bufferSizeBytes;
            if (tmp > Integer.MAX_VALUE) {
                bufferSizeBytes = Integer.MAX_VALUE - 1024;
                LOG.warn("Computed buffer size too big, exceeds Integer.MAX_VALUE. Truncating");
            } else {
                bufferSizeBytes = (int) tmp;
            }
            LOG.info("Working memory: {} MB, number of buffers: {}, size of each buffer: {}, number of entries per buffer: {}",
                    workingMemoryMB, numberOfBuffers, bufferSizeBytes, maxNumberOfEntriesPerBuffer);

            // download -> transform thread.
            ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue = new ArrayBlockingQueue<>(mongoDocBlockQueueSize);

            // transform <-> sort and save threads
            // Queue with empty buffers, used by the transform task
            ArrayBlockingQueue<NodeStateEntryBatch> emptyBatchesQueue = new ArrayBlockingQueue<>(numberOfBuffers);
            // Queue with buffers filled by the transform task, used by the sort and save task
            ArrayBlockingQueue<NodeStateEntryBatch> nonEmptyBatchesQueue = new ArrayBlockingQueue<>(numberOfBuffers);

            // Queue between sort and save thread and the parent thread (the one executing this code).
            ArrayBlockingQueue<File> sortedFilesQueue = new ArrayBlockingQueue<>(1000);

            ScheduledFuture<?> monitorTask = monitorThreadPool.scheduleWithFixedDelay(
                    new MonitorTask(mongoDocQueue, emptyBatchesQueue, nonEmptyBatchesQueue, sortedFilesQueue),
                    0, 1, TimeUnit.SECONDS
            );

            // Create empty buffers
            for (int i = 0; i < numberOfBuffers; i++) {
                emptyBatchesQueue.add(NodeStateEntryBatch.createNodeStateEntryBatch(bufferSizeBytes, maxNumberOfEntriesPerBuffer));
            }

            Stopwatch start = Stopwatch.createStarted();
            PipelineMongoDownloadTask downloadTask = new PipelineMongoDownloadTask(
                    docStore,
                    Collection.NODES,
                    s -> true,
                    mongoBatchSize,
                    mongoDocQueue);
            ecs.submit(downloadTask);

            for (int i = 0; i < transformThreads; i++) {
                NodeStateEntryWriter entryWriter = new NodeStateEntryWriter(blobStore);
                PipelinedTransformTask transformTask = new PipelinedTransformTask(
                        docStore,
                        documentNodeStore,
                        Collection.NODES,
                        rootRevision,
                        pathPredicate,
                        entryWriter,
                        mongoDocQueue,
                        emptyBatchesQueue,
                        nonEmptyBatchesQueue
                );
                ecs.submit(transformTask);
            }

            PipelinedSortBatchTask sortTask = new PipelinedSortBatchTask(
                    storeDir,
                    pathComparator,
                    algorithm,
                    emptyBatchesQueue,
                    nonEmptyBatchesQueue,
                    sortedFilesQueue
            );
            ecs.submit(sortTask);

            try {
                LOG.info("Waiting for tasks to complete.");
                int tasksFinished = 0;
                int transformTasksFinished = 0;
                while (tasksFinished < numberOfThreads) {
                    Future completedTask = ecs.take();
                    try {
                        Object result = completedTask.get();
                        if (result instanceof PipelineMongoDownloadTask.Result) {
                            PipelineMongoDownloadTask.Result downloadResult = (PipelineMongoDownloadTask.Result) result;
                            LOG.info("Download task finished. Documents downloaded: {}", downloadResult.getDocumentsDownloaded());
                            // Signal the end of documents to the transform threads.
                            for (int i = 0; i < transformThreads; i++) {
                                mongoDocQueue.put(SENTINEL_MONGO_DOCUMENT);
                            }
                        } else if (result instanceof PipelinedTransformTask.Result) {
                            PipelinedTransformTask.Result transformResult = (PipelinedTransformTask.Result) result;
                            LOG.info("Transform task finished. Entries processed: {}", transformResult.getEntryCount());
                            entryCount += transformResult.getEntryCount();
                            transformTasksFinished++;
                            if (transformTasksFinished == transformThreads) {
                                LOG.info("All transform tasks finished. Node states retrieved: {}", entryCount);
                                // At this point, only the sort thread is listening to the exchanger.
                                nonEmptyBatchesQueue.put(SENTINEL_NSE_BUFFER);
                            }

                        } else if (result instanceof PipelinedSortBatchTask.Result) {
                            PipelinedSortBatchTask.Result sortTaskResult = (PipelinedSortBatchTask.Result) result;
                            LOG.info("Sort task finished. Entries processed: {}", sortTaskResult.getTotalEntries());
                            sortedFilesQueue.stream().iterator().forEachRemaining(sortedFiles::add);

                        } else {
                            throw new RuntimeException("Unknown result type: " + result);
                        }
                        tasksFinished++;
                    } catch (ExecutionException ex) {
                        LOG.warn("Execution error dumping from MongoDB: " + ex + ". Shutting down all threads.");
                        threadPool.shutdownNow();
                        boolean terminated = threadPool.awaitTermination(5, TimeUnit.SECONDS);
                        if (!terminated) {
                            LOG.warn("Thread pool failed to terminate");
                        }
                        throw new RuntimeException(ex.getCause());
                    } catch (Throwable ex) {
                        LOG.warn("Error dumping from MongoDB: " + ex);
                        threadPool.shutdownNow();
                        boolean terminated = threadPool.awaitTermination(5, TimeUnit.SECONDS);
                        if (!terminated) {
                            LOG.warn("Thread pool failed to terminate");
                        }
                        throw new RuntimeException(ex);
                    }
                }
                LOG.info("Dumped {} nodestates in json format in {}", entryCount, start);
                LOG.info("Created {} sorted files of size {} to merge",
                        sortedFiles.size(), humanReadableByteCount(sizeOf(sortedFiles)));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                // No longer need to monitor the size of the queues,
                monitorTask.cancel(true);
            }
            return sortStoreFile(sortedFiles);
        } finally {
            threadPool.shutdown();
            monitorThreadPool.shutdown();
        }
    }

    @Override
    public long getEntryCount() {
        return entryCount;
    }

    private File sortStoreFile(List<File> sortedFilesBatch) throws IOException {
        LOG.info("Proceeding to perform merge of {} sorted files", sortedFilesBatch.size());
        Stopwatch w = Stopwatch.createStarted();
        File sortedFile = new File(storeDir, getSortedStoreFileName(algorithm));
        PipelinedNodeStateHolderFactory factory = new PipelinedNodeStateHolderFactory();
        try (BufferedWriter writer = createWriter(sortedFile, algorithm)) {
            Function<String, PipelinedNodeStateHolder> func1 = (line) -> line == null ? null : factory.create(line);
            Function<PipelinedNodeStateHolder, String> func2 = holder -> holder == null ? null : holder.getLine();
            ExternalSort.mergeSortedFiles(sortedFilesBatch,
                    writer,
                    comparator,
                    charset,
                    true, //distinct
                    algorithm,
                    func2,
                    func1
            );
        }
        LOG.info("Merging of sorted files completed in {}", w);
        return sortedFile;
    }
}
