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
import java.nio.charset.StandardCharsets;
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

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createWriter;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.sizeOf;

/**
 * Downloads the contents of the MongoDB repository dividing the tasks in a pipeline with the following stages:
 * <ul>
 * <li>Download - Downloads from Mongo all the documents in the node store.
 * <li>Transform - Converts Mongo documents to node state entries.
 * <li>Sort and save - Sorts the batch of node state entries and saves them to disk
 * <li>Merge sorted files - Merge the intermediate sorted files into a single file (the final FlatFileStore).
 * </ul>
 * <p>
 * <h2>Memory management</h2>
 * <p>
 * For efficiency, the intermediate sorted files should be as large as possible given the memory constraints.
 * This strategy accumulates the entries that will be stored in each of these files in memory until reaching a maximum
 * configurable size, at which point it sorts the data and writes it to a file. The data is accumulated in instances of
 * {@link NodeStateEntryBatch}. This class contains two data structures:
 * <ul>
 * <li>A {@link java.nio.ByteBuffer} for the binary representation of the entry, that is, the byte array that will be written to the file.
 * This buffer contains length-prefixed byte arrays, that is, each entry is {@code <size><data>}, where size is a 4 byte int.
 * <li>An array of {@link SortKey} instances, which contain the paths of each entry and are used to sort the entries. Each element
 * in this array also contains the position in the ByteBuffer of the serialized representation of the entry.
 * </ul>
 * This representation has several advantages:
 * <ul>
 * <li>It is compact, as a String object in the heap requires more memory than a length-prefixed byte array in the ByteBuffer.
 * <li>Predictable memory usage - the memory used by the {@link java.nio.ByteBuffer} is fixed and allocated at startup
 * (more on this later). The memory used by the array of {@link SortKey} is not bounded, but these objects are small,
 * as they contain little more than the path of the entry, and we can easily put limits on the maximum number of entries
 * kept in a buffer.
 * </ul>
 * <p>
 * The instances of {@link NodeStateEntryBatch} are created at launch time. We create {@code #transformThreads+1} buffers.
 * This way, except for some rare situations, each transform thread will have its own buffer where to write the entries
 * and there will be an extra buffer to be used by the Save-and-Sort thread, so that all the transform and sort threads
 * can operate concurrently.
 * <p>
 * These buffers are reused. Once the Save-and-Sort thread finishes processing a buffer, it clears it and sends it back
 * to the transform threads. For this, we use two queues, one with empty buffers, from where the transform threads take
 * their buffers when they need one, and another with full buffers, which are read by the Save-and-Sort thread.
 * <p>
 * Reusing the buffers reduces significantly the pressure on the garbage collector and ensures that we do not run out
 * of memory, as the largest blocks of memory are pre-allocated and reused.
 * <p>
 * The total amount of memory used by the buffers is a configurable parameter (env variable {@link #OAK_INDEXER_PIPELINED_WORKING_MEMORY_MB}).
 * This memory is divided in {@code numberOfBuffers + 1 </code>} regions, each of
 * {@code regionSize = PIPELINED_WORKING_MEMORY_MB/(#numberOfBuffers + 1)} size.
 * Each ByteBuffer is of {@code regionSize} big. The extra region is to account for the memory taken by the {@link SortKey}
 * entries. There is also a maximum limit on the number of entries, which is calculated based on regionSize
 * (we assume each {@link SortKey} entry requires 256 bytes).
 * <p>
 * The transform threads will stop filling a buffer and enqueue it for sorting and saving once either the byte buffer is
 * full or the number of entries in the buffer reaches the limit.
 * <p>
 *
 * <h2>Retrials on broken MongoDB connections</h2>
 */
public class PipelinedStrategy implements SortStrategy {
    public static final String OAK_INDEXER_PIPELINED_MONGO_DOC_QUEUE_SIZE = "oak.indexer.pipelined.mongoDocQueueSize";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_MONGO_DOC_QUEUE_SIZE = 1000;
    public static final String OAK_INDEXER_PIPELINED_MONGO_DOC_BATCH_SIZE = "oak.indexer.pipelined.mongoDocBatchSize";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_MONGO_DOC_BATCH_SIZE = 50;
    public static final String OAK_INDEXER_PIPELINED_TRANSFORM_THREADS = "oak.indexer.pipelined.transformThreads";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_TRANSFORM_THREADS = 3;
    public static final String OAK_INDEXER_PIPELINED_WORKING_MEMORY_MB = "oak.indexer.pipelined.workingMemoryMB";
    // 0 means autodetect
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_WORKING_MEMORY_MB = 0;

    static final BasicDBObject[] SENTINEL_MONGO_DOCUMENT = new BasicDBObject[0];
    static final NodeStateEntryBatch SENTINEL_NSE_BUFFER = new NodeStateEntryBatch(ByteBuffer.allocate(0), 0);
    static final Charset FLATFILESTORE_CHARSET = StandardCharsets.UTF_8;

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedStrategy.class);
    private static final int MIN_ENTRY_BATCH_BUFFER_SIZE_MB = 64;

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


    private int autodetectWorkingMemory() {
        int maxHeapSizeMB = (int) (Runtime.getRuntime().maxMemory() / FileUtils.ONE_MB);
        int workingMemoryMB = maxHeapSizeMB - 2048;
        LOG.info("Auto detecting working memory. Maximum heap size: {} MB, selected working memory: {} MB", maxHeapSizeMB, workingMemoryMB);
        return workingMemoryMB;
    }

    @Override
    public File createSortedStoreFile() throws IOException {
        int mongoDocBlockQueueSize = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_MONGO_DOC_QUEUE_SIZE, DEFAULT_OAK_INDEXER_PIPELINED_MONGO_DOC_QUEUE_SIZE);
        int mongoBatchSize = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_MONGO_DOC_BATCH_SIZE, DEFAULT_OAK_INDEXER_PIPELINED_MONGO_DOC_BATCH_SIZE);
        int transformThreads = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_TRANSFORM_THREADS, DEFAULT_OAK_INDEXER_PIPELINED_TRANSFORM_THREADS);
        int workingMemoryMB = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_WORKING_MEMORY_MB, DEFAULT_OAK_INDEXER_PIPELINED_WORKING_MEMORY_MB);
        if (workingMemoryMB == 0) {
            workingMemoryMB = autodetectWorkingMemory();
        }

        int numberOfThreads = 1 + transformThreads + 1; // dump, transform, sort threads.
        ExecutorService threadPool = Executors.newFixedThreadPool(numberOfThreads,
                new ThreadFactoryBuilder().setNameFormat("mongo-dump").setDaemon(true).build()
        );
        // This executor can wait for several tasks at the same time. We use this below to wait at the same time for
        // all the tasks, so that if one of them fails, we can abort the whole pipeline. Otherwise, if we wait on
        // Future instances, we can only wait on one of them, so that if any of the others fail, we have no easy way
        // to detect this failure.
        ExecutorCompletionService ecs = new ExecutorCompletionService(threadPool);
        ScheduledExecutorService monitorThreadPool = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("monitor").setDaemon(true).build()
        );
        try {
            int numberOfBuffers = 1 + transformThreads;
            // The extra memory arena is to account for the memory taken by the SortKey entries that are part of the
            // entry batches.
            int memoryArenas = numberOfBuffers + 1;
            int memoryForEntriesMB = workingMemoryMB / memoryArenas;
            int maxNumberOfEntries = memoryForEntriesMB * 1024 * 4; // Assuming that 1KB is enough for 4 entries.
            int maxNumberOfEntriesPerBuffer = maxNumberOfEntries / numberOfBuffers;

            // A ByteBuffer can be at most Integer.MAX_VALUE bytes
            int bufferSizeBytes;
            {
                long bufferSizeAsLong = (workingMemoryMB * FileUtils.ONE_MB) / memoryArenas;
                if (bufferSizeAsLong > Integer.MAX_VALUE) {
                    // Probably not necessary to subtract 16, just a safeguard to avoid boundary conditions.
                    bufferSizeBytes = Integer.MAX_VALUE - 16;
                    LOG.warn("Computed buffer size too big, exceeds Integer.MAX_VALUE. Truncating");
                } else {
                    bufferSizeBytes = (int) bufferSizeAsLong;
                }
            }
            if (bufferSizeBytes < MIN_ENTRY_BATCH_BUFFER_SIZE_MB * FileUtils.ONE_MB) {
                throw new IllegalArgumentException("Entry batch buffer size too small: " + bufferSizeBytes +
                        "bytes. Must be at least " + MIN_ENTRY_BATCH_BUFFER_SIZE_MB + " MB. " +
                        "To increase the size of the buffers, either increase the size of the working memory region " +
                        "(system property" + OAK_INDEXER_PIPELINED_WORKING_MEMORY_MB + ") or decrease the number of transform " +
                        "threads (" + OAK_INDEXER_PIPELINED_TRANSFORM_THREADS + ")");
            }
            LOG.info("Working memory: {} MB, number of buffers: {}, size of each buffer: {} MB, number of entries per buffer: {}",
                    workingMemoryMB, numberOfBuffers, bufferSizeBytes / FileUtils.ONE_MB, maxNumberOfEntriesPerBuffer);

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
                    docStore, Collection.NODES, s -> true, mongoBatchSize, mongoDocQueue
            );
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
                    storeDir, pathComparator, algorithm, emptyBatchesQueue, nonEmptyBatchesQueue, sortedFilesQueue
            );
            ecs.submit(sortTask);

            try {
                LOG.info("Waiting for tasks to complete.");
                int tasksFinished = 0;
                int transformTasksFinished = 0;
                while (tasksFinished < numberOfThreads) {
                    Future<?> completedTask = ecs.take();
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
                    FLATFILESTORE_CHARSET,
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
