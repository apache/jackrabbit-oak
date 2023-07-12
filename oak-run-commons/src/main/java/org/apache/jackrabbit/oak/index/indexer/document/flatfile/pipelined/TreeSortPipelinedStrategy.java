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
import com.mongodb.client.MongoCollection;
import org.apache.jackrabbit.guava.common.base.Preconditions;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.SortStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Compression;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreHelper;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.SENTINEL_MONGO_DOCUMENT;

public class TreeSortPipelinedStrategy implements SortStrategy {
    public static final String OAK_INDEXER_PIPELINED_MONGO_DOC_QUEUE_SIZE = "oak.indexer.pipelined.mongoDocQueueSize";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_MONGO_DOC_QUEUE_SIZE = 100;
    public static final String OAK_INDEXER_PIPELINED_MONGO_DOC_BATCH_SIZE = "oak.indexer.pipelined.mongoDocBatchSize";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_MONGO_DOC_BATCH_SIZE = 500;
    public static final String OAK_INDEXER_PIPELINED_TRANSFORM_THREADS = "oak.indexer.pipelined.transformThreads";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_TRANSFORM_THREADS = 3;
    private static final Logger LOG = LoggerFactory.getLogger(TreeSortPipelinedStrategy.class);


    static final List<NodeStateEntryJson> SENTINEL_NODE_STATE_ENTRY_JSON = new ArrayList<>(0);

    private class MonitorTask implements Runnable {
        private final ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue;
        private final ArrayBlockingQueue<List<NodeStateEntryJson>> nseQueue;
        private final ArrayBlockingQueue<File> sortedFilesQueue;
        private final TransformStageStatistics transformStageStatistics;

        public MonitorTask(ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue,
                           ArrayBlockingQueue<List<NodeStateEntryJson>> nseQueue,
                           ArrayBlockingQueue<File> sortedFilesQueue,
                           TransformStageStatistics transformStageStatistics) {
            this.mongoDocQueue = mongoDocQueue;
            this.nseQueue = nseQueue;
            this.sortedFilesQueue = sortedFilesQueue;
            this.transformStageStatistics = transformStageStatistics;
        }

        @Override
        public void run() {
            try {
                printStatistics(mongoDocQueue, nseQueue, sortedFilesQueue, transformStageStatistics, false);
            } catch (Exception e) {
                LOG.error("Error while logging queue sizes", e);
            }
        }
    }

    private void printStatistics(ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue,
                                 ArrayBlockingQueue<List<NodeStateEntryJson>> nseQueue,
                                 ArrayBlockingQueue<File> sortedFilesQueue,
                                 TransformStageStatistics transformStageStatistics,
                                 boolean printHistogramsAtInfo) {
        // Summary stats
        LOG.info("Queue sizes: {mongoDocQueue:{}, nseQueue:{}, sortedFilesQueue:{}}; Transform Stats: {}",
                mongoDocQueue.size(), nseQueue.size(), sortedFilesQueue.size(), transformStageStatistics.formatStats());
        prettyPrintTransformStatisticsHistograms(transformStageStatistics, printHistogramsAtInfo);
    }

    private void prettyPrintTransformStatisticsHistograms(TransformStageStatistics transformStageStatistics, boolean printHistogramAtInfo) {
        if (printHistogramAtInfo) {
            LOG.info("Top hidden paths rejected: {}.", transformStageStatistics.getHiddenPathsRejectedHistogram().prettyPrint());
            LOG.info("Top paths filtered: {}.", transformStageStatistics.getFilteredPathsRejectedHistogram().prettyPrint());
            LOG.info("Top empty node state documents: {}", transformStageStatistics.getEmptyNodeStateHistogram().prettyPrint());
        } else {
            LOG.debug("Top hidden paths rejected: {}.", transformStageStatistics.getHiddenPathsRejectedHistogram().prettyPrint());
            LOG.debug("Top paths filtered: {}.", transformStageStatistics.getFilteredPathsRejectedHistogram().prettyPrint());
            LOG.debug("Top empty node state documents: {}", transformStageStatistics.getEmptyNodeStateHistogram().prettyPrint());
        }
    }

    private final MongoDocumentStore docStore;
    private final DocumentNodeStore documentNodeStore;
    private final RevisionVector rootRevision;
    private final BlobStore blobStore;
    private final File storeDir;
    private final Compression algorithm;
    private long entryCount;
    private final Predicate<String> pathPredicate;

    // Store node state entries as Strings instead of serializing them
    public TreeSortPipelinedStrategy(MongoDocumentStore documentStore,
                                     DocumentNodeStore documentNodeStore,
                                     RevisionVector rootRevision,
                                     BlobStore blobStore,
                                     File storeDir,
                                     Compression algorithm,
                                     Predicate<String> pathPredicate) {
        this.docStore = documentStore;
        this.documentNodeStore = documentNodeStore;
        this.rootRevision = rootRevision;
        this.blobStore = blobStore;
        this.storeDir = storeDir;
        this.pathPredicate = pathPredicate;
        this.algorithm = algorithm;

        Preconditions.checkState(documentStore.isReadOnly(), "Traverser can only be used with readOnly store");
    }

    @Override
    public File createSortedStoreFile() throws IOException {
        int mongoDocBlockQueueSize = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_MONGO_DOC_QUEUE_SIZE, DEFAULT_OAK_INDEXER_PIPELINED_MONGO_DOC_QUEUE_SIZE);
        Preconditions.checkArgument(mongoDocBlockQueueSize > 0,
                "Invalid value for property " + OAK_INDEXER_PIPELINED_MONGO_DOC_QUEUE_SIZE + ": " + mongoDocBlockQueueSize + ". Must be > 0");

        int mongoBatchSize = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_MONGO_DOC_BATCH_SIZE, DEFAULT_OAK_INDEXER_PIPELINED_MONGO_DOC_BATCH_SIZE);
        Preconditions.checkArgument(mongoBatchSize > 0,
                "Invalid value for property " + OAK_INDEXER_PIPELINED_MONGO_DOC_BATCH_SIZE + ": " + mongoBatchSize + ". Must be > 0");

        int transformThreads = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_TRANSFORM_THREADS, DEFAULT_OAK_INDEXER_PIPELINED_TRANSFORM_THREADS);
        Preconditions.checkArgument(transformThreads > 0,
                "Invalid value for property " + OAK_INDEXER_PIPELINED_TRANSFORM_THREADS + ": " + transformThreads + ". Must be > 0");

        int numberOfThreads = 1 + transformThreads + 1; // dump, transform, sort threads, sorted files merge
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
            // download -> transform thread.
            ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue = new ArrayBlockingQueue<>(mongoDocBlockQueueSize);

            // transform <->  save threads
            ArrayBlockingQueue<List<NodeStateEntryJson>> nseQueue = new ArrayBlockingQueue<>(50);

            // Queue between sort-and-save thread and the merge-sorted-files thread
            ArrayBlockingQueue<File> sortedFilesQueue = new ArrayBlockingQueue<>(64);

            TransformStageStatistics transformStageStatistics = new TransformStageStatistics();

            ScheduledFuture<?> monitorFuture = monitorThreadPool.scheduleWithFixedDelay(
                    new MonitorTask(mongoDocQueue, nseQueue, sortedFilesQueue, transformStageStatistics),
                    10, 30, TimeUnit.SECONDS);

            Stopwatch start = Stopwatch.createStarted();
            MongoCollection<BasicDBObject> dbCollection = MongoDocumentStoreHelper.getDBCollection(docStore, Collection.NODES);
            PipelinedMongoDownloadTask downloadTask = new PipelinedMongoDownloadTask(dbCollection, mongoBatchSize, mongoDocQueue);
            ecs.submit(downloadTask);

            File treeStoreDirectory = null;

            for (int i = 0; i < transformThreads; i++) {
                NodeStateEntryWriter entryWriter = new NodeStateEntryWriter(blobStore);
                TreeSortPipelinedTransformTask transformTask = new TreeSortPipelinedTransformTask(
                        docStore,
                        documentNodeStore,
                        Collection.NODES,
                        rootRevision,
                        pathPredicate,
                        entryWriter,
                        mongoDocQueue,
                        nseQueue,
                        transformStageStatistics
                );
                ecs.submit(transformTask);
            }

            TreeSortPipelinedBuildTreeTask addToTreeTask = new TreeSortPipelinedBuildTreeTask(
                    storeDir, algorithm, nseQueue
            );
            ecs.submit(addToTreeTask);

            try {
                LOG.info("Waiting for tasks to complete.");
                int tasksFinished = 0;
                int transformTasksFinished = 0;
                while (tasksFinished < numberOfThreads) {
                    Future<?> completedTask = ecs.take();
                    try {
                        Object result = completedTask.get();
                        if (result instanceof PipelinedMongoDownloadTask.Result) {
                            PipelinedMongoDownloadTask.Result downloadResult = (PipelinedMongoDownloadTask.Result) result;
                            LOG.info("Download task finished. Documents downloaded: {}", downloadResult.getDocumentsDownloaded());
                            // Signal the end of documents to the transform threads.
                            for (int i = 0; i < transformThreads; i++) {
                                mongoDocQueue.put(SENTINEL_MONGO_DOCUMENT);
                            }

                        } else if (result instanceof TreeSortPipelinedTransformTask.Result) {
                            TreeSortPipelinedTransformTask.Result transformResult = (TreeSortPipelinedTransformTask.Result) result;
                            transformTasksFinished++;
                            entryCount += transformResult.getEntryCount();
                            LOG.info("Transform thread {} finished. Entries processed: {}",
                                    transformResult.getThreadId(), transformResult.getEntryCount());
                            if (transformTasksFinished == transformThreads) {
                                LOG.info("All transform tasks finished. Node states retrieved: {}", entryCount);
                                // No need to keep monitoring the queues, the download and transform threads are done.
                                monitorFuture.cancel(false);
                                // Terminate the sort thread.
                                nseQueue.put(SENTINEL_NODE_STATE_ENTRY_JSON);
                                // All transform tasks are done
                            }

                        } else if (result instanceof TreeSortPipelinedBuildTreeTask.Result) {
                            TreeSortPipelinedBuildTreeTask.Result sortTaskResult = (TreeSortPipelinedBuildTreeTask.Result) result;
                            LOG.info("Sort task finished. Entries processed: {}", sortTaskResult.getTotalEntries());
                            printStatistics(mongoDocQueue, nseQueue, sortedFilesQueue, transformStageStatistics, true);
                            treeStoreDirectory = sortTaskResult.getSortedTreeDirectory();

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
                printStatistics(mongoDocQueue, nseQueue, sortedFilesQueue, transformStageStatistics, true);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                // No longer need to monitor the size of the queues,
                monitorFuture.cancel(true);
            }
            return treeStoreDirectory;
        } finally {
            threadPool.shutdown();
            monitorThreadPool.shutdown();
        }
    }

    @Override
    public long getEntryCount() {
        return entryCount;
    }
}
