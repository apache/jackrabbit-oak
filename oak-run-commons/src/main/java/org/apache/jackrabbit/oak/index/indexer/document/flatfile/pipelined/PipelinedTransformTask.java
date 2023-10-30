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

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreHelper;
import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.apache.jackrabbit.oak.plugins.index.MetricsFormatter;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.SENTINEL_MONGO_DOCUMENT;

/**
 * Receives batches of Mongo documents, converts them to node state entries, batches them in a {@link NodeStateEntryBatch}
 * buffer and when the buffer is full, passes the buffer to the sort-and-save task.
 */
class PipelinedTransformTask implements Callable<PipelinedTransformTask.Result> {
    public static class Result {
        private final int transformThreadId;
        private final long entryCount;

        public Result(int threadId, long entryCount) {
            this.transformThreadId = threadId;
            this.entryCount = entryCount;
        }

        public long getEntryCount() {
            return entryCount;
        }

        public int getThreadId() {
            return transformThreadId;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedTransformTask.class);
    private static final AtomicInteger threadIdGenerator = new AtomicInteger();
    private static final String THREAD_NAME_PREFIX = "mongo-transform-";

    private final MongoDocumentStore mongoStore;
    private final DocumentNodeStore documentNodeStore;
    private final RevisionVector rootRevision;
    private final NodeStateEntryWriter entryWriter;
    private final Predicate<String> pathPredicate;
    // Input queue
    private final ArrayBlockingQueue<NodeDocument[]> mongoDocQueue;
    // Output queue
    private final ArrayBlockingQueue<NodeStateEntryBatch> nonEmptyBatchesQueue;
    // Queue with empty (recycled) buffers
    private final ArrayBlockingQueue<NodeStateEntryBatch> emptyBatchesQueue;
    private final TransformStageStatistics statistics;
    private final int threadId = threadIdGenerator.getAndIncrement();
    private long totalEnqueueDelayMillis = 0;

    public PipelinedTransformTask(MongoDocumentStore mongoStore,
                                  DocumentNodeStore documentNodeStore,
                                  RevisionVector rootRevision,
                                  Predicate<String> pathPredicate,
                                  NodeStateEntryWriter entryWriter,
                                  ArrayBlockingQueue<NodeDocument[]> mongoDocQueue,
                                  ArrayBlockingQueue<NodeStateEntryBatch> emptyBatchesQueue,
                                  ArrayBlockingQueue<NodeStateEntryBatch> nonEmptyBatchesQueue,
                                  TransformStageStatistics statsCollector) {
        this.mongoStore = mongoStore;
        this.documentNodeStore = documentNodeStore;
        this.rootRevision = rootRevision;
        this.pathPredicate = pathPredicate;
        this.entryWriter = entryWriter;
        this.mongoDocQueue = mongoDocQueue;
        this.emptyBatchesQueue = emptyBatchesQueue;
        this.nonEmptyBatchesQueue = nonEmptyBatchesQueue;
        this.statistics = statsCollector;
    }

    @Override
    public Result call() throws Exception {
        String originalName = Thread.currentThread().getName();
        String threadName = THREAD_NAME_PREFIX + threadId;
        Thread.currentThread().setName(threadName);
        try {
            LOG.info("[TASK:{}:START] Starting transform task", threadName.toUpperCase(Locale.ROOT));
            NodeDocumentCache nodeCache = MongoDocumentStoreHelper.getNodeDocumentCache(mongoStore);
            Stopwatch taskStartWatch = Stopwatch.createStarted();
            long totalDocumentQueueWaitTimeMillis = 0;
            long totalEntryCount = 0;
            long mongoObjectsProcessed = 0;
            LOG.debug("Waiting for an empty buffer");
            NodeStateEntryBatch nseBatch = emptyBatchesQueue.take();

            // Used to serialize a node state entry before writing it to the buffer
            ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
            OutputStreamWriter writer = new OutputStreamWriter(baos, PipelinedStrategy.FLATFILESTORE_CHARSET);
            LOG.debug("Obtained an empty buffer. Starting to convert Mongo documents to node state entries");

            ArrayList<NodeStateEntry> nodeStateEntries = new ArrayList<>();
            Stopwatch docQueueWaitStopwatch = Stopwatch.createUnstarted();
            while (true) {
                docQueueWaitStopwatch.reset().start();
                NodeDocument[] nodeDocumentBatch = mongoDocQueue.take();
                totalDocumentQueueWaitTimeMillis += docQueueWaitStopwatch.elapsed(TimeUnit.MILLISECONDS);
                if (nodeDocumentBatch == SENTINEL_MONGO_DOCUMENT) {
                    String totalDocumentQueueWaitPercentage = String.format("%1.2f", (100.0 * totalDocumentQueueWaitTimeMillis) / taskStartWatch.elapsed(TimeUnit.MILLISECONDS));
                    String totalEnqueueDelayPercentage = String.format("%1.2f", (100.0 * totalEnqueueDelayMillis) / taskStartWatch.elapsed(TimeUnit.MILLISECONDS));
                    String metrics = MetricsFormatter.newBuilder()
                            .add("duration", FormattingUtils.formatToSeconds(taskStartWatch))
                            .add("durationSeconds", taskStartWatch.elapsed(TimeUnit.SECONDS))
                            .add("nodeStateEntriesGenerated", totalEntryCount)
                            .add("enqueueDelayMillis", totalEnqueueDelayMillis)
                            .add("enqueueDelayPercentage", totalEnqueueDelayPercentage)
                            .add("documentQueueWaitMillis", totalDocumentQueueWaitTimeMillis)
                            .add("documentQueueWaitPercentage", totalDocumentQueueWaitPercentage)
                            .build();
                    LOG.info("[TASK:{}:END] Metrics: {}", threadName.toUpperCase(Locale.ROOT), metrics);
                    //Save the last batch
                    nseBatch.getBuffer().flip();
                    tryEnqueue(nseBatch);
                    return new Result(threadId, totalEntryCount);
                } else {
                    for (NodeDocument nodeDoc : nodeDocumentBatch) {
                        statistics.incrementMongoDocumentsTraversed();
                        mongoObjectsProcessed++;
                        if (mongoObjectsProcessed % 50000 == 0) {
                            LOG.info("Mongo objects: {}, total entries: {}, current batch: {}, Size: {}/{} MB",
                                    mongoObjectsProcessed, totalEntryCount, nseBatch.numberOfEntries(),
                                    nseBatch.sizeOfEntries() / FileUtils.ONE_MB,
                                    nseBatch.capacity() / FileUtils.ONE_MB
                            );
                        }
                        //TODO Review the cache update approach where tracker has to track *all* docs
                        // TODO: should we cache splitDocuments? Maybe this can be moved to after the check for split document
                        nodeCache.put(nodeDoc);
                        if (nodeDoc.isSplitDocument()) {
                            statistics.addSplitDocument(nodeDoc.getId());
                        } else {
                            nodeStateEntries.clear();
                            extractNodeStateEntries(nodeDoc, nodeStateEntries);
                            if (nodeStateEntries.isEmpty()) {
                                statistics.addEmptyNodeStateEntry(nodeDoc.getId());
                            } else {
                                for (NodeStateEntry nse : nodeStateEntries) {
                                    String path = nse.getPath();
                                    if (!NodeStateUtils.isHiddenPath(path) && pathPredicate.test(path)) {
                                        statistics.incrementEntriesAccepted();
                                        totalEntryCount++;
                                        // Serialize entry
                                        entryWriter.writeTo(writer, nse);
                                        writer.flush();
                                        byte[] entryData = baos.toByteArray();
                                        baos.reset();
                                        statistics.incrementTotalExtractedEntriesSize(entryData.length);
                                        if (!nseBatch.hasSpaceForEntry(entryData)) {
                                            LOG.info("Buffer full, passing buffer to sort task. Total entries: {}, entries in buffer {}, buffer size: {}",
                                                    totalEntryCount, nseBatch.numberOfEntries(), nseBatch.sizeOfEntries());
                                            nseBatch.flip();
                                            tryEnqueue(nseBatch);
                                            // Get an empty buffer
                                            nseBatch = emptyBatchesQueue.take();
                                        }
                                        // Write entry to buffer
                                        nseBatch.addEntry(nse.getPath(), entryData);
                                    } else {
                                        statistics.incrementEntriesRejected();
                                        if (NodeStateUtils.isHiddenPath(path)) {
                                            statistics.addRejectedHiddenPath(path);
                                        }
                                        if (!pathPredicate.test(path)) {
                                            statistics.addRejectedFilteredPath(path);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (InterruptedException t) {
            LOG.warn("Thread interrupted", t);
            throw t;
        } catch (Throwable t) {
            LOG.warn("Thread terminating with exception", t);
            throw t;
        } finally {
            Thread.currentThread().setName(originalName);
        }
    }

    private void tryEnqueue(NodeStateEntryBatch nseBatch) throws InterruptedException {
        Stopwatch enqueueDelayStopwatch = Stopwatch.createStarted();
        nonEmptyBatchesQueue.put(nseBatch);
        long enqueueDelay = enqueueDelayStopwatch.elapsed(TimeUnit.MILLISECONDS);
        totalEnqueueDelayMillis += enqueueDelay;
        if (enqueueDelay > 1) {
            LOG.info("Enqueuing of node state entries batch was delayed, took {} ms. nonEmptyBatchesQueue size {}. ",
                    enqueueDelay, nonEmptyBatchesQueue.size());
        }
    }

    private void extractNodeStateEntries(NodeDocument doc, ArrayList<NodeStateEntry> nodeStateEntries) {
        Path path = doc.getPath();
        DocumentNodeState nodeState = documentNodeStore.getNode(path, rootRevision);
        //At DocumentNodeState api level the nodeState can be null
        if (nodeState == null || !nodeState.exists()) {
            return;
        }
        nodeStateEntries.add(toNodeStateEntry(doc, nodeState));
        for (DocumentNodeState dns : nodeState.getAllBundledNodesStates()) {
            nodeStateEntries.add(toNodeStateEntry(doc, dns));
        }
    }

    private NodeStateEntry toNodeStateEntry(NodeDocument doc, DocumentNodeState dns) {
        NodeStateEntry.NodeStateEntryBuilder builder = new NodeStateEntry.NodeStateEntryBuilder(dns, dns.getPath().toString());
        if (doc.getModified() != null) {
            builder.withLastModified(doc.getModified());
        }
        builder.withID(doc.getId());
        return builder.build();
    }
}
