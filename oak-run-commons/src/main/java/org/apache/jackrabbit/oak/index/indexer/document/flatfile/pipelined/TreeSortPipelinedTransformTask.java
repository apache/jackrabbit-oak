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

import com.mongodb.BasicDBObject;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreHelper;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
class TreeSortPipelinedTransformTask implements Callable<TreeSortPipelinedTransformTask.Result> {
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

    private static final Logger LOG = LoggerFactory.getLogger(TreeSortPipelinedTransformTask.class);
    private static final AtomicInteger threadIdGenerator = new AtomicInteger();

    private static final int MAX_NSE_BATCH_SIZE = 20000;

    private final MongoDocumentStore mongoStore;
    private final DocumentNodeStore documentNodeStore;
    private final RevisionVector rootRevision;
    private final Collection<NodeDocument> collection;
    private final NodeStateEntryWriter entryWriter;
    private final Predicate<String> pathPredicate;
    // Input queue
    private final ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue;
    // Output queue
    private final ArrayBlockingQueue<List<NodeStateEntryJson>> nseBatchQueue;
    private final TransformStageStatistics statistics;
    private final int threadId = threadIdGenerator.getAndIncrement();
    private long totalEnqueueWaitTimeMillis = 0;

    public TreeSortPipelinedTransformTask(MongoDocumentStore mongoStore,
                                          DocumentNodeStore documentNodeStore,
                                          Collection<NodeDocument> collection,
                                          RevisionVector rootRevision,
                                          Predicate<String> pathPredicate,
                                          NodeStateEntryWriter entryWriter,
                                          ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue,
                                          ArrayBlockingQueue<List<NodeStateEntryJson>> nseBatchQueue,
                                          TransformStageStatistics statsCollector) {
        this.mongoStore = mongoStore;
        this.documentNodeStore = documentNodeStore;
        this.collection = collection;
        this.rootRevision = rootRevision;
        this.pathPredicate = pathPredicate;
        this.entryWriter = entryWriter;
        this.mongoDocQueue = mongoDocQueue;
        this.nseBatchQueue = nseBatchQueue;
        this.statistics = statsCollector;
    }



    @Override
    public Result call() throws Exception {
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName("mongo-transform-" + threadId);
        try {
            LOG.info("Starting transform task");
            NodeDocumentCache nodeCache = MongoDocumentStoreHelper.getNodeDocumentCache(mongoStore);
            Stopwatch taskStartWatch = Stopwatch.createStarted();
            long totalEntryCount = 0;
            long mongoObjectsProcessed = 0;

            ArrayList<NodeStateEntryJson> nseBatch = new ArrayList<>(MAX_NSE_BATCH_SIZE);

            LOG.info("Obtained an empty buffer. Starting to convert Mongo documents to node state entries");
            while (true) {
                BasicDBObject[] dbObjectBatch = mongoDocQueue.take();
                if (dbObjectBatch == SENTINEL_MONGO_DOCUMENT) {
                    LOG.info("Task terminating. Dumped {} nodestates in json format in {}. Total enqueue delay: {} ms ({}%)",
                            totalEntryCount, taskStartWatch, totalEnqueueWaitTimeMillis,
                            String.format("%1.2f", (100.0 * totalEnqueueWaitTimeMillis) / taskStartWatch.elapsed(TimeUnit.MILLISECONDS)));
                    //Save the last batch
                    tryEnqueue(nseBatch);
                    return new Result(threadId, totalEntryCount);
                } else {
                    for (BasicDBObject dbObject : dbObjectBatch) {
                        statistics.incrementMongoDocumentsProcessed();
                        mongoObjectsProcessed++;
                        if (mongoObjectsProcessed % 50000 == 0) {
                            LOG.info("Mongo objects: {}, total entries: {}, current batch: {}",
                                    mongoObjectsProcessed, totalEntryCount, nseBatch.size()
                            );
                        }
                        //TODO Review the cache update approach where tracker has to track *all* docs
                        NodeDocument nodeDoc = MongoDocumentStoreHelper.convertFromDBObject(mongoStore, collection, dbObject);
                        // TODO: should we cache splitDocuments? Maybe this can be moved to after the check for split document
                        nodeCache.put(nodeDoc);
                        if (nodeDoc.isSplitDocument()) {
                            statistics.addSplitDocument(dbObject.getString(Document.ID));
                        } else {
                            List<NodeStateEntry> entries = getEntries(nodeDoc);
                            if (entries.isEmpty()) {
                                statistics.addEmptyNodeStateEntry(dbObject.getString(Document.ID));
                            }
                            for (NodeStateEntry nse : entries) {
                                String path = nse.getPath();
                                if (!NodeStateUtils.isHiddenPath(path) && pathPredicate.test(path)) {
                                    statistics.incrementEntriesAccepted();
                                    totalEntryCount++;

                                    if (nseBatch.size() == MAX_NSE_BATCH_SIZE) {
//                                        LOG.info("Buffer full, passing buffer to sort task. Total entries: {}, entries in buffer {}",
//                                                totalEntryCount, nseBatch.size());
                                        tryEnqueue(nseBatch);
                                        nseBatch = new ArrayList<>(MAX_NSE_BATCH_SIZE);
                                    }
                                    // Write entry to buffer
                                    nseBatch.add(new NodeStateEntryJson(nse.getPath(), entryWriter.asJson(nse.getNodeState())));
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
        } catch (InterruptedException t) {
            LOG.warn("Thread interrupted", t);
            throw t;
        } catch (Throwable t) {
            LOG.warn("Thread terminating with exception.", t);
            throw t;
        } finally {
            Thread.currentThread().setName(originalName);
        }
    }

    private void tryEnqueue(ArrayList<NodeStateEntryJson> nseBatch) throws InterruptedException {
//        LOG.info("Enqueuing batch of {} entries", nseBatch.size());
        Collections.sort(nseBatch);
        Stopwatch stopwatch = Stopwatch.createStarted();
        nseBatchQueue.put(nseBatch);
        long enqueueDelay = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        if (enqueueDelay > 1) {
            totalEnqueueWaitTimeMillis += enqueueDelay;
            LOG.info("Enqueuing of node state entries batch was delayed, took {} ms. nonEmptyBatchesQueue size {}. ",
                    enqueueDelay, nseBatchQueue.size());
        }
    }

    private List<NodeStateEntry> getEntries(NodeDocument doc) {
        Path path = doc.getPath();
        DocumentNodeState nodeState = documentNodeStore.getNode(path, rootRevision);
        //At DocumentNodeState api level the nodeState can be null
        if (nodeState == null || !nodeState.exists()) {
            return List.of();
        }
        ArrayList<NodeStateEntry> nodeStateEntries = new ArrayList<>(2);
        nodeStateEntries.add(toNodeStateEntry(doc, nodeState));
        for (DocumentNodeState dns : nodeState.getAllBundledNodesStates()) {
            nodeStateEntries.add(toNodeStateEntry(doc, dns));
        }
        return nodeStateEntries;
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
