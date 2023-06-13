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
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreHelper;
import org.apache.jackrabbit.oak.plugins.document.mongo.TraversingRange;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

class PipelineMongoDownloadTask implements Callable<PipelineMongoDownloadTask.Result> {
    public static class Result {
        private final long documentsDownloaded;

        public Result(long documentsDownloaded) {
            this.documentsDownloaded = documentsDownloaded;
        }

        public long getDocumentsDownloaded() {
            return documentsDownloaded;
        }
    }

    public static final String PIPELINED_DOWNLOAD_RETRY_DURING_SECONDS = "PIPELINED_DOWNLOAD_RETRY_DURING_SECONDS";
    // Short retrial time, in most cases if the connection to a replica fails, trying again will establish a connection
    // to another replica which is up
    private final static long retryInitialIntervalMillis = 100;
    private final static long retryMaxIntervalMillis = 10_000;
    private static final Logger LOG = LoggerFactory.getLogger(PipelineMongoDownloadTask.class);
    private static final Duration MONGO_QUEUE_OFFER_TIMEOUT = Duration.ofMinutes(2);
    private static final String TRAVERSER_ID_PREFIX = "NSET";

    private final MongoDocumentStore mongoStore;
    private final Collection<? extends Document> collection;
    private final Predicate<String> filter;
    private final int batchSize;
    private final ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue;
    private final int retryDuringSeconds;
    private final Logger traversalLog = LoggerFactory.getLogger(PipelineMongoDownloadTask.class.getName() + ".traversal");
    private final MongoCollection<BasicDBObject> dbCollection;
    private final ReadPreference readPreference;

    private long documentsRead = 0;
    private long nextLastModified;
    private String lastIdDownloaded = null;

    public <T extends Document> PipelineMongoDownloadTask(MongoDocumentStore mongoStore,
                                                          Collection<T> collection,
                                                          Predicate<String> filter,
                                                          int batchSize,
                                                          ArrayBlockingQueue<BasicDBObject[]> queue) {
        this.mongoStore = mongoStore;
        this.collection = collection;
        this.batchSize = batchSize;
        // TODO
//        this.filter = filter;
        this.mongoDocQueue = queue;

        IndexingProgressReporter progressReporterPerTask =
                new IndexingProgressReporter(IndexUpdateCallback.NOOP, NodeTraversalCallback.NOOP);
        progressReporterPerTask.setMessagePrefix("Dumping from " + TRAVERSER_ID_PREFIX);

        this.filter = (id) -> {
            try {
                progressReporterPerTask.traversedNode(() -> id);
            } catch (CommitFailedException e) {
                throw new RuntimeException(e);
            }
            traversalLog.trace(id);
            return true;
        };

        // Default retries for 5 minutes.
        this.retryDuringSeconds = ConfigHelper.getEnvVariableAsInt(PIPELINED_DOWNLOAD_RETRY_DURING_SECONDS, 300);
        this.dbCollection = MongoDocumentStoreHelper.getDBCollection(mongoStore, collection);

        //TODO This may lead to reads being routed to secondary depending on MongoURI
        //So caller must ensure that its safe to read from secondary
        this.readPreference = MongoDocumentStoreHelper.getConfiguredReadPreference(mongoStore, collection);
        LOG.info("Using read preference {}", readPreference.getName());

    }

    @Override
    public Result call() throws Exception {
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName("mongo-dump");
        try {
            LOG.info("Starting to download from MongoDB.");
            MongoCollection<BasicDBObject> dbCollection = MongoDocumentStoreHelper.getDBCollection(mongoStore, collection);
            //TODO This may lead to reads being routed to secondary depending on MongoURI
            //So caller must ensure that its safe to read from secondary

            ReadPreference readPreference = MongoDocumentStoreHelper.getConfiguredReadPreference(mongoStore, collection);
            LOG.info("Using read preference {}", readPreference.getName());

            this.nextLastModified = getFirstLastModified(dbCollection);
            long upperBoundLastModified = getLatestLastModified(dbCollection) + 1;
            this.lastIdDownloaded = null;
            Instant failureTimestamp = null;
            long retryInterval = retryInitialIntervalMillis;

            Map<String, Integer> exceptions = new HashMap<>();
            while (true) {
                try {
                    if (lastIdDownloaded != null) {
                        LOG.info("Recovering from broken connection, finishing downloading documents with _modified={}", nextLastModified);
                        downloadRange(new TraversingRange(new LastModifiedRange(nextLastModified, nextLastModified + 1), lastIdDownloaded));
                        // Reset the retries
                        failureTimestamp = null;
                        // Continue downloading everything starting from the next _lastmodified value
                        downloadRange(new TraversingRange(new LastModifiedRange(nextLastModified + 1, upperBoundLastModified), null));
                    } else {
                        downloadRange(new TraversingRange(new LastModifiedRange(nextLastModified, upperBoundLastModified), null));
                    }
                    LOG.info("Terminating thread, processed {} documents", documentsRead);
                    return new Result(documentsRead);
                } catch (MongoException e) {
                    if (failureTimestamp == null) {
                        failureTimestamp = Instant.now();
                    }
                    LOG.warn("Connection error downloading from MongoDB.", e);
                    if (Duration.between(failureTimestamp, Instant.now()).toSeconds() > retryDuringSeconds) {
                        // Give up. Get a string of all exceptions that were thrown
                        StringBuilder summary = new StringBuilder();
                        for (Map.Entry<String, Integer> entry : exceptions.entrySet()) {
                            summary.append("\n\t").append(entry.getValue()).append("x: ").append(entry.getKey());
                        }
                        throw new RetryException(retryDuringSeconds, summary.toString(), e);
                    } else {
                        LOG.warn("Retrying download after {} ms; number of times failed: {}", retryInterval, failureTimestamp);
                        exceptions.compute(e.getClass().getSimpleName() + " - " + e.getMessage(),
                                (key, val) -> val == null ? 1 : val + 1
                        );
                        try {
                            Thread.sleep(retryInterval);
                        } catch (InterruptedException ignore) {
                        }
                        // simple exponential backoff mechanism
                        retryInterval = Math.min(retryMaxIntervalMillis, retryInterval * 2);
                    }
                }
            }
        } catch (Throwable t) {
            LOG.warn("Thread terminating with exception: " + t);
            throw t;
        } finally {
            Thread.currentThread().setName(originalName);
        }
    }

    private void downloadRange(TraversingRange range) throws InterruptedException, TimeoutException {
        BasicDBObject[] block = new BasicDBObject[batchSize];
        int nextIndex = 0;
        BsonDocument findQuery = range.getFindQuery();
        LOG.info("Traversing from {}: query: {}", range, findQuery);
        FindIterable<BasicDBObject> mongoIterable = dbCollection
                .withReadPreference(readPreference)
                .find(findQuery)
                .sort(ascending(NodeDocument.MODIFIED_IN_SECS, NodeDocument.ID));
        try (MongoCursor<BasicDBObject> cursor = mongoIterable.iterator()) {
            while (cursor.hasNext()) {
                BasicDBObject next = cursor.next();
                String id = next.getString(Document.ID);
                this.nextLastModified = next.getLong(NodeDocument.MODIFIED_IN_SECS);
                this.lastIdDownloaded = id;
                this.documentsRead++;
                if (filter.test(id)) {
                    block[nextIndex] = next;
                    nextIndex++;
                    if (nextIndex == batchSize) {
                        tryEnqueue(block);
                        block = new BasicDBObject[batchSize];
                        nextIndex = 0;
                    }
                }
            }
        }
        LOG.info("Finished downloading range: {}", range);
        if (nextIndex > 0) {
            LOG.info("Enqueueing last block of size: {}", nextIndex);
            BasicDBObject[] lastBlock = new BasicDBObject[nextIndex];
            System.arraycopy(block, 0, lastBlock, 0, nextIndex);
            tryEnqueue(lastBlock);
        }
    }

    private void tryEnqueue(BasicDBObject[] block) throws TimeoutException, InterruptedException {
        if (!mongoDocQueue.offer(block, MONGO_QUEUE_OFFER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Timeout trying to enqueue batch of MongoDB documents. Waited " + MONGO_QUEUE_OFFER_TIMEOUT);
        }
    }

    private long getFirstLastModified(MongoCollection<BasicDBObject> dbCollection) {
        return getModified(dbCollection, ascending(NodeDocument.MODIFIED_IN_SECS));
    }

    private long getLatestLastModified(MongoCollection<BasicDBObject> dbCollection) {
        return getModified(dbCollection, descending(NodeDocument.MODIFIED_IN_SECS));
    }

    private long getModified(MongoCollection<BasicDBObject> dbCollection, Bson sort) {
        BsonDocument query = new BsonDocument();
        query.append(NodeDocument.MODIFIED_IN_SECS, new BsonDocument().append("$ne", new BsonNull()));
        try (MongoCursor<BasicDBObject> cursor = dbCollection
                .find(query)
                .sort(sort)
                .limit(1)
                .iterator()) {
            if (!cursor.hasNext()) {
                return -1;
            }
            return cursor.next().getLong(NodeDocument.MODIFIED_IN_SECS);
        }
    }

    private static class RetryException extends RuntimeException {

        private final int retrialDurationSeconds;

        public RetryException(int retrialDurationSeconds, String message, Throwable cause) {
            super(message, cause);
            this.retrialDurationSeconds = retrialDurationSeconds;
        }

        @Override
        public String toString() {
            return "Tried for " + retrialDurationSeconds + " seconds: \n" + super.toString();
        }
    }
}
