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
import com.mongodb.MongoIncompatibleDriverException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreHelper;
import org.bson.BsonDocument;
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

import static com.mongodb.client.model.Sorts.ascending;

class PipelinedMongoDownloadTask implements Callable<PipelinedMongoDownloadTask.Result> {
    public static class Result {
        private final long documentsDownloaded;

        public Result(long documentsDownloaded) {
            this.documentsDownloaded = documentsDownloaded;
        }

        public long getDocumentsDownloaded() {
            return documentsDownloaded;
        }
    }

    public static final String OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS = "oak.indexer.pipelined.mongoConnectionRetrySeconds";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS = 300;
    // Use a short initial retry interval. In most cases if the connection to a replica fails, there will be other
    // replicas available so a reconnection attempt will succeed immediately.
    private final static long retryInitialIntervalMillis = 100;
    private final static long retryMaxIntervalMillis = 10_000;
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMongoDownloadTask.class);
    private static final Duration MONGO_QUEUE_OFFER_TIMEOUT = Duration.ofMinutes(2);
    private static final int MIN_INTERVAL_BETWEEN_DELAYED_ENQUEUING_MESSAGES = 10;

    private final int batchSize;
    private final ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue;
    private final int retryDuringSeconds;
    private final Logger traversalLog = LoggerFactory.getLogger(PipelinedMongoDownloadTask.class.getName() + ".traversal");
    private final MongoCollection<BasicDBObject> dbCollection;
    private final ReadPreference readPreference;
    private final Stopwatch downloadStartWatch = Stopwatch.createUnstarted();

    private long totalEnqueueWaitTimeMillis = 0;
    private Instant lastDelayedEnqueueWarningMessageLoggedTimestamp = Instant.now();
    private long documentsRead = 0;
    private long nextLastModified = 0;
    private String lastIdDownloaded = null;

    public <T extends Document> PipelinedMongoDownloadTask(MongoDocumentStore mongoStore,
                                                           Collection<T> collection,
                                                           int batchSize,
                                                           ArrayBlockingQueue<BasicDBObject[]> queue) {
        this.batchSize = batchSize;
        this.mongoDocQueue = queue;
        // Default retries for 5 minutes.
        this.retryDuringSeconds = ConfigHelper.getSystemPropertyAsInt(
                OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS);
        this.dbCollection = MongoDocumentStoreHelper.getDBCollection(mongoStore, collection);

        //TODO This may lead to reads being routed to secondary depending on MongoURI
        //So caller must ensure that its safe to read from secondary
//        this.readPreference = MongoDocumentStoreHelper.getConfiguredReadPreference(mongoStore, collection);
        this.readPreference = ReadPreference.secondaryPreferred();
        LOG.info("Using read preference {}", readPreference.getName());
    }

    @Override
    public Result call() throws Exception {
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName("mongo-dump");
        try {
            LOG.info("Starting to download from MongoDB.");
            //TODO This may lead to reads being routed to secondary depending on MongoURI
            //So caller must ensure that its safe to read from secondary

            this.nextLastModified = 0;
            this.lastIdDownloaded = null;
            Instant failureTimestamp = null;
            long retryInterval = retryInitialIntervalMillis;

            downloadStartWatch.start();
            Map<String, Integer> exceptions = new HashMap<>();
            while (true) {
                try {
                    if (lastIdDownloaded != null) {
                        LOG.info("Recovering from broken connection, finishing downloading documents with _modified={}", nextLastModified);
                        downloadRange(new DownloadRange(nextLastModified, nextLastModified + 1, lastIdDownloaded));
                        // Reset the retries
                        failureTimestamp = null;
                        // Continue downloading everything starting from the next _lastmodified value
                        downloadRange(new DownloadRange(nextLastModified + 1, Long.MAX_VALUE, null));
                    } else {
                        downloadRange(new DownloadRange(nextLastModified, Long.MAX_VALUE, null));
                    }
                    LOG.info("Terminating thread, downloaded {} Mongo documents in {}. Time waiting for transform threads: {} seconds ({}%)",
                            documentsRead, downloadStartWatch,
                            totalEnqueueWaitTimeMillis / 1000,
                            String.format("%1.2f", (100.0 * totalEnqueueWaitTimeMillis) / downloadStartWatch.elapsed(TimeUnit.MILLISECONDS)));
                    return new Result(documentsRead);
                } catch (MongoException e) {
                    if (e instanceof MongoInterruptedException || e instanceof MongoIncompatibleDriverException) {
                        // Non-recoverable exceptions
                        throw e;
                    }
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
        } catch (InterruptedException t) {
            LOG.warn("Thread interrupted");
            throw t;
        } catch (Throwable t) {
            LOG.warn("Thread terminating with exception.", t);
            throw t;
        } finally {
            Thread.currentThread().setName(originalName);
        }
    }

    private void reportProgress(String id) {
        if (this.documentsRead % 10000 == 0) {
            double rate = ((double) this.documentsRead) / downloadStartWatch.elapsed(TimeUnit.SECONDS);
            String formattedRate = String.format("%1.2f nodes/s, %1.2f nodes/hr", rate, rate * 3600);
            LOG.info("Dumping from NSET Traversed #{} {} [{}] (Elapsed {})",
                    this.documentsRead, id, formattedRate, downloadStartWatch);
        }
        traversalLog.trace(id);
    }

    private void downloadRange(DownloadRange range) throws InterruptedException, TimeoutException {
        BasicDBObject[] block = new BasicDBObject[batchSize];
        int nextIndex = 0;
        BsonDocument findQuery = range.getFindQuery();
        LOG.info("Traversing: {}. Query: {}", range, findQuery);
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
                reportProgress(id);
                block[nextIndex] = next;
                nextIndex++;
                if (nextIndex == batchSize) {
                    tryEnqueue(block);
                    block = new BasicDBObject[batchSize];
                    nextIndex = 0;
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
        Stopwatch stopwatch = Stopwatch.createStarted();
        if (!mongoDocQueue.offer(block, MONGO_QUEUE_OFFER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Timeout trying to enqueue batch of MongoDB documents. Waited " + MONGO_QUEUE_OFFER_TIMEOUT);
        }
        long enqueueDelay = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        totalEnqueueWaitTimeMillis += enqueueDelay;
        if (enqueueDelay > 1) {
            logWithRateLimit(() ->
                    LOG.info("Enqueuing of Mongo document batch was delayed, took {} ms. mongoDocQueue size {}. " +
                                    "Consider increasing the number of Transform threads. " +
                                    "(This message is logged at most once every {} seconds)",
                            enqueueDelay, mongoDocQueue.size(), MIN_INTERVAL_BETWEEN_DELAYED_ENQUEUING_MESSAGES)
            );
        }
    }

    private void logWithRateLimit(Runnable f) {
        Instant now = Instant.now();
        if (Duration.between(lastDelayedEnqueueWarningMessageLoggedTimestamp, now).toSeconds() > MIN_INTERVAL_BETWEEN_DELAYED_ENQUEUING_MESSAGES) {
            f.run();
            lastDelayedEnqueueWarningMessageLoggedTimestamp = now;
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
