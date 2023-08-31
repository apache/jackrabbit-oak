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
import org.apache.jackrabbit.guava.common.base.Preconditions;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.plugins.index.MetricsFormatter;
import org.apache.jackrabbit.oak.plugins.index.FormatingUtils;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.mongodb.client.model.Sorts.ascending;

public class PipelinedMongoDownloadTask implements Callable<PipelinedMongoDownloadTask.Result> {
    public static class Result {
        private final long documentsDownloaded;

        public Result(long documentsDownloaded) {
            this.documentsDownloaded = documentsDownloaded;
        }

        public long getDocumentsDownloaded() {
            return documentsDownloaded;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMongoDownloadTask.class);

    /**
     * Whether to retry on connection errors to MongoDB.
     * This property affects the query that is used to download the documents from MongoDB. If set to true, the query
     * will traverse the results by order of the _modified property (does an index scan), which allows it to resume after
     * a failed connection from where it left off. If set to false, it uses a potentially more efficient query that does
     * not impose any order on the results (does a simple column scan).
     */
    public static final String OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS = "oak.indexer.pipelined.retryOnConnectionErrors";
    public static final boolean DEFAULT_OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS = true;
    public static final String OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS = "oak.indexer.pipelined.mongoConnectionRetrySeconds";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS = 300;
    // Use a short initial retry interval. In most cases if the connection to a replica fails, there will be other
    // replicas available so a reconnection attempt will succeed immediately.
    private final static long retryInitialIntervalMillis = 100;
    private final static long retryMaxIntervalMillis = 10_000;

    // TODO: Revise this timeout. It is used to prevent the indexer from blocking forever if the queue is full.
    private static final Duration MONGO_QUEUE_OFFER_TIMEOUT = Duration.ofMinutes(30);
    private static final int MIN_INTERVAL_BETWEEN_DELAYED_ENQUEUING_MESSAGES = 10;

    private static final String THREAD_NAME = "mongo-dump";

    private final int batchSize;
    private final BlockingQueue<BasicDBObject[]> mongoDocQueue;
    private final int retryDuringSeconds;
    private final boolean retryOnConnectionErrors;
    private final Logger traversalLog = LoggerFactory.getLogger(PipelinedMongoDownloadTask.class.getName() + ".traversal");
    private final MongoCollection<BasicDBObject> dbCollection;
    private final ReadPreference readPreference;
    private final Stopwatch downloadStartWatch = Stopwatch.createUnstarted();

    private long totalEnqueueWaitTimeMillis = 0;
    private Instant lastDelayedEnqueueWarningMessageLoggedTimestamp = Instant.now();
    private long documentsRead = 0;
    private long nextLastModified = 0;
    private String lastIdDownloaded = null;

    public PipelinedMongoDownloadTask(MongoCollection<BasicDBObject> dbCollection,
                                      int batchSize,
                                      BlockingQueue<BasicDBObject[]> queue) {
        this.dbCollection = dbCollection;
        this.batchSize = batchSize;
        this.mongoDocQueue = queue;
        // Default retries for 5 minutes.
        this.retryDuringSeconds = ConfigHelper.getSystemPropertyAsInt(
                OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS);
        Preconditions.checkArgument(retryDuringSeconds > 0,
                "Property " + OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS + " must be > 0. Was: " + retryDuringSeconds);
        this.retryOnConnectionErrors = ConfigHelper.getSystemPropertyAsBoolean(
                OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS,
                DEFAULT_OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS);

        //TODO This may lead to reads being routed to secondary depending on MongoURI
        //So caller must ensure that its safe to read from secondary
//        this.readPreference = MongoDocumentStoreHelper.getConfiguredReadPreference(mongoStore, collection);
        this.readPreference = ReadPreference.secondaryPreferred();
        LOG.info("Using read preference {}", readPreference.getName());
    }

    @Override
    public Result call() throws Exception {
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName(THREAD_NAME);
        LOG.info("[TASK:{}:START] Starting to download from MongoDB.", THREAD_NAME.toUpperCase(Locale.ROOT));
        try {
            this.nextLastModified = 0;
            this.lastIdDownloaded = null;

            downloadStartWatch.start();
            if (!retryOnConnectionErrors) {
                downloadAll();
            } else {
                Instant failuresStartTimestamp = null; // When the last series of failures started
                long retryIntervalMs = retryInitialIntervalMillis;
                int numberOfFailures = 0;
                boolean downloadCompleted = false;
                Map<String, Integer> exceptions = new HashMap<>();
                while (!downloadCompleted) {
                    try {
                        if (lastIdDownloaded != null) {
                            LOG.info("Recovering from broken connection, finishing downloading documents with _modified={}", nextLastModified);
                            downloadRange(new DownloadRange(nextLastModified, nextLastModified + 1, lastIdDownloaded));
                            // We have managed to reconnect, reset the failure timestamp
                            failuresStartTimestamp = null;
                            numberOfFailures = 0;
                            // Continue downloading everything starting from the next _lastmodified value
                            downloadRange(new DownloadRange(nextLastModified + 1, Long.MAX_VALUE, null));
                        } else {
                            downloadRange(new DownloadRange(nextLastModified, Long.MAX_VALUE, null));
                        }
                        downloadCompleted = true;
                    } catch (MongoException e) {
                        if (e instanceof MongoInterruptedException || e instanceof MongoIncompatibleDriverException) {
                            // Non-recoverable exceptions
                            throw e;
                        }
                        if (failuresStartTimestamp == null) {
                            failuresStartTimestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS);
                        }
                        LOG.warn("Connection error downloading from MongoDB.", e);
                        long secondsSinceStartOfFailures = Duration.between(failuresStartTimestamp, Instant.now()).toSeconds();
                        if (secondsSinceStartOfFailures > retryDuringSeconds) {
                            // Give up. Get a string of all exceptions that were thrown
                            StringBuilder summary = new StringBuilder();
                            for (Map.Entry<String, Integer> entry : exceptions.entrySet()) {
                                summary.append("\n\t").append(entry.getValue()).append("x: ").append(entry.getKey());
                            }
                            throw new RetryException(retryDuringSeconds, summary.toString(), e);
                        } else {
                            numberOfFailures++;
                            LOG.warn("Retrying download in {} ms; number of times failed: {}; current series of failures started at: {} ({} seconds ago)",
                                    retryIntervalMs, numberOfFailures, failuresStartTimestamp, secondsSinceStartOfFailures);
                            exceptions.compute(e.getClass().getSimpleName() + " - " + e.getMessage(),
                                    (key, val) -> val == null ? 1 : val + 1
                            );
                            try {
                                Thread.sleep(retryIntervalMs);
                            } catch (InterruptedException ignore) {
                            }
                            // simple exponential backoff mechanism
                            retryIntervalMs = Math.min(retryMaxIntervalMillis, retryIntervalMs * 2);
                        }
                    }
                }
            }

            String enqueueingDelayPercentage = String.format("%1.2f", (100.0 * totalEnqueueWaitTimeMillis) / downloadStartWatch.elapsed(TimeUnit.MILLISECONDS));
            String metrics = MetricsFormatter.newBuilder()
                    .add("duration", FormatingUtils.formatToSeconds(downloadStartWatch))
                    .add("durationSeconds", downloadStartWatch.elapsed(TimeUnit.SECONDS))
                    .add("documentsDownloaded", documentsRead)
                    .add("enqueueingDelayMs", totalEnqueueWaitTimeMillis)
                    .add("enqueueingDelayPercentage", enqueueingDelayPercentage)
                    .build();

            LOG.info("[TASK:{}:END] Metrics: {}", THREAD_NAME.toUpperCase(Locale.ROOT), metrics);
            return new Result(documentsRead);
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
        BsonDocument findQuery = range.getFindQuery();
        LOG.info("Traversing: {}. Query: {}", range, findQuery);
        FindIterable<BasicDBObject> mongoIterable = dbCollection
                .withReadPreference(readPreference)
                .find(findQuery)
                .sort(ascending(NodeDocument.MODIFIED_IN_SECS, NodeDocument.ID));
        download(mongoIterable);
    }

    private void downloadAll() throws InterruptedException, TimeoutException {
        LOG.info("Traversing all documents");
        FindIterable<BasicDBObject> mongoIterable = dbCollection
                .withReadPreference(readPreference)
                .find();
        download(mongoIterable);
    }

    private void download(FindIterable<BasicDBObject> mongoIterable) throws InterruptedException, TimeoutException {
        try (MongoCursor<BasicDBObject> cursor = mongoIterable.iterator()) {
            BasicDBObject[] block = new BasicDBObject[batchSize];
            int nextIndex = 0;
            try {
                while (cursor.hasNext()) {
                    BasicDBObject next = cursor.next();
                    String id = next.getString(Document.ID);
                    // If we are retrying on connection errors, we need to keep track of the last _modified value
                    if (retryOnConnectionErrors) {
                        this.nextLastModified = next.getLong(NodeDocument.MODIFIED_IN_SECS);
                    }
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
                if (nextIndex > 0) {
                    LOG.info("Enqueueing last block of size: {}", nextIndex);
                    enqueuePartialBlock(block, nextIndex);
                }
            } catch (MongoException e) {
                if (e instanceof MongoInterruptedException || e instanceof MongoIncompatibleDriverException) {
                    // Non-recoverable exceptions
                    throw e;
                }
                // There may be some documents in the current batch, enqueue them and rethrow the exception
                if (nextIndex > 0) {
                    LOG.info("Connection interrupted with recoverable failure. Enqueueing partial block of size: {}", nextIndex);
                    enqueuePartialBlock(block, nextIndex);
                }
                throw e;
            }
        }
    }

    private void enqueuePartialBlock(BasicDBObject[] block, int nextIndex) throws InterruptedException, TimeoutException {
        if (block.length == nextIndex) {
            tryEnqueue(block);
        } else {
            BasicDBObject[] partialBlock = new BasicDBObject[nextIndex];
            System.arraycopy(block, 0, partialBlock, 0, nextIndex);
            tryEnqueue(partialBlock);
        }
    }

    private void tryEnqueue(BasicDBObject[] block) throws TimeoutException, InterruptedException {
        Stopwatch enqueueDelayStopwatch = Stopwatch.createStarted();
        if (!mongoDocQueue.offer(block, MONGO_QUEUE_OFFER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Timeout trying to enqueue batch of MongoDB documents. Waited " + MONGO_QUEUE_OFFER_TIMEOUT);
        }
        long enqueueDelay = enqueueDelayStopwatch.elapsed(TimeUnit.MILLISECONDS);
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
