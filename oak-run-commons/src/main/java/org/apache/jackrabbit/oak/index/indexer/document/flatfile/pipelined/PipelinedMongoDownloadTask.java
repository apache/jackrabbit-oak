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
import com.mongodb.client.model.Filters;
import org.apache.jackrabbit.guava.common.base.Preconditions;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.regex;
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
    public static final String OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING = "oak.indexer.pipelined.mongoRegexPathFiltering";
    public static final boolean DEFAULT_OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING = true;
    // Use a short initial retry interval. In most cases if the connection to a replica fails, there will be other
    // replicas available so a reconnection attempt will succeed immediately.
    private final static long retryInitialIntervalMillis = 100;
    private final static long retryMaxIntervalMillis = 10_000;

    // TODO: Revise this timeout. It is used to prevent the indexer from blocking forever if the queue is full.
    private static final Duration MONGO_QUEUE_OFFER_TIMEOUT = Duration.ofMinutes(30);
    private static final int MIN_INTERVAL_BETWEEN_DELAYED_ENQUEUING_MESSAGES = 10;
    private final static BsonDocument NATURAL_HINT =  BsonDocument.parse("{ $natural:1}");

    private final int batchSize;
    private final BlockingQueue<BasicDBObject[]> mongoDocQueue;
    private final List<PathFilter> pathFilters;
    private final int retryDuringSeconds;
    private final boolean retryOnConnectionErrors;
    private final boolean regexPathFiltering;
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
                                      BlockingQueue<BasicDBObject[]> queue,
                                      List<PathFilter> pathFilters) {
        this.dbCollection = dbCollection;
        this.batchSize = batchSize;
        this.mongoDocQueue = queue;
        this.pathFilters = pathFilters;
        // Default retries for 5 minutes.
        this.retryDuringSeconds = ConfigHelper.getSystemPropertyAsInt(
                OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS);
        Preconditions.checkArgument(retryDuringSeconds > 0,
                "Property " + OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS + " must be > 0. Was: " + retryDuringSeconds);
        this.retryOnConnectionErrors = ConfigHelper.getSystemPropertyAsBoolean(
                OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS,
                DEFAULT_OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS);
        this.regexPathFiltering = ConfigHelper.getSystemPropertyAsBoolean(
                OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING);

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

            downloadStartWatch.start();
            if (retryOnConnectionErrors) {
                downloadWithRetryOnConnectionErrors();
            } else {
                downloadWithNaturalOrdering();
            }
            LOG.info("Terminating task. Downloaded {} Mongo documents in {}. Total enqueuing delay: {} ms ({}%)",
                    documentsRead, downloadStartWatch,
                    totalEnqueueWaitTimeMillis,
                    String.format("%1.2f", (100.0 * totalEnqueueWaitTimeMillis) / downloadStartWatch.elapsed(TimeUnit.MILLISECONDS)));
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


    private void downloadWithRetryOnConnectionErrors() throws InterruptedException, TimeoutException {
        // If regex filtering is enabled, start by downloading the ancestors of the path used for filtering.
        // That is, download "/", "/content", "/content/dam" for a base path of "/content/dam". These nodes will not be
        // matched by the regex used in the Mongo query, which assumes a prefix of "--:/content/dam"
        Bson childrenFilter = null;
        String regexBasePath = getPathForRegexFiltering();
        if (regexBasePath != null) {
            // Download the ancestors in a separate query. No retrials done on this query, as it will take only a few
            // seconds and is done at the start of the job, so if it fails, the job can be retried without losing much work
            LOG.info("Using regex filtering with path {}", regexBasePath);
            Bson ancestorsQuery = ancestorsFilter(regexBasePath);
            LOG.info("Downloading ancestors of {}", regexBasePath);
            // Let Mongo decide which index to use for this query, it will return very few documents
            FindIterable<BasicDBObject> mongoIterable = dbCollection
                    .withReadPreference(readPreference)
                    .find(ancestorsQuery);
            download(mongoIterable);
            childrenFilter = childrenFilter(regexBasePath);
        }

        Instant failuresStartTimestamp = null; // When the last series of failures started
        long retryIntervalMs = retryInitialIntervalMillis;
        int numberOfFailures = 0;
        boolean downloadCompleted = false;
        Map<String, Integer> exceptions = new HashMap<>();
        this.nextLastModified = 0;
        this.lastIdDownloaded = null;
        while (!downloadCompleted) {
            try {
                if (lastIdDownloaded != null) {
                    LOG.info("Recovering from broken connection, finishing downloading documents with _modified={}", nextLastModified);
                    downloadRange(new DownloadRange(nextLastModified, nextLastModified + 1, lastIdDownloaded), childrenFilter);
                    // We have managed to reconnect, reset the failure timestamp
                    failuresStartTimestamp = null;
                    numberOfFailures = 0;
                    // Continue downloading everything starting from the next _lastmodified value
                    downloadRange(new DownloadRange(nextLastModified + 1, Long.MAX_VALUE, null), childrenFilter);
                } else {
                    downloadRange(new DownloadRange(nextLastModified, Long.MAX_VALUE, null), childrenFilter);
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

    private void downloadRange(DownloadRange range, Bson filter) throws InterruptedException, TimeoutException {
        Bson findQuery = range.getFindQuery();
        if (filter != null) {
            findQuery = Filters.and(findQuery, filter);
        }
        LOG.info("Traversing: {}. Query: {}", range, findQuery);
        FindIterable<BasicDBObject> mongoIterable = dbCollection
                .withReadPreference(readPreference)
                .find(findQuery)
                .sort(ascending(NodeDocument.MODIFIED_IN_SECS, NodeDocument.ID));
        download(mongoIterable);
    }

    private void downloadWithNaturalOrdering() throws InterruptedException, TimeoutException {
        // We are downloading potentially a large fraction of the repository, so using an index scan will be
        // inefficient. So we pass the natural hint to force MongoDB to use natural ordering, that is, column scan
        String regexBasePath = getPathForRegexFiltering();
        if (regexBasePath == null) {
            LOG.info("Downloading full repository using natural order");
            FindIterable<BasicDBObject> mongoIterable = dbCollection
                    .withReadPreference(readPreference)
                    .find()
                    .hint(NATURAL_HINT);
            download(mongoIterable);

        } else {
            Bson ancestorQuery = ancestorsFilter(regexBasePath);
            // Do not use natural order to download ancestors. The number of ancestors will always be small, likely less
            // than 10, so let MongoDB use an index to find them.
            LOG.info("Downloading using regex path filtering. Downloading ancestors: {}.", ancestorQuery);
            FindIterable<BasicDBObject> ancestorsIterable = dbCollection
                    .withReadPreference(readPreference)
                    .find(ancestorQuery);
            download(ancestorsIterable);

            Bson childrenQuery = childrenFilter(regexBasePath);
            LOG.info("Downloading using regex path filtering. Downloading children: {}.", childrenQuery);
            FindIterable<BasicDBObject> childrenIterable = dbCollection
                    .withReadPreference(readPreference)
                    .find(childrenQuery)
                    .hint(NATURAL_HINT);
            download(childrenIterable);
        }
    }

    private String getPathForRegexFiltering() {
        if (!regexPathFiltering) {
            LOG.info("Regex path filtering disabled.");
            return null;
        }
        LOG.info("Creating regex filter from pathFilters: " + pathFilters);
        if (pathFilters == null) {
            return null;
        }
        if (pathFilters.size() != 1) {
            return null;
        }
        PathFilter pathFilter = pathFilters.get(0);
        List<String> includePaths = pathFilter.getIncludedPaths();
        List<String> excludePaths = pathFilter.getExcludedPaths();

        if (excludePaths.isEmpty() && includePaths.size() == 1) {
            return includePaths.get(0);
        } else {
            return null;
        }
    }

    private static Bson childrenFilter(String path) {
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        String quotedPath = Pattern.quote(path);
        // For entries with path sizes above a certain threshold, the _id field contains a hash instead of the path of
        // the entry. The path is stored instead in the _path field. Therefore, we have to include in the filter also
        // the documents with matching _path.
        return Filters.or(
                regex(NodeDocument.ID, Pattern.compile("^[0-9]{1,3}:" + quotedPath + ".*$")),
                regex(NodeDocument.PATH, Pattern.compile(quotedPath + ".*$"))
        );
    }

    private static Bson ancestorsFilter(String path) {
        ArrayList<Bson> parentFilters = new ArrayList<>();
        int depth = PathUtils.getDepth(path);
        while (true) {
            parentFilters.add(Filters.eq(NodeDocument.ID, depth + ":" + path));
            if (depth == 0) {
                break;
            }
            path = PathUtils.getParentPath(path);
            depth--;
        }
        return Filters.or(parentFilters);
    }

    private void download(FindIterable<BasicDBObject> mongoIterable) throws InterruptedException, TimeoutException {
        try (MongoCursor<BasicDBObject> cursor = mongoIterable.iterator()) {
            BasicDBObject[] block = new BasicDBObject[batchSize];
            int nextIndex = 0;
            try {
                while (cursor.hasNext()) {
                    BasicDBObject next = cursor.next();
                    String id = next.getString(NodeDocument.ID);
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
