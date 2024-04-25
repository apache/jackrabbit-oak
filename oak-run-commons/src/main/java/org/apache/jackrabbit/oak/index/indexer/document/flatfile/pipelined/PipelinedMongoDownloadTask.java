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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.MongoIncompatibleDriverException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.apache.jackrabbit.guava.common.base.Preconditions;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.MongoRegexPathFilterFactory.MongoFilterPaths;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreHelper;
import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexingReporter;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class PipelinedMongoDownloadTask implements Callable<PipelinedMongoDownloadTask.Result> {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMongoDownloadTask.class);

    private static final Logger TRAVERSAL_LOG = LoggerFactory.getLogger(PipelinedMongoDownloadTask.class.getName() + ".traversal");

    public static class Result {
        private final long documentsDownloaded;

        public Result(long documentsDownloaded) {
            this.documentsDownloaded = documentsDownloaded;
        }

        public long getDocumentsDownloaded() {
            return documentsDownloaded;
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

    /**
     * Whether to do path filtering in the Mongo query instead of doing a full traversal of the document store and
     * filtering in the indexing job. This feature may significantly reduce the number of documents downloaded from
     * Mongo.
     * The performance gains may not be proportional to the reduction in the number of documents downloaded because Mongo
     * still has to traverse all the documents. This is required because the regex expression used for path filtering
     * starts with a wildcard (because the _id starts with the depth of the path, so the regex expression must ignore
     * this part). Because of the wildcard at the start, Mongo cannot use of the index on _id.
     */
    public static final String OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING = "oak.indexer.pipelined.mongoRegexPathFiltering";
    public static final boolean DEFAULT_OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING = false;
    /**
     * Any document with a path that matches this regex pattern will not be downloaded. This pattern will be included
     * in the Mongo query, that is, the filtering is done by server-side at Mongo, which avoids downloading the documents
     * matching this query. This is typically a _suffix_, for example "/metadata.xml$|/renditions/.*.jpg$".
     * To exclude subtrees such as /content/abc, use mongoFilterPaths instead.
     */
    public static final String OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX = "oak.indexer.pipelined.mongoCustomExcludeEntriesRegex";
    public static final String DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX = "";

    /**
     * Maximum number of elements in the included/excluded paths list used for regex path filtering. If after
     * merging and de-deduplication of the paths of all the path filters the number of included or excluded paths exceeds
     * this value, then disable path filtering to avoid creating Mongo queries with large number of filters
     */
    public static final String OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING_MAX_PATHS = "oak.indexer.pipelined.mongoRegexPathFilteringMaxPaths";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING_MAX_PATHS = 20;

    /**
     * Additional Oak paths to exclude from downloading from Mongo. This is a comma-separated list of paths.
     * These paths are only filtered if the included paths computed from the indexes resolve to the root tree (/),
     * otherwise the value of this property is ignored.
     */
    public static final String OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS = "oak.indexer.pipelined.mongoCustomExcludedPaths";
    public static final String DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS = "";

    /**
     * Whether to download in parallel from Mongo with two streams, one per each secondary.
     * This applies only if Mongo is a cluster with two secondaries.
     * One thread downloads in ascending order of (_modified, _id) and the other in descending order, until they cross.
     * This feature requires that the retryOnConnectionErrors property is set to true, because it relies on downloading
     * in a given order (if retryOnConnectionErrors is false, the download is done in natural order, that is, it is
     * undefined).
     */
    public static final String OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP = "oak.indexer.pipelined.mongoParallelDump";
    public static final boolean DEFAULT_OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP = false;

    // Use a short initial retry interval. In most cases if the connection to a replica fails, there will be other
    // replicas available so a reconnection attempt will succeed immediately.
    private static final long retryInitialIntervalMillis = 100;
    private static final long retryMaxIntervalMillis = 10_000;

    // TODO: Revise this timeout. It is used to prevent the indexer from blocking forever if the queue is full.
    private static final Duration MONGO_QUEUE_OFFER_TIMEOUT = Duration.ofMinutes(30);
    private static final int MIN_INTERVAL_BETWEEN_DELAYED_ENQUEUING_MESSAGES = 10;
    private static final BsonDocument NATURAL_HINT = BsonDocument.parse("{ $natural: 1 }");
    private static final BsonDocument ID_INDEX_HINT = BsonDocument.parse("{ _id: 1 }");

    static final String THREAD_NAME_PREFIX = "mongo-dump";


    private final MongoClientURI mongoClientURI;
    private final MongoDocumentStore docStore;
    private final int maxBatchSizeBytes;
    private final int maxBatchNumberOfDocuments;
    private final BlockingQueue<NodeDocument[]> mongoDocQueue;
    private final List<PathFilter> pathFilters;
    private final StatisticsProvider statisticsProvider;
    private final IndexingReporter reporter;

    private final int retryDuringSeconds;
    private final boolean retryOnConnectionErrors;
    private final boolean regexPathFiltering;
    private final String customExcludeEntriesRegex;
    private final List<String> customExcludedPaths;
    private final boolean parallelDump;
    private final MongoRegexPathFilterFactory regexPathFilterFactory;

    private MongoCollection<NodeDocument> dbCollection;
    private PipelinedMongoServerSelector mongoServerSelector;
    private final Stopwatch downloadStartWatch = Stopwatch.createUnstarted();
    private final DownloadStageStatistics downloadStageStatistics = new DownloadStageStatistics();
    private Instant lastDelayedEnqueueWarningMessageLoggedTimestamp = Instant.now();
    private MongoParallelDownloadCoordinator mongoParallelDownloadCoordinator;

    public PipelinedMongoDownloadTask(MongoClientURI mongoClientURI,
                                      MongoDocumentStore docStore,
                                      int maxBatchSizeBytes,
                                      int maxBatchNumberOfDocuments,
                                      BlockingQueue<NodeDocument[]> queue,
                                      List<PathFilter> pathFilters,
                                      StatisticsProvider statisticsProvider,
                                      IndexingReporter reporter) {
        this.mongoClientURI = mongoClientURI;
        this.docStore = docStore;
        this.statisticsProvider = statisticsProvider;
        this.reporter = reporter;
        this.maxBatchSizeBytes = maxBatchSizeBytes;
        this.maxBatchNumberOfDocuments = maxBatchNumberOfDocuments;
        this.mongoDocQueue = queue;
        this.pathFilters = pathFilters;

        // Default retries for 5 minutes.
        this.retryDuringSeconds = ConfigHelper.getSystemPropertyAsInt(
                OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS);
        Preconditions.checkArgument(retryDuringSeconds > 0,
                "Property " + OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS + " must be > 0. Was: " + retryDuringSeconds);
        this.reporter.addConfig(OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS, String.valueOf(retryDuringSeconds));

        this.retryOnConnectionErrors = ConfigHelper.getSystemPropertyAsBoolean(
                OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS,
                DEFAULT_OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS);
        this.reporter.addConfig(OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, String.valueOf(retryOnConnectionErrors));

        this.regexPathFiltering = ConfigHelper.getSystemPropertyAsBoolean(
                OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING);
        this.reporter.addConfig(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, String.valueOf(regexPathFiltering));

        int regexPathFilteringMaxNumberOfPaths = ConfigHelper.getSystemPropertyAsInt(
                OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING_MAX_PATHS,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING_MAX_PATHS);
        this.reporter.addConfig(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING_MAX_PATHS, String.valueOf(regexPathFilteringMaxNumberOfPaths));
        this.regexPathFilterFactory = new MongoRegexPathFilterFactory(regexPathFilteringMaxNumberOfPaths);

        var parallelDumpConfig = ConfigHelper.getSystemPropertyAsBoolean(OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP, DEFAULT_OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP);
        if (parallelDumpConfig && !retryOnConnectionErrors) {
            LOG.warn("Parallel dump requires " + OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS + " to be set to true, but it is false. Disabling parallel dump.");
            this.parallelDump = false;
        } else {
            this.parallelDump = parallelDumpConfig;
        }
        this.reporter.addConfig(OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP, String.valueOf(parallelDump));

        this.customExcludeEntriesRegex = ConfigHelper.getSystemPropertyAsString(
                OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX
        );
        this.reporter.addConfig(OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX, customExcludeEntriesRegex);

        var excludePathsString = ConfigHelper.getSystemPropertyAsString(
                OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS
        ).trim();
        this.reporter.addConfig(OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS, excludePathsString);
        if (excludePathsString.isEmpty()) {
            this.customExcludedPaths = List.of();
        } else if (!regexPathFiltering) {
            LOG.info("Ignoring custom excluded paths because regex path filtering is disabled");
            this.customExcludedPaths = List.of();
        } else {
            this.customExcludedPaths = Arrays.stream(excludePathsString.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
        }
        var invalidPaths = customExcludedPaths.stream()
                .filter(p -> !PathUtils.isValid(p) || !PathUtils.isAbsolute(p) || PathUtils.denotesRoot(p))
                .collect(Collectors.toList());
        if (!invalidPaths.isEmpty()) {
            throw new IllegalArgumentException("Invalid paths in " + OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS + " " +
                    " system property: " + invalidPaths + ". Paths must be valid, must be absolute and must not be the root.");
        }
    }

    @Override
    public Result call() throws Exception {
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName(THREAD_NAME_PREFIX);
        try {
            // When downloading in parallel, we must create the connection to Mongo using a custom instance of ServerSelector
            // instead of using the default policy defined by readPreference configuration setting.
            // Here we create the configuration that is common to the two cases (parallelDump true or false).
            NodeDocumentCodecProvider nodeDocumentCodecProvider = new NodeDocumentCodecProvider(docStore, Collection.NODES);
            CodecRegistry nodeDocumentCodecRegistry = CodecRegistries.fromRegistries(
                    CodecRegistries.fromProviders(nodeDocumentCodecProvider),
                    MongoClientSettings.getDefaultCodecRegistry()
            );

            MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(mongoClientURI.getURI()))
                    .readPreference(ReadPreference.secondaryPreferred());
            if (parallelDump) {
                // Set a custom server selector that is able to distribute the two connections between the two secondaries.
                // Overrides the readPreference setting. We also need to listen for changes in the cluster to detect
                // when a node is promoted to primary, so we can stop downloading from that node
                this.mongoServerSelector = new PipelinedMongoServerSelector();
                settingsBuilder.applyToClusterSettings(builder -> builder
                        .serverSelector(mongoServerSelector)
                        .addClusterListener(mongoServerSelector)
                );
                // Shared object between the two download threads to store the last document download by either of them
                // and determine when the two downloads have crossed
                this.mongoParallelDownloadCoordinator = new MongoParallelDownloadCoordinator();
            } else {
                this.mongoParallelDownloadCoordinator = null;
                this.mongoServerSelector = null;
            }

            String mongoDatabaseName = MongoDocumentStoreHelper.getMongoDatabaseName(docStore);
            try (MongoClient client = MongoClients.create(settingsBuilder.build())) {
                MongoDatabase mongoDatabase = client.getDatabase(mongoDatabaseName);
                this.dbCollection = mongoDatabase
                        .withCodecRegistry(nodeDocumentCodecRegistry)
                        .getCollection(Collection.NODES.toString(), NodeDocument.class);

                LOG.info("[TASK:{}:START] Starting to download from MongoDB", Thread.currentThread().getName().toUpperCase(Locale.ROOT));
                try {
                    downloadStartWatch.start();
                    if (retryOnConnectionErrors) {
                        downloadWithRetryOnConnectionErrors();
                    } else {
                        downloadWithNaturalOrdering();
                    }
                    downloadStartWatch.stop();
                    long durationMillis = downloadStartWatch.elapsed(TimeUnit.MILLISECONDS);
                    downloadStageStatistics.publishStatistics(statisticsProvider, reporter, durationMillis);
                    String metrics = downloadStageStatistics.formatStats(durationMillis);
                    LOG.info("[TASK:{}:END] Metrics: {}", Thread.currentThread().getName().toUpperCase(Locale.ROOT), metrics);
                    reporter.addTiming("Mongo dump", FormattingUtils.formatToSeconds(downloadStartWatch));
                    return new PipelinedMongoDownloadTask.Result(downloadStageStatistics.getDocumentsDownloadedTotal());
                } catch (InterruptedException t) {
                    LOG.warn("Thread interrupted", t);
                    throw t;
                } catch (Throwable t) {
                    LOG.warn("Thread terminating with exception.", t);
                    throw t;
                }
            }
        } finally {
            Thread.currentThread().setName(originalName);
        }
    }

    private void downloadWithNaturalOrdering() throws InterruptedException, TimeoutException {
        LOG.info("Downloading with natural order");
        // We are downloading potentially a large fraction of the repository, so using an index scan will be
        // inefficient. So we pass the natural hint to force MongoDB to use natural ordering, that is, column scan
        MongoFilterPaths mongoFilterPaths = getPathsForRegexFiltering();
        Bson mongoFilter = MongoDownloaderRegexUtils.computeMongoQueryFilter(mongoFilterPaths, customExcludeEntriesRegex);

        if (mongoFilter == null) {
            LOG.info("Downloading full repository from Mongo with natural order");
            FindIterable<NodeDocument> mongoIterable = dbCollection
                    .find()
                    .hint(NATURAL_HINT);
            DownloadTask downloadTask = new DownloadTask(DownloadOrder.UNDEFINED, downloadStageStatistics);
            downloadTask.download(mongoIterable);
            downloadTask.reportFinalResults();

        } else {
            DownloadTask ancestorsDownloadTask = new DownloadTask(DownloadOrder.UNDEFINED, downloadStageStatistics);
            ancestorsDownloadTask.downloadAncestors(mongoFilterPaths.included);

            DownloadTask downloadTask = new DownloadTask(DownloadOrder.UNDEFINED, downloadStageStatistics);
            LOG.info("Downloading from Mongo with natural order using filter: {}", mongoFilter);
            FindIterable<NodeDocument> findIterable = dbCollection
                    .find(mongoFilter)
                    .hint(NATURAL_HINT);
            downloadTask.download(findIterable);
            downloadTask.reportFinalResults();
        }
    }

    private void downloadWithRetryOnConnectionErrors() throws InterruptedException, TimeoutException {
        LOG.info("Downloading with retry on connection errors, index scan");
        // If regex filtering is enabled, start by downloading the ancestors of the path used for filtering.
        // That is, download "/", "/content", "/content/dam" for a base path of "/content/dam". These nodes will not be
        // matched by the regex used in the Mongo query, which assumes a prefix of "???:/content/dam"
        MongoFilterPaths mongoFilterPaths = getPathsForRegexFiltering();
        Bson mongoFilter = MongoDownloaderRegexUtils.computeMongoQueryFilter(mongoFilterPaths, customExcludeEntriesRegex);
        if (mongoFilter == null) {
            LOG.info("Downloading full repository");
        } else {
            LOG.info("Downloading from Mongo using filter: {}", mongoFilter);
            // Regex path filtering is enabled
            // Download the ancestors in a separate query. No retrials done on this query, as it will take only a few
            // seconds and is done at the start of the job, so if it fails, the job can be retried without losing much work
            DownloadTask ancestorsDownloadTask = new DownloadTask(DownloadOrder.UNDEFINED, downloadStageStatistics);
            ancestorsDownloadTask.downloadAncestors(mongoFilterPaths.included);
        }
        if (parallelDump) {
            DownloadTask ascendingDownloadTask = new DownloadTask(DownloadOrder.ASCENDING, downloadStageStatistics);
            DownloadTask descendingDownloadTask = new DownloadTask(DownloadOrder.DESCENDING, downloadStageStatistics);
            LOG.info("Downloading in parallel with two connections, one in ascending the other in descending order");

            // Launch a thread to monitor the total download rate, that is, the sum of the metrics of each download thread
            ScheduledExecutorService monitorThreadPool = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setNameFormat(THREAD_NAME_PREFIX).setDaemon(true).build()
            );
            ScheduledFuture<?> monitorTask = monitorThreadPool.scheduleWithFixedDelay(() -> {
                long secondsElapsed = downloadStartWatch.elapsed(TimeUnit.SECONDS);
                String formattedRate;
                if (secondsElapsed == 0) {
                    formattedRate = "N/A nodes/s, N/A nodes/hr, N/A /s";
                } else {
                    double docRate = ((double) downloadStageStatistics.getDocumentsDownloadedTotal()) / secondsElapsed;
                    double bytesRate = ((double) downloadStageStatistics.getDocumentsDownloadedTotalBytes()) / secondsElapsed;
                    formattedRate = String.format(Locale.ROOT, "%1.2f nodes/s, %1.2f nodes/hr, %s/s",
                            docRate, docRate * 3600, IOUtils.humanReadableByteCountBin((long) bytesRate));
                }
                LOG.info("Dumping from NSET Traversed #{} - [{}] (Elapsed {})",
                        downloadStageStatistics.getDocumentsDownloadedTotal(), formattedRate, FormattingUtils.formatToSeconds(secondsElapsed));
            }, 10, 10, TimeUnit.SECONDS);

            // The current thread will download in ascending order. We launch a separate thread to download in
            // descending order.
            var originalName = Thread.currentThread().getName();
            Thread.currentThread().setName(THREAD_NAME_PREFIX + "-ascending");
            try {
                var descendingDownloadThread = new Thread(() -> {
                    try {
                        descendingDownloadTask.download(mongoFilter);
                        descendingDownloadTask.reportFinalResults();
                    } catch (InterruptedException | TimeoutException e) {
                        throw new RuntimeException(e);
                    } finally {
                        mongoServerSelector.threadFinished();
                    }
                }, THREAD_NAME_PREFIX + "-descending");
                descendingDownloadThread.start();

                // Download in ascending order in the current thread.
                ascendingDownloadTask.download(mongoFilter);
                ascendingDownloadTask.reportFinalResults();
                LOG.info("Ascending download finished. Waiting for descending download to finish.");
                descendingDownloadThread.join();
            } finally {
                mongoServerSelector.threadFinished();
                Thread.currentThread().setName(originalName);
                monitorTask.cancel(false);
                monitorThreadPool.shutdown();
            }
            LOG.info("Download complete.");
        } else {
            // Single threaded dump
            DownloadTask downloadTask = new DownloadTask(DownloadOrder.UNDEFINED, downloadStageStatistics);
            downloadTask.download(mongoFilter);
        }
    }

    private MongoFilterPaths getPathsForRegexFiltering() {
        if (!regexPathFiltering) {
            LOG.info("Regex path filtering disabled.");
            return MongoFilterPaths.DOWNLOAD_ALL;
        } else {
            LOG.info("Computing included/excluded paths for Mongo regex path filtering. PathFilters: {}",
                    pathFilters.stream()
                            .map(pf -> "PF{includedPaths=" + pf.getIncludedPaths() + ", excludedPaths=" + pf.getExcludedPaths() + "}")
                            .collect(Collectors.joining(", "))
            );
            MongoFilterPaths mongoFilterPaths = this.regexPathFilterFactory.buildMongoFilter(pathFilters, customExcludedPaths);
            LOG.info("Paths used for regex filtering on Mongo: {}", mongoFilterPaths);
            return mongoFilterPaths;
        }
    }

    private void logWithRateLimit(Runnable f) {
        Instant now = Instant.now();
        if (Duration.between(lastDelayedEnqueueWarningMessageLoggedTimestamp, now).toSeconds() > MIN_INTERVAL_BETWEEN_DELAYED_ENQUEUING_MESSAGES) {
            f.run();
            lastDelayedEnqueueWarningMessageLoggedTimestamp = now;
        }
    }

    enum DownloadOrder {
        ASCENDING,
        DESCENDING,
        UNDEFINED;

        public boolean downloadInAscendingOrder() {
            return this == ASCENDING || this == UNDEFINED;
        }
    }

    private class DownloadTask {
        private final DownloadOrder downloadOrder;
        private final DownloadStageStatistics downloadStatics;
        private long documentsDownloadedTotalBytes;
        private long documentsDownloadedTotal;
        private long totalEnqueueWaitTimeMillis;
        private long nextLastModified;
        private String lastIdDownloaded;

        DownloadTask(DownloadOrder downloadOrder, DownloadStageStatistics downloadStatics) {
            this.downloadOrder = downloadOrder;
            this.downloadStatics = downloadStatics;
            this.documentsDownloadedTotalBytes = 0;
            this.documentsDownloadedTotal = 0;
            this.totalEnqueueWaitTimeMillis = 0;
            this.nextLastModified = downloadOrder.downloadInAscendingOrder() ? 0 : Long.MAX_VALUE;
            this.lastIdDownloaded = null;
        }

        private Instant failuresStartTimestamp = null; // When the last series of failures started
        private int numberOfFailures = 0;

        private void download(Bson mongoQueryFilter) throws InterruptedException, TimeoutException {
            failuresStartTimestamp = null; // When the last series of failures started
            numberOfFailures = 0;
            long retryIntervalMs = retryInitialIntervalMillis;
            boolean downloadCompleted = false;
            Map<String, Integer> exceptions = new HashMap<>();
            while (!downloadCompleted) {
                try {
                    if (lastIdDownloaded == null) {
                        // lastIdDownloaded is null only when starting the download or if there is a connection error
                        // before anything is downloaded
                        var firstRange = new DownloadRange(0, Long.MAX_VALUE, null, downloadOrder.downloadInAscendingOrder());
                        downloadRange(firstRange, mongoQueryFilter, downloadOrder);
                    } else {
                        LOG.info("Recovering from broken connection, finishing downloading documents with _modified={}", nextLastModified);
                        var partialLastModifiedRange = new DownloadRange(nextLastModified, nextLastModified, lastIdDownloaded, downloadOrder.downloadInAscendingOrder());
                        downloadRange(partialLastModifiedRange, mongoQueryFilter, downloadOrder);
                        // Downloaded everything from _nextLastModified. Continue with the next timestamp for _modified
                        var nextRange = downloadOrder.downloadInAscendingOrder() ?
                                new DownloadRange(nextLastModified + 1, Long.MAX_VALUE, null, true) :
                                new DownloadRange(0, nextLastModified - 1, null, false);
                        downloadRange(nextRange, mongoQueryFilter, downloadOrder);
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
                    if (parallelDump && mongoServerSelector.atLeastOneConnectionActive()) {
                        // Special case, the cluster is up because one of the connections is active. This happens when
                        // there is a single secondary, maybe because of a scale-up/down. In this case, do not abort the
                        // download, keep retrying to connect forever
                        int retryTime = 1000;
                        LOG.info("At least one connection is active. Retrying download in {} ms", retryTime);
                        Thread.sleep(retryTime);
                    } else if (secondsSinceStartOfFailures > retryDuringSeconds) {
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
                        Thread.sleep(retryIntervalMs);
                        // simple exponential backoff mechanism
                        retryIntervalMs = Math.min(retryMaxIntervalMillis, retryIntervalMs * 2);
                    }
                }
            }
        }

        private void downloadRange(DownloadRange range, Bson filter, DownloadOrder downloadOrder) throws InterruptedException, TimeoutException {
            Bson findQuery = range.getFindQuery();
            if (filter != null) {
                findQuery = Filters.and(findQuery, filter);
            }
            Bson sortOrder = downloadOrder.downloadInAscendingOrder() ?
                    Sorts.ascending(NodeDocument.MODIFIED_IN_SECS, NodeDocument.ID) :
                    Sorts.descending(NodeDocument.MODIFIED_IN_SECS, NodeDocument.ID);

            FindIterable<NodeDocument> mongoIterable = dbCollection
                    .find(findQuery)
                    .sort(sortOrder);

            LOG.info("Traversing: {}. Query: {}, Traverse order: {}", range, findQuery, sortOrder);
            download(mongoIterable);
        }

        void download(FindIterable<NodeDocument> mongoIterable) throws InterruptedException, TimeoutException {
            try (MongoCursor<NodeDocument> cursor = mongoIterable.iterator()) {
                NodeDocument[] batch = new NodeDocument[maxBatchNumberOfDocuments];
                int nextIndex = 0;
                int batchSize = 0;
                if (cursor.hasNext()) {
                    // We have managed to reconnect, reset the failure timestamp
                    failuresStartTimestamp = null;
                    numberOfFailures = 0;
                }
                try {
                    while (cursor.hasNext()) {
                        NodeDocument next = cursor.next();
                        String id = next.getId();
                        this.nextLastModified = next.getModified();
                        this.lastIdDownloaded = id;
                        this.documentsDownloadedTotal++;
                        downloadStatics.incrementDocumentsDownloadedTotal();
                        reportProgress(id);

                        batch[nextIndex] = next;
                        nextIndex++;
                        int docSize = (int) next.remove(NodeDocumentCodec.SIZE_FIELD);
                        batchSize += docSize;
                        documentsDownloadedTotalBytes += docSize;
                        downloadStageStatistics.incrementDocumentsDownloadedTotalBytes(docSize);
                        if (batchSize >= maxBatchSizeBytes || nextIndex == batch.length) {
                            LOG.trace("Enqueuing block with {} elements, estimated size: {} bytes", nextIndex, batchSize);
                            var downloadCompleted = tryEnqueueCopy(batch, nextIndex);
                            if (downloadCompleted) {
                                LOG.info("Download of range with download order {} completed, intersected with other download.", downloadOrder);
                                return;
                            }
                            nextIndex = 0;
                            batchSize = 0;
                            if (parallelDump && mongoServerSelector.isConnectedToPrimary()) {
                                LOG.info("Connected to primary. Will disconnect from MongoDB to force a new connection to a secondary.");
                                throw new MongoException("Disconnecting from MongoDB primary to force a new connection");

                            }
                        }
                    }
                    if (nextIndex > 0) {
                        LOG.info("Enqueueing last block with {} elements, estimated size: {}",
                                nextIndex, IOUtils.humanReadableByteCountBin(batchSize));
                        tryEnqueueCopy(batch, nextIndex);
                    }
                } catch (MongoException e) {
                    if (e instanceof MongoInterruptedException || e instanceof MongoIncompatibleDriverException) {
                        // Non-recoverable exceptions
                        throw e;
                    }
                    // There may be some documents in the current batch, enqueue them and rethrow the exception
                    if (nextIndex > 0) {
                        LOG.info("Connection interrupted with recoverable failure. Enqueueing partial block with {} elements, estimated size: {}",
                                nextIndex, IOUtils.humanReadableByteCountBin(batchSize));
                        tryEnqueueCopy(batch, nextIndex);
                    }
                    throw e;
                }
            }
        }

        private void downloadAncestors(List<String> basePath) throws InterruptedException, TimeoutException {
            if (basePath.size() == 1 && basePath.get(0).equals("/")) {
                return; // No need to download ancestors of root, the root will be downloaded as part of the normal traversal
            }
            Bson ancestorQuery = MongoDownloaderRegexUtils.ancestorsFilter(basePath);
            LOG.info("Downloading ancestors of: {}, Query: {}.", basePath, ancestorQuery);
            FindIterable<NodeDocument> ancestorsIterable = dbCollection
                    .find(ancestorQuery)
                    // Use the index on _id: this query returns very few documents and the filter condition is on _id.
                    .hint(ID_INDEX_HINT);
            download(ancestorsIterable);
        }

        private boolean tryEnqueueCopy(NodeDocument[] batch, int sizeOfBatch) throws TimeoutException, InterruptedException {
            // Find the first element that was not yet added
            boolean completedDownload = false;
            int effectiveSize = sizeOfBatch;
            if (downloadOrder != DownloadOrder.UNDEFINED) {
                var sizeOfRangeNotAdded = downloadOrder == DownloadOrder.ASCENDING ?
                        mongoParallelDownloadCoordinator.extendLowerRange(batch, sizeOfBatch) :
                        mongoParallelDownloadCoordinator.extendUpperRange(batch, sizeOfBatch);
                if (sizeOfRangeNotAdded != sizeOfBatch) {
                    completedDownload = true;
                    effectiveSize = sizeOfRangeNotAdded;
                    var firstAlreadySeen = batch[sizeOfRangeNotAdded];
                    LOG.info("Download complete, reached already seen document: {}: {}", firstAlreadySeen.getModified(), firstAlreadySeen.getId());
                }
            }
            if (effectiveSize == 0) {
                LOG.info("Batch is empty, not enqueuing.");
            } else {
                NodeDocument[] copyOfBatch = Arrays.copyOfRange(batch, 0, effectiveSize);
                Stopwatch enqueueDelayStopwatch = Stopwatch.createStarted();
                if (!mongoDocQueue.offer(copyOfBatch, MONGO_QUEUE_OFFER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new TimeoutException("Timeout trying to enqueue batch of MongoDB documents. Waited " + MONGO_QUEUE_OFFER_TIMEOUT);
                }
                long enqueueDelay = enqueueDelayStopwatch.elapsed(TimeUnit.MILLISECONDS);
                this.totalEnqueueWaitTimeMillis += enqueueDelay;
                downloadStageStatistics.incrementTotalEnqueueWaitTimeMillis(enqueueDelay);
                if (enqueueDelay > 1) {
                    logWithRateLimit(() ->
                            LOG.info("Enqueuing of Mongo document batch was delayed, took {} ms. mongoDocQueue size {}. " +
                                            "Consider increasing the number of Transform threads. " +
                                            "(This message is logged at most once every {} seconds)",
                                    enqueueDelay, mongoDocQueue.size(), MIN_INTERVAL_BETWEEN_DELAYED_ENQUEUING_MESSAGES)
                    );
                }
            }
            return completedDownload;
        }

        void reportFinalResults() {
            long secondsElapsed = downloadStartWatch.elapsed(TimeUnit.SECONDS);
            String formattedRate;
            if (secondsElapsed == 0) {
                formattedRate = "N/A nodes/s, N/A nodes/hr, N/A /s";
            } else {
                double docRate = ((double) this.documentsDownloadedTotal) / secondsElapsed;
                double bytesRate = ((double) this.documentsDownloadedTotalBytes) / secondsElapsed;
                formattedRate = String.format(Locale.ROOT, "%1.2f nodes/s, %1.2f nodes/hr, %s/s",
                        docRate, docRate * 3600, IOUtils.humanReadableByteCountBin((long) bytesRate));
            }
            LOG.info("Finished download task. Dumped {} documents. Rate: {}. Elapsed {}.",
                    this.documentsDownloadedTotal, formattedRate, FormattingUtils.formatToSeconds(downloadStartWatch));
        }

        private void reportProgress(String id) {
            if (this.documentsDownloadedTotal % 10000 == 0) {
                long secondsElapsed = downloadStartWatch.elapsed(TimeUnit.SECONDS);
                String formattedRate;
                if (secondsElapsed == 0) {
                    formattedRate = "N/A nodes/s, N/A nodes/hr, N/A /s";
                } else {
                    double docRate = ((double) this.documentsDownloadedTotal) / secondsElapsed;
                    double bytesRate = ((double) this.documentsDownloadedTotalBytes) / secondsElapsed;
                    formattedRate = String.format(Locale.ROOT, "%1.2f nodes/s, %1.2f nodes/hr, %s/s",
                            docRate, docRate * 3600, IOUtils.humanReadableByteCountBin((long) bytesRate));
                }
                LOG.info("Dumping from NSET Traversed #{} {} [{}] (Elapsed {})",
                        this.documentsDownloadedTotal, id, formattedRate, FormattingUtils.formatToSeconds(secondsElapsed));
            }
            TRAVERSAL_LOG.trace(id);
        }
    }
}
