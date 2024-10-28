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
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.MongoRegexPathFilterFactory.MongoFilterPaths;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.MongoParallelDownloadCoordinator.RawBsonDocumentWrapper;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreHelper;
import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexingReporter;
import org.apache.jackrabbit.oak.plugins.index.MetricsFormatter;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.ByteBuf;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.bson.io.ByteBufferBsonInput;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.INDEXING_PHASE_LOGGER;

public class PipelinedMongoDownloadTask implements Callable<PipelinedMongoDownloadTask.Result> {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMongoDownloadTask.class);

    private static final Logger TRAVERSAL_LOG = LoggerFactory.getLogger(PipelinedMongoDownloadTask.class.getName() + ".traversal");
    public static final RawBsonDocument[] SENTINEL_MONGO_DOCUMENT = new RawBsonDocument[0];

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

    /**
     * When using parallel download, allow downloading from any replica. By default, we download only from secondaries,
     * allowing only a single download from each of the secondaries. This is done to minimize the load on the primary
     * and to spread the load between the two secondaries. But in some cases it may be preferable to allow unrestricted
     * download from any replica, for instance, if downloading from a standalone Mongo cluster. Even when there is a
     * single replica, downloading in parallel with two connections might yield better performance. This is also useful
     * for testing.
     */
    public static final String OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP_SECONDARIES_ONLY = "oak.indexer.pipelined.mongoParallelDump.secondariesOnly";
    public static boolean DEFAULT_OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP_SECONDARIES_ONLY = true;

    // For testing purposes.
    // Set the maximum number of documents in the batches returned by Mongo while iterating over the results.
    // The default police is fine for normal use cases (initial batch: 101, then as large as the maximum batch size in
    // bytes), but for testing the behavior under failures it may be necessary to change the batch size
    static final String OAK_INDEXER_PIPELINED_MONGO_BATCH_SIZE = "oak.indexer.pipelined.mongoBatchSize";
    private final int mongoBatchSize = Integer.getInteger(OAK_INDEXER_PIPELINED_MONGO_BATCH_SIZE, -1);

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
    private final BlockingQueue<RawBsonDocument[]> mongoDocQueue;
    private final List<PathFilter> pathFilters;
    private final ThreadFactory threadFactory;
    private final StatisticsProvider statisticsProvider;
    private final IndexingReporter reporter;

    private final int retryDuringSeconds;
    private final boolean retryOnConnectionErrors;
    private final boolean regexPathFiltering;
    private final String customExcludeEntriesRegex;
    private final List<String> customExcludedPaths;
    private final boolean parallelDump;
    private final boolean parallelDumpSecondariesOnly;
    private final MongoRegexPathFilterFactory regexPathFilterFactory;

    private MongoCollection<RawBsonDocument> dbCollection;
    private PipelinedMongoServerSelector mongoServerSelector;
    private final Stopwatch downloadStartWatch = Stopwatch.createUnstarted();
    private final DownloadStageStatistics downloadStageStatistics = new DownloadStageStatistics();
    private Instant lastDelayedEnqueueWarningMessageLoggedTimestamp = Instant.now();
    private final long minModified;

    public PipelinedMongoDownloadTask(MongoClientURI mongoClientURI,
                                      MongoDocumentStore docStore,
                                      int maxBatchSizeBytes,
                                      int maxBatchNumberOfDocuments,
                                      BlockingQueue<RawBsonDocument[]> queue,
                                      List<PathFilter> pathFilters,
                                      StatisticsProvider statisticsProvider,
                                      IndexingReporter reporter,
                                      ThreadFactory threadFactory) {
        this(mongoClientURI, docStore, maxBatchSizeBytes, maxBatchNumberOfDocuments,
                queue, pathFilters, statisticsProvider, reporter, threadFactory, 0);
    }

    public PipelinedMongoDownloadTask(MongoClientURI mongoClientURI,
                                      MongoDocumentStore docStore,
                                      int maxBatchSizeBytes,
                                      int maxBatchNumberOfDocuments,
                                      BlockingQueue<RawBsonDocument[]> queue,
                                      List<PathFilter> pathFilters,
                                      StatisticsProvider statisticsProvider,
                                      IndexingReporter reporter,
                                      ThreadFactory threadFactory,
                                      long minModified) {
        this.mongoClientURI = mongoClientURI;
        this.docStore = docStore;
        this.statisticsProvider = statisticsProvider;
        this.reporter = reporter;
        this.maxBatchSizeBytes = maxBatchSizeBytes;
        this.maxBatchNumberOfDocuments = maxBatchNumberOfDocuments;
        this.mongoDocQueue = queue;
        this.pathFilters = pathFilters;
        this.threadFactory = threadFactory;
        this.minModified = minModified;

        // Default retries for 5 minutes.
        this.retryDuringSeconds = ConfigHelper.getSystemPropertyAsInt(
                OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS);
        Validate.checkArgument(retryDuringSeconds > 0,
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

        boolean parallelDumpConfig = ConfigHelper.getSystemPropertyAsBoolean(OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP, DEFAULT_OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP);
        if (parallelDumpConfig && !retryOnConnectionErrors) {
            LOG.warn("Parallel dump requires " + OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS + " to be set to true, but it is false. Disabling parallel dump.");
            this.parallelDump = false;
        } else {
            this.parallelDump = parallelDumpConfig;
        }
        this.reporter.addConfig(OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP, String.valueOf(parallelDump));

        this.parallelDumpSecondariesOnly = ConfigHelper.getSystemPropertyAsBoolean(
                OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP_SECONDARIES_ONLY,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP_SECONDARIES_ONLY
        );
        this.reporter.addConfig(OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP_SECONDARIES_ONLY, String.valueOf(parallelDumpSecondariesOnly));

        this.customExcludeEntriesRegex = ConfigHelper.getSystemPropertyAsString(
                OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX
        );
        this.reporter.addConfig(OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX, customExcludeEntriesRegex);

        String excludePathsString = ConfigHelper.getSystemPropertyAsString(
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
        List<String> invalidPaths = customExcludedPaths.stream()
                .filter(p -> !PathUtils.isValid(p) || !PathUtils.isAbsolute(p) || PathUtils.denotesRoot(p))
                .collect(Collectors.toList());
        if (!invalidPaths.isEmpty()) {
            throw new IllegalArgumentException("Invalid paths in " + OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS + " " +
                    " system property: " + invalidPaths + ". Paths must be valid, must be absolute and must not be the root.");
        }
    }

    private Bson getModifiedFieldFilter() {
        return Filters.gte(NodeDocument.MODIFIED_IN_SECS, minModified);
    }

    @Override
    public Result call() throws Exception {
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName(THREAD_NAME_PREFIX);
        try {
            // When downloading in parallel, we must create the connection to Mongo using a custom instance of ServerSelector
            // instead of using the default policy defined by readPreference configuration setting.
            // Here we create the configuration that is common to the two cases (parallelDump true or false).
            MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(mongoClientURI.getURI()))
                    .readPreference(ReadPreference.secondaryPreferred());
            if (parallelDump && parallelDumpSecondariesOnly) {
                // Set a custom server selector that is able to distribute the two connections between the two secondaries.
                // Overrides the readPreference setting. We also need to listen for changes in the cluster to detect
                // when a node is promoted to primary, so we can stop downloading from that node
                this.mongoServerSelector = new PipelinedMongoServerSelector(THREAD_NAME_PREFIX + "-");
                settingsBuilder.applyToClusterSettings(builder -> builder
                        .serverSelector(mongoServerSelector)
                        .addClusterListener(mongoServerSelector)
                );
            } else {
                // otherwise use the default server selector policy.
                this.mongoServerSelector = null;
            }

            String mongoDatabaseName = MongoDocumentStoreHelper.getMongoDatabaseName(docStore);
            try (MongoClient client = MongoClients.create(settingsBuilder.build())) {
                MongoDatabase mongoDatabase = client.getDatabase(mongoDatabaseName);
                this.dbCollection = mongoDatabase.getCollection(Collection.NODES.toString(), RawBsonDocument.class);

                INDEXING_PHASE_LOGGER.info("[TASK:{}:START] Starting to download from MongoDB", Thread.currentThread().getName().toUpperCase(Locale.ROOT));
                try {
                    downloadStartWatch.start();
                    if (retryOnConnectionErrors) {
                        downloadWithRetryOnConnectionErrors();
                    } else {
                        downloadWithNaturalOrdering();
                    }
                    downloadStartWatch.stop();
                    // Signal the end of the download
                    mongoDocQueue.put(SENTINEL_MONGO_DOCUMENT);
                    long durationMillis = downloadStartWatch.elapsed(TimeUnit.MILLISECONDS);
                    downloadStageStatistics.publishStatistics(statisticsProvider, reporter, durationMillis);
                    String metrics = downloadStageStatistics.formatStats(durationMillis);
                    INDEXING_PHASE_LOGGER.info("[TASK:{}:END] Metrics: {}", Thread.currentThread().getName().toUpperCase(Locale.ROOT), metrics);
                    reporter.addTiming("Mongo dump", FormattingUtils.formatToSeconds(downloadStartWatch));
                    return new PipelinedMongoDownloadTask.Result(downloadStageStatistics.getDocumentsDownloadedTotal());
                } catch (Throwable t) {
                    INDEXING_PHASE_LOGGER.info("[TASK:{}:FAIL] Metrics: {}, Error: {}",
                            Thread.currentThread().getName().toUpperCase(Locale.ROOT),
                            MetricsFormatter.createMetricsWithDurationOnly(downloadStartWatch),
                            t.toString());
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
            FindIterable<RawBsonDocument> mongoIterable = dbCollection
                    .find(getModifiedFieldFilter()) // Download only documents that have _modified set
                    .hint(NATURAL_HINT);
            DownloadTask downloadTask = new DownloadTask(DownloadOrder.UNDEFINED, downloadStageStatistics);
            downloadTask.download(mongoIterable);
            downloadTask.reportFinalResults();

        } else {
            DownloadTask ancestorsDownloadTask = new DownloadTask(DownloadOrder.UNDEFINED, downloadStageStatistics);
            ancestorsDownloadTask.downloadAncestors(mongoFilterPaths.included);

            DownloadTask downloadTask = new DownloadTask(DownloadOrder.UNDEFINED, downloadStageStatistics);
            LOG.info("Downloading from Mongo with natural order using filter: {}", mongoFilter);
            FindIterable<RawBsonDocument> findIterable = dbCollection
                    .find(Filters.and(getModifiedFieldFilter(), mongoFilter))
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
        mongoFilter = addMinModifiedToMongoFilter(mongoFilter);
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
            LOG.info("Downloading in parallel with two connections, one in ascending the other in descending order");
            // Shared object between the two download threads to store the last document download by either of them
            // and determine when the two downloads have crossed
            MongoParallelDownloadCoordinator parallelDownloadCoordinator = new MongoParallelDownloadCoordinator();
            DownloadTask ascendingDownloadTask = new DownloadTask(DownloadOrder.ASCENDING, downloadStageStatistics, parallelDownloadCoordinator);
            DownloadTask descendingDownloadTask = new DownloadTask(DownloadOrder.DESCENDING, downloadStageStatistics, parallelDownloadCoordinator);

            ExecutorService downloadThreadPool = Executors.newFixedThreadPool(2, threadFactory);
            ExecutorCompletionService<Void> ecs = new ExecutorCompletionService<>(downloadThreadPool);
            Future<?> ascendingDownloadFuture = submitDownloadTask(ecs, ascendingDownloadTask, mongoFilter, THREAD_NAME_PREFIX + "-ascending");
            Future<?> descendingDownloadFuture = submitDownloadTask(ecs, descendingDownloadTask, mongoFilter, THREAD_NAME_PREFIX + "-descending");
            // When using parallel download, we stop the download when the streams cross, that is, when they download
            // a document that was already downloaded by the other. This works well if both ascending and descending
            // queries return documents frequently, so that we can check for crossing frequently. But with regex filtering
            // enabled, a Mongo query may go for a long time without returning any documents, because it may be traversing
            // a part of the index that does not match the regex expression. In the extreme case, the query does not match
            // any document in the collection, so both streams will scan the full index without ever realizing they have
            // crossed, because this check is only done when one of the streams receives enough documents to fill up a batch.
            // In this case, one of the queries will finish after completing its scan, while the other thread may still be
            // ongoing for some time. I have observed that the ascending download thread completes faster than the
            // descending thread, by a significant margin, so that the descending download thread may continue running
            // for a long time even after the ascending completed the full scan. For this reason, it is important to
            // cancel the other thread as soon as one thread completes the scan.
            // Therefore, we use the following termination conditions:
            // - the threads have crossed.
            // - one of the threads finished the download.
            // In both cases, when a thread finishes, we cancel the other thread because by then we have downloaded all
            // documents.
            try {
                while (true) {
                    // The parent thread waits for the download to complete, reporting progress periodically
                    Future<Void> completedTask = ecs.poll(10, TimeUnit.SECONDS);
                    if (completedTask == null) {
                        // null return means that the poll timed-out, so the download tasks are still ongoing. Report
                        // progress and then go back to waiting for the tasks to complete
                        long secondsElapsed = downloadStartWatch.elapsed(TimeUnit.SECONDS);
                        long ascTaskDownloadTotal = ascendingDownloadTask.getDocumentsDownloadedTotal();
                        long descTaskDownloadTotal = descendingDownloadTask.getDocumentsDownloadedTotal();
                        String formattedRate;
                        if (secondsElapsed == 0) {
                            formattedRate = "N/A nodes/s, N/A nodes/hr, N/A /s";
                        } else {
                            double docRate = ((double) downloadStageStatistics.getDocumentsDownloadedTotal()) / secondsElapsed;
                            double bytesRate = ((double) downloadStageStatistics.getDocumentsDownloadedTotalBytes()) / secondsElapsed;
                            formattedRate = String.format(Locale.ROOT, "%1.2f nodes/s, %1.2f nodes/hr, %s/s",
                                    docRate, docRate * 3600, IOUtils.humanReadableByteCountBin((long) bytesRate));
                        }
                        LOG.info("Total documents dumped from Mongo {} (asc: {}, desc: {}) [{}] (Elapsed {})",
                                downloadStageStatistics.getDocumentsDownloadedTotal(), ascTaskDownloadTotal, descTaskDownloadTotal,
                                formattedRate, FormattingUtils.formatToSeconds(secondsElapsed));
                    } else {
                        // If the task failed with an exception, the exception will be thrown by get()
                        completedTask.get();
                        // One of the download tasks has completed. Cancel the other one.
                        if (completedTask == ascendingDownloadFuture) {
                            LOG.info("Ascending download task has completed. Cancelling descending download task.");
                            // This closes the Mongo cursor, which will cause the download task to abort next time it
                            // performs an operation on the cursor or if it is blocked on the cursor.
                            descendingDownloadTask.cancelDownload();
                            // In case the thread is not currently operating on the Mongo cursor, we interrupt the thread
                            // to ensure that it terminates quickly.
                            descendingDownloadFuture.cancel(true);
                            // Notes:
                            // 1. Calling close() on a Mongo cursor will fail if the cursor was already interrupted. So
                            //   we cancel the cursor before interrupting the thread. Any exception thrown by calling
                            //   close() on the cursor will be ignored, but it's better if we avoid them.
                            // 2. Interrupting the thread is not enough if the thread is blocked waiting on a socket.
                            //   In that state, the thread does not check for interrupts, it will only check when it
                            //   finishes the I/O operation, which can take a long time. So we need to close the cursor,
                            //   which will abort the I/O operation.
                        } else if (completedTask == descendingDownloadFuture) {
                            LOG.info("Descending download task has completed. Cancelling ascending download task.");
                            ascendingDownloadTask.cancelDownload();
                            ascendingDownloadFuture.cancel(true);
                        } else {
                            throw new IllegalStateException("Unknown download task completed: " + completedTask);
                        }

                        // Download any documents that may have been skipped by the descending thread because they were
                        // modified during the download. Any modification will change the _modified field to
                        // a value higher than the highest value that existed when the download started. If the connection
                        // to Mongo fails, the descending thread starts a new query in Mongo to continue downloading down
                        // from the last document that it had downloaded. The document that was modified will not be
                        // retrieved by this query. See https://issues.apache.org/jira/browse/OAK-10903 for more details.
                        long highestModifiedSeen = descendingDownloadTask.getFirstModifiedValueSeen();
                        if (highestModifiedSeen == -1) {
                            LOG.warn("No documents downloaded by descending download task.");
                        } else {
                            // Include also the documents with _modified == highestModifiedSeen, because there may have
                            // been documents written to Mongo with this _modified value after the descending
                            // thread started its query. Therefore, the thread will download again some of the documents
                            // that were already downloaded, but this is not an issue because the merge-sort will
                            // de-duplicate the entries.
                            LOG.info("Downloading documents modified since the start of the download: _modified >= {}", highestModifiedSeen);
                            DownloadTask updatedDocsTask = new DownloadTask(DownloadOrder.ASCENDING, downloadStageStatistics, highestModifiedSeen, Long.MAX_VALUE, null);
                            Future<?> updateDocsDownloadFuture = submitDownloadTask(ecs, updatedDocsTask, mongoFilter, THREAD_NAME_PREFIX + "-updated-docs");
                            LOG.info("Waiting for download of updated documents to complete.");
                            updateDocsDownloadFuture.get();
                        }
                        return;
                    }
                }
            } catch (ExecutionException e) {
                // One of the download tasks finished with an exception. The finally block will cancel the other task.
                LOG.info("Error during download: {}", e.toString());
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                LOG.info("Thread interrupted.");
                // The parent thread was interrupted. The finally block will cancel any task that may still be running.
                throw e;
            } finally {
                LOG.info("Shutting down download thread pool.");
                new ExecutorCloser(downloadThreadPool, 1, TimeUnit.SECONDS).close();
                LOG.info("Download thread pool shutdown complete.");
            }
        } else {
            // Single threaded dump
            DownloadTask downloadTask = new DownloadTask(DownloadOrder.UNDEFINED, downloadStageStatistics);
            downloadTask.download(mongoFilter);
        }
    }

    /**
     * If minModified is set, add this condition to the MongoDB filter.
     * If minModified is not set, the old filter is returned.
     * This method accepts null, and may return null.
     *
     * @param mongoFilter the previous filter (may be null)
     * @return the combined filter (may be null)
     */
    private Bson addMinModifiedToMongoFilter(Bson mongoFilter) {
        if (minModified == 0) {
            // the is no minModified condition: return the unchanged filter
            return mongoFilter;
        }
        Bson minModifiedFilter = getModifiedFieldFilter();
        if (mongoFilter == null) {
            // there is no previous filter: return the minModified filter
            return minModifiedFilter;
        }
        // combine both filters
        return Filters.and(mongoFilter, minModifiedFilter);
    }

    private Future<?> submitDownloadTask(ExecutorCompletionService<Void> executor, DownloadTask downloadTask, Bson mongoFilter, String name) {
        return executor.submit(() -> {
            String originalName = Thread.currentThread().getName();
            Thread.currentThread().setName(name);
            try {
                downloadTask.download(mongoFilter);
                downloadTask.reportFinalResults();
            } finally {
                if (mongoServerSelector != null) {
                    mongoServerSelector.threadFinished();
                }
                Thread.currentThread().setName(originalName);
            }
            return null;
        });
    }

    private MongoFilterPaths getPathsForRegexFiltering() {
        if (!regexPathFiltering) {
            LOG.info("Regex path filtering disabled.");
            reporter.addInformation("Mongo regex filter paths: " + MongoFilterPaths.DOWNLOAD_ALL.prettyPrint());
            return MongoFilterPaths.DOWNLOAD_ALL;
        } else {
            LOG.info("Computing included/excluded paths for Mongo regex path filtering. PathFilters: {}",
                    pathFilters.stream()
                            .map(pf -> "PF{includedPaths=" + pf.getIncludedPaths() + ", excludedPaths=" + pf.getExcludedPaths() + "}")
                            .collect(Collectors.joining(", "))
            );
            MongoFilterPaths mongoFilterPaths = this.regexPathFilterFactory.buildMongoFilter(pathFilters, customExcludedPaths);
            LOG.info("Paths used for regex filtering on Mongo: {}", mongoFilterPaths);
            reporter.addInformation("Mongo regex filter paths: " + mongoFilterPaths.prettyPrint());
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

    /**
     * Downloads a given range from Mongo. Instances of this class should be used for downloading a single range.
     * To download multiple ranges, create multiple instances of this class.
     */
    private class DownloadTask {
        private final Stopwatch downloadTaskStart = Stopwatch.createUnstarted();
        private final DownloadOrder downloadOrder;
        private final DownloadStageStatistics downloadStatics;
        private final long fromModified;
        private final long toModified;
        private final MongoParallelDownloadCoordinator parallelDownloadCoordinator;
        private long documentsDownloadedTotalBytes;
        private long documentsDownloadedTotal;
        private long nextLastModified;
        private String lastIdDownloaded;
        // Accessed from the main download thread
        private volatile long firstModifiedValueSeen = -1;
        private volatile MongoCursor<RawBsonDocument> mongoCursor = null;
        private final AtomicBoolean cancelled = new AtomicBoolean(false);

        DownloadTask(DownloadOrder downloadOrder, DownloadStageStatistics downloadStatics) {
            this(downloadOrder, downloadStatics, null);
        }

        DownloadTask(DownloadOrder downloadOrder, DownloadStageStatistics downloadStatics, MongoParallelDownloadCoordinator parallelDownloadCoordinator) {
            this(downloadOrder, downloadStatics, 0, Long.MAX_VALUE, parallelDownloadCoordinator);
        }

        DownloadTask(DownloadOrder downloadOrder, DownloadStageStatistics downloadStatics, long fromModified, long toModified, MongoParallelDownloadCoordinator parallelDownloadCoordinator) {
            this.downloadOrder = downloadOrder;
            this.downloadStatics = downloadStatics;
            this.fromModified = fromModified;
            this.toModified = toModified;
            this.parallelDownloadCoordinator = parallelDownloadCoordinator;
            this.documentsDownloadedTotalBytes = 0;
            this.documentsDownloadedTotal = 0;
            this.nextLastModified = downloadOrder.downloadInAscendingOrder() ? 0 : Long.MAX_VALUE;
            this.lastIdDownloaded = null;
        }

        private Instant failuresStartTimestamp = null; // When the last series of failures started
        private int numberOfFailures = 0;

        public long getDocumentsDownloadedTotalBytes() {
            return documentsDownloadedTotalBytes;
        }

        public long getDocumentsDownloadedTotal() {
            return documentsDownloadedTotal;
        }

        public long getFirstModifiedValueSeen() {
            return firstModifiedValueSeen;
        }

        private void download(Bson mongoQueryFilter) throws InterruptedException, TimeoutException {
            failuresStartTimestamp = null; // When the last series of failures started
            numberOfFailures = 0;
            long retryIntervalMs = retryInitialIntervalMillis;
            boolean downloadCompleted = false;
            Map<String, Integer> exceptions = new HashMap<>();
            downloadTaskStart.start();
            try {
                while (!downloadCompleted) {
                    try {
                        if (lastIdDownloaded == null) {
                            // lastIdDownloaded is null only when starting the download or if there is a connection error
                            // before anything is downloaded
                            DownloadRange firstRange = new DownloadRange(fromModified, toModified, null, downloadOrder.downloadInAscendingOrder());
                            downloadRange(firstRange, mongoQueryFilter, downloadOrder);
                        } else {
                            LOG.info("Recovering from broken connection, finishing downloading documents with _modified={}", nextLastModified);
                            DownloadRange partialLastModifiedRange = new DownloadRange(nextLastModified, nextLastModified, lastIdDownloaded, downloadOrder.downloadInAscendingOrder());
                            downloadRange(partialLastModifiedRange, mongoQueryFilter, downloadOrder);
                            // Downloaded everything from _nextLastModified. Continue with the next timestamp for _modified
                            DownloadRange nextRange = downloadOrder.downloadInAscendingOrder() ?
                                    new DownloadRange(nextLastModified + 1, Long.MAX_VALUE, null, true) :
                                    new DownloadRange(0, nextLastModified - 1, null, false);
                            downloadRange(nextRange, mongoQueryFilter, downloadOrder);
                        }
                        downloadCompleted = true;
                    } catch (IllegalStateException | InterruptedException | MongoInterruptedException e) {
                        if (cancelled.get()) {
                            LOG.info("Download task was cancelled: {}", e.toString());
                            return;
                        } else {
                            throw e;
                        }
                    } catch (MongoException e) {
                        if (e instanceof MongoIncompatibleDriverException) {
                            // Non-recoverable exceptions
                            throw e;
                        }
                        if (failuresStartTimestamp == null) {
                            failuresStartTimestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS);
                        }
                        LOG.info("Non-fatal connection interruption downloading from MongoDB: {}", e.toString());
                        long secondsSinceStartOfFailures = Duration.between(failuresStartTimestamp, Instant.now()).toSeconds();
                        if (parallelDump && parallelDumpSecondariesOnly && mongoServerSelector.atLeastOneConnectionActive()) {
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
            } finally {
                downloadTaskStart.stop();
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

            FindIterable<RawBsonDocument> mongoIterable = dbCollection
                    .find(findQuery)
                    .sort(sortOrder);

            LOG.info("Traversing: {}. Query: {}, Traverse order: {}", range, findQuery, sortOrder);
            download(mongoIterable);
        }

        public void cancelDownload() {
            boolean alreadyCancelled = cancelled.getAndSet(true);
            if (alreadyCancelled) {
                LOG.info("Download task was already cancelled.");
            } else {
                LOG.info("Cancelling download for {} order task, closing Mongo cursor.", downloadOrder);
                if (mongoCursor != null) {
                    try {
                        mongoCursor.close();
                    } catch (Throwable e) {
                        LOG.info("Error closing Mongo cursor, the cursor may already be closed: {}", e.toString());
                    }
                }
            }
        }

        void download(FindIterable<RawBsonDocument> mongoIterable) throws InterruptedException, TimeoutException {
            if (mongoBatchSize != -1) {
                LOG.info("Setting Mongo batch size to {}", mongoBatchSize);
                mongoIterable.batchSize(mongoBatchSize);
            }
            try (MongoCursor<RawBsonDocument> cursor = mongoIterable.iterator()) {
                this.mongoCursor = cursor;
                RawBsonDocumentWrapper[] batch = new RawBsonDocumentWrapper[maxBatchNumberOfDocuments];
                int nextIndex = 0;
                int batchSize = 0;
                if (cursor.hasNext()) {
                    // We have managed to reconnect, reset the failure timestamp
                    failuresStartTimestamp = null;
                    numberOfFailures = 0;
                }
                try {
                    while (cursor.hasNext()) {
                        RawBsonDocument rawBsonDocument = cursor.next();
                        ByteBuf byteBuffer = rawBsonDocument.getByteBuffer();
                        int docSize = byteBuffer.remaining();

                        documentsDownloadedTotalBytes += docSize;
                        documentsDownloadedTotal++;
                        downloadStageStatistics.incrementDocumentsDownloadedTotalBytes(docSize);
                        downloadStatics.incrementDocumentsDownloadedTotal();

                        // Extract _id and _modified fields, they are necessary to keep track of progress so that we can
                        // resume the download in case of connection failure
                        String id = null;
                        long modified = -1;
                        try (BsonBinaryReader bsonReader = new BsonBinaryReader(new ByteBufferBsonInput(byteBuffer))) {
                            bsonReader.readStartDocument();
                            while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                                String fieldName = bsonReader.readName();
                                switch (fieldName) {
                                    case NodeDocument.ID:
                                        id = bsonReader.readString();
                                        break;
                                    case NodeDocument.MODIFIED_IN_SECS:
                                        modified = bsonReader.readInt64();
                                        break;
                                    default:
                                        bsonReader.skipValue();
                                }
                                if (id != null && modified != -1) {
                                    break;
                                }
                            }
                        }
                        if (id == null || modified == -1) {
                            throw new IllegalStateException("Document does not have _id or _modified field: " + rawBsonDocument);
                        }

                        // All the Mongo queries in this class have a requirement on the _modified field, so the
                        // documents downloaded will all have the field defined.
                        this.nextLastModified = modified;
                        if (this.firstModifiedValueSeen == -1) {
                            this.firstModifiedValueSeen = this.nextLastModified;
                        }
                        this.lastIdDownloaded = id;
                        if (this.documentsDownloadedTotal % 50_000 == 0) {
                            reportProgress(id);
                        }
                        TRAVERSAL_LOG.trace(id);

                        batch[nextIndex] = new RawBsonDocumentWrapper(rawBsonDocument, modified, id);
                        nextIndex++;
                        batchSize += docSize;
                        if (batchSize >= maxBatchSizeBytes || nextIndex == batch.length) {
                            LOG.trace("Enqueuing block with {} elements, estimated size: {} bytes", nextIndex, batchSize);
                            boolean downloadCompleted = tryEnqueueCopy(batch, nextIndex);
                            if (downloadCompleted) {
                                LOG.info("Download of range with download order {} completed, intersected with other download.", downloadOrder);
                                return;
                            }
                            nextIndex = 0;
                            batchSize = 0;
                            if (parallelDump && parallelDumpSecondariesOnly && mongoServerSelector.isConnectedToPrimary()) {
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
                        boolean downloadCompleted = tryEnqueueCopy(batch, nextIndex);
                        if (downloadCompleted) {
                            LOG.info("Download of range with download order {} completed, intersected with other download.", downloadOrder);
                            return;
                        }
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
            FindIterable<RawBsonDocument> ancestorsIterable = dbCollection
                    .find(ancestorQuery)
                    // Use the index on _id: this query returns very few documents and the filter condition is on _id.
                    .hint(ID_INDEX_HINT);
            download(ancestorsIterable);
        }

        private boolean tryEnqueueCopy(RawBsonDocumentWrapper[] batch, int sizeOfBatch) throws TimeoutException, InterruptedException {
            // Find the first element that was not yet added
            boolean completedDownload = false;
            int effectiveSize = sizeOfBatch;
            if (parallelDownloadCoordinator != null) {
                int sizeOfRangeNotAdded = downloadOrder == DownloadOrder.ASCENDING ?
                        parallelDownloadCoordinator.extendLowerRange(batch, sizeOfBatch) :
                        parallelDownloadCoordinator.extendUpperRange(batch, sizeOfBatch);
                if (sizeOfRangeNotAdded != sizeOfBatch) {
                    completedDownload = true;
                    effectiveSize = sizeOfRangeNotAdded;
                    RawBsonDocumentWrapper firstAlreadySeen = batch[sizeOfRangeNotAdded];
                    LOG.info("Download complete, reached already seen document: {}: {}", firstAlreadySeen.modified, firstAlreadySeen.id);
                }
            }
            if (effectiveSize == 0) {
                LOG.info("Batch is empty, not enqueuing.");
            } else {
                RawBsonDocument[] copyOfBatch = new RawBsonDocument[effectiveSize];
                for (int i = 0; i < effectiveSize; i++) {
                    copyOfBatch[i] = batch[i].rawBsonDocument;
                }
                Stopwatch enqueueDelayStopwatch = Stopwatch.createStarted();
                if (!mongoDocQueue.offer(copyOfBatch, MONGO_QUEUE_OFFER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new TimeoutException("Timeout trying to enqueue batch of MongoDB documents. Waited " + MONGO_QUEUE_OFFER_TIMEOUT);
                }
                long enqueueDelay = enqueueDelayStopwatch.elapsed(TimeUnit.MILLISECONDS);
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
            long secondsElapsed = downloadTaskStart.elapsed(TimeUnit.SECONDS);
            String formattedRate;
            if (secondsElapsed == 0) {
                formattedRate = "N/A nodes/s, N/A nodes/hr, N/A /s";
            } else {
                double docRate = ((double) this.documentsDownloadedTotal) / secondsElapsed;
                double bytesRate = ((double) this.documentsDownloadedTotalBytes) / secondsElapsed;
                formattedRate = String.format(Locale.ROOT, "%1.2f nodes/s, %1.2f nodes/hr, %s/s",
                        docRate, docRate * 3600, IOUtils.humanReadableByteCountBin((long) bytesRate));
            }
            LOG.info("Finished download task. Dumped {} documents. Rate: {}. Elapsed {}",
                    this.documentsDownloadedTotal, formattedRate, FormattingUtils.formatToSeconds(downloadTaskStart));
        }

        private void reportProgress(String id) {
            long secondsElapsed = downloadTaskStart.elapsed(TimeUnit.SECONDS);
            String formattedRate;
            if (secondsElapsed == 0) {
                formattedRate = "N/A nodes/s, N/A nodes/hr, N/A /s";
            } else {
                double docRate = ((double) this.documentsDownloadedTotal) / secondsElapsed;
                double bytesRate = ((double) this.documentsDownloadedTotalBytes) / secondsElapsed;
                formattedRate = String.format(Locale.ROOT, "%1.2f nodes/s, %1.2f nodes/hr, %s/s",
                        docRate, docRate * 3600, IOUtils.humanReadableByteCountBin((long) bytesRate));
            }
            String msgPrefix = "";
            switch (downloadOrder) {
                case ASCENDING:
                    msgPrefix = "Dumping in ascending order";
                    break;
                case DESCENDING:
                    msgPrefix = "Dumping in descending order";
                    break;
                case UNDEFINED:
                    msgPrefix = "Dumping ";
                    break;
            }
            LOG.info("{} from NSET Traversed #{} {} [{}] (Elapsed {})",
                    msgPrefix, this.documentsDownloadedTotal, id, formattedRate, FormattingUtils.formatToSeconds(secondsElapsed));
        }
    }
}
