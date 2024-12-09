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

package org.apache.jackrabbit.oak.index.indexer.document;

import com.codahale.metrics.MetricRegistry;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.ConfigHelper;
import org.apache.jackrabbit.oak.index.indexer.document.incrementalstore.IncrementalStoreBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStore;
import org.apache.jackrabbit.oak.index.indexer.document.tree.ParallelIndexStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.TraversingRange;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.MetricsFormatter;
import org.apache.jackrabbit.oak.plugins.index.MetricsUtils;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingReporter;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.index.progress.MetricRateEstimator;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.index.IndexerMetrics.METRIC_INDEXING_INDEX_DATA_SIZE;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_SORTED_FILE_PATH;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.DEFAULT_OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.INDEXING_PHASE_LOGGER;

public abstract class DocumentStoreIndexerBase implements Closeable {
    public static final String INDEXER_METRICS_PREFIX = "oak_indexer_";
    public static final String METRIC_INDEXING_DURATION_SECONDS = INDEXER_METRICS_PREFIX + "indexing_duration_seconds";
    public static final String METRIC_FULL_INDEX_CREATION_DURATION_SECONDS = INDEXER_METRICS_PREFIX + "full_index_creation_duration_seconds";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Logger traversalLog = LoggerFactory.getLogger(DocumentStoreIndexerBase.class.getName() + ".traversal");
    protected final Closer closer = Closer.create();
    protected final IndexHelper indexHelper;
    private final IndexingReporter indexingReporter;
    private final StatisticsProvider statisticsProvider;
    protected List<NodeStateIndexerProvider> indexerProviders;
    protected final IndexerSupport indexerSupport;
    private static final int MAX_DOWNLOAD_ATTEMPTS = Integer.parseInt(System.getProperty("oak.indexer.maxDownloadRetries", "5")) + 1;

    private static final int TOP_SLOWEST_PATHS_TO_LOG = ConfigHelper.getSystemPropertyAsInt(
            "oak.indexer.topSlowestPathsToLog", 20);

    public DocumentStoreIndexerBase(IndexHelper indexHelper, IndexerSupport indexerSupport) {
        this.indexHelper = indexHelper;
        this.indexerSupport = indexerSupport;
        this.indexingReporter = indexHelper.getIndexReporter();
        this.statisticsProvider = indexHelper.getStatisticsProvider();
    }

    protected void setProviders() throws IOException {
        this.indexerProviders = createProviders();
    }

    private static class MongoNodeStateEntryTraverserFactory implements NodeStateEntryTraverserFactory {

        /**
         * This counter is part of this traverser's id and is helpful in identifying logs from different traversers that
         * run concurrently.
         */
        private static final AtomicInteger traverserInstanceCounter = new AtomicInteger(0);
        /**
         * An prefix for ID of traversers (value is acronym for NodeStateEntryTraverser).
         */
        private static final String TRAVERSER_ID_PREFIX = "NSET";
        private final RevisionVector rootRevision;
        private final DocumentNodeStore documentNodeStore;
        private final MongoDocumentStore documentStore;
        private final Logger traversalLogger;

        private MongoNodeStateEntryTraverserFactory(RevisionVector rootRevision, DocumentNodeStore documentNodeStore,
                                                    MongoDocumentStore documentStore, Logger traversalLogger) {
            this.rootRevision = rootRevision;
            this.documentNodeStore = documentNodeStore;
            this.documentStore = documentStore;
            this.traversalLogger = traversalLogger;
        }

        @Override
        public NodeStateEntryTraverser create(TraversingRange traversingRange) {
            IndexingProgressReporter progressReporterPerTask =
                    new IndexingProgressReporter(IndexUpdateCallback.NOOP, NodeTraversalCallback.NOOP);
            String entryTraverserID = TRAVERSER_ID_PREFIX + traverserInstanceCounter.incrementAndGet();
            //As first traversal is for dumping change the message prefix
            progressReporterPerTask.setMessagePrefix("Dumping from " + entryTraverserID);
            return new NodeStateEntryTraverser(entryTraverserID, rootRevision,
                    documentNodeStore, documentStore, traversingRange)
                    .withProgressCallback((id) -> {
                        try {
                            progressReporterPerTask.traversedNode(() -> id);
                        } catch (CommitFailedException e) {
                            throw new RuntimeException(e);
                        }
                        traversalLogger.trace(id);
                    });
        }
    }

    private List<IndexStore> buildFlatFileStoreList(NodeState checkpointedState,
                                                       CompositeIndexer indexer,
                                                       Predicate<String> pathPredicate,
                                                       Set<String> preferredPathElements,
                                                       boolean splitFlatFile,
                                                       Set<IndexDefinition> indexDefinitions,
                                                       IndexingReporter reporter) throws IOException {
        List<IndexStore> storeList = new ArrayList<>();

        Stopwatch indexStoreWatch = Stopwatch.createStarted();
        int executionCount = 1;
        CompositeException lastException = null;
        List<File> previousDownloadDirs = new ArrayList<>();
        //TODO How to ensure we can safely read from secondary
        DocumentNodeState rootDocumentState = (DocumentNodeState) checkpointedState;
        DocumentNodeStore nodeStore = (DocumentNodeStore) indexHelper.getNodeStore();

        FlatFileNodeStoreBuilder builder = null;
        int backOffTimeInMillis = 5000;
        while (storeList.isEmpty() && executionCount <= MAX_DOWNLOAD_ATTEMPTS) {
            try {
                builder = new FlatFileNodeStoreBuilder(indexHelper.getWorkDir())
                        .withBlobStore(indexHelper.getGCBlobStore())
                        .withPreferredPathElements((preferredPathElements != null) ? preferredPathElements : indexer.getRelativeIndexedNodeNames())
                        .addExistingDataDumpDir(indexerSupport.getExistingDataDumpDir())
                        .withPathPredicate(pathPredicate)
                        .withIndexDefinitions(indexDefinitions)
                        .withRootRevision(rootDocumentState.getRootRevision())
                        .withNodeStore(nodeStore)
                        .withMongoDocumentStore(getMongoDocumentStore())
                        .withMongoClientURI(getMongoClientURI())
                        .withNodeStateEntryTraverserFactory(new MongoNodeStateEntryTraverserFactory(rootDocumentState.getRootRevision(),
                                nodeStore, getMongoDocumentStore(), traversalLog))
                        .withCheckpoint(indexerSupport.getCheckpoint())
                        .withMinModified(indexerSupport.getMinModified())
                        .withStatisticsProvider(indexHelper.getStatisticsProvider())
                        .withIndexingReporter(reporter)
                        .withAheadOfTimeBlobDownloader(true);

                for (File dir : previousDownloadDirs) {
                    builder.addExistingDataDumpDir(dir);
                }
                if (splitFlatFile) {
                    storeList = builder.buildList(indexHelper, indexerSupport, indexDefinitions);
                } else {
                    log.info("Building index store");
                    IndexStore store = builder.build(indexHelper, indexer);
                    int threads = IndexerConfiguration.indexThreadPoolSize();
                    if (store instanceof ParallelIndexStore && threads > 1) {
                        log.info("Indexing with {} threads", threads);
                        ParallelIndexStore ps = (ParallelIndexStore) store;
                        storeList.addAll(ps.buildParallelStores(threads));
                    } else {
                        storeList.add(store);
                    }
                }
                for (IndexStore item : storeList) {
                    closer.register(item);
                }
            } catch (CompositeException e) {
                e.logAllExceptions("Underlying throwable caught during download", log);
                log.info("Could not build index store. Execution count {}. Retries left {}. Time elapsed {}",
                        executionCount, MAX_DOWNLOAD_ATTEMPTS - executionCount, indexStoreWatch);
                lastException = e;
                previousDownloadDirs.add(builder.getFlatFileStoreDir());
                if (executionCount < MAX_DOWNLOAD_ATTEMPTS) {
                    try {
                        log.info("Waiting for {} millis before retrying", backOffTimeInMillis);
                        Thread.sleep(backOffTimeInMillis);
                        backOffTimeInMillis *= 2;
                    } catch (InterruptedException ie) {
                        log.error("Interrupted while waiting before retrying download ", ie);
                    }
                }
            }
            executionCount++;
        }
        if (storeList.isEmpty()) {
            throw new IOException("Could not build index store", lastException);
        }
        log.info("Completed the index store build in {}", indexStoreWatch);
        return storeList;
    }

    public IndexStore buildTreeStore() throws IOException, CommitFailedException {
        String old = System.setProperty(FlatFileNodeStoreBuilder.OAK_INDEXER_SORT_STRATEGY_TYPE,
                FlatFileNodeStoreBuilder.SortStrategyType.PIPELINED_TREE.name());
        try {
            return buildFlatFileStore();
        } finally {
            System.setProperty(FlatFileNodeStoreBuilder.OAK_INDEXER_SORT_STRATEGY_TYPE, old);
        }
    }

    public IndexStore buildStore() throws IOException, CommitFailedException {
        return buildFlatFileStore();
    }

    public IndexStore buildStore(String initialCheckpoint, String finalCheckpoint) throws IOException, CommitFailedException {
        IncrementalStoreBuilder builder;
        IndexStore incrementalStore;
        Set<IndexDefinition> indexDefinitions = indexerSupport.getIndexDefinitions();
        Set<String> preferredPathElements = indexerSupport.getPreferredPathElements(indexDefinitions);
        Stopwatch incrementalStoreWatch = Stopwatch.createStarted();
        Predicate<String> predicate = indexerSupport.getFilterPredicate(indexDefinitions, Function.identity());

        String customExcludeEntriesRegex = ConfigHelper.getSystemPropertyAsString(
                OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX
        );

        if (!customExcludeEntriesRegex.isEmpty()) {
            predicate = predicate.and(indexerSupport.getFilterPredicateBasedOnCustomRegex(Pattern.compile(customExcludeEntriesRegex), Function.identity()));
        }

        // Handle custom excluded paths if provided. This is only applicable if regex path filtering is enabled.
        // Any paths whose ancestor is in the custom excluded paths list will be excluded from incremental index store.
        // This is to keep in line with the custom exclude paths implementation in the pipelined strategy.
        boolean regexPathFiltering = ConfigHelper.getSystemPropertyAsBoolean(
                OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING);
        List<String> customExcludedPaths;
        String excludePathsString = ConfigHelper.getSystemPropertyAsString(
                OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS
        ).trim();

        if (regexPathFiltering && !excludePathsString.isEmpty()) {
            customExcludedPaths = Arrays.stream(excludePathsString.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());

            if (!customExcludedPaths.isEmpty()) {
                // Add an AND condition to the existing predicate to filter out paths that are ancestors of the custom excluded paths.
                predicate = predicate.and(t -> customExcludedPaths.stream().noneMatch(excludedPath -> PathUtils.isAncestor(excludedPath, t)));
            }
        }

        try {
            builder = new IncrementalStoreBuilder(indexHelper.getWorkDir(), indexHelper, initialCheckpoint, finalCheckpoint)
                    .withPreferredPathElements(preferredPathElements)
                    .withPathPredicate(predicate)
                    .withBlobStore(indexHelper.getGCBlobStore());
            incrementalStore = builder.build();
            closer.register(incrementalStore);
        } catch (Exception e) {
            throw new IOException("Could not build incremental store", e);
        }
        log.info("Completed incremental store build in {}", incrementalStoreWatch);
        return incrementalStore;
    }

    /**
     * @return an Instance of FlatFileStore, whose getFlatFileStorePath() method can be used to get the absolute path to this store.
     * @throws IOException
     * @throws CommitFailedException
     * @deprecated replaced by {@link #buildStore()}
     */
    @Deprecated
    public IndexStore buildFlatFileStore() throws IOException, CommitFailedException {
        NodeState checkpointedState = indexerSupport.retrieveNodeStateForCheckpoint();
        Set<IndexDefinition> indexDefinitions = indexerSupport.getIndexDefinitions();
        Set<String> preferredPathElements = indexerSupport.getPreferredPathElements(indexDefinitions);
        Predicate<String> predicate = indexerSupport.getFilterPredicate(indexDefinitions, Function.identity());
        IndexStore indexStore = buildFlatFileStoreList(checkpointedState, null, predicate,
                preferredPathElements, IndexerConfiguration.parallelIndexEnabled(), indexDefinitions, indexingReporter).get(0);
        log.info("Store built. To use this store in a reindex step, set the system property {} to {}",
                OAK_INDEXER_SORTED_FILE_PATH, indexStore.getStorePath());
        return indexStore;
    }

    public void reindex() throws CommitFailedException, IOException {
        INDEXING_PHASE_LOGGER.info("[TASK:FULL_INDEX_CREATION:START] Starting indexing job");
        List<String> indexNames = indexerSupport.getIndexDefinitions().stream().map(IndexDefinition::getIndexName).collect(Collectors.toList());
        indexingReporter.setIndexNames(indexNames);
        Stopwatch indexJobWatch = Stopwatch.createStarted();
        try {
            IndexingProgressReporter progressReporter =
                    new IndexingProgressReporter(IndexUpdateCallback.NOOP, NodeTraversalCallback.NOOP);
            configureEstimators(progressReporter);

            NodeState checkpointedState = indexerSupport.retrieveNodeStateForCheckpoint();
            NodeStore copyOnWriteStore = new MemoryNodeStore(checkpointedState);
            indexerSupport.switchIndexLanesAndReindexFlag(copyOnWriteStore);
            NodeBuilder builder = copyOnWriteStore.getRoot().builder();
            CompositeIndexer indexer = prepareIndexers(copyOnWriteStore, builder, progressReporter);
            if (indexer.isEmpty()) {
                return;
            }

            closer.register(indexer);

            List<IndexStore> indexStores = buildFlatFileStoreList(
                    checkpointedState,
                    indexer,
                    indexer::shouldInclude,
                    null,
                    IndexerConfiguration.parallelIndexEnabled(),
                    indexerSupport.getIndexDefinitions(),
                    indexingReporter);

            progressReporter.reset();

            progressReporter.reindexingTraversalStart("/");

            preIndexOperations(indexer.getIndexers());

            INDEXING_PHASE_LOGGER.info("[TASK:INDEXING:START] Starting indexing");
            Stopwatch indexerWatch = Stopwatch.createStarted();
            try {
                if (indexStores.size() > 1) {
                    indexParallel(indexStores, indexer, progressReporter);
                } else if (indexStores.size() == 1) {
                    IndexStore indexStore = indexStores.get(0);
                    TopKSlowestPaths slowestTopKElements = new TopKSlowestPaths(TOP_SLOWEST_PATHS_TO_LOG);
                    indexer.onIndexingStarting();
                    long entryStart = System.nanoTime();
                    for (NodeStateEntry entry : indexStore) {
                        reportDocumentRead(entry.getPath(), progressReporter);
                        indexer.index(entry);
                        // Avoid calling System.nanoTime() twice per each entry, by reusing the timestamp taken at the end
                        // of indexing an entry as the start time of the following entry. This is less accurate, because
                        // the measured times will also include the bookkeeping at the end of indexing each entry, but
                        // we are only interested in entries that take a significant time to index, so this extra
                        // inaccuracy will not significantly change the results.
                        long entryEnd = System.nanoTime();
                        long elapsedMillis = (entryEnd - entryStart) / 1_000_000;
                        entryStart = entryEnd;
                        slowestTopKElements.add(entry.getPath(), elapsedMillis);
                        if (elapsedMillis > 1000) {
                            log.info("Indexing {} took {} ms", entry.getPath(), elapsedMillis);
                        }
                    }
                    log.info("Top slowest nodes to index (ms): {}", slowestTopKElements);

                    indexerProviders.forEach(indexProvider -> {
                        ExtractedTextCache extractedTextCache = indexProvider.getTextCache();
                        CacheStats cacheStats = extractedTextCache == null ? null : extractedTextCache.getCacheStats();
                        log.info("Text extraction cache statistics: {}", cacheStats == null ? "N/A" : cacheStats.cacheInfoAsString());
                    });
                }

                progressReporter.reindexingTraversalEnd();
                progressReporter.logReport();
                long indexingDurationSeconds = indexerWatch.elapsed(TimeUnit.SECONDS);
                log.info("Completed indexing in {}", FormattingUtils.formatToSeconds(indexingDurationSeconds));
                INDEXING_PHASE_LOGGER.info("[TASK:INDEXING:END] Metrics: {}", MetricsFormatter.createMetricsWithDurationOnly(indexingDurationSeconds));
                MetricsUtils.addMetric(statisticsProvider, indexingReporter, METRIC_INDEXING_DURATION_SECONDS, indexingDurationSeconds);
                indexingReporter.addTiming("Build Lucene Index", FormattingUtils.formatToSeconds(indexingDurationSeconds));
            } catch (Throwable t) {
                INDEXING_PHASE_LOGGER.info("[TASK:INDEXING:FAIL] Metrics: {}, Error: {}",
                        MetricsFormatter.createMetricsWithDurationOnly(indexerWatch), t.toString());
                throw t;
            }
            copyOnWriteStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            indexerSupport.postIndexWork(copyOnWriteStore);

            long fullIndexCreationDurationSeconds = indexJobWatch.elapsed(TimeUnit.SECONDS);
            INDEXING_PHASE_LOGGER.info("[TASK:FULL_INDEX_CREATION:END] Metrics {}", MetricsFormatter.createMetricsWithDurationOnly(fullIndexCreationDurationSeconds));
            long totalSize = indexerSupport.computeSizeOfGeneratedIndexData();
            MetricsUtils.addMetricByteSize(statisticsProvider, indexingReporter, METRIC_INDEXING_INDEX_DATA_SIZE, totalSize);
            MetricsUtils.addMetric(statisticsProvider, indexingReporter, METRIC_FULL_INDEX_CREATION_DURATION_SECONDS, fullIndexCreationDurationSeconds);
            indexingReporter.addTiming("Total time", FormattingUtils.formatToSeconds(fullIndexCreationDurationSeconds));
        } catch (Throwable t) {
            INDEXING_PHASE_LOGGER.info("[TASK:FULL_INDEX_CREATION:FAIL] Metrics: {}, Error: {}",
                    MetricsFormatter.createMetricsWithDurationOnly(indexJobWatch), t.toString());
            throw t;
        }
    }

    private void indexParallel(List<IndexStore> storeList, CompositeIndexer indexer, IndexingProgressReporter progressReporter)
            throws IOException {
        ExecutorService service = Executors.newFixedThreadPool(IndexerConfiguration.indexThreadPoolSize());
        List<Future<Boolean>> futureList = new ArrayList<>();

        indexer.onIndexingStarting();
        for (IndexStore item : storeList) {
            Future<Boolean> future = service.submit(() -> {
                for (NodeStateEntry entry : item) {
                    reportDocumentRead(entry.getPath(), progressReporter);
                    log.trace("Indexing : {}", entry.getPath());
                    indexer.index(entry);
                }
                return true;
            });
            futureList.add(future);
        }

        try {
            for (Future<Boolean> future : futureList) {
                future.get();
            }
            log.info("All {} indexing jobs are done", storeList.size());
        } catch (InterruptedException | ExecutionException e) {
            String errMsg = "Failure getting indexing job result";
            log.error(errMsg, e);
            throw new IOException(errMsg, e);
        } finally {
            new ExecutorCloser(service).close();
        }
    }

    private MongoDocumentStore getMongoDocumentStore() {
        return requireNonNull(indexHelper.getService(MongoDocumentStore.class));
    }

    private MongoClientURI getMongoClientURI() {
        return requireNonNull(indexHelper.getService(MongoClientURI.class));
    }

    private MongoDatabase getMongoDatabase() {
        return requireNonNull(indexHelper.getService(MongoDatabase.class));
    }

    private void configureEstimators(IndexingProgressReporter progressReporter) {
        StatisticsProvider statsProvider = indexHelper.getStatisticsProvider();
        if (statsProvider instanceof MetricStatisticsProvider) {
            MetricRegistry registry = ((MetricStatisticsProvider) statsProvider).getRegistry();
            progressReporter.setTraversalRateEstimator(new MetricRateEstimator("async", registry));
        }
        long nodesCount = getEstimatedDocumentCount();
        if (nodesCount > 0) {
            progressReporter.setNodeCountEstimator((String basePath, Set<String> indexPaths) -> nodesCount);
            progressReporter.setEstimatedCount(nodesCount);
            log.info("Estimated number of documents in Mongo are {}", nodesCount);
        }
    }

    private long getEstimatedDocumentCount() {
        MongoConnection mongoConnection = indexHelper.getService(MongoConnection.class);
        if (mongoConnection != null) {
            return mongoConnection.getDatabase().getCollection("nodes").count();
        }
        return 0;
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }

    private void reportDocumentRead(String id, IndexingProgressReporter progressReporter) {
        try {
            progressReporter.traversedNode(() -> id);
        } catch (CommitFailedException e) {
            throw new RuntimeException(e);
        }
        traversalLog.trace(id);
    }

    protected CompositeIndexer prepareIndexers(NodeStore copyOnWriteStore, NodeBuilder builder,
                                               IndexingProgressReporter progressReporter) {
        NodeState root = copyOnWriteStore.getRoot();
        List<NodeStateIndexer> indexers = new ArrayList<>();
        for (String indexPath : indexHelper.getIndexPaths()) {
            NodeState indexState = NodeStateUtils.getNode(root, indexPath);
            NodeBuilder idxBuilder = IndexerSupport.childBuilder(builder, indexPath, false);
            String type = indexState.getString(TYPE_PROPERTY_NAME);
            if (type == null) {
                log.warn("No 'type' property found on indexPath [{}]. Skipping it", indexPath);
                continue;
            }

            removeIndexState(idxBuilder);

            idxBuilder.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, false);

            for (NodeStateIndexerProvider indexerProvider : indexerProviders) {
                NodeStateIndexer indexer = indexerProvider.getIndexer(type, indexPath, idxBuilder, root, progressReporter);
                if (indexer != null) {
                    indexers.add(indexer);
                    closer.register(indexer);
                    progressReporter.registerIndex(indexPath, true, -1);
                }
            }
        }

        return new CompositeIndexer(indexers);
    }

    protected abstract List<NodeStateIndexerProvider> createProviders() throws IOException;

    protected abstract void preIndexOperations(List<NodeStateIndexer> indexers);

    //TODO OAK-7098 - Taken from IndexUpdate. Refactor to abstract out common logic like this
    private void removeIndexState(NodeBuilder definition) {
        // as we don't know the index content node name
        // beforehand, we'll remove all child nodes
        for (String rm : definition.getChildNodeNames()) {
            if (NodeStateUtils.isHidden(rm)) {
                NodeBuilder childNode = definition.getChildNode(rm);
                if (!childNode.getBoolean(IndexConstants.REINDEX_RETAIN)) {
                    definition.getChildNode(rm).remove();
                }
            }
        }
    }
}
