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
import com.mongodb.client.MongoDatabase;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStore;
import org.apache.jackrabbit.oak.index.indexer.document.incrementalstore.IncrementalStoreBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStore;
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
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.index.progress.MetricRateEstimator;
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

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_SORTED_FILE_PATH;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

public abstract class DocumentStoreIndexerBase implements Closeable {
    public static final String INDEXER_METRICS_PREFIX = "oak_indexer_";
    public static final String METRIC_INDEXING_DURATION_SECONDS = INDEXER_METRICS_PREFIX + "indexing_duration_seconds";
    public static final String METRIC_MERGE_NODE_STORE_DURATION_SECONDS = INDEXER_METRICS_PREFIX + "merge_node_store_duration_seconds";
    public static final String METRIC_FULL_INDEX_CREATION_DURATION_SECONDS = INDEXER_METRICS_PREFIX + "full_index_creation_duration_seconds";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Logger traversalLog = LoggerFactory.getLogger(DocumentStoreIndexerBase.class.getName() + ".traversal");
    protected final Closer closer = Closer.create();
    protected final IndexHelper indexHelper;
    protected List<NodeStateIndexerProvider> indexerProviders;
    protected final IndexerSupport indexerSupport;
    private static final int MAX_DOWNLOAD_ATTEMPTS = Integer.parseInt(System.getProperty("oak.indexer.maxDownloadRetries", "5")) + 1;

    public DocumentStoreIndexerBase(IndexHelper indexHelper, IndexerSupport indexerSupport) {
        this.indexHelper = indexHelper;
        this.indexerSupport = indexerSupport;
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

    private List<FlatFileStore> buildFlatFileStoreList(NodeState checkpointedState, CompositeIndexer indexer, Predicate<String> pathPredicate, Set<String> preferredPathElements,
                                                       boolean splitFlatFile, Set<IndexDefinition> indexDefinitions) throws IOException {
        List<FlatFileStore> storeList = new ArrayList<>();

        Stopwatch flatFileStoreWatch = Stopwatch.createStarted();
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
                        .withMongoDatabase(getMongoDatabase())
                        .withNodeStateEntryTraverserFactory(new MongoNodeStateEntryTraverserFactory(rootDocumentState.getRootRevision(),
                                nodeStore, getMongoDocumentStore(), traversalLog))
                        .withCheckpoint(indexerSupport.getCheckpoint())
                        .withStatisticsProvider(indexHelper.getStatisticsProvider());

                for (File dir : previousDownloadDirs) {
                    builder.addExistingDataDumpDir(dir);
                }
                if (splitFlatFile) {
                    storeList = builder.buildList(indexHelper, indexerSupport, indexDefinitions);
                } else {
                    storeList.add(builder.build());
                }
                for (FlatFileStore item : storeList) {
                    closer.register(item);
                }
            } catch (CompositeException e) {
                e.logAllExceptions("Underlying throwable caught during download", log);
                log.info("Could not build flat file store. Execution count {}. Retries left {}. Time elapsed {}",
                        executionCount, MAX_DOWNLOAD_ATTEMPTS - executionCount, flatFileStoreWatch);
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
            throw new IOException("Could not build flat file store", lastException);
        }
        log.info("Completed the flat file store build in {}", flatFileStoreWatch);
        return storeList;
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
    public FlatFileStore buildFlatFileStore() throws IOException, CommitFailedException {
        NodeState checkpointedState = indexerSupport.retrieveNodeStateForCheckpoint();
        Set<IndexDefinition> indexDefinitions = indexerSupport.getIndexDefinitions();
        Set<String> preferredPathElements = indexerSupport.getPreferredPathElements(indexDefinitions);
        Predicate<String> predicate = indexerSupport.getFilterPredicate(indexDefinitions, Function.identity());
        FlatFileStore flatFileStore = buildFlatFileStoreList(checkpointedState, null, predicate,
                preferredPathElements, IndexerConfiguration.parallelIndexEnabled(), indexDefinitions).get(0);
        log.info("FlatFileStore built at {}. To use this flatFileStore in a reindex step, set System Property-{} with value {}",
                flatFileStore.getStorePath(), OAK_INDEXER_SORTED_FILE_PATH, flatFileStore.getStorePath());
        return flatFileStore;
    }

    public void reindex() throws CommitFailedException, IOException {
        log.info("[TASK:FULL_INDEX_CREATION:START] Starting indexing job");
        Stopwatch indexJobWatch = Stopwatch.createStarted();
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

        List<FlatFileStore> flatFileStores = buildFlatFileStoreList(checkpointedState, indexer,
                indexer::shouldInclude, null, IndexerConfiguration.parallelIndexEnabled(), indexerSupport.getIndexDefinitions());

        progressReporter.reset();

        progressReporter.reindexingTraversalStart("/");

        preIndexOperations(indexer.getIndexers());

        log.info("[TASK:INDEXING:START] Starting indexing");
        Stopwatch indexerWatch = Stopwatch.createStarted();

        if (flatFileStores.size() > 1) {
            indexParallel(flatFileStores, indexer, progressReporter);
        } else if (flatFileStores.size() == 1) {
            FlatFileStore flatFileStore = flatFileStores.get(0);
            for (NodeStateEntry entry : flatFileStore) {
                reportDocumentRead(entry.getPath(), progressReporter);
                indexer.index(entry);
            }
        }

        progressReporter.reindexingTraversalEnd();
        progressReporter.logReport();
        long indexingDurationSeconds = indexerWatch.elapsed(TimeUnit.SECONDS);
        log.info("Completed the indexing in {}", FormattingUtils.formatToSeconds(indexingDurationSeconds));
        log.info("[TASK:INDEXING:END] Metrics: {}", MetricsFormatter.newBuilder()
                .add("duration", FormattingUtils.formatToSeconds(indexingDurationSeconds))
                .add("durationSeconds", indexingDurationSeconds)
                .build());
        MetricsUtils.setCounterOnce(indexHelper.getStatisticsProvider(), METRIC_INDEXING_DURATION_SECONDS, indexingDurationSeconds);

        log.info("[TASK:MERGE_NODE_STORE:START] Starting merge node store");
        Stopwatch mergeNodeStoreWatch = Stopwatch.createStarted();
        copyOnWriteStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        long mergeNodeStoreDurationSeconds = mergeNodeStoreWatch.elapsed(TimeUnit.SECONDS);
        log.info("[TASK:MERGE_NODE_STORE:END] Metrics: {}", MetricsFormatter.newBuilder()
                .add("duration", FormattingUtils.formatToSeconds(mergeNodeStoreDurationSeconds))
                .add("durationSeconds", mergeNodeStoreDurationSeconds)
                .build());
        MetricsUtils.setCounterOnce(indexHelper.getStatisticsProvider(), METRIC_MERGE_NODE_STORE_DURATION_SECONDS, mergeNodeStoreDurationSeconds);

        indexerSupport.postIndexWork(copyOnWriteStore);

        long fullIndexCreationDurationSeconds = indexJobWatch.elapsed(TimeUnit.SECONDS);
        log.info("[TASK:FULL_INDEX_CREATION:END] Metrics {}", MetricsFormatter.newBuilder()
                .add("duration", FormattingUtils.formatToSeconds(fullIndexCreationDurationSeconds))
                .add("durationSeconds", fullIndexCreationDurationSeconds)
                .build());
        MetricsUtils.setCounterOnce(indexHelper.getStatisticsProvider(), METRIC_FULL_INDEX_CREATION_DURATION_SECONDS, fullIndexCreationDurationSeconds);
    }

    private void indexParallel(List<FlatFileStore> storeList, CompositeIndexer indexer, IndexingProgressReporter progressReporter)
            throws IOException {
        ExecutorService service = Executors.newFixedThreadPool(IndexerConfiguration.indexThreadPoolSize());
        List<Future> futureList = new ArrayList<>();

        for (FlatFileStore item : storeList) {
            Future future = service.submit(() -> {
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
            for (Future future : futureList) {
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
        return checkNotNull(indexHelper.getService(MongoDocumentStore.class));
    }

    private MongoDatabase getMongoDatabase() {
        return checkNotNull(indexHelper.getService(MongoDatabase.class));
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
