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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Stopwatch;
import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.DefaultMemoryManager;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStore;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.MemoryManager;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.DocumentStoreSplitter;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentTraverser;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.index.progress.MetricRateEstimator;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_SORTED_FILE_PATH;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

public abstract class DocumentStoreIndexerBase implements Closeable{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Logger traversalLog = LoggerFactory.getLogger(DocumentStoreIndexerBase.class.getName()+".traversal");
    protected final Closer closer = Closer.create();
    protected final IndexHelper indexHelper;
    protected List<NodeStateIndexerProvider> indexerProviders;
    protected final IndexerSupport indexerSupport;
    private final Set<String> indexerPaths = new HashSet<>();
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
        private final CompositeIndexer indexer;

        private MongoNodeStateEntryTraverserFactory(RevisionVector rootRevision, DocumentNodeStore documentNodeStore,
                                                    MongoDocumentStore documentStore, Logger traversalLogger, CompositeIndexer indexer) {
            this.rootRevision = rootRevision;
            this.documentNodeStore = documentNodeStore;
            this.documentStore = documentStore;
            this.traversalLogger = traversalLogger;
            this.indexer = indexer;
        }

        @Override
        public NodeStateEntryTraverser create(MongoDocumentTraverser.TraversingRange traversingRange) {
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

        DocumentStoreSplitter splitter = new DocumentStoreSplitter(getMongoDocumentStore());
        List<Long> lastModifiedBreakPoints = splitter.split(Collection.NODES, 0L ,10);
        FlatFileNodeStoreBuilder builder = null;
        int backOffTimeInMillis = 5000;
        MemoryManager memoryManager = new DefaultMemoryManager();
        while (storeList.isEmpty() && executionCount <= MAX_DOWNLOAD_ATTEMPTS) {
            try {
                builder = new FlatFileNodeStoreBuilder(indexHelper.getWorkDir(), memoryManager)
                        .withLastModifiedBreakPoints(lastModifiedBreakPoints)
                        .withBlobStore(indexHelper.getGCBlobStore())
                        .withPreferredPathElements((preferredPathElements != null) ? preferredPathElements : indexer.getRelativeIndexedNodeNames())
                        .addExistingDataDumpDir(indexerSupport.getExistingDataDumpDir())
                        .withPathPredicate(pathPredicate)
                        .withNodeStateEntryTraverserFactory(new MongoNodeStateEntryTraverserFactory(rootDocumentState.getRootRevision(),
                                nodeStore, getMongoDocumentStore(), traversalLog, indexer));
                for (File dir : previousDownloadDirs) {
                    builder.addExistingDataDumpDir(dir);
                }
                if (splitFlatFile) {
                    storeList = builder.buildList(indexHelper, indexerSupport, indexDefinitions);
                } else {
                    storeList.add(builder.build());
                }
                for (FlatFileStore item: storeList) {
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

    /**
     *
     * @return an Instance of FlatFileStore, whose getFlatFileStorePath() method can be used to get the absolute path to this store.
     * @throws IOException
     * @throws CommitFailedException
     */
    public FlatFileStore buildFlatFileStore() throws IOException, CommitFailedException {
        NodeState checkpointedState = indexerSupport.retrieveNodeStateForCheckpoint();
        Set<String> preferredPathElements = new HashSet<>();
        Set<IndexDefinition> indexDefinitions = getIndexDefinitions();
        for (IndexDefinition indexDf : indexDefinitions) {
            preferredPathElements.addAll(indexDf.getRelativeNodeNames());
        }
        Predicate<String> predicate = s -> indexDefinitions.stream().anyMatch(indexDef -> indexDef.getPathFilter().filter(s) != PathFilter.Result.EXCLUDE);
        FlatFileStore flatFileStore = buildFlatFileStoreList(checkpointedState, null, predicate, preferredPathElements, false, indexDefinitions).get(0);
        log.info("FlatFileStore built at {}. To use this flatFileStore in a reindex step, set System Property-{} with value {}",
                flatFileStore.getFlatFileStorePath(), OAK_INDEXER_SORTED_FILE_PATH, flatFileStore.getFlatFileStorePath());
        return flatFileStore;
    }

    public void reindex() throws CommitFailedException, IOException {
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
            indexer::shouldInclude, null, IndexerConfiguration.parallelIndexEnabled(), getIndexDefinitions());

        progressReporter.reset();

        progressReporter.reindexingTraversalStart("/");

        preIndexOpertaions(indexer.getIndexers());

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
        log.info("Completed the indexing in {}", indexerWatch);

        copyOnWriteStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        indexerSupport.postIndexWork(copyOnWriteStore);
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

    private Set<IndexDefinition> getIndexDefinitions() throws IOException, CommitFailedException {
        NodeState checkpointedState = indexerSupport.retrieveNodeStateForCheckpoint();
        NodeStore copyOnWriteStore = new MemoryNodeStore(checkpointedState);
        NodeBuilder builder = copyOnWriteStore.getRoot().builder();
        NodeState root = builder.getNodeState();
        indexerSupport.updateIndexDefinitions(builder);
        IndexDefinition.Builder indexDefBuilder = new IndexDefinition.Builder();

        Set<IndexDefinition> indexDefinitions = new HashSet<>();

        for (String indexPath : indexHelper.getIndexPaths()) {
            NodeBuilder idxBuilder = IndexerSupport.childBuilder(builder, indexPath, false);
            IndexDefinition indexDf = indexDefBuilder.defn(idxBuilder.getNodeState()).indexPath(indexPath).root(root).build();
            indexDefinitions.add(indexDf);
        }
        return indexDefinitions;
    }

    private MongoDocumentStore getMongoDocumentStore() {
        return checkNotNull(indexHelper.getService(MongoDocumentStore.class));
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

    private long getEstimatedDocumentCount(){
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
                    indexerPaths.add(indexPath);
                }
            }
        }

        return new CompositeIndexer(indexers);
    }

    protected abstract List<NodeStateIndexerProvider> createProviders() throws IOException;

    protected abstract void preIndexOpertaions(List<NodeStateIndexer> indexers);

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
