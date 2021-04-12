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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Stopwatch;
import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.index.progress.MetricRateEstimator;
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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

public abstract class DocumentStoreIndexerBase implements Closeable{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Logger traversalLog = LoggerFactory.getLogger(DocumentStoreIndexerBase.class.getName()+".traversal");
    protected final Closer closer = Closer.create();
    protected final IndexHelper indexHelper;
    protected List<NodeStateIndexerProvider> indexerProviders;
    protected final IndexerSupport indexerSupport;
    private final IndexingProgressReporter progressReporter =
            new IndexingProgressReporter(IndexUpdateCallback.NOOP, NodeTraversalCallback.NOOP);
    private final Set<String> indexerPaths = new HashSet<>();

    public DocumentStoreIndexerBase(IndexHelper indexHelper, IndexerSupport indexerSupport) throws IOException {
        this.indexHelper = indexHelper;
        this.indexerSupport = indexerSupport;
    }

    protected void setProviders() throws IOException {
        this.indexerProviders = createProviders();
    }

    public void reindex() throws CommitFailedException, IOException {
        configureEstimators();

        NodeState checkpointedState = indexerSupport.retrieveNodeStateForCheckpoint();
        NodeStore copyOnWriteStore = new MemoryNodeStore(checkpointedState);
        indexerSupport.switchIndexLanesAndReindexFlag(copyOnWriteStore);

        NodeBuilder builder = copyOnWriteStore.getRoot().builder();
        CompositeIndexer indexer = prepareIndexers(copyOnWriteStore, builder);
        if (indexer.isEmpty()) {
            return;
        }

        closer.register(indexer);

        //TODO How to ensure we can safely read from secondary
        DocumentNodeState rootDocumentState = (DocumentNodeState) checkpointedState;
        DocumentNodeStore nodeStore = (DocumentNodeStore) indexHelper.getNodeStore();

        NodeStateEntryTraverser nsep =
                new NodeStateEntryTraverser(rootDocumentState.getRootRevision(),
                        nodeStore, getMongoDocumentStore())
                        .withProgressCallback(this::reportDocumentRead)
                        .withPathPredicate(indexer::shouldInclude);
        closer.register(nsep);

        //As first traversal is for dumping change the message prefix
        progressReporter.setMessagePrefix("Dumping");

        //TODO Use flatFileStore only if we have relative nodes to be indexed
        FlatFileStore flatFileStore = new FlatFileNodeStoreBuilder(nsep, indexHelper.getWorkDir())
                .withBlobStore(indexHelper.getGCBlobStore())
                .withPreferredPathElements(indexer.getRelativeIndexedNodeNames())
                .build();
        closer.register(flatFileStore);

        progressReporter.reset();
        if (flatFileStore.getEntryCount() > 0){
            progressReporter.setNodeCountEstimator((String basePath, Set<String> indexPaths) -> flatFileStore.getEntryCount());
        }

        progressReporter.reindexingTraversalStart("/");

        preIndexOpertaions(indexer.getIndexers());

        Stopwatch indexerWatch = Stopwatch.createStarted();
        for (NodeStateEntry entry : flatFileStore) {
            reportDocumentRead(entry.getPath());
            indexer.index(entry);
        }

        progressReporter.reindexingTraversalEnd();
        progressReporter.logReport();
        log.info("Completed the indexing in {}", indexerWatch);

        copyOnWriteStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        indexerSupport.postIndexWork(copyOnWriteStore);
    }

    private MongoDocumentStore getMongoDocumentStore() {
        return checkNotNull(indexHelper.getService(MongoDocumentStore.class));
    }

    private void configureEstimators() {
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

    private void reportDocumentRead(String id) {
        try {
            progressReporter.traversedNode(() -> id);
        } catch (CommitFailedException e) {
            throw new RuntimeException(e);
        }
        traversalLog.trace(id);
    }

    protected CompositeIndexer prepareIndexers(NodeStore copyOnWriteStore, NodeBuilder builder) {
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
