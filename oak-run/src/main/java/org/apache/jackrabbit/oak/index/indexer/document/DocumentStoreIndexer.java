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
import java.util.List;
import java.util.Set;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentTraverser;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
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

public class DocumentStoreIndexer implements Closeable{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Logger traversalLog = LoggerFactory.getLogger(DocumentStoreIndexer.class.getName()+".traversal");
    private final Closer closer = Closer.create();
    private final IndexHelper indexHelper;
    private final List<NodeStateIndexerProvider> indexerProviders;
    private final IndexerSupport indexerSupport;
    private final IndexingProgressReporter progressReporter =
            new IndexingProgressReporter(IndexUpdateCallback.NOOP, NodeTraversalCallback.NOOP);

    public DocumentStoreIndexer(IndexHelper indexHelper, IndexerSupport indexerSupport) throws IOException {
        this.indexHelper = indexHelper;
        this.indexerSupport = indexerSupport;
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

        progressReporter.reindexingTraversalStart("/");
        for (NodeDocument doc : getIncludedDocs(indexer)) {
            String path = doc.getPath();

            DocumentNodeState nodeState = nodeStore.getNode(path, rootDocumentState.getRootRevision());

            //At DocumentNodeState api level the nodeState can be null
            if (nodeState == null || !nodeState.exists()) {
                continue;
            }

            NodeStateEntry entry = new NodeStateEntry(nodeState, path);
            indexer.index(entry);

            for (DocumentNodeState bundledState : nodeState.getAllBundledNodesStates()) {
               indexer.index(new NodeStateEntry(bundledState, bundledState.getPath()));
            }
        }

        progressReporter.reindexingTraversalEnd();
        progressReporter.logReport();

        copyOnWriteStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        indexerSupport.postIndexWork(copyOnWriteStore);
    }

    private void configureEstimators() {
        StatisticsProvider statsProvider = indexHelper.getStatisticsProvider();
        if (statsProvider instanceof MetricStatisticsProvider) {
            MetricRegistry registry = ((MetricStatisticsProvider) statsProvider).getRegistry();
            progressReporter.setTraversalRateEstimator(new MetricRateEstimator("async", registry));
        }

        MongoConnection mongoConnection = indexHelper.getService(MongoConnection.class);
        if (mongoConnection != null) {
            long nodesCount = mongoConnection.getDB().getCollection("nodes").count();
            progressReporter.setNodeCountEstimator((String basePath, Set<String> indexPaths) -> nodesCount);
            log.info("Estimated number of documents in Mongo are {}", nodesCount);
        }
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }

    @SuppressWarnings("Guava")
    private Iterable<NodeDocument> getIncludedDocs(CompositeIndexer indexer) {
        return FluentIterable.from(getDocsFilteredByPath(indexer))
                .filter(doc -> !doc.isSplitDocument())
                .filter(indexer::shouldInclude);
    }

    private Iterable<NodeDocument> getDocsFilteredByPath(CompositeIndexer indexer) {
        CloseableIterable<NodeDocument> docs = findAllDocuments(indexer);
        closer.register(docs);
        return docs;
    }

    private CloseableIterable<NodeDocument> findAllDocuments(CompositeIndexer indexer) {
        MongoDocumentStore mds = indexHelper.getService(MongoDocumentStore.class);
        checkNotNull(mds);
        return new MongoDocumentTraverser(mds).getAllDocuments(Collection.NODES, id -> includeId(id, indexer));
    }

    private boolean includeId(String id, NodeStateIndexer indexer) {
        reportDocumentRead(id);
        //Cannot interpret long paths as they are hashed. So let them
        //be included
        if (Utils.isIdFromLongPath(id)){
            return true;
        }

        //Not easy to determine path for previous docs
        //Given there count is pretty low compared to others
        //include them all
        if (Utils.isPreviousDocId(id)){
            return true;
        }

        String path = Utils.getPathFromId(id);

        //Exclude hidden nodes from index data
        if (NodeStateUtils.isHiddenPath(path)){
            return false;
        }

        return indexer.shouldInclude(path);
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

    private List<NodeStateIndexerProvider> createProviders() throws IOException {
        List<NodeStateIndexerProvider> providers = ImmutableList.of(
          createLuceneIndexProvider()
        );

        providers.forEach(closer::register);
        return providers;
    }

    private NodeStateIndexerProvider createLuceneIndexProvider() throws IOException {
        return new LuceneIndexerProvider(indexHelper, indexerSupport);
    }
}
