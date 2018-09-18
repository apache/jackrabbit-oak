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

package org.apache.jackrabbit.oak.plugins.index.importer;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.importer.AsyncIndexerLock.LockToken;
import org.apache.jackrabbit.oak.plugins.index.upgrade.IndexDisabler;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_COUNT;
import static org.apache.jackrabbit.oak.plugins.index.importer.IndexDefinitionUpdater.INDEX_DEFINITIONS_JSON;
import static org.apache.jackrabbit.oak.plugins.index.importer.NodeStoreUtils.mergeWithConcurrentCheck;

public class IndexImporter {
    /**
     * Symbolic name use to indicate sync indexes
     */
    static final String ASYNC_LANE_SYNC = "sync";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final NodeStore nodeStore;
    private final File indexDir;
    private final Map<String, IndexImporterProvider> importers = new HashMap<>();
    private final IndexerInfo indexerInfo;
    private final Map<String, File> indexes;
    private final ListMultimap<String, IndexInfo> asyncLaneToIndexMapping;
    private final NodeState indexedState;
    private final IndexEditorProvider indexEditorProvider;
    private final AsyncIndexerLock indexerLock;
    private final IndexDefinitionUpdater indexDefinitionUpdater;

    public IndexImporter(NodeStore nodeStore, File indexDir, IndexEditorProvider indexEditorProvider,
                         AsyncIndexerLock indexerLock) throws IOException {
        checkArgument(indexDir.exists() && indexDir.isDirectory(), "Path [%s] does not point " +
                "to existing directory", indexDir.getAbsolutePath());
        this.nodeStore = nodeStore;
        this.indexDir = indexDir;
        this.indexEditorProvider = indexEditorProvider;
        indexerInfo = IndexerInfo.fromDirectory(indexDir);
        this.indexerLock = indexerLock;
        indexes = indexerInfo.getIndexes();
        indexedState = checkNotNull(nodeStore.retrieve(indexerInfo.checkpoint), "Cannot retrieve " +
                "checkpointed state [%s]", indexerInfo.checkpoint);
        this.indexDefinitionUpdater = new IndexDefinitionUpdater(new File(indexDir, INDEX_DEFINITIONS_JSON));
        this.asyncLaneToIndexMapping = mapIndexesToLanes(indexes);
    }

    public void importIndex() throws IOException, CommitFailedException {
        log.info("Proceeding to import {} indexes from {}", indexes.keySet(), indexDir.getAbsolutePath());

        //TODO Need to review it for idempotent design. A failure in any step should not
        //leave setup in in consistent state and provide option for recovery

        //Step 1 - Switch the index lanes so that async indexer does not touch them
        //while we are importing the index data
        switchLanes();
        log.info("Done with switching of index lanes before import");

        //Step 2 - Import the existing index data
        importIndexData();
        log.info("Done with importing of index data");

        //Step 3 - Bring index upto date
        bringIndexUpToDate();

        //Step 4 - Release the checkpoint
        releaseCheckpoint();
    }

    public void addImporterProvider(IndexImporterProvider importerProvider) {
        importers.put(importerProvider.getType(), importerProvider);
    }

    void switchLanes() throws CommitFailedException, IOException {
        NodeState root = nodeStore.getRoot();
        NodeBuilder builder = root.builder();

        for (IndexInfo indexInfo : asyncLaneToIndexMapping.values()){
            if (!indexInfo.newIndex) {
                NodeBuilder idxBuilder = NodeStoreUtils.childBuilder(builder, indexInfo.indexPath);
                AsyncLaneSwitcher.switchLane(idxBuilder, AsyncLaneSwitcher.getTempLaneName(indexInfo.asyncLaneName));
            }
        }
        mergeWithConcurrentCheck(nodeStore, builder);
    }

    void importIndexData() throws CommitFailedException, IOException {
        NodeState root = nodeStore.getRoot();
        NodeBuilder rootBuilder = root.builder();
        IndexDisabler indexDisabler = new IndexDisabler(rootBuilder);
        for (IndexInfo indexInfo : asyncLaneToIndexMapping.values()) {
            log.info("Importing index data for {}", indexInfo.indexPath);
            NodeBuilder idxBuilder = indexDefinitionUpdater.apply(rootBuilder, indexInfo.indexPath);

            if (indexInfo.newIndex) {
                AsyncLaneSwitcher.switchLane(idxBuilder, AsyncLaneSwitcher.getTempLaneName(indexInfo.asyncLaneName));
            } else {
                //For existing ind
                NodeState existing = NodeStateUtils.getNode(root, indexInfo.indexPath);
                copyLaneProps(existing, idxBuilder);
            }
            //TODO How to support CompositeNodeStore where some of the child nodes would be hidden
            incrementReIndexCount(idxBuilder);
            getImporter(indexInfo.type).importIndex(root, idxBuilder, indexInfo.indexDir);

            indexDisabler.markDisableFlagIfRequired(indexInfo.indexPath, idxBuilder);
        }
        mergeWithConcurrentCheck(nodeStore, rootBuilder, indexEditorProvider);
    }

    private void bringIndexUpToDate() throws CommitFailedException {
        for (String laneName : asyncLaneToIndexMapping.keySet()) {
            if (ASYNC_LANE_SYNC.equals(laneName)){
                continue; //TODO Handle sync indexes
            }
            bringAsyncIndexUpToDate(laneName, asyncLaneToIndexMapping.get(laneName));
        }
    }

    private void bringAsyncIndexUpToDate(String laneName, List<IndexInfo> indexInfos) throws CommitFailedException {
        LockToken lockToken = interruptCurrentIndexing(laneName);
        boolean success = false;
        try {
            String checkpoint = getAsync().getString(laneName);
            checkNotNull(checkpoint, "No current checkpoint found for lane [%s]", laneName);

            //TODO Support case where checkpoint got lost or complete reindexing is done

            NodeState after = nodeStore.retrieve(checkpoint);
            checkNotNull(after, "No state found for checkpoint [%s] for lane [%s]",checkpoint, laneName);
            log.info("Proceeding to update imported indexes {} to checkpoint [{}] for lane [{}]",
                    indexInfos, checkpoint, laneName);

            NodeState before = indexedState;

            NodeBuilder builder = nodeStore.getRoot().builder();

            IndexUpdate indexUpdate = new IndexUpdate(
                    indexEditorProvider,
                    AsyncLaneSwitcher.getTempLaneName(laneName),
                    nodeStore.getRoot(),
                    builder,
                    IndexUpdateCallback.NOOP
            );

            CommitFailedException exception =
                    EditorDiff.process(VisibleEditor.wrap(indexUpdate), before, after);

            if (exception != null) {
                throw exception;
            }

            revertLaneChange(builder, indexInfos);

            mergeWithConcurrentCheck(nodeStore, builder);
            success = true;
            log.info("Imported index is updated to repository state at checkpoint [{}] for " +
                    "indexing lane [{}]", checkpoint, laneName);
        } finally {
            try {
                resumeCurrentIndexing(lockToken);
            } catch (CommitFailedException | RuntimeException e) {
                log.warn("Error occurred while releasing indexer lock", e);
                if (success) {
                    throw e;
                }
            }
        }

        log.info("Import done for indexes {}", indexInfos);
    }

    private void revertLaneChange(NodeBuilder builder, List<IndexInfo> indexInfos) {
        for (IndexInfo info : indexInfos) {
            NodeBuilder idxBuilder = NodeStoreUtils.childBuilder(builder, info.indexPath);
            AsyncLaneSwitcher.revertSwitch(idxBuilder, info.indexPath);
        }
    }

    private void resumeCurrentIndexing(LockToken lockToken) throws CommitFailedException {
        indexerLock.unlock(lockToken);
    }

    private LockToken interruptCurrentIndexing(String laneName) throws CommitFailedException {
        return indexerLock.lock(laneName);
    }

    private IndexImporterProvider getImporter(String type) {
        IndexImporterProvider provider = importers.get(type);
        return checkNotNull(provider, "No IndexImporterProvider found for type [%s]", type);
    }

    private ListMultimap<String, IndexInfo> mapIndexesToLanes(Map<String, File> indexes) {
        NodeState rootState = nodeStore.getRoot();
        ListMultimap<String, IndexInfo> map = ArrayListMultimap.create();
        for (Map.Entry<String, File> e : indexes.entrySet()) {
            String indexPath = e.getKey();


            NodeState indexState = indexDefinitionUpdater.getIndexState(indexPath);
            checkArgument(indexState.exists(), "No index node found at path [%s]", indexPath);

            boolean newIndex = !NodeStateUtils.getNode(rootState, indexPath).exists();

            String type = indexState.getString(IndexConstants.TYPE_PROPERTY_NAME);
            checkNotNull(type, "No 'type' property found for index at path [%s]", indexPath);

            String asyncName = getAsyncLaneName(indexPath, indexState);
            if (asyncName == null) {
                asyncName = ASYNC_LANE_SYNC;
            }

            map.put(asyncName, new IndexInfo(indexPath, e.getValue(), asyncName, type, newIndex));
        }
        return map;
    }

    private static void copyLaneProps(NodeState existing, NodeBuilder indexBuilder) {
        copy(IndexConstants.ASYNC_PROPERTY_NAME, existing, indexBuilder);
        copy(AsyncLaneSwitcher.ASYNC_PREVIOUS, existing, indexBuilder);
    }

    private static void copy(String propName, NodeState existing, NodeBuilder indexBuilder) {
        PropertyState ps = existing.getProperty(propName);
        if (ps != null) {
            indexBuilder.setProperty(ps);
        }
    }


    /**
     * Determines the async lane name. This method also check if lane was previously switched
     * then it uses the actual lane name prior to switch was done
     *
     * @param indexPath path of index. Mostly used in reporting exception
     * @param indexState nodeState for index at given path
     *
     * @return async lane name or null which would be the case for sync indexes
     */
    static String getAsyncLaneName(String indexPath, NodeState indexState) {
        PropertyState asyncPrevious = indexState.getProperty(AsyncLaneSwitcher.ASYNC_PREVIOUS);
        if (asyncPrevious != null && !AsyncLaneSwitcher.isNone(asyncPrevious)){
            return IndexUtils.getAsyncLaneName(indexState, indexPath, asyncPrevious);
        }
        return IndexUtils.getAsyncLaneName(indexState, indexPath);
    }

    private void releaseCheckpoint() {
        nodeStore.release(indexerInfo.checkpoint);
        log.info("Released the referred checkpoint [{}]", indexerInfo.checkpoint);
    }

    private void incrementReIndexCount(NodeBuilder definition) {
        long count = 0;
        if(definition.hasProperty(REINDEX_COUNT)){
            count = definition.getProperty(REINDEX_COUNT).getValue(Type.LONG);
        }
        definition.setProperty(REINDEX_COUNT, count + 1);
    }

    private NodeState getAsync() {
        return nodeStore.getRoot().getChildNode(":async");
    }

    private static class IndexInfo {
        final String indexPath;
        final File indexDir;
        final String asyncLaneName;
        final String type;
        final boolean newIndex;

        private IndexInfo(String indexPath, File indexDir, String asyncLaneName, String type, boolean newIndex) {
            this.indexPath = indexPath;
            this.indexDir = indexDir;
            this.asyncLaneName = asyncLaneName;
            this.type = type;
            this.newIndex = newIndex;
        }

        @Override
        public String toString() {
            return indexPath;
        }
    }

}
