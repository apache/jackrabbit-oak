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

import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.collect.ArrayListMultimap;
import org.apache.jackrabbit.guava.common.collect.ListMultimap;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexingReporter;
import org.apache.jackrabbit.oak.plugins.index.MetricsFormatter;
import org.apache.jackrabbit.oak.plugins.index.MetricsUtils;
import org.apache.jackrabbit.oak.plugins.index.importer.AsyncIndexerLock.LockToken;
import org.apache.jackrabbit.oak.plugins.index.upgrade.IndexDisabler;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_COUNT;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.INDEXING_PHASE_LOGGER;
import static org.apache.jackrabbit.oak.plugins.index.importer.IndexDefinitionUpdater.INDEX_DEFINITIONS_JSON;
import static org.apache.jackrabbit.oak.plugins.index.importer.NodeStoreUtils.mergeWithConcurrentCheck;

public class IndexImporter {
    /**
     * Symbolic name use to indicate sync indexes
     */
    static final String ASYNC_LANE_SYNC = "sync";
    /*
     * System property name for flag for preserve checkpoint. If this is set to true, then checkpoint cleanup will be skipped.
     * Default is set to false.
     */
    public static final String OAK_INDEX_IMPORTER_PRESERVE_CHECKPOINT = "oak.index.importer.preserveCheckpoint";

    private static final Logger LOG = LoggerFactory.getLogger(IndexImporter.class);
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
    private final boolean preserveCheckpoint = Boolean.getBoolean(OAK_INDEX_IMPORTER_PRESERVE_CHECKPOINT);

    static final int RETRIES = Integer.getInteger("oak.index.import.retries", 5);
    public static final String INDEX_IMPORT_STATE_KEY = "indexImportState";
    private final Set<String> indexPathsToUpdate;
    private final StatisticsProvider statisticsProvider;
    private final IndexingReporter indexingReporter;

    public IndexImporter(NodeStore nodeStore, File indexDir, IndexEditorProvider indexEditorProvider,
                         AsyncIndexerLock indexerLock) throws IOException {
        this(nodeStore, indexDir, indexEditorProvider, indexerLock, StatisticsProvider.NOOP, IndexingReporter.NOOP);
    }

    public IndexImporter(NodeStore nodeStore, File indexDir, IndexEditorProvider indexEditorProvider,
                         AsyncIndexerLock indexerLock, StatisticsProvider statisticsProvider) throws IOException {
        this(nodeStore, indexDir, indexEditorProvider, indexerLock, statisticsProvider, IndexingReporter.NOOP);
    }

    public IndexImporter(NodeStore nodeStore, File indexDir, IndexEditorProvider indexEditorProvider,
                         AsyncIndexerLock indexerLock, StatisticsProvider statisticsProvider, IndexingReporter indexingReporter) throws IOException {
        this.statisticsProvider = statisticsProvider;
        this.indexingReporter = indexingReporter;
        checkArgument(indexDir.exists() && indexDir.isDirectory(),
                "Path [%s] does not point to existing directory", indexDir.getAbsolutePath());
        this.nodeStore = nodeStore;
        this.indexDir = indexDir;
        this.indexEditorProvider = indexEditorProvider;
        this.indexerInfo = IndexerInfo.fromDirectory(indexDir);
        this.indexerLock = indexerLock;
        this.indexes = indexerInfo.getIndexes();
        this.indexedState = requireNonNull(nodeStore.retrieve(indexerInfo.checkpoint),
                "Cannot retrieve checkpointed state [" + indexerInfo.checkpoint + "]");
        this.indexDefinitionUpdater = new IndexDefinitionUpdater(new File(indexDir, INDEX_DEFINITIONS_JSON));
        this.asyncLaneToIndexMapping = mapIndexesToLanes(indexes);
        this.indexPathsToUpdate = new HashSet<>();
    }

    enum IndexImportState {
        NULL, SWITCH_LANE, IMPORT_INDEX_DATA, BRING_INDEX_UPTODATE, RELEASE_CHECKPOINT
    }

    public void importIndex() throws IOException, CommitFailedException {
        try {
            indexingReporter.setIndexNames(List.copyOf(indexes.keySet()));
            if (indexes.isEmpty()) {
                LOG.warn("No indexes to import (possibly index definitions outside of a oak:index node?)");
            }
            LOG.info("Proceeding to import {} indexes from {}", indexes.keySet(), indexDir.getAbsolutePath());

            //TODO Need to review it for idempotent design. A failure in any step should not
            //leave setup in in consistent state and provide option for recovery

            //Step 1 - Switch the index lanes so that async indexer does not touch them
            //while we are importing the index data
            runWithRetry(RETRIES, IndexImportState.SWITCH_LANE, this::switchLanes);
            LOG.info("Done with switching of index lanes before import");

            //Step 2 - Import the existing index data.
            // In this step we are:
            //      switching lane for new index
            //      incrementing reindex count.
            //      marking index as disabled in case of superseded index
            // after this step new index is available in repository
            runWithRetry(RETRIES, IndexImportState.IMPORT_INDEX_DATA, this::importIndexData);
            LOG.info("Done with importing of index data");

            //Step 3 - Bring index upto date.
            // In this step we are:
            //      interrupting current indexing.
            //      reverting lane back to async
            //      resuming current indexing;
            runWithRetry(RETRIES, IndexImportState.BRING_INDEX_UPTODATE, this::bringIndexUpToDate);
            LOG.info("Done with bringing index up-to-date");
            //Step 4 - Release the checkpoint
            // this is again an idempotent function
            runWithRetry(RETRIES, IndexImportState.RELEASE_CHECKPOINT, this::releaseCheckpoint);
            LOG.info("Done with releasing checkpoint");

            // Remove indexImportState property on successful import. In case of preserveCheckpoint is enabled,
            // we assume that index import is done without releaseCheckpoint. In that case this method below will be NOOP
            // as currentIndexImportState is null and doesn't match RELEASE_CHECKPOINT state
            updateIndexImporterState(IndexImportState.RELEASE_CHECKPOINT, null, true);
            LOG.info("Done with removing index import state");

        } catch (CommitFailedException | IOException e) {

            LOG.error("Failure while index import", e);
            try {
                runWithRetry(RETRIES, null, () -> {
                    NodeState root = nodeStore.getRoot();
                    NodeBuilder builder = root.builder();
                    revertLaneChange(builder, indexPathsToUpdate);
                    mergeWithConcurrentCheck(nodeStore, builder);
                });
            } catch (CommitFailedException commitFailedException) {
                LOG.error("Unable to revert back index lanes for: {}",
                        indexPathsToUpdate.stream()
                                .collect(StringBuilder::new, StringBuilder::append, (a, b) -> a.append(",").append(b)), commitFailedException);
                throw e;
            }
        }
    }

    public void addImporterProvider(IndexImporterProvider importerProvider) {
        importers.put(importerProvider.getType(), importerProvider);
    }

    void switchLanes() throws CommitFailedException {
        try {
            NodeState root = nodeStore.getRoot();
            NodeBuilder builder = root.builder();

            for (IndexInfo indexInfo : asyncLaneToIndexMapping.values()) {
                if (!indexInfo.newIndex) {
                    NodeBuilder idxBuilder = NodeStoreUtils.childBuilder(builder, indexInfo.indexPath);
                    indexPathsToUpdate.add(indexInfo.indexPath);
                    AsyncLaneSwitcher.switchLane(idxBuilder, AsyncLaneSwitcher.getTempLaneName(indexInfo.asyncLaneName));
                }
            }
            updateIndexImporterState(builder, IndexImportState.NULL, IndexImportState.SWITCH_LANE, false);
            mergeWithConcurrentCheck(nodeStore, builder);
        } catch (CommitFailedException e) {
            LOG.error("Failed while performing switchLanes and updating indexImportState from  [{}] to  [{}]",
                    IndexImportState.NULL, IndexImportState.SWITCH_LANE);
            throw e;
        }
    }

    void importIndexData() throws CommitFailedException, IOException {
        try {
            NodeState root = nodeStore.getRoot();
            NodeBuilder rootBuilder = root.builder();
            IndexDisabler indexDisabler = new IndexDisabler(rootBuilder);
            for (IndexInfo indexInfo : asyncLaneToIndexMapping.values()) {
                LOG.info("Importing index data for {}", indexInfo.indexPath);
                // current index node contains : temp-async and async-previous.
                // old state is indexdefinition with async=async
                // indexDefinitionUpdater contains base index-def i.e. without async-previous and temp-async
                // this apply method take current state and return Nodebuilder with old indexdefinitions i.e. without async-temp.
                // rootbuilder also get updated in the process
                NodeBuilder idxBuilder = indexDefinitionUpdater.apply(rootBuilder, indexInfo.indexPath);

                if (indexInfo.newIndex) {
                    AsyncLaneSwitcher.switchLane(idxBuilder, AsyncLaneSwitcher.getTempLaneName(indexInfo.asyncLaneName));
                    indexPathsToUpdate.add(indexInfo.indexPath);
                } else {
                    //For existing index
                    NodeState existing = NodeStateUtils.getNode(root, indexInfo.indexPath);
                    // copyLaneProps copies property values for async and previous-async property that we set in method
                    // "switchLanes" to idxBuilder from indexDefinitionUpdater i.e. nodestate before index import started
                    copyLaneProps(existing, idxBuilder);
                }
                //TODO How to support CompositeNodeStore where some of the child nodes would be hidden
                incrementReIndexCount(idxBuilder);
                // importIndex copies data from current folder to new older. Updates idxbuilder with new uid.
                getImporter(indexInfo.type).importIndex(root, idxBuilder, indexInfo.indexDir);
                indexDisabler.markDisableFlagIfRequired(indexInfo.indexPath, idxBuilder);
            }
            updateIndexImporterState(root.builder(), IndexImportState.SWITCH_LANE, IndexImportState.IMPORT_INDEX_DATA, false);
            mergeWithConcurrentCheck(nodeStore, rootBuilder, indexEditorProvider);
        } catch (CommitFailedException e) {
            LOG.error("Failed while performing importIndexData and updating indexImportState from  [{}] to  [{}]",
                    IndexImportState.SWITCH_LANE, IndexImportState.IMPORT_INDEX_DATA);
            throw e;
        }
    }

    private void bringIndexUpToDate() throws CommitFailedException {
        for (String laneName : asyncLaneToIndexMapping.keySet()) {
            if (ASYNC_LANE_SYNC.equals(laneName)) {
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
            requireNonNull(checkpoint, "No current checkpoint found for lane [" + laneName + "]");

            //TODO Support case where checkpoint got lost or complete reindexing is done

            NodeState after = nodeStore.retrieve(checkpoint);
            requireNonNull(after, "No state found for checkpoint [" + checkpoint + "] for lane [" + laneName + "]");
            LOG.info("Proceeding to update imported indexes {} to checkpoint [{}] for lane [{}]",
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
            updateIndexImporterState(builder, IndexImportState.IMPORT_INDEX_DATA, IndexImportState.BRING_INDEX_UPTODATE, false);
            mergeWithConcurrentCheck(nodeStore, builder);
            success = true;
            LOG.info("Imported index is updated to repository state at checkpoint [{}] for indexing lane [{}]",
                    checkpoint, laneName);
        } catch (CommitFailedException e) {
            LOG.error("Failed while performing bringIndexUpToDate and updating indexImportState from  [{}] to  [{}]",
                    IndexImportState.IMPORT_INDEX_DATA, IndexImportState.BRING_INDEX_UPTODATE);
            throw e;
        } finally {
            try {
                resumeCurrentIndexing(lockToken);
            } catch (CommitFailedException | RuntimeException e) {
                LOG.warn("Error occurred while releasing indexer lock", e);
                if (success) {
                    throw e;
                }
            }
        }

        LOG.info("Import done for indexes {}", indexInfos);
    }

    private void revertLaneChange(NodeBuilder builder, List<IndexInfo> indexInfos) {
        for (IndexInfo info : indexInfos) {
            NodeBuilder idxBuilder = NodeStoreUtils.childBuilder(builder, info.indexPath);
            AsyncLaneSwitcher.revertSwitch(idxBuilder, info.indexPath);
        }
    }

    private void revertLaneChange(NodeBuilder builder, Set<String> indexPaths) {
        for (String indexPath : indexPaths) {
            NodeBuilder idxBuilder = NodeStoreUtils.childBuilder(builder, indexPath);
            AsyncLaneSwitcher.revertSwitch(idxBuilder, indexPath);
        }
    }

    private IndexImportState getIndexImportState(NodeBuilder nodeBuilder) {
        if (nodeBuilder.getProperty(INDEX_IMPORT_STATE_KEY) == null || nodeBuilder.getProperty(INDEX_IMPORT_STATE_KEY).getValue(Type.STRING) == null) {
            return IndexImportState.NULL;
        } else {
            return IndexImportState.valueOf(nodeBuilder.getProperty(INDEX_IMPORT_STATE_KEY).getValue(Type.STRING));
        }
    }

    // updateIndexImporterState is an idempotent process and only updates state if currentImportState matches.
    private void updateIndexImporterState(IndexImportState currentImportState, IndexImportState nextImportState, boolean shouldCommit) throws CommitFailedException {
        NodeState root = nodeStore.getRoot();
        NodeBuilder builder = root.builder();
        updateIndexImporterState(builder, currentImportState, nextImportState, shouldCommit);
    }

    // updateIndexImporterState is an idempotent process and only updates state if currentImportState matches nodeStoreIndexImportState.
    private void updateIndexImporterState(NodeBuilder builder, IndexImportState currentImportState, IndexImportState nextImportState, boolean shouldCommit) throws CommitFailedException {
        for (String indexPath : indexPathsToUpdate) {
            NodeBuilder idxBuilder = NodeStoreUtils.childBuilder(builder, indexPath);
            IndexImportState nodeStoreIndexImportState = getIndexImportState(idxBuilder);

            if (nodeStoreIndexImportState == currentImportState) {
                if (nextImportState == IndexImportState.NULL) {
                    idxBuilder.removeProperty(INDEX_IMPORT_STATE_KEY);
                } else {
                    idxBuilder.setProperty(INDEX_IMPORT_STATE_KEY, nextImportState.toString(), Type.STRING);
                }
            }
        }
        if (shouldCommit) {
            mergeWithConcurrentCheck(nodeStore, builder);
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
        return requireNonNull(provider, "No IndexImporterProvider found for type [" + type + "]");
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
            requireNonNull(type, "No 'type' property found for index at path [" + indexPath + "]");

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
     * @param indexPath  path of index. Mostly used in reporting exception
     * @param indexState nodeState for index at given path
     * @return async lane name or null which would be the case for sync indexes
     */
    static String getAsyncLaneName(String indexPath, NodeState indexState) {
        PropertyState asyncPrevious = indexState.getProperty(AsyncLaneSwitcher.ASYNC_PREVIOUS);
        if (asyncPrevious != null && !AsyncLaneSwitcher.isNone(asyncPrevious)) {
            return IndexUtils.getAsyncLaneName(indexState, indexPath, asyncPrevious);
        }
        return IndexUtils.getAsyncLaneName(indexState, indexPath);
    }

    private void releaseCheckpoint() throws CommitFailedException {
        if (preserveCheckpoint) {
            LOG.info("Preserving the referred checkpoint [{}]. This could have been done in case this checkpoint is needed by a process later on." +
                    " Please make sure to remove the checkpoint once it's no longer needed.", indexerInfo.checkpoint);
            // We are assuming that indexImport is complete from our end as checkpoint need to be preserved
            updateIndexImporterState(IndexImportState.BRING_INDEX_UPTODATE, null, true);
        } else {
            if (nodeStore.release(indexerInfo.checkpoint)) {
                LOG.info("Released the referred checkpoint [{}]", indexerInfo.checkpoint);
                updateIndexImporterState(IndexImportState.BRING_INDEX_UPTODATE, IndexImportState.RELEASE_CHECKPOINT, true);
            }
        }
    }

    private void incrementReIndexCount(NodeBuilder definition) {
        long count = 0;
        PropertyState reindexCountProp = definition.getProperty(REINDEX_COUNT);
        if (reindexCountProp != null) {
            count = reindexCountProp.getValue(Type.LONG);
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

    interface IndexImporterStepExecutor {
        void execute() throws CommitFailedException, IOException;
    }

    void runWithRetry(int maxRetries, IndexImportState indexImportState, IndexImporterStepExecutor step) throws CommitFailedException, IOException {
        String indexImportPhaseName = indexImportState == null ? "null" : indexImportState.toString();
        int count = 1;
        Stopwatch start = Stopwatch.createStarted();
        INDEXING_PHASE_LOGGER.info("[TASK:{}:START]", indexImportPhaseName);
        try {
            while (count <= maxRetries) {
                LOG.info("IndexImporterStepExecutor:{}, count:{}", indexImportPhaseName, count);
                try {
                    step.execute();
                    long durationSeconds = start.elapsed(TimeUnit.SECONDS);
                    INDEXING_PHASE_LOGGER.info("[TASK:{}:END] Metrics: {}",
                            indexImportPhaseName,
                            MetricsFormatter.createMetricsWithDurationOnly(durationSeconds)
                    );
                    MetricsUtils.setCounterOnce(statisticsProvider,
                            "oak_indexer_import_" + indexImportPhaseName.toLowerCase() + "_duration_seconds",
                            durationSeconds);
                    indexingReporter.addTiming("oak_indexer_import_" + indexImportPhaseName.toLowerCase(),
                            FormattingUtils.formatToSeconds(durationSeconds));
                    indexingReporter.addMetric("oak_indexer_import_" + indexImportPhaseName.toLowerCase() + "_duration_seconds",
                            durationSeconds);

                    break;
                } catch (CommitFailedException | IOException e) {
                    LOG.warn("IndexImporterStepExecutor: {} fail count: {}, retries left: {}", indexImportState, count, maxRetries - count, e);
                    if (count++ >= maxRetries) {
                        LOG.warn("IndexImporterStepExecutor: {} failed after {} retries", indexImportState, maxRetries, e);
                        throw e;
                    }
                }
            }
        } catch (Throwable t) {
            INDEXING_PHASE_LOGGER.info("[TASK:{}:FAIL] Metrics: {}, Error: {}",
                    indexImportPhaseName,
                    MetricsFormatter.createMetricsWithDurationOnly(start),
                    t.toString());
            throw t;
        }
    }

}
