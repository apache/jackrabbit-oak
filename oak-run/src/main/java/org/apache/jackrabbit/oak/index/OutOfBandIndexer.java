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

package org.apache.jackrabbit.oak.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Stopwatch;
import com.google.common.io.Closer;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.CorruptIndexHandler;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexerInfo;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.FSDirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.progress.MetricRateEstimator;
import org.apache.jackrabbit.oak.plugins.index.progress.NodeCounterMBeanEstimator;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;

public class OutOfBandIndexer implements Closeable, IndexUpdateCallback, NodeTraversalCallback {
    private final Logger log = LoggerFactory.getLogger(getClass());
    /**
     * Index lane name which is used for indexing
     */
    private static final String REINDEX_LANE = "offline-reindex-async";
    /**
     * Property name where previous value of 'async' is stored
     */
    private static final String ASYNC_PREVIOUS = "async-previous";
    /**
     * Value stored in previous async property if the index is not async
     * i.e. when a sync index is reindexed in out of band mode
     */
    private static final String ASYNC_PREVIOUS_NONE = "none";
    /**
     * Directory name in output directory under which indexes are
     * stored
     */
    public static final String LOCAL_INDEX_ROOT_DIR = "indexes";

    /**
     * Checkpoint value which indicate that head state needs to be used
     * This would be mostly used for testing purpose
     */
    private static final String HEAD_AS_CHECKPOINT = "head";

    private final Closer closer = Closer.create();
    private final IndexHelper indexHelper;
    private final String checkpoint;
    private Map<String, String> checkpointInfo = Collections.emptyMap();
    private NodeStore copyOnWriteStore;
    private File localIndexDir;

    //TODO Support for providing custom index definition i.e. where definition is not
    //present in target repository

    public OutOfBandIndexer(IndexHelper indexHelper, String checkpoint) {
        this.indexHelper = checkNotNull(indexHelper);
        this.checkpoint = checkNotNull(checkpoint);
    }

    public File reindex() throws CommitFailedException, IOException {
        Stopwatch w = Stopwatch.createStarted();

        NodeState checkpointedState = retrieveNodeStateForCheckpoint();

        copyOnWriteStore = new MemoryNodeStore(checkpointedState);
        NodeState baseState = copyOnWriteStore.getRoot();
        //TODO Check for indexPaths being empty

        log.info("Proceeding to index {} upto checkpoint {} {}", indexHelper.getIndexPaths(), checkpoint, checkpointInfo);

        switchIndexLanesAndReindexFlag();
        preformIndexUpdate(baseState);
        writeMetaInfo();
        File destDir = copyIndexFilesToOutput();

        log.info("Indexing completed for indexes {} in {} and index files are copied to {}",
                indexHelper.getIndexPaths(), w, IndexCommand.getPath(destDir));
        return destDir;
    }

    private File getLocalIndexDir() throws IOException {
        if (localIndexDir == null) {
            localIndexDir = new File(indexHelper.getWorkDir(), LOCAL_INDEX_ROOT_DIR);
            FileUtils.forceMkdir(localIndexDir);
        }
        return localIndexDir;
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }

    //~---------------------------------------------------< callbacks >

    @Override
    public void indexUpdate() throws CommitFailedException {

    }

    @Override
    public void traversedNode(PathSource pathSource) throws CommitFailedException {

    }

    private void preformIndexUpdate(NodeState baseState) throws IOException, CommitFailedException {
        NodeBuilder builder = copyOnWriteStore.getRoot().builder();

        IndexUpdate indexUpdate = new IndexUpdate(
                createIndexEditorProvider(),
                REINDEX_LANE,
                copyOnWriteStore.getRoot(),
                builder,
                this,
                this,
                CommitInfo.EMPTY,
                CorruptIndexHandler.NOOP
        );

        configureEstimators(indexUpdate);

        //Do not use EmptyState as before otherwise the IndexUpdate would
        //unnecessary traverse the whole repo post reindexing. With use of baseState
        //It would only traverse the diff i.e. those index definitions paths
        //whose lane has been changed
        NodeState before = baseState;
        NodeState after = copyOnWriteStore.getRoot();

        CommitFailedException exception =
                EditorDiff.process(VisibleEditor.wrap(indexUpdate), before, after);

        if (exception != null) {
            throw exception;
        }
    }

    private IndexEditorProvider createIndexEditorProvider() throws IOException {
        IndexEditorProvider lucene = createLuceneEditorProvider();
        IndexEditorProvider property = new PropertyIndexEditorProvider().with(indexHelper.getMountInfoProvider());

        return CompositeIndexEditorProvider.compose(asList(lucene, property));
    }

    private IndexEditorProvider createLuceneEditorProvider() throws IOException {
        LuceneIndexHelper luceneIndexHelper = indexHelper.getLuceneIndexHelper();
        DirectoryFactory dirFactory = new FSDirectoryFactory(getLocalIndexDir());
        luceneIndexHelper.setDirectoryFactory(dirFactory);
        return luceneIndexHelper.createEditorProvider();
    }

    private void switchIndexLanesAndReindexFlag() throws CommitFailedException {
        NodeBuilder builder = copyOnWriteStore.getRoot().builder();

        for (String indexPath : indexHelper.getIndexPaths()) {
            //TODO Do it only for lucene indexes for now
            NodeBuilder idxBuilder = NodeStoreUtils.childBuilder(builder, indexPath);
            idxBuilder.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
            switchLane(idxBuilder);
        }

        copyOnWriteStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        log.info("Switched the async lane for indexes at {} to {} and marked them for reindex", indexHelper.getIndexPaths(), REINDEX_LANE);
    }

    private NodeState retrieveNodeStateForCheckpoint() {
        NodeState checkpointedState;
        if (HEAD_AS_CHECKPOINT.equals(checkpoint)) {
            checkpointedState = indexHelper.getNodeStore().getRoot();
            log.warn("Using head state for indexing. Such an index cannot be imported back");
        } else {
            checkpointedState = indexHelper.getNodeStore().retrieve(checkpoint);
            checkNotNull(checkpointedState, "Not able to retrieve revision referred via checkpoint [%s]", checkpoint);
            checkpointInfo = indexHelper.getNodeStore().checkpointInfo(checkpoint);
        }
        return checkpointedState;
    }

    /**
     * Make a copy of current async value and replace it with one required for offline reindexing
     */
    static void switchLane(NodeBuilder idxBuilder) {
        PropertyState currentAsyncState = idxBuilder.getProperty(ASYNC_PROPERTY_NAME);
        PropertyState newAsyncState = PropertyStates.createProperty(ASYNC_PROPERTY_NAME, REINDEX_LANE, Type.STRING);

        PropertyState previousAsyncState;
        if (currentAsyncState == null) {
            previousAsyncState = PropertyStates.createProperty(ASYNC_PREVIOUS, ASYNC_PREVIOUS_NONE);
        } else {
            //Ensure that previous state is copied with correct type
            if (currentAsyncState.isArray()) {
                previousAsyncState = PropertyStates.createProperty(ASYNC_PREVIOUS, currentAsyncState.getValue(Type.STRINGS), Type.STRINGS);
            } else {
                previousAsyncState = PropertyStates.createProperty(ASYNC_PREVIOUS, currentAsyncState.getValue(Type.STRING), Type.STRING);
            }
        }

        idxBuilder.setProperty(previousAsyncState);
        idxBuilder.setProperty(newAsyncState);
    }

    private void writeMetaInfo() throws IOException {
        new IndexerInfo(getLocalIndexDir(), checkpoint).save();
    }

    private File copyIndexFilesToOutput() throws IOException {
        File destDir = new File(indexHelper.getOutputDir(), getLocalIndexDir().getName());
        FileUtils.moveDirectoryToDirectory(getLocalIndexDir(), indexHelper.getOutputDir(), true);
        return destDir;
    }

    private void configureEstimators(IndexUpdate indexUpdate) {
        StatisticsProvider statsProvider = indexHelper.getStatisticsProvider();
        if (statsProvider instanceof MetricStatisticsProvider) {
            MetricRegistry registry = ((MetricStatisticsProvider) statsProvider).getRegistry();
            indexUpdate.setTraversalRateEstimator(new MetricRateEstimator(REINDEX_LANE, registry));
        }

        NodeCounterMBeanEstimator estimator = new NodeCounterMBeanEstimator(indexHelper.getNodeStore());
        indexUpdate.setNodeCountEstimator(estimator);
    }
}
