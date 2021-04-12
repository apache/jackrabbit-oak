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

import com.codahale.metrics.MetricRegistry;
import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.*;
import org.apache.jackrabbit.oak.plugins.index.progress.MetricRateEstimator;
import org.apache.jackrabbit.oak.plugins.index.progress.NodeCounterMBeanEstimator;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class OutOfBandIndexerBase implements Closeable, IndexUpdateCallback, NodeTraversalCallback{

    protected final Closer closer = Closer.create();
    private final IndexHelper indexHelper;
    private NodeStore copyOnWriteStore;
    private IndexerSupport indexerSupport;

    /**
     * Index lane name which is used for indexing
     */
    private static final String REINDEX_LANE = "offline-reindex-async";
    /**
     * Directory name in output directory under which indexes are
     * stored
     */
    public static final String LOCAL_INDEX_ROOT_DIR = "indexes";

    //TODO Support for providing custom index definition i.e. where definition is not
    //present in target repository

    public OutOfBandIndexerBase(IndexHelper indexHelper, IndexerSupport indexerSupport) {
        this.indexHelper = checkNotNull(indexHelper);
        this.indexerSupport = checkNotNull(indexerSupport);
    }

    public void reindex() throws CommitFailedException, IOException {
        NodeState checkpointedState = indexerSupport.retrieveNodeStateForCheckpoint();

        copyOnWriteStore = new MemoryNodeStore(checkpointedState);
        NodeState baseState = copyOnWriteStore.getRoot();
        //TODO Check for indexPaths being empty

        indexerSupport.switchIndexLanesAndReindexFlag(copyOnWriteStore);
        preformIndexUpdate(baseState);
        indexerSupport.postIndexWork(copyOnWriteStore);
    }

    protected File getLocalIndexDir() throws IOException {
        return indexerSupport.getLocalIndexDir();
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
    public void traversedNode(NodeTraversalCallback.PathSource pathSource) throws CommitFailedException {

    }

    protected void preformIndexUpdate(NodeState baseState) throws IOException, CommitFailedException {
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

        copyOnWriteStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    protected abstract IndexEditorProvider createIndexEditorProvider() throws IOException;

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
