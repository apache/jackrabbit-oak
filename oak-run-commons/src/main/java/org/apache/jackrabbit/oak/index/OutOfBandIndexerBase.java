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
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.*;
import org.apache.jackrabbit.oak.plugins.index.progress.MetricRateEstimator;
import org.apache.jackrabbit.oak.plugins.index.progress.NodeCounterMBeanEstimator;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.index.IndexerMetrics.METRIC_INDEXING_INDEX_DATA_SIZE;
import static org.apache.jackrabbit.oak.index.IndexerMetrics.METRIC_INDEXING_PUBLISH_DURATION_SECONDS;
import static org.apache.jackrabbit.oak.index.IndexerMetrics.METRIC_INDEXING_PUBLISH_NODES_INDEXED;
import static org.apache.jackrabbit.oak.index.IndexerMetrics.METRIC_INDEXING_PUBLISH_NODES_TRAVERSED;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.INDEXING_PHASE_LOGGER;

public abstract class OutOfBandIndexerBase implements Closeable, IndexUpdateCallback, NodeTraversalCallback {

    protected final Closer closer = Closer.create();
    private final IndexHelper indexHelper;
    private final IndexerSupport indexerSupport;
    private final IndexingReporter indexingReporter;
    private final StatisticsProvider statisticsProvider;
    private NodeStore copyOnWriteStore;
    private long nodesTraversed = 0;
    private long nodesIndexed = 0;

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
        this.indexHelper = requireNonNull(indexHelper);
        this.indexerSupport = requireNonNull(indexerSupport);
        this.indexingReporter = indexHelper.getIndexReporter();
        this.statisticsProvider = indexHelper.getStatisticsProvider();
    }

    public void reindex() throws CommitFailedException, IOException {
        List<String> indexNames = indexerSupport.getIndexDefinitions().stream().map(IndexDefinition::getIndexName).collect(Collectors.toList());
        indexingReporter.setIndexNames(indexNames);
        INDEXING_PHASE_LOGGER.info("[TASK:FULL_INDEX_CREATION:START] Starting indexing job");
        Stopwatch indexJobWatch = Stopwatch.createStarted();
        try {
            NodeState checkpointedState = indexerSupport.retrieveNodeStateForCheckpoint();

            copyOnWriteStore = new MemoryNodeStore(checkpointedState);
            NodeState baseState = copyOnWriteStore.getRoot();

            indexerSupport.switchIndexLanesAndReindexFlag(copyOnWriteStore);
            preformIndexUpdate(baseState);
            indexerSupport.postIndexWork(copyOnWriteStore);

            long indexingDurationSeconds = indexJobWatch.elapsed(TimeUnit.SECONDS);
            long totalSize = indexerSupport.computeSizeOfGeneratedIndexData();
            INDEXING_PHASE_LOGGER.info("[TASK:INDEXING:END] Metrics: {}", MetricsFormatter.createMetricsWithDurationOnly(indexingDurationSeconds));
            MetricsUtils.addMetric(statisticsProvider, indexingReporter, METRIC_INDEXING_PUBLISH_DURATION_SECONDS, indexingDurationSeconds);
            MetricsUtils.addMetric(statisticsProvider, indexingReporter, METRIC_INDEXING_PUBLISH_NODES_TRAVERSED, nodesTraversed);
            MetricsUtils.addMetric(statisticsProvider, indexingReporter, METRIC_INDEXING_PUBLISH_NODES_INDEXED, nodesIndexed);
            MetricsUtils.addMetricByteSize(statisticsProvider, indexingReporter, METRIC_INDEXING_INDEX_DATA_SIZE, totalSize);
            indexingReporter.addTiming("Build Lucene Index", FormattingUtils.formatToSeconds(indexingDurationSeconds));
        } catch (Throwable t) {
            INDEXING_PHASE_LOGGER.info("[TASK:FULL_INDEX_CREATION:FAIL] Metrics: {}, Error: {}",
                    MetricsFormatter.createMetricsWithDurationOnly(indexJobWatch), t.toString());
            throw t;
        }
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
    public void indexUpdate() {
        nodesIndexed++;
    }

    @Override
    public void traversedNode(NodeTraversalCallback.PathSource pathSource) {
        nodesTraversed++;
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
