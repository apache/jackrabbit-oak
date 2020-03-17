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

package org.apache.jackrabbit.oak.plugins.index.progress;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.TimeDurationFormatter;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingProgressReporter implements NodeTraversalCallback {
    private static final String REINDEX_MSG = "Reindexing";
    private static final String INDEX_MSG = "Incremental indexing";

    private final Logger log = LoggerFactory.getLogger(IndexUpdate.class);
    private Stopwatch watch = Stopwatch.createStarted();
    private final IndexUpdateCallback updateCallback;
    private final NodeTraversalCallback traversalCallback;
    private final Map<String, IndexUpdateState> indexUpdateStates = new HashMap<>();
    private long traversalCount;
    private String messagePrefix = INDEX_MSG;
    private TraversalRateEstimator traversalRateEstimator = new SimpleRateEstimator();
    private NodeCountEstimator nodeCountEstimator = NodeCountEstimator.NOOP;
    private long estimatedCount;

    public IndexingProgressReporter(IndexUpdateCallback updateCallback,
                                    NodeTraversalCallback traversalCallback) {
        this.updateCallback = updateCallback;
        this.traversalCallback = traversalCallback;
    }

    public Editor wrapProgress(Editor editor) {
        return ProgressTrackingEditor.wrap(editor, this);
    }

    /**
     * Invoked to indicate that reindexing phase has started in current
     * indexing cycle
     * @param path
     */
    public void reindexingTraversalStart(String path) {
        estimatedCount = nodeCountEstimator.getEstimatedNodeCount(path, getReindexedIndexPaths());
        if (estimatedCount >= 0) {
            log.info("Estimated node count to be traversed for reindexing under {} is [{}]", path, estimatedCount);
        }
        messagePrefix = REINDEX_MSG;
    }

    /**
     * Invoked to indicate that reindexing phase has ended in current
     * indexing cycle
     */
    public void reindexingTraversalEnd() {
        messagePrefix = INDEX_MSG;
    }

    public void setMessagePrefix(String messagePrefix) {
        this.messagePrefix = messagePrefix;
    }

    public void traversedNode(PathSource pathSource) throws CommitFailedException {
        if (++traversalCount % 10000 == 0) {
            double rate = traversalRateEstimator.getNodesTraversedPerSecond();
            String formattedRate = String.format("%1.2f nodes/s, %1.2f nodes/hr", rate, rate * 3600);
            String estimate = estimatePendingTraversal(rate);
            log.info("{} Traversed #{} {} [{}] {}", messagePrefix, traversalCount, pathSource.getPath(), formattedRate, estimate);
        }
        traversalCallback.traversedNode(pathSource);
        traversalRateEstimator.traversedNode();
    }

    /**
     * Registers the index for progress tracking
     *
     * @param indexPath path of index
     * @param reindexing true if the index is being reindexed
     * @param estimatedCount an estimate of count of number of entries in the index. If less
     *                       than zero then it indicates that estimation cannot be done
     */
    public void registerIndex(String indexPath, boolean reindexing, long estimatedCount) {
        indexUpdateStates.put(indexPath, new IndexUpdateState(indexPath, reindexing, estimatedCount));
    }

    /**
     * Callback to indicate that index at give path has got an update
     */
    public void indexUpdate(String indexPath) throws CommitFailedException {
        indexUpdateStates.get(indexPath).indexUpdate();
    }

    public void logReport(){
        if (isReindexingPerformed()){
            log.info(getReport());
            log.info("Reindexing completed");
        } else if (log.isDebugEnabled() && somethingIndexed()){
            log.debug(getReport());
        }
    }

    public List<String> getReindexStats() {
        return indexUpdateStates.values().stream()
                .filter(st -> st.reindex)
                .map(Object::toString)
                .collect(Collectors.toList());
    }

    /**
     * Returns true if any reindexing is performed in current indexing
     * cycle
     */
    public boolean isReindexingPerformed() {
        return indexUpdateStates.values().stream().anyMatch(st -> st.reindex);
    }

    /**
     * Set of indexPaths which have been updated or accessed
     * in this indexing cycle.
     */
    public Set<String> getUpdatedIndexPaths() {
        return indexUpdateStates.keySet();
    }

    /**
     * Set of indexPaths which have been reindexed
     */
    public Set<String> getReindexedIndexPaths() {
        return indexUpdateStates.values().stream()
                .filter(st -> st.reindex)
                .map(st -> st.indexPath)
                .collect(Collectors.toSet());
    }

    public boolean somethingIndexed() {
        return indexUpdateStates.values().stream().anyMatch(st -> st.updateCount > 0);
    }

    public void setTraversalRateEstimator(TraversalRateEstimator traversalRate) {
        this.traversalRateEstimator = traversalRate;
    }

    public void setNodeCountEstimator(NodeCountEstimator nodeCountEstimator) {
        this.nodeCountEstimator = nodeCountEstimator;
    }

    public void setEstimatedCount(long estimatedCount) {
        this.estimatedCount = estimatedCount;
    }

    public void reset(){
        watch = Stopwatch.createStarted();
        traversalCount = 0;
        messagePrefix = INDEX_MSG;
    }

    private String estimatePendingTraversal(double nodesPerSecond) {
        if (estimatedCount >= 0) {
            if (estimatedCount > traversalCount){
                long pending = estimatedCount - traversalCount;
                long timeRequired = (long)(pending / nodesPerSecond);
                double percentComplete = ((double) traversalCount/estimatedCount) * 100;
                return String.format("(Elapsed %s, Expected %s, Completed %1.2f%%)",
                        watch,
                        TimeDurationFormatter.forLogging().format(timeRequired, TimeUnit.SECONDS),
                        percentComplete
                );
            } else {
                return String.format("(Elapsed %s)", watch);
            }
        }
        return "";
    }

    private String getReport() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.println("Indexing report");
        for (IndexUpdateState st : indexUpdateStates.values()) {
            if (!log.isDebugEnabled() && !st.reindex) {
                continue;
            }
            if (st.updateCount > 0 || st.reindex) {
                pw.printf("    - %s%n", st);
            }
        }
        return sw.toString();
    }

    private class IndexUpdateState {
        final String indexPath;
        final boolean reindex;
        final long estimatedCount;
        final Stopwatch watch = Stopwatch.createStarted();
        long updateCount;

        public IndexUpdateState(String indexPath, boolean reindex, long estimatedCount) {
            this.indexPath = indexPath;
            this.reindex = reindex;
            this.estimatedCount = estimatedCount;
        }

        public void indexUpdate() throws CommitFailedException {
            updateCount++;
            if (updateCount % 10000 == 0) {
                log.info("{} => Indexed {} nodes in {} ...", indexPath, updateCount, watch);
                watch.reset().start();
            }
            updateCallback.indexUpdate();
        }

        @Override
        public String toString() {
            String reindexMarker = reindex ? "*" : "";
            return indexPath + reindexMarker + "(" + updateCount + ")";
        }
    }
}
