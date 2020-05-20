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

package org.apache.jackrabbit.oak.plugins.index.lucene.property;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.ResetCommitAttributeHook;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROPERTY_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.simplePropertyIndex;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.uniquePropertyIndex;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;

public class PropertyIndexCleaner implements Runnable{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final NodeStore nodeStore;
    private final IndexPathService indexPathService;
    private final AsyncIndexInfoService asyncIndexInfoService;
    private UniqueIndexCleaner uniqueIndexCleaner = new UniqueIndexCleaner(TimeUnit.HOURS, 1);
    private Map<String, Long> lastAsyncInfo = Collections.emptyMap();

    private final TimerStats cleanupTime;
    private final MeterStats noopMeter;
    private boolean recursiveDelete;

    public PropertyIndexCleaner(NodeStore nodeStore, IndexPathService indexPathService,
                                AsyncIndexInfoService asyncIndexInfoService,
                                StatisticsProvider statsProvider) {
        this.nodeStore = checkNotNull(nodeStore);
        this.indexPathService = checkNotNull(indexPathService);
        this.asyncIndexInfoService = checkNotNull(asyncIndexInfoService);

        this.cleanupTime = statsProvider.getTimer("HYBRID_PROPERTY_CLEANER", StatsOptions.METRICS_ONLY);
        this.noopMeter = statsProvider.getMeter("HYBRID_PROPERTY_NOOP", StatsOptions.METRICS_ONLY);
    }

    @Override
    public void run() {
        try{
            performCleanup(false);
        } catch (Exception e) {
            log.warn("Cleanup run failed with error", e);
        }
    }

    /**
     * Performs the cleanup run
     *
     * @param forceCleanup if true then clean up would attempted even if no change
     *                     is found in async indexer state
     */
    public CleanupStats performCleanup(boolean forceCleanup) throws CommitFailedException {
        CleanupStats stats = new CleanupStats();
        Stopwatch w = Stopwatch.createStarted();
        Map<String, Long> asyncInfo = asyncIndexInfoService.getIndexedUptoPerLane();
        if (lastAsyncInfo.equals(asyncInfo) && !forceCleanup) {
            log.debug("No change found in async state from last run {}. Skipping the run", asyncInfo);
            noopMeter.mark();
            return stats;
        }

        stats.cleanupPerformed = true;
        List<String> syncIndexes = getSyncIndexPaths();
        IndexInfo indexInfo = switchBucketsAndCollectIndexData(syncIndexes, asyncInfo, stats);

        purgeOldBuckets(indexInfo.oldBucketPaths, stats);
        purgeOldUniqueIndexEntries(indexInfo.uniqueIndexPaths, stats);
        lastAsyncInfo = asyncInfo;

        if (w.elapsed(TimeUnit.MINUTES) > 5) {
            log.info("Property index cleanup done in {}. {}", w, stats);
        } else {
            log.debug("Property index cleanup done in {}. {}", w, stats);
        }

        cleanupTime.update(w.elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);

        return stats;
    }

    /**
     * Specifies the threshold for created time such that only those entries
     * in unique indexes are purged which have
     *
     *     async indexer time - creation time &gt; threshold
     *
     * @param unit time unit
     * @param time time value in given unit
     */
    public void setCreatedTimeThreshold(TimeUnit unit, long time) {
        uniqueIndexCleaner = new UniqueIndexCleaner(unit, time);
    }


    public boolean isRecursiveDelete() {
        return recursiveDelete;
    }

    public void setRecursiveDelete(boolean recursiveDelete) {
        this.recursiveDelete = recursiveDelete;
    }

    List<String> getSyncIndexPaths() {
        List<String> indexPaths = new ArrayList<>();
        NodeState root = nodeStore.getRoot();
        for (String indexPath : indexPathService.getIndexPaths()) {
            NodeState idx = getNode(root, indexPath);
            if (TYPE_LUCENE.equals(idx.getString(TYPE_PROPERTY_NAME))
                    && idx.hasChildNode(PROPERTY_INDEX)) {
                indexPaths.add(indexPath);
            }
        }
        return indexPaths;
    }

    private IndexInfo switchBucketsAndCollectIndexData(List<String> indexPaths,
                                                       Map<String, Long> asyncInfo, CleanupStats stats)
            throws CommitFailedException {
        IndexInfo indexInfo = new IndexInfo();
        NodeState root = nodeStore.getRoot();
        NodeBuilder builder = root.builder();

        boolean modified = false;
        for (String indexPath : indexPaths) {
            NodeState idx = getNode(root, indexPath);
            NodeBuilder idxb = child(builder, indexPath);
            String laneName = IndexUtils.getAsyncLaneName(idx, indexPath);
            Long lastIndexedTo = asyncInfo.get(laneName);

            if (lastIndexedTo == null) {
                log.warn("Not able to determine async index info for lane {}. " +
                        "Known lanes {}", laneName, asyncInfo.keySet());
                continue;
            }

            NodeState propertyIndexNode = idx.getChildNode(PROPERTY_INDEX);
            NodeBuilder propIndexNodeBuilder = idxb.getChildNode(PROPERTY_INDEX);

            for (ChildNodeEntry cne : propertyIndexNode.getChildNodeEntries()) {
                NodeState propIdxState = cne.getNodeState();
                String propName = cne.getName();
                if (simplePropertyIndex(propIdxState)) {

                    NodeBuilder propIdx = propIndexNodeBuilder.getChildNode(propName);
                    BucketSwitcher bs = new BucketSwitcher(propIdx);

                    modified |= bs.switchBucket(lastIndexedTo);

                    for (String bucketName : bs.getOldBuckets()) {
                        String bucketPath = PathUtils.concat(indexPath, PROPERTY_INDEX, propName, bucketName);
                        indexInfo.oldBucketPaths.add(bucketPath);
                        stats.purgedIndexPaths.add(indexPath);
                    }
                } else if (uniquePropertyIndex(propIdxState)) {
                    String indexNodePath = PathUtils.concat(indexPath, PROPERTY_INDEX, propName);
                    indexInfo.uniqueIndexPaths.put(indexNodePath, lastIndexedTo);
                }
            }
        }

        if (modified) {
            merge(builder);

        }
        return indexInfo;
    }

    private void purgeOldBuckets(List<String> bucketPaths, CleanupStats stats) throws CommitFailedException {
        if (bucketPaths.isEmpty()) {
            return;
        }

        if (recursiveDelete) {
            RecursiveDelete rd = new RecursiveDelete(nodeStore, createCommitHook(),
                    PropertyIndexCleaner::createCommitInfo);
            rd.run(bucketPaths);
            stats.numOfNodesDeleted += rd.getNumRemoved();
        } else {
            NodeState root = nodeStore.getRoot();
            NodeBuilder builder = root.builder();

            for (String path : bucketPaths) {
                NodeBuilder bucket = child(builder, path);
                bucket.remove();
            }

            merge(builder);
        }
        stats.purgedBucketCount = bucketPaths.size();
    }

    private void purgeOldUniqueIndexEntries(Map<String, Long> asyncInfo, CleanupStats stats) throws CommitFailedException {
        NodeState root = nodeStore.getRoot();
        NodeBuilder builder = root.builder();

        for (Map.Entry<String, Long> e : asyncInfo.entrySet()) {
            String indexNodePath = e.getKey();
            NodeBuilder idxb = child(builder, indexNodePath);
            int removalCount = uniqueIndexCleaner.clean(idxb, e.getValue());
            if (removalCount > 0) {
                stats.purgedIndexPaths.add(PathUtils.getAncestorPath(indexNodePath, 2));
                log.debug("Removed [{}] entries from [{}]", removalCount, indexNodePath);
            }
            stats.uniqueIndexEntryRemovalCount += removalCount;
        }

        if (stats.uniqueIndexEntryRemovalCount > 0) {
            merge(builder);
        }
    }

    private void merge(NodeBuilder builder) throws CommitFailedException {
        //TODO Configure validator
        CompositeHook hooks = createCommitHook();
        nodeStore.merge(builder, hooks, createCommitInfo());
    }

    private CompositeHook createCommitHook() {
        return new CompositeHook(
                    ResetCommitAttributeHook.INSTANCE,
                    new ConflictHook(new AnnotatingConflictHandler()),
                    new EditorHook(CompositeEditorProvider.compose(singletonList(new ConflictValidatorProvider())))
            );
    }

    private static CommitInfo createCommitInfo() {
        Map<String, Object> info = ImmutableMap.of(CommitContext.NAME, new SimpleCommitContext());
        return new CommitInfo(CommitInfo.OAK_UNKNOWN, CommitInfo.OAK_UNKNOWN, info);
    }

    private static NodeBuilder child(NodeBuilder nb, String path) {
        for (String name : PathUtils.elements(checkNotNull(path))) {
            //Use getChildNode to avoid creating new entries by default
            nb = nb.getChildNode(name);
        }
        return nb;
    }

    private static final class IndexInfo {
        final List<String> oldBucketPaths = new ArrayList<>();

        /* indexPath, lastIndexedTo */
        final Map<String, Long> uniqueIndexPaths = new HashMap<>();
    }

    public static class CleanupStats {
        public int uniqueIndexEntryRemovalCount;
        public int purgedBucketCount;
        public Set<String> purgedIndexPaths = new HashSet<>();
        public boolean cleanupPerformed;
        public int numOfNodesDeleted;

        @Override
        public String toString() {
            String nodeCountMsg = numOfNodesDeleted > 0 ? String.format("(%d nodes)", numOfNodesDeleted) : "";
            return String.format("Removed %d index buckets %s, %d unique index entries " +
                    "from indexes %s", purgedBucketCount, nodeCountMsg, uniqueIndexEntryRemovalCount, purgedIndexPaths);
        }
    }
}
