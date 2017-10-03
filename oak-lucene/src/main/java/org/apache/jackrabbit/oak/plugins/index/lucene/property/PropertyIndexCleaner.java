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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROPERTY_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_STORAGE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.STORAGE_TYPE_CONTENT_MIRROR;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.STORAGE_TYPE_UNIQUE;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;

public class PropertyIndexCleaner {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final NodeStore nodeStore;
    private final IndexPathService indexPathService;
    private final AsyncIndexInfoService asyncIndexInfoService;
    private UniqueIndexCleaner uniqueIndexCleaner = new UniqueIndexCleaner(TimeUnit.HOURS, 1);

    public PropertyIndexCleaner(NodeStore nodeStore, IndexPathService indexPathService,
                                AsyncIndexInfoService asyncIndexInfoService) {
        this.nodeStore = nodeStore;
        this.indexPathService = indexPathService;
        this.asyncIndexInfoService = asyncIndexInfoService;
    }

    public void run() throws CommitFailedException {
        Map<String, Long> asyncInfo = getAsyncInfo();
        List<String> syncIndexes = getSyncIndexPaths();
        IndexInfo indexInfo = switchBucketsAndCollectIndexData(syncIndexes, asyncInfo);

        purgeOldBuckets(indexInfo.oldBucketPaths);
        purgeOldUniqueIndexEntries(indexInfo.uniqueIndexPaths);
    }

    /**
     * Specifies the threshold for created time such that only those entries
     * in unique indexes are purged which have
     *
     *     async indexer time - creation time > threshold
     *
     * @param unit time unit
     * @param time time value in given unit
     */
    public void setCreatedTimeThreshold(TimeUnit unit, long time) {
        uniqueIndexCleaner = new UniqueIndexCleaner(unit, time);
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
                                                       Map<String, Long> asyncInfo)
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

    private void purgeOldBuckets(List<String> bucketPaths) throws CommitFailedException {
        if (bucketPaths.isEmpty()) {
            return;
        }

        NodeState root = nodeStore.getRoot();
        NodeBuilder builder = root.builder();

        for (String path : bucketPaths) {
            NodeBuilder bucket = child(builder, path);
            //TODO Recursive delete to avoid large transaction
            bucket.remove();
        }

        merge(builder);
    }

    private void purgeOldUniqueIndexEntries(Map<String, Long> asyncInfo) throws CommitFailedException {
        NodeState root = nodeStore.getRoot();
        NodeBuilder builder = root.builder();

        boolean modified = false;
        for (Map.Entry<String, Long> e : asyncInfo.entrySet()) {
            NodeBuilder idxb = child(builder, e.getKey());
            modified |= uniqueIndexCleaner.clean(idxb, e.getValue());
        }

        if (modified) {
            merge(builder);
        }
    }

    private void merge(NodeBuilder builder) throws CommitFailedException {
        //TODO Configure conflict hooks
        //TODO Configure validator
        //Configure CommitContext
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private Map<String, Long> getAsyncInfo() {
        Map<String, Long> infos = new HashMap<>();
        for (String asyncLane : asyncIndexInfoService.getAsyncLanes()) {
            AsyncIndexInfo info = asyncIndexInfoService.getInfo(asyncLane);
            if (info != null) {
                infos.put(asyncLane, info.getLastIndexedTo());
            } else {
                log.warn("No AsyncIndexInfo found for lane name [{}]", asyncLane);
            }
        }
        return infos;
    }

    private static NodeBuilder child(NodeBuilder nb, String path) {
        for (String name : PathUtils.elements(checkNotNull(path))) {
            //Use getChildNode to avoid creating new entries by default
            nb = nb.getChildNode(name);
        }
        return nb;
    }

    private static boolean simplePropertyIndex(NodeState propIdxState) {
        return STORAGE_TYPE_CONTENT_MIRROR.equals(propIdxState.getString(PROP_STORAGE_TYPE));
    }

    private static boolean uniquePropertyIndex(NodeState propIdxState) {
        return STORAGE_TYPE_UNIQUE.equals(propIdxState.getString(PROP_STORAGE_TYPE));
    }

    private static final class IndexInfo {
        final List<String> oldBucketPaths = new ArrayList<>();

        /* indexPath, lastIndexedTo */
        final Map<String, Long> uniqueIndexPaths = new HashMap<>();
    }
}
