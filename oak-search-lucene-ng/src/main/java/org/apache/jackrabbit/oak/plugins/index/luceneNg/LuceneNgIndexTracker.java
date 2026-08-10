/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tracks Lucene 9 indexes and provides access to index nodes.
 * Scans the repository for lucene9 type indexes and maintains a cache.
 */
public class LuceneNgIndexTracker {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgIndexTracker.class);

    private final ConcurrentMap<String, LuceneNgIndexNode> indices = new ConcurrentHashMap<>();
    private NodeState root;

    /**
     * Updates the tracker with new repository state.
     * Scans /oak:index for lucene9 indexes and updates the cache.
     *
     * @param root the new root state
     */
    public void update(@NotNull NodeState root) {
        this.root = root;
        refreshIndexes();
    }

    /**
     * Acquires an index node for the given path. The caller MUST call
     * {@link LuceneNgIndexNode.AcquiredNode#release()} when done.
     *
     * @param indexPath the path to the index (e.g., "/oak:index/myIndex")
     * @return an acquired node, or null if not found or not yet populated
     */
    @Nullable
    public LuceneNgIndexNode.AcquiredNode acquireIndexNode(@NotNull String indexPath) {
        LuceneNgIndexNode node = indices.get(indexPath);
        return node != null ? node.acquire() : null;
    }

    /**
     * Get paths of all tracked indexes.
     *
     * @return set of index paths
     */
    public Set<String> getIndexPaths() {
        return new HashSet<>(indices.keySet());
    }

    /**
     * Closes all tracked index nodes and releases their resources.
     * Must be called on OSGi deactivation to prevent file descriptor leaks.
     */
    public void close() {
        for (LuceneNgIndexNode node : indices.values()) {
            node.close();
        }
        indices.clear();
        LOG.debug("LuceneNgIndexTracker closed");
    }

    /**
     * Refreshes the index cache by scanning for Lucene 9 indexes.
     */
    private void refreshIndexes() {
        if (root == null) {
            return;
        }

        // Scan /oak:index for lucene9 indexes
        NodeState oakIndex = root.getChildNode("oak:index");
        if (!oakIndex.exists()) {
            return;
        }

        Set<String> seen = new HashSet<>();

        for (String indexName : oakIndex.getChildNodeNames()) {
            String indexPath = "/oak:index/" + indexName;
            NodeState indexState = oakIndex.getChildNode(indexName);

            // Check if it's a lucene9 index
            org.apache.jackrabbit.oak.api.PropertyState typeProp = indexState.getProperty("type");
            if (typeProp != null) {
                String type = typeProp.getValue(org.apache.jackrabbit.oak.api.Type.STRING);
                if (LuceneNgIndexConstants.TYPE_LUCENE9.equals(type)) {
                    seen.add(indexPath);
                    LuceneNgIndexNode existing = indices.get(indexPath);
                    if (existing == null) {
                        LOG.debug("Tracking new Lucene 9 index: {}", indexPath);
                        indices.put(indexPath, new LuceneNgIndexNode(indexPath, root, indexState));
                    } else {
                        NodeState currentStorage = LuceneNgIndexStorage.storageState(indexState);
                        boolean definitionChanged = !existing.getIndexState().equals(indexState);
                        boolean storageChanged = !existing.getStorageState().equals(currentStorage);
                        if (definitionChanged || storageChanged) {
                            LOG.debug("Refreshing Lucene 9 index node due to {}{}: {}",
                                    definitionChanged ? "definition change" : "",
                                    storageChanged ? (definitionChanged ? " and storage change" : "storage change") : "",
                                    indexPath);
                            existing.close();
                            indices.put(indexPath, new LuceneNgIndexNode(indexPath, root, indexState));
                        }
                    }
                }
            }
        }

        // Remove entries that are no longer lucene9 indexes.
        Set<String> tracked = new HashSet<>(indices.keySet());
        for (String trackedPath : tracked) {
            if (!seen.contains(trackedPath)) {
                LuceneNgIndexNode removed = indices.remove(trackedPath);
                if (removed != null) {
                    removed.close();
                    LOG.debug("Stopped tracking Lucene 9 index: {}", trackedPath);
                }
            }
        }
    }
}
