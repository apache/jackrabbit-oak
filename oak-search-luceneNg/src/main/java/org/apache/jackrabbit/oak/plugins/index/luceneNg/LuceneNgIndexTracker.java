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
     * Acquires an index node for the given path.
     *
     * @param indexPath the path to the index (e.g., "/oak:index/myIndex")
     * @return the index node, or null if not found
     */
    @Nullable
    public LuceneNgIndexNode acquireIndexNode(@NotNull String indexPath) {
        return indices.get(indexPath);
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

        for (String indexName : oakIndex.getChildNodeNames()) {
            String indexPath = "/oak:index/" + indexName;
            NodeState indexState = oakIndex.getChildNode(indexName);

            // Check if it's a lucene9 index
            org.apache.jackrabbit.oak.api.PropertyState typeProp = indexState.getProperty("type");
            if (typeProp != null) {
                String type = typeProp.getValue(org.apache.jackrabbit.oak.api.Type.STRING);
                if (LuceneNgIndexConstants.TYPE_LUCENE9.equals(type)) {
                    // Create or update index node
                    indices.computeIfAbsent(indexPath, path -> {
                        LOG.debug("Tracking new Lucene 9 index: {}", path);
                        return new LuceneNgIndexNode(path, root, indexState);
                    });
                }
            }
        }
    }
}
