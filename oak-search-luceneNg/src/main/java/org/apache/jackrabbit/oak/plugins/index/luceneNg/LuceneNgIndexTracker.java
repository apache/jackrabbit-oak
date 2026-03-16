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

import org.apache.jackrabbit.oak.plugins.index.IndexDefinitionHelper;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Tracks Lucene 9 indexes and provides access to index nodes.
 *
 * <p>Updated on every repository commit via the {@code Observer} mechanism.
 * The internal index map is replaced atomically on each update, so readers
 * always see a consistent snapshot without locking.</p>
 */
public class LuceneNgIndexTracker {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgIndexTracker.class);

    /**
     * Atomic snapshot: immutable map replaced on every {@link #update}.
     * Reads require no synchronization; writes are serialized via {@code synchronized}.
     */
    private volatile Map<String, LuceneNgIndexNode> indices = Collections.emptyMap();

    private NodeState root;

    /**
     * Updates the tracker with new repository state.
     * Scans /oak:index for indexes whose activeTarget is lucene9.
     * Entries whose activeTarget has changed or whose definition was removed are
     * evicted automatically.
     *
     * @param root the new root state
     */
    public synchronized void update(@NotNull NodeState root) {
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
     * Returns paths of all currently tracked indexes.
     */
    public Set<String> getIndexPaths() {
        return indices.keySet();
    }

    /**
     * Full scan of /oak:index. Builds a fresh map of all indexes whose
     * activeTarget (or legacy type) is lucene9, then atomically replaces
     * the current map. Entries removed from the definition or whose activeTarget
     * has changed away from lucene9 are automatically evicted.
     */
    private void refreshIndexes() {
        if (root == null) {
            return;
        }

        NodeState oakIndex = root.getChildNode("oak:index");
        if (!oakIndex.exists()) {
            indices = Collections.emptyMap();
            return;
        }

        Map<String, LuceneNgIndexNode> oldIndices = indices;
        Map<String, LuceneNgIndexNode> newIndices = new HashMap<>();

        for (String indexName : oakIndex.getChildNodeNames()) {
            String indexPath = "/oak:index/" + indexName;
            NodeState indexState = oakIndex.getChildNode(indexName);

            try {
                String activeTarget = IndexDefinitionHelper.getActiveTarget(indexState);
                if (LuceneNgIndexConstants.TYPE_LUCENE9.equals(activeTarget)) {
                    newIndices.put(indexPath, new LuceneNgIndexNode(indexPath, root, indexState));
                    if (!oldIndices.containsKey(indexPath)) {
                        LOG.debug("Now tracking Lucene 9 index: {}", indexPath);
                    }
                }
            } catch (IllegalArgumentException e) {
                // Not a valid index definition (no type/activeTarget), skip
            }
        }

        // Log removals
        for (String removed : oldIndices.keySet()) {
            if (!newIndices.containsKey(removed)) {
                LOG.debug("Stopped tracking Lucene 9 index (removed or activeTarget changed): {}", removed);
            }
        }

        this.indices = Collections.unmodifiableMap(newIndices);
    }
}
