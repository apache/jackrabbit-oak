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

import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgIndexNode;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgIndexNodeManager;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexTracker;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Tracks Lucene 9 ({@code type=lucene9}) indexes for the query engine, via the shared
 * {@link FulltextIndexTracker} (lazy per-path discovery + targeted subtree diffing — see
 * that class for the discovery/refresh contract this inherits).
 */
public class LuceneNgIndexTracker extends FulltextIndexTracker<LuceneNgIndexNodeManager, LuceneNgIndexNode> {

    @Override
    protected LuceneNgIndexNodeManager openIndex(String path, NodeState root, NodeState node) {
        LuceneNgIndexNode indexNode = new LuceneNgIndexNode(path, root, node);
        if (!indexNode.hasSearcher()) {
            return null;
        }
        return new LuceneNgIndexNodeManager(path, indexNode);
    }

    /**
     * Overridden because {@link FulltextIndexTracker}'s default checks only the
     * {@code :status} and {@code :index-definition} hidden nodes for changes — neither of
     * which {@link LuceneNgIndexEditor} ever writes (this module has no NRT/status-marker
     * story yet; see module README). The Lucene segment files instead live directly under
     * the index definition node itself ({@link LuceneNgIndexStorage#STORAGE_NODE_NAME}), so
     * a plain whole-subtree comparison is what actually detects both definition and content
     * (storage) changes here.
     */
    @Override
    public boolean isUpdateNeeded(NodeState before, NodeState after) {
        return !EqualsDiff.equals(before, after);
    }

    @Nullable
    public LuceneNgIndexNode acquireIndexNode(@NotNull String indexPath) {
        return super.acquireIndexNode(indexPath, LuceneNgIndexConstants.TYPE_LUCENE9);
    }
}
