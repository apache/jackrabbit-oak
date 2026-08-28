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
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Tracks Lucene 9 ({@code type=lucene9}) indexes for the query engine, via the shared
 * {@link FulltextIndexTracker} (lazy per-path discovery + targeted subtree diffing — see
 * that class for the discovery/refresh contract this inherits). {@code isUpdateNeeded} is not
 * overridden: the inherited default (which checks only the {@code :status} and
 * {@code :index-definition} hidden nodes) suffices here, for two independent reasons covering
 * the two ways content changes reach this index:
 * <ul>
 *     <li>Incremental (non-reindex) updates: {@link LuceneNgIndexEditor} (via the shared
 *     {@code FulltextIndexEditorContext.closeWriter()}) writes {@code :status/lastUpdated}
 *     whenever {@code LuceneNgFulltextIndexWriter.close()} reports that a write actually
 *     happened.</li>
 *     <li>Reindex (including a reindex that ends up matching zero documents): {@code oak-core}'s
 *     {@code IndexUpdate.removeIndexState()} unconditionally strips all hidden child nodes
 *     (including {@code :status} and {@code :index-definition}) before every reindex, regardless
 *     of this module's own dirty-tracking — so the default's {@code isIndexDefinitionChanged}/
 *     {@code isStatusChanged} checks always see a diff on reindex, even one that indexes nothing.</li>
 * </ul>
 * See module README, "Performance", for the full dependency this relies on.
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

    @Nullable
    public LuceneNgIndexNode acquireIndexNode(@NotNull String indexPath) {
        return super.acquireIndexNode(indexPath, LuceneNgIndexConstants.TYPE_LUCENE9);
    }
}
