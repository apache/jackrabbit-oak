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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.ContextAwareCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IndexEditorProvider for Lucene 9 indexes.
 * Handles write operations for {@code type=lucene9} index definitions.
 *
 * <p>Index data is written to {@code /var/indexing/lucene/<indexName>/} in the
 * repository, keeping it separate from the index definition subtree. This allows
 * both lucene47 and lucene9 editors to write to the same index definition without
 * overwriting each other's data.</p>
 */
public class LuceneNgIndexEditorProvider implements IndexEditorProvider {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgIndexEditorProvider.class);

    private final LuceneNgIndexTracker indexTracker;

    public LuceneNgIndexEditorProvider(@NotNull LuceneNgIndexTracker indexTracker) {
        this.indexTracker = indexTracker;
    }

    @Override
    @Nullable
    public Editor getIndexEditor(@NotNull String type,
                                 @NotNull NodeBuilder definition,
                                 @NotNull NodeState root,
                                 @NotNull IndexUpdateCallback callback)
            throws CommitFailedException {

        if (!LuceneNgIndexConstants.TYPE_LUCENE9.equals(type)) {
            return null;
        }

        String indexPath = "/oak:index/unknown";
        boolean reindex = false;
        NodeBuilder rootBuilder = null;

        if (callback instanceof ContextAwareCallback) {
            ContextAwareCallback ctx = (ContextAwareCallback) callback;
            IndexingContext indexingContext = ctx.getIndexingContext();
            indexPath = indexingContext.getIndexPath();
            reindex = indexingContext.isReindexing();
            rootBuilder = ctx.getRootBuilder();
        }

        if (rootBuilder == null) {
            LOG.warn("No root builder available for lucene9 index at {} — cannot write to /var/indexing/lucene", indexPath);
            return null;
        }

        String indexName = PathUtils.getName(indexPath);
        NodeBuilder storageBuilder = rootBuilder
                .child("var")
                .child("indexing")
                .child("lucene")
                .child(indexName);

        LOG.debug("Creating Lucene 9 index editor for {} (storage: /var/indexing/lucene/{}{})",
                indexPath, indexName, reindex ? ", reindex" : "");

        try {
            return new LuceneNgIndexEditor("/", indexPath, storageBuilder, definition, root, reindex);
        } catch (Exception e) {
            throw new CommitFailedException("Lucene9", 1,
                    "Failed to create LuceneNgIndexEditor for " + indexPath, e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
