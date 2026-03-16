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
 * <p>Uses {@link ContextAwareCallback} (when available) to obtain the correct
 * index path and to detect reindex cycles so the existing index data can be
 * cleared before a full rebuild.</p>
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

        // Extract context if available (IndexUpdate always provides ContextAwareCallback)
        String indexPath = "/";
        boolean reindex = false;
        if (callback instanceof ContextAwareCallback) {
            IndexingContext ctx = ((ContextAwareCallback) callback).getIndexingContext();
            indexPath = ctx.getIndexPath(); // index definition path, used for logging
            reindex = ctx.isReindexing();
        }

        LOG.debug("Creating Lucene 9 index editor for index at {}{}", indexPath,
                reindex ? " (reindex)" : "");

        try {
            // Content traversal always starts from repository root "/"
            return new LuceneNgIndexEditor("/", definition, root, reindex);
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
