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
import org.apache.jackrabbit.oak.plugins.index.CatchUpCapable;
import org.apache.jackrabbit.oak.plugins.index.ContextAwareCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexDefinitionHelper;
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
 */
public class LuceneNgIndexEditorProvider implements IndexEditorProvider, CatchUpCapable {
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

        if (!IndexDefinitionHelper.shouldWrite(definition.getNodeState(), LuceneNgIndexConstants.TYPE_LUCENE9)) {
            return null;
        }

        // Block normal indexing while catch-up is in progress for this target.
        // Catch-up calls pass type=lucene9 explicitly and are allowed through.
        NodeBuilder trackingNode = definition.getChildNode(CatchUpCapable.CATCH_UP_TRACKING_NODE);
        if (trackingNode.exists() && trackingNode.hasProperty(LuceneNgIndexConstants.TYPE_LUCENE9)) {
            if (!LuceneNgIndexConstants.TYPE_LUCENE9.equals(type)) {
                return null;
            }
        }

        if (!(callback instanceof ContextAwareCallback)) {
            throw new IllegalStateException("callback instance not of type ContextAwareCallback [" + callback + "]");
        }
        IndexingContext indexingContext = ((ContextAwareCallback) callback).getIndexingContext();
        String indexPath = indexingContext.getIndexPath();
        boolean reindex = indexingContext.isReindexing();

        try {
            NodeBuilder storage = LuceneNgIndexStorage.getOrCreateStorageBuilder(definition);
            return new LuceneNgIndexEditor("/", indexPath, storage, definition, root, reindex, callback);
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
