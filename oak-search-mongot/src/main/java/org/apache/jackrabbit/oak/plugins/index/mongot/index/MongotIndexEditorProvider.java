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
package org.apache.jackrabbit.oak.plugins.index.mongot.index;

import org.apache.jackrabbit.oak.plugins.index.ContextAwareCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class MongotIndexEditorProvider implements IndexEditorProvider {

    private final MongoConnection connection;
    private final ExtractedTextCache extractedTextCache;

    public MongotIndexEditorProvider(@NotNull MongoConnection connection,
                                    @Nullable ExtractedTextCache extractedTextCache) {
        this.connection = connection;
        this.extractedTextCache = extractedTextCache == null
                ? new ExtractedTextCache(0, 0)
                : extractedTextCache;
    }

    @Override
    public @Nullable Editor getIndexEditor(@NotNull String type,
                                           @NotNull NodeBuilder definition,
                                           @NotNull NodeState root,
                                           @NotNull IndexUpdateCallback callback) {
        if (!MongotIndexDefinition.TYPE_MONGOT.equals(type)) {
            return null;
        }
        if (!(callback instanceof ContextAwareCallback)) {
            throw new IllegalStateException("callback must implement ContextAwareCallback");
        }

        IndexingContext indexingContext = ((ContextAwareCallback) callback).getIndexingContext();
        MongotIndexDefinition indexDefinition = new MongotIndexDefinition(
                root, definition.getNodeState(), indexingContext.getIndexPath());
        MongotIndexEditorContext context = new MongotIndexEditorContext(
                root,
                definition,
                indexDefinition,
                callback,
                new MongotIndexWriterFactory(connection),
                extractedTextCache,
                indexingContext);
        return new FulltextIndexEditor<>(context);
    }
}
