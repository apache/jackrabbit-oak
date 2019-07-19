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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.index;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.ContextAwareCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexCoordinateFactory;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexConstants.TYPE_ELASTICSEARCH;

public class ElasticsearchIndexEditorProvider implements IndexEditorProvider {

    private final ElasticsearchIndexCoordinateFactory esIndexCoordFactory;
    private final ExtractedTextCache extractedTextCache;

    public ElasticsearchIndexEditorProvider(@NotNull ElasticsearchIndexCoordinateFactory esIndexCoordFactory,
                                            ExtractedTextCache extractedTextCache) {
        this.esIndexCoordFactory = esIndexCoordFactory;
        this.extractedTextCache = extractedTextCache != null ? extractedTextCache : new ExtractedTextCache(0, 0);
    }

    @Override
    public @Nullable Editor getIndexEditor(@NotNull String type,
                                           @NotNull NodeBuilder definition, @NotNull NodeState root,
                                           @NotNull IndexUpdateCallback callback) throws CommitFailedException {
        if (TYPE_ELASTICSEARCH.equals(type)) {
            checkArgument(callback instanceof ContextAwareCallback, "callback instance not of type " +
                    "ContextAwareCallback [%s]", callback);
            IndexingContext indexingContext = ((ContextAwareCallback)callback).getIndexingContext();

            String indexPath = indexingContext.getIndexPath();
            ElasticsearchIndexDefinition indexDefinition =
                    new ElasticsearchIndexDefinition(root, definition.getNodeState(), indexPath);

            ElasticsearchIndexWriterFactory writerFactory = new ElasticsearchIndexWriterFactory(esIndexCoordFactory);

            ElasticsearchIndexEditorContext context = new ElasticsearchIndexEditorContext(root,
                    definition, indexDefinition,
                    callback,
                    writerFactory,
                    extractedTextCache,
                    indexingContext,
                    true);

            return new ElasticsearchIndexEditor(context);
        }
        return null;
    }

    public ExtractedTextCache getExtractedTextCache() {
        return extractedTextCache;
    }
}
