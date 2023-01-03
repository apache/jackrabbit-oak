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
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.ContextAwareCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditor;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditorContext;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition.TYPE_ELASTICSEARCH;

public class ElasticIndexEditorProvider implements IndexEditorProvider {

    private final ElasticIndexTracker indexTracker;
    private final ElasticConnection elasticConnection;
    private final ExtractedTextCache extractedTextCache;

    public final static String OAK_INDEX_ELASTIC_WRITER_DISABLE_KEY = "oak.index.elastic.writer.disable";

    private final boolean OAK_INDEX_ELASTIC_WRITER_DISABLE = Boolean.getBoolean(OAK_INDEX_ELASTIC_WRITER_DISABLE_KEY);

    public ElasticIndexEditorProvider(@NotNull ElasticIndexTracker indexTracker,
                                      @NotNull ElasticConnection elasticConnection,
                                      ExtractedTextCache extractedTextCache) {
        this.indexTracker = indexTracker;
        this.elasticConnection = elasticConnection;
        this.extractedTextCache = extractedTextCache != null ? extractedTextCache : new ExtractedTextCache(0, 0);
    }

    @Override
    public @Nullable Editor getIndexEditor(@NotNull String type,
                                           @NotNull NodeBuilder definition, @NotNull NodeState root,
                                           @NotNull IndexUpdateCallback callback) {
        if (TYPE_ELASTICSEARCH.equals(type)) {
            if (!(callback instanceof ContextAwareCallback)) {
                throw new IllegalStateException("callback instance not of type ContextAwareCallback [" + callback + "]");
            }
            IndexingContext indexingContext = ((ContextAwareCallback) callback).getIndexingContext();

            String indexPath = indexingContext.getIndexPath();
            ElasticIndexDefinition indexDefinition =
                    new ElasticIndexDefinition(root, definition.getNodeState(), indexPath, elasticConnection.getIndexPrefix());

            ElasticIndexWriterFactory writerFactory = new ElasticIndexWriterFactory(elasticConnection, indexTracker);

            ElasticIndexEditorContext context = new ElasticIndexEditorContext(root,
                    definition, indexDefinition,
                    callback,
                    writerFactory,
                    extractedTextCache,
                    indexingContext,
                    true);
            if (OAK_INDEX_ELASTIC_WRITER_DISABLE) {
                return new NOOPIndexEditor<>(context);
            } else {
                return new ElasticIndexEditor(context);
            }
        }
        return null;
    }

    public ExtractedTextCache getExtractedTextCache() {
        return extractedTextCache;
    }

    /**
     * This is a no-op editor, so that elastic index is not updated
     * where OAK_INDEX_ELASTIC_WRITER_DISABLE = true is set as system property.
     */
    private class NOOPIndexEditor<D> extends FulltextIndexEditor<D> {
        public NOOPIndexEditor(FulltextIndexEditorContext<D> context) {
            super(context);
        }

        @Override
        public void enter(NodeState before, NodeState after) {
        }

        @Override
        public void leave(NodeState before, NodeState after) {
        }

        @Override
        public String getPath() {
            return ROOT_PATH;
        }

        @Override
        public void propertyAdded(PropertyState after) {
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
        }

        @Override
        public void propertyDeleted(PropertyState before) {
        }

        @Override
        public Editor childNodeAdded(String name, NodeState after) {
            return this;
        }

        @Override
        public Editor childNodeChanged(String name, NodeState before, NodeState after) {
            return this;
        }

        @Override
        public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
            return this;
        }

        @Override
        public FulltextIndexEditorContext<D> getContext() {
            return super.getContext();
        }

        @Override
        public void markDirty() {
        }
    }
}
