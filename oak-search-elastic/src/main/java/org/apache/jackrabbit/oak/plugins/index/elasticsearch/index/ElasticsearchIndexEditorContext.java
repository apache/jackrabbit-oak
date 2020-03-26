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

import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.DocumentMaker;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditorContext;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public class ElasticsearchIndexEditorContext extends FulltextIndexEditorContext<ElasticsearchDocument> {
    ElasticsearchIndexEditorContext(NodeState root,
                                    NodeBuilder definition, @Nullable IndexDefinition indexDefinition,
                                    IndexUpdateCallback updateCallback,
                                    ElasticsearchIndexWriterFactory indexWriterFactory,
                                    ExtractedTextCache extractedTextCache,
                                    IndexingContext indexingContext,
                                    boolean asyncIndexing) {
        super(root, definition, indexDefinition, updateCallback, indexWriterFactory, extractedTextCache, indexingContext, asyncIndexing);
    }

    @Override
    public IndexDefinition.Builder newDefinitionBuilder() {
        return new IndexDefinition.Builder();
    }

    @Override
    public DocumentMaker<ElasticsearchDocument> newDocumentMaker(IndexDefinition.IndexingRule rule, String path) {
        return new ElasticsearchDocumentMaker(getTextExtractor(), getDefinition(), rule, path);
    }

    @Override
    public void enableReindexMode() {
        super.enableReindexMode();

        // Now, that index definition _might_ have been migrated by super call, it would be ok to
        // get writer and provision index settings and mappings
        try {
            getWriter().provisionIndex();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to provision index", e);
        }
    }

    @Override
    public ElasticsearchIndexWriter getWriter() {
        return (ElasticsearchIndexWriter) super.getWriter();
    }
}