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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.editor;

import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.DocumentMaker;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditorContext;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetsConfig;
import org.jetbrains.annotations.Nullable;

/**
 * Lucene 9 {@link FulltextIndexEditorContext}. Supplies the shared
 * {@link org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditor} framework
 * with this module's definition builder ({@link LuceneNgIndexDefinition.Builder}), index writer
 * factory ({@link LuceneNgFulltextIndexWriterFactory}) and document maker
 * ({@link LuceneNgDocumentMaker}). Mirrors {@code oak-lucene}'s {@code LuceneIndexEditorContext}.
 */
public class LuceneNgIndexEditorContext extends FulltextIndexEditorContext<Document> {

    /**
     * Built once (lazily) from the resolved {@link IndexDefinition} and reused for every
     * {@link #newDocumentMaker} call. {@code newDocumentMaker} is invoked once per indexed node
     * (see {@code FulltextIndexEditor.addOrUpdate}); caching avoids rebuilding this on every node.
     * The definition is stable for the context's lifetime by the time indexing begins (any
     * reindex-mode swap happens in the root editor's {@code enter()}, before the first document
     * is made), so a single build is safe.
     */
    private FacetsConfig facetsConfig;

    /**
     * @param root             the repository root node state
     * @param definition       the index definition {@link NodeBuilder}
     * @param indexDefinition  a pre-built definition, or {@code null} to have the base class build
     *                         one via {@link #newDefinitionBuilder()}
     * @param updateCallback   the index update callback
     * @param indexingContext  the indexing context (carries index path, reindex/async flags)
     * @param asyncIndexing    whether this is an async indexing cycle
     */
    public LuceneNgIndexEditorContext(NodeState root, NodeBuilder definition,
                                      @Nullable IndexDefinition indexDefinition,
                                      IndexUpdateCallback updateCallback,
                                      IndexingContext indexingContext, boolean asyncIndexing) {
        super(root, definition, indexDefinition, updateCallback,
                new LuceneNgFulltextIndexWriterFactory(),
                // maxWeight=0 disables the in-memory extracted-text cache (see ExtractedTextCache:
                // "if (maxWeight > 0) ... else cache = null"). This module has no binary text
                // extraction (LuceneNgDocumentMaker.addBinary is a no-op), so nothing is cached
                // anyway; this matches oak-lucene's own "Disable the cache by default" convention.
                new ExtractedTextCache(0, 0),
                indexingContext, asyncIndexing);
    }

    @Override
    public LuceneNgIndexDefinition.Builder newDefinitionBuilder() {
        return new LuceneNgIndexDefinition.Builder();
    }

    @Override
    public DocumentMaker<Document> newDocumentMaker(IndexDefinition.IndexingRule rule, String path) {
        // Mirrors oak-lucene's LuceneIndexEditorContext.newDocumentMaker plumbing: getTextExtractor()
        // is null for sync indexing (this module never extracts binaries), getDefinition() is the
        // resolved (possibly reindex-swapped) definition. LuceneNgDocumentMaker's real constructor
        // takes decomposed pieces (textExtractor, definition, rule, path, facetsConfig) rather than a
        // context object.
        return new LuceneNgDocumentMaker(getTextExtractor(), getDefinition(), rule, path, getFacetsConfig());
    }

    /**
     * Builds (once) and returns the {@link FacetsConfig} registering each faceted property's
     * dimension -> index-field-name mapping and its multi-valued flag. Port of the former
     * {@code LuceneNgIndexEditor.buildFacetsConfig}.
     */
    private FacetsConfig getFacetsConfig() {
        if (facetsConfig == null) {
            FacetsConfig config = new FacetsConfig();
            for (IndexDefinition.IndexingRule rule : getDefinition().getDefinedRules()) {
                for (PropertyDefinition pd : rule.getProperties()) {
                    if (pd.facet) {
                        config.setIndexFieldName(pd.name, FieldNames.createFacetFieldName(pd.name));
                        config.setMultiValued(pd.name, true);
                    }
                }
            }
            facetsConfig = config;
        }
        return facetsConfig;
    }
}
