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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;

/**
 * Service that provides Lucene based {@link IndexEditor}s
 * 
 * @see LuceneIndexEditor
 * @see IndexEditorProvider
 * 
 */
public class LuceneIndexEditorProvider implements IndexEditorProvider {
    private final IndexCopier indexCopier;
    private final ExtractedTextCache extractedTextCache;

    public LuceneIndexEditorProvider() {
        this(null);
    }

    public LuceneIndexEditorProvider(@Nullable IndexCopier indexCopier) {
        //Disable the cache by default in ExtractedTextCache
        this(indexCopier, new ExtractedTextCache(0, 0));
    }

    public LuceneIndexEditorProvider(@Nullable IndexCopier indexCopier,
                                     ExtractedTextCache extractedTextCache) {
        this.indexCopier = indexCopier;
        this.extractedTextCache = extractedTextCache;
    }

    @Override
    public Editor getIndexEditor(
            @Nonnull String type, @Nonnull NodeBuilder definition, @Nonnull NodeState root,
            @Nonnull IndexUpdateCallback callback)
            throws CommitFailedException {
        if (TYPE_LUCENE.equals(type)) {
            return new LuceneIndexEditor(root, definition, callback, indexCopier, extractedTextCache);
        }
        return null;
    }

    IndexCopier getIndexCopier() {
        return indexCopier;
    }

    ExtractedTextCache getExtractedTextCache() {
        return extractedTextCache;
    }
}
