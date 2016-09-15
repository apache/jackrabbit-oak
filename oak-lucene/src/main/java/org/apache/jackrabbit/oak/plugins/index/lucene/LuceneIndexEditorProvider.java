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
import org.apache.jackrabbit.oak.plugins.index.ContextAwareCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.LocalIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.DefaultIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriterFactory;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
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
    private final IndexAugmentorFactory augmentorFactory;
    private final LuceneIndexWriterFactory indexWriterFactory;
    private final IndexTracker indexTracker;

    public LuceneIndexEditorProvider() {
        this(null);
    }

    public LuceneIndexEditorProvider(@Nullable IndexCopier indexCopier) {
        //Disable the cache by default in ExtractedTextCache
        this(indexCopier, new ExtractedTextCache(0, 0));
    }

    public LuceneIndexEditorProvider(@Nullable IndexCopier indexCopier,
                                     ExtractedTextCache extractedTextCache) {
        this(indexCopier, extractedTextCache, null, Mounts.defaultMountInfoProvider());
    }

    public LuceneIndexEditorProvider(@Nullable IndexCopier indexCopier,
                                     ExtractedTextCache extractedTextCache,
                                     @Nullable IndexAugmentorFactory augmentorFactory,
                                     MountInfoProvider mountInfoProvider) {
        this(indexCopier, null, extractedTextCache, augmentorFactory, mountInfoProvider);
    }

    public LuceneIndexEditorProvider(@Nullable IndexCopier indexCopier,
                                     @Nullable IndexTracker indexTracker,
                                     ExtractedTextCache extractedTextCache,
                                     @Nullable IndexAugmentorFactory augmentorFactory,
                                     MountInfoProvider mountInfoProvider) {
        this.indexCopier = indexCopier;
        this.indexTracker = indexTracker;
        this.extractedTextCache = extractedTextCache != null ? extractedTextCache : new ExtractedTextCache(0, 0);
        this.augmentorFactory = augmentorFactory;
        this.indexWriterFactory = new DefaultIndexWriterFactory(checkNotNull(mountInfoProvider), indexCopier);
    }

    @Override
    public Editor getIndexEditor(
            @Nonnull String type, @Nonnull NodeBuilder definition, @Nonnull NodeState root,
            @Nonnull IndexUpdateCallback callback)
            throws CommitFailedException {
        if (TYPE_LUCENE.equals(type)) {
            checkArgument(callback instanceof ContextAwareCallback, "callback instance not of type " +
                    "ContextAwareCallback [%s]", callback);
            IndexingContext indexingContext = ((ContextAwareCallback)callback).getIndexingContext();
            LuceneIndexWriterFactory writerFactory = indexWriterFactory;
            IndexDefinition indexDefinition = null;
            if (!indexingContext.isAsync() && IndexDefinition.supportsSyncIndexing(definition)) {

                //Would not participate in reindexing. Only interested in
                //incremental indexing
                if (indexingContext.isReindexing()){
                    return null;
                }

                writerFactory = new LocalIndexWriterFactory(indexingContext);

                //IndexDefinition from tracker might differ from one passed here for reindexing
                //case which should be fine. However reusing existing definition would avoid
                //creating definition instance for each commit as this gets executed for each commit
                if (indexTracker != null){
                    indexDefinition = indexTracker.getIndexDefinition(indexingContext.getIndexPath());
                }

                //Pass on a read only builder to ensure that nothing gets written
                //at all to NodeStore for local indexing.
                //TODO [hybrid] This would cause issue with Facets as for faceted fields
                //some stuff gets written to NodeBuilder. That logic should be refactored
                //to be moved to LuceneIndexWriter
                definition = new ReadOnlyBuilder(definition.getNodeState());
            }

            LuceneIndexEditorContext context = new LuceneIndexEditorContext(root, definition, indexDefinition, callback,
                    writerFactory, extractedTextCache, augmentorFactory);
            return new LuceneIndexEditor(context);
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
