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

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.*;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier.COWDirectoryTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.ActiveDeletedBlobCollector;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.BlobDeletionCallback;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.CopyOnWriteDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DefaultDirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.IndexingQueue;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.LocalIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.LuceneDocumentHolder;
import org.apache.jackrabbit.oak.plugins.index.lucene.property.LuceneIndexPropertyQuery;
import org.apache.jackrabbit.oak.plugins.index.lucene.property.PropertyIndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.lucene.property.PropertyQuery;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.DefaultIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriterConfig;
import org.apache.jackrabbit.oak.plugins.index.search.CompositePropertyUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriterFactory;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;

/**
 * Service that provides Lucene based {@link IndexEditor}s
 *
 * @see LuceneIndexEditor
 * @see IndexEditorProvider
 *
 */
public class LuceneIndexEditorProvider implements IndexEditorProvider {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final IndexCopier indexCopier;
    private final ExtractedTextCache extractedTextCache;
    private final IndexAugmentorFactory augmentorFactory;
    private final IndexTracker indexTracker;
    private final MountInfoProvider mountInfoProvider;
    private final ActiveDeletedBlobCollector activeDeletedBlobCollector;
    private GarbageCollectableBlobStore blobStore;
    private IndexingQueue indexingQueue;
    private boolean nrtIndexingEnabled;
    private LuceneIndexWriterConfig writerConfig = new LuceneIndexWriterConfig();

    private final LuceneIndexMBean mbean;
    private final StatisticsProvider statisticsProvider;

    /**
     * Number of indexed Lucene document that can be held in memory
     * This ensures that for very large commit memory consumption
     * is bounded
     */
    private int inMemoryDocsLimit = Integer.getInteger("oak.lucene.inMemoryDocsLimit", 500);
    private AsyncIndexesSizeStatsUpdate asyncIndexesSizeStatsUpdate;

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
        this(indexCopier, indexTracker, extractedTextCache, augmentorFactory, mountInfoProvider,
                ActiveDeletedBlobCollectorFactory.NOOP, null, null);
    }
    public LuceneIndexEditorProvider(@Nullable IndexCopier indexCopier,
                                     @Nullable IndexTracker indexTracker,
                                     ExtractedTextCache extractedTextCache,
                                     @Nullable IndexAugmentorFactory augmentorFactory,
                                     MountInfoProvider mountInfoProvider,
                                     @NotNull ActiveDeletedBlobCollectorFactory.ActiveDeletedBlobCollector activeDeletedBlobCollector,
                                     @Nullable LuceneIndexMBean mbean,
                                     @Nullable StatisticsProvider statisticsProvider) {
        this.indexCopier = indexCopier;
        this.indexTracker = indexTracker;
        this.extractedTextCache = extractedTextCache != null ? extractedTextCache : new ExtractedTextCache(0, 0);
        this.augmentorFactory = augmentorFactory;
        this.mountInfoProvider = requireNonNull(mountInfoProvider);
        this.activeDeletedBlobCollector = activeDeletedBlobCollector;
        this.mbean = mbean;
        this.statisticsProvider = statisticsProvider;
    }

    public LuceneIndexEditorProvider withAsyncIndexesSizeStatsUpdate(AsyncIndexesSizeStatsUpdate asyncIndexesSizeStatsUpdate) {
        this.asyncIndexesSizeStatsUpdate = asyncIndexesSizeStatsUpdate;
        return this;
    }

    @Override
    public Editor getIndexEditor(
        @NotNull String type, @NotNull NodeBuilder definition, @NotNull NodeState root,
        @NotNull IndexUpdateCallback callback)
            throws CommitFailedException {
        if (TYPE_LUCENE.equals(type)) {
            checkArgument(callback instanceof ContextAwareCallback, "callback instance not of type " +
                    "ContextAwareCallback [%s]", callback);
            IndexingContext indexingContext = ((ContextAwareCallback)callback).getIndexingContext();
            BlobDeletionCallback blobDeletionCallback = activeDeletedBlobCollector.getBlobDeletionCallback();
            indexingContext.registerIndexCommitCallback(blobDeletionCallback);
            FulltextIndexWriterFactory writerFactory = null;
            LuceneIndexDefinition indexDefinition = null;
            boolean asyncIndexing = true;
            String indexPath = indexingContext.getIndexPath();
            Collection<PropertyUpdateCallback> callbacks = new LinkedList<>();
            PropertyIndexUpdateCallback propertyIndexUpdateCallback = null;

            if (nrtIndexingEnabled() && !indexingContext.isAsync() && IndexDefinition.supportsSyncOrNRTIndexing(definition)) {

                //Would not participate in reindexing. Only interested in
                //incremental indexing
                if (indexingContext.isReindexing()){
                    return null;
                }

                CommitContext commitContext = getCommitContext(indexingContext);
                if (commitContext == null){
                    //Logically there should not be any commit without commit context. But
                    //some initializer code does the commit with out it. So ignore such calls with
                    //warning now
                    //TODO Revisit use of warn level once all such cases are analyzed
                    log.warn("No CommitContext found for commit", new Exception());
                    return null;
                }

                //TODO Also check if index has been done once


                writerFactory = new LocalIndexWriterFactory(getDocumentHolder(commitContext),
                        indexPath);

                //IndexDefinition from tracker might differ from one passed here for reindexing
                //case which should be fine. However reusing existing definition would avoid
                //creating definition instance for each commit as this gets executed for each commit
                if (indexTracker != null){
                    indexDefinition = indexTracker.getIndexDefinition(indexPath);
                    if (indexDefinition != null && !indexDefinition.hasMatchingNodeTypeReg(root)){
                        log.debug("Detected change in NodeType registry for index {}. Would not use " +
                                "existing index definition", indexDefinition.getIndexPath());
                        indexDefinition = null;
                    }
                }

                if (indexDefinition == null) {
                    indexDefinition = LuceneIndexDefinition.newBuilder(root, definition.getNodeState(),
                            indexPath).build();
                }

                if (indexDefinition.hasSyncPropertyDefinitions()) {
                    propertyIndexUpdateCallback = new PropertyIndexUpdateCallback(indexPath, definition, root);
                    if (indexTracker != null) {
                        PropertyQuery query = new LuceneIndexPropertyQuery(indexTracker, indexPath);
                        propertyIndexUpdateCallback.getUniquenessConstraintValidator().setSecondStore(query);
                    }
                }

                //Pass on a read only builder to ensure that nothing gets written
                //at all to NodeStore for local indexing.
                //TODO [hybrid] This would cause issue with Facets as for faceted fields
                //some stuff gets written to NodeBuilder. That logic should be refactored
                //to be moved to LuceneIndexWriter
                definition = new ReadOnlyBuilder(definition.getNodeState());

                asyncIndexing = false;
            }

            if (writerFactory == null) {
                COWDirectoryCleanupCallback cowDirectoryCleanupCallback = new COWDirectoryCleanupCallback();
                indexingContext.registerIndexCommitCallback(cowDirectoryCleanupCallback);

                writerFactory = new DefaultIndexWriterFactory(mountInfoProvider,
                        newDirectoryFactory(blobDeletionCallback, cowDirectoryCleanupCallback),
                        writerConfig);
            }

            LuceneIndexEditorContext context = new LuceneIndexEditorContext(root, definition, indexDefinition, callback,
                    writerFactory, extractedTextCache, augmentorFactory, indexingContext, asyncIndexing);

            if (propertyIndexUpdateCallback != null) {
                callbacks.add(propertyIndexUpdateCallback);
            }
            if (mbean != null && statisticsProvider != null) {
                // Below mentioned callback (LuceneIndexStatsUpdateCallback) is only executed
                // in async indexing flow. There is a check on
                // indexingContext.isAsync()
                callbacks.add(new LuceneIndexStatsUpdateCallback(indexPath, mbean, statisticsProvider,
                        asyncIndexesSizeStatsUpdate, indexingContext));
            }

            if (!callbacks.isEmpty()) {
                CompositePropertyUpdateCallback compositePropertyUpdateCallback = new CompositePropertyUpdateCallback(callbacks);
                context.setPropertyUpdateCallback(compositePropertyUpdateCallback);
            }
            return new LuceneIndexEditor(context);
        }
        return null;
    }

    IndexCopier getIndexCopier() {
        return indexCopier;
    }

    IndexingQueue getIndexingQueue() {
        return indexingQueue;
    }

    public ExtractedTextCache getExtractedTextCache() {
        return extractedTextCache;
    }

    public void setInMemoryDocsLimit(int inMemoryDocsLimit) {
        this.inMemoryDocsLimit = inMemoryDocsLimit;
    }

    protected DirectoryFactory newDirectoryFactory(BlobDeletionCallback blobDeletionCallback,
                                                   COWDirectoryTracker cowDirectoryTracker) {
        return new DefaultDirectoryFactory(indexCopier, blobStore, blobDeletionCallback, cowDirectoryTracker);
    }

    private LuceneDocumentHolder getDocumentHolder(CommitContext commitContext){
        LuceneDocumentHolder holder = (LuceneDocumentHolder) commitContext.get(LuceneDocumentHolder.NAME);
        if (holder == null) {
            holder = new LuceneDocumentHolder(indexingQueue, inMemoryDocsLimit);
            commitContext.set(LuceneDocumentHolder.NAME, holder);
        }
        return holder;
    }

    public void setBlobStore(@Nullable GarbageCollectableBlobStore blobStore) {
        this.blobStore = blobStore;
    }

    public void setIndexingQueue(IndexingQueue indexingQueue) {
        this.indexingQueue = indexingQueue;
        this.nrtIndexingEnabled = indexingQueue != null;
    }

    public void setWriterConfig(LuceneIndexWriterConfig writerConfig) {
        this.writerConfig = writerConfig;
    }

    GarbageCollectableBlobStore getBlobStore() {
        return blobStore;
    }

    private boolean nrtIndexingEnabled() {
        return nrtIndexingEnabled;
    }

    private static CommitContext getCommitContext(IndexingContext indexingContext) {
        return (CommitContext) indexingContext.getCommitInfo().getInfo().get(CommitContext.NAME);
    }

    private static class COWDirectoryCleanupCallback implements IndexCommitCallback, COWDirectoryTracker {
        private static final Logger LOG = LoggerFactory.getLogger(COWDirectoryCleanupCallback.class);

        private List<CopyOnWriteDirectory> openedCoWDirectories = new ArrayList<>();
        private List<File> reindexingLocalDirectories = new ArrayList<>();

        @Override
        public void commitProgress(IndexProgress indexProgress) {
            // we only worry about failed indexing
            if (indexProgress == IndexProgress.COMMIT_FAILED) {
                for (CopyOnWriteDirectory d : openedCoWDirectories) {
                    try {
                        d.close();
                    } catch (Exception e) {
                        LOG.warn("Error occurred while closing {}", d, e);
                    }
                }

                for (File f : reindexingLocalDirectories) {
                    if ( ! FileUtils.deleteQuietly(f)) {
                        LOG.warn("Failed to delete {}", f);
                    }
                }
            }
        }

        @Override
        public void registerOpenedDirectory(@NotNull CopyOnWriteDirectory directory) {
            openedCoWDirectories.add(directory);
        }

        @Override
        public void registerReindexingLocalDirectory(@NotNull File dir) {
            reindexingLocalDirectories.add(dir);
        }
    }
}
