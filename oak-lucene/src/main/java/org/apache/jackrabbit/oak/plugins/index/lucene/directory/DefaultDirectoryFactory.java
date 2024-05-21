/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.SUGGEST_DATA_CHILD_NAME;
import static org.apache.lucene.store.NoLockFactory.getNoLockFactory;

import java.io.File;
import java.io.IOException;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier.COWDirectoryTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.BlobDeletionCallback;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class DefaultDirectoryFactory implements DirectoryFactory {

    private static final boolean READ_BEFORE_WRITE = !Boolean.getBoolean(
        "oak.lucene.readBeforeWriteDisabled");
    private final IndexCopier indexCopier;
    private final GarbageCollectableBlobStore blobStore;
    private final BlobDeletionCallback blobDeletionCallback;
    private final COWDirectoryTracker cowDirectoryTracker;

    public DefaultDirectoryFactory(@Nullable IndexCopier indexCopier,
        @Nullable GarbageCollectableBlobStore blobStore) {
        this(indexCopier, blobStore, BlobDeletionCallback.NOOP, COWDirectoryTracker.NOOP);
    }

    public DefaultDirectoryFactory(@Nullable IndexCopier indexCopier,
        @Nullable GarbageCollectableBlobStore blobStore,
        @NotNull ActiveDeletedBlobCollectorFactory.BlobDeletionCallback blobDeletionCallback,
        @NotNull IndexCopier.COWDirectoryTracker cowDirectoryTracker) {
        this.indexCopier = indexCopier;
        this.blobStore = blobStore;
        this.blobDeletionCallback = blobDeletionCallback;
        this.cowDirectoryTracker = cowDirectoryTracker;
    }

    @Override
    public Directory newInstance(LuceneIndexDefinition definition, NodeBuilder builder,
        String dirName, boolean reindex) throws IOException {
        Directory directory = newIndexDirectory(definition, builder, dirName);
        if (indexCopier != null && !(SUGGEST_DATA_CHILD_NAME.equals(dirName)
            && definition.getUniqueId() == null)) {
            if (READ_BEFORE_WRITE) {
                // prefetch the index when writing to it
                // (copy from the remote directory to the local directory)
                // to avoid having to stream it when merging
                String indexPath = definition.getIndexPath();

                // Here we create a new index directory, because
                // re-using the existing directory (opened above) would
                // mean that the directory returned by the method
                // shares the same OakDirectory instance.
                // That in turn could result in concurrently changing
                // the NodeBuilder on close, as d.close() closes the directory
                // _asynchronously_ (after the method returned).
                // With newIndexDirectory, the internal OakDirectory is not shared
                Directory readerDir = newIndexDirectory(definition, builder, dirName);
                Directory d = indexCopier.wrapForRead(indexPath, definition, readerDir, dirName);

                // closing is done asynchronously
                d.close();
            }
            directory = indexCopier.wrapForWrite(definition, directory, reindex, dirName,
                cowDirectoryTracker);
        }
        return directory;
    }

    @Override
    public boolean remoteDirectory() {
        return indexCopier == null;
    }

    private Directory newIndexDirectory(LuceneIndexDefinition indexDefinition,
        NodeBuilder definition, String dirName)
        throws IOException {
        String path = null;
        if (FulltextIndexConstants.PERSISTENCE_FILE.equalsIgnoreCase(
            definition.getString(FulltextIndexConstants.PERSISTENCE_NAME))) {
            path = definition.getString(FulltextIndexConstants.PERSISTENCE_PATH);
        }
        if (path == null) {
            if (!remoteDirectory()) {
                return new BufferedOakDirectory(definition, dirName, indexDefinition, blobStore,
                    blobDeletionCallback);
            } else {
                return new OakDirectory(definition, dirName, indexDefinition, false, blobStore,
                    blobDeletionCallback);
            }
        } else {
            // try {
            File file = new File(path);
            file.mkdirs();
            // TODO: no locking used
            // --> using the FS backend for the index is in any case
            // troublesome in clustering scenarios and for backup
            // etc. so instead of fixing these issues we'd better
            // work on making the in-content index work without
            // problems (or look at the Solr indexer as alternative)
            return FSDirectory.open(file, getNoLockFactory());
        }
    }
}
