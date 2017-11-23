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
package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.blob.BlobTrackingStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.BlobTracker;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

public abstract class AbstractActiveDeletedBlobTest extends AbstractQueryTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));
    @Rule
    public TemporaryFolder blobCollectionRoot = new TemporaryFolder(new File("target"));
    @Rule
    public TemporaryFolder fileDataStoreRoot = new TemporaryFolder(new File("target"));

    protected ExecutorService executorService = Executors.newFixedThreadPool(2);

    protected CountingBlobStore blobStore = null;

    protected Clock clock = new Clock.Virtual();

    protected ActiveDeletedBlobCollectorFactory.ActiveDeletedBlobCollectorImpl adbc = null;

    protected AsyncIndexUpdate asyncIndexUpdate;

    protected LuceneIndexEditorProvider editorProvider;

    protected NodeStore nodeStore;

    protected LuceneIndexProvider provider;

    @After
    public void after() {
        new ExecutorCloser(executorService).close();
        executorService.shutdown();
        IndexDefinition.setDisableStoredIndexDefinition(false);
    }

    public static Tree createIndex(Tree index, String name, Set<String> propNames) throws CommitFailedException {
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(ASYNC_PROPERTY_NAME, "async");
        def.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        def.setProperty(PropertyStates.createProperty(LuceneIndexConstants.INCLUDE_PROPERTY_NAMES, propNames, Type.STRINGS));
        def.setProperty(LuceneIndexConstants.SAVE_DIR_LISTING, true);
        return index.getChild(INDEX_DEFINITIONS_NAME).getChild(name);
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    protected IndexCopier createIndexCopier() {
        try {
            return new IndexCopier(executorService, temporaryFolder.getRoot());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected Tree createIndex(String name, Set<String> propNames) throws CommitFailedException {
        Tree index = root.getTree("/");
        return createIndex(index, name, propNames);
    }

    class CountingBlobStore implements GarbageCollectableBlobStore, BlobTrackingStore {

        private final GarbageCollectableBlobStore delegate;
        protected long numChunks = 0;

        CountingBlobStore(GarbageCollectableBlobStore delegate) {
            this.delegate = delegate;
        }

        @Override public String writeBlob(InputStream in) throws IOException {
            String blobId = delegate.writeBlob(in);
            numChunks += Iterators.size(delegate.resolveChunks(blobId));
            return blobId;
        }

        @Override public void setBlockSize(int x) {
            delegate.setBlockSize(x);
        }

        @Override public String writeBlob(InputStream in, BlobOptions options) throws IOException {
            String blobId = delegate.writeBlob(in, options);
            numChunks += Iterators.size(delegate.resolveChunks(blobId));
            return blobId;
        }

        @Override public String writeBlob(String tempFileName) throws IOException {
            String blobId = delegate.writeBlob(tempFileName);
            numChunks += Iterators.size(delegate.resolveChunks(blobId));
            return blobId;
        }

        @Override public int sweep() throws IOException {
            return delegate.sweep();
        }

        @Override public void startMark() throws IOException {
            delegate.startMark();
        }

        @Override public int readBlob(String blobId, long pos, byte[] buff, int off, int length) throws IOException {
            return delegate.readBlob(blobId, pos, buff, off, length);
        }

        @Override public void clearInUse() {
            delegate.clearInUse();
        }

        @Override public void clearCache() {
            delegate.clearCache();
        }

        @Override public long getBlobLength(String blobId) throws IOException {
            return delegate.getBlobLength(blobId);
        }

        @Override public long getBlockSizeMin() {
            return delegate.getBlockSizeMin();
        }

        @Override public Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception {
            return delegate.getAllChunkIds(maxLastModifiedTime);
        }

        @Override public InputStream getInputStream(String blobId) throws IOException {
            return delegate.getInputStream(blobId);
        }

        @Override @Deprecated public boolean deleteChunks(List<String> chunkIds, long maxLastModifiedTime)
            throws Exception {
            numChunks -= chunkIds.size();
            return delegate.deleteChunks(chunkIds, maxLastModifiedTime);
        }

        @Override @CheckForNull public String getBlobId(@Nonnull String reference) {
            return delegate.getBlobId(reference);
        }

        @Override @CheckForNull public String getReference(@Nonnull String blobId) {
            return delegate.getReference(blobId);
        }

        @Override public long countDeleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
            long numDeleted = delegate.countDeleteChunks(chunkIds, maxLastModifiedTime);
            numChunks -= numDeleted;
            return numDeleted;
        }

        @Override public Iterator<String> resolveChunks(String blobId) throws IOException {
            return delegate.resolveChunks(blobId);
        }

        @Override public void addTracker(BlobTracker tracker) {
            if (delegate instanceof BlobTrackingStore) {
                ((BlobTrackingStore) delegate).addTracker(tracker);
            }
        }

        @Override public BlobTracker getTracker() {
            if (delegate instanceof BlobTrackingStore) {
                return ((BlobTrackingStore) delegate).getTracker();
            }
            return null;
        }

        @Override public void addMetadataRecord(InputStream stream, String name) throws DataStoreException {
            if (delegate instanceof BlobTrackingStore) {
                ((BlobTrackingStore) delegate).addMetadataRecord(stream, name);
            }
        }

        @Override public void addMetadataRecord(File f, String name) throws DataStoreException {
            if (delegate instanceof BlobTrackingStore) {
                ((BlobTrackingStore) delegate).addMetadataRecord(f, name);
            }
        }

        @Override public DataRecord getMetadataRecord(String name) {
            if (delegate instanceof BlobTrackingStore) {
                return ((BlobTrackingStore) delegate).getMetadataRecord(name);
            }
            return null;
        }

        @Override public List<DataRecord> getAllMetadataRecords(String prefix) {
            if (delegate instanceof BlobTrackingStore) {
                return ((BlobTrackingStore) delegate).getAllMetadataRecords(prefix);
            }
            return null;
        }

        @Override public boolean deleteMetadataRecord(String name) {
            if (delegate instanceof BlobTrackingStore) {
                ((BlobTrackingStore) delegate).deleteMetadataRecord(name);
            }
            return false;
        }

        @Override public void deleteAllMetadataRecords(String prefix) {
            if (delegate instanceof BlobTrackingStore) {
                ((BlobTrackingStore) delegate).deleteAllMetadataRecords(prefix);
            }
        }

        @Override public Iterator<DataRecord> getAllRecords() throws DataStoreException {
            if (delegate instanceof BlobTrackingStore) {
                return ((BlobTrackingStore) delegate).getAllRecords();
            }
            return Iterators.emptyIterator();
        }

        @Override public DataRecord getRecordForId(DataIdentifier id) throws DataStoreException {
            if (delegate instanceof BlobTrackingStore) {
                return ((BlobTrackingStore) delegate).getRecordForId(id);
            }
            return null;
        }

        @Override public Type getType() {
            if (delegate instanceof BlobTrackingStore) {
                ((BlobTrackingStore) delegate).getType();
            }
            return Type.DEFAULT;
        }
    }
}
