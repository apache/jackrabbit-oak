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

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.ActiveDeletedBlobCollectorImpl;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class ActiveDeletedBlobCollectionIT extends AbstractQueryTest {
    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));
    @Rule
    public TemporaryFolder blobCollectionRoot = new TemporaryFolder(new File("target"));
    @Rule
    public TemporaryFolder fileDataStoreRoot = new TemporaryFolder(new File("target"));
    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    private MongoConnection mongoConnection = null;
    private CountingBlobStore blobStore = null;

    private Clock clock = new Clock.Virtual();
    private ActiveDeletedBlobCollectorImpl adbc = null;

    private AsyncIndexUpdate asyncIndexUpdate;

    private LuceneIndexEditorProvider editorProvider;

    private NodeStore nodeStore;

    private LuceneIndexProvider provider;

    private final DataStoreType dataStoreType;

    @BeforeClass
    public static void assumeMongo() {
        assumeTrue(MongoUtils.isAvailable());
    }

    @After
    public void after() {
        new ExecutorCloser(executorService).close();
        executorService.shutdown();
        IndexDefinition.setDisableStoredIndexDefinition(false);
    }

    enum DataStoreType {
        WITH_FDS,
        WITHOUT_FDS
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> fixtures() {
        List<Object[]> result = Lists.newArrayList();
        result.add(new Object[]{DataStoreType.WITHOUT_FDS});
        result.add(new Object[]{DataStoreType.WITH_FDS});
        return result;
    }

    public ActiveDeletedBlobCollectionIT(DataStoreType dataStoreType) {
        this.dataStoreType = dataStoreType;
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Override
    protected ContentRepository createRepository() {
        adbc = new ActiveDeletedBlobCollectorImpl(clock,
                new File(blobCollectionRoot.getRoot(), "deleted-blobs"), executorService);

        IndexCopier copier = createIndexCopier();
        editorProvider = new LuceneIndexEditorProvider(copier, null,
                new ExtractedTextCache(10* FileUtils.ONE_MB,100),
                null, Mounts.defaultMountInfoProvider(), adbc);
        provider = new LuceneIndexProvider(copier);
        mongoConnection = connectionFactory.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDB());
        if (dataStoreType == DataStoreType.WITHOUT_FDS) {
            MongoBlobStore blobStore = new MongoBlobStore(mongoConnection.getDB());
            blobStore.setBlockSize(128);
            blobStore.setBlockSizeMin(48);
            this.blobStore = new CountingBlobStore(blobStore);
        } else {
            FileDataStore fds = new FileDataStore();
            fds.init(fileDataStoreRoot.getRoot().getAbsolutePath());
            DataStoreBlobStore dsbs = new DataStoreBlobStore(fds);
            dsbs.setBlockSize(128);
            this.blobStore = new CountingBlobStore(dsbs);
        }
        nodeStore = new DocumentMK.Builder()
                .setMongoDB(mongoConnection.getDB())
                .setBlobStore(this.blobStore)
                .getNodeStore();
        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, editorProvider);
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .createContentRepository();
    }

    private IndexCopier createIndexCopier() {
        try {
            return new IndexCopier(executorService, temporaryFolder.getRoot());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void simpleAsyncIndexUpdateBasedBlobCollection() throws Exception {
        createIndex("test1", of("propa"));
        root.getTree("/oak:index/counter").remove();
        root.commit();
        asyncIndexUpdate.run();
        long initialNumChunks = blobStore.numChunks;

        root.getTree("/").addChild("test").setProperty("propa", "foo");
        root.commit();
        asyncIndexUpdate.run();
        long firstCommitNumChunks = blobStore.numChunks;
        adbc.purgeBlobsDeleted(0, blobStore);//hack to purge file
        long time = clock.getTimeIncreasing();
        long hackPurgeNumChunks = blobStore.numChunks;
        Assert.assertEquals("Hack purge must not purge any blob (first commit)",
                firstCommitNumChunks, hackPurgeNumChunks);

        root.getTree("/").addChild("test").setProperty("propa", "foo1");
        root.commit();
        asyncIndexUpdate.run();
        long secondCommitNumChunks = blobStore.numChunks;
        adbc.purgeBlobsDeleted(0, blobStore);//hack to purge file
        hackPurgeNumChunks = blobStore.numChunks;
        Assert.assertEquals("Hack purge must not purge any blob (second commit)",
                secondCommitNumChunks, hackPurgeNumChunks);

        adbc.purgeBlobsDeleted(time, blobStore);
        long firstGCNumChunks = blobStore.numChunks;
        adbc.purgeBlobsDeleted(clock.getTimeIncreasing(), blobStore);
        long secondGCNumChunks = blobStore.numChunks;

        Assert.assertTrue("First commit must create some chunks", firstCommitNumChunks > initialNumChunks);
        Assert.assertTrue("First commit must create some chunks", secondCommitNumChunks > firstCommitNumChunks);
        Assert.assertTrue("First GC should delete some chunks", firstGCNumChunks < secondCommitNumChunks);
        Assert.assertTrue("Second GC should delete some chunks too", secondGCNumChunks < firstGCNumChunks);
    }

    private Tree createIndex(String name, Set<String> propNames) throws CommitFailedException {
        Tree index = root.getTree("/");
        return createIndex(index, name, propNames);
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

    class CountingBlobStore implements GarbageCollectableBlobStore {

        private final GarbageCollectableBlobStore delegate;
        private long numChunks = 0;

        CountingBlobStore(GarbageCollectableBlobStore delegate) {
                this.delegate = delegate;
        }

        @Override
        public String writeBlob(InputStream in) throws IOException {
            String blobId = delegate.writeBlob(in);
            numChunks += Iterators.size(delegate.resolveChunks(blobId));
            return blobId;
        }

        @Override
        public void setBlockSize(int x) {
            delegate.setBlockSize(x);
        }

        @Override
        public String writeBlob(InputStream in, BlobOptions options) throws IOException {
            String blobId = delegate.writeBlob(in, options);
            numChunks += Iterators.size(delegate.resolveChunks(blobId));
            return blobId;
        }

        @Override
        public String writeBlob(String tempFileName) throws IOException {
            String blobId = delegate.writeBlob(tempFileName);
            numChunks += Iterators.size(delegate.resolveChunks(blobId));
            return blobId;
        }

        @Override
        public int sweep() throws IOException {
            return delegate.sweep();
        }

        @Override
        public void startMark() throws IOException {
            delegate.startMark();
        }

        @Override
        public int readBlob(String blobId, long pos, byte[] buff, int off, int length) throws IOException {
            return delegate.readBlob(blobId, pos, buff, off, length);
        }

        @Override
        public void clearInUse() {
            delegate.clearInUse();
        }

        @Override
        public void clearCache() {
            delegate.clearCache();
        }

        @Override
        public long getBlobLength(String blobId) throws IOException {
            return delegate.getBlobLength(blobId);
        }

        @Override
        public long getBlockSizeMin() {
            return delegate.getBlockSizeMin();
        }

        @Override
        public Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception {
            return delegate.getAllChunkIds(maxLastModifiedTime);
        }

        @Override
        public InputStream getInputStream(String blobId) throws IOException {
            return delegate.getInputStream(blobId);
        }

        @Override
        @Deprecated
        public boolean deleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
            numChunks -= chunkIds.size();
            return delegate.deleteChunks(chunkIds, maxLastModifiedTime);
        }

        @Override
        @CheckForNull
        public String getBlobId(@Nonnull String reference) {
            return delegate.getBlobId(reference);
        }

        @Override
        @CheckForNull
        public String getReference(@Nonnull String blobId) {
            return delegate.getReference(blobId);
        }

        @Override
        public long countDeleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
            long numDeleted = delegate.countDeleteChunks(chunkIds, maxLastModifiedTime);
            numChunks -= numDeleted;
            return numDeleted;
        }

        @Override
        public Iterator<String> resolveChunks(String blobId) throws IOException {
            return delegate.resolveChunks(blobId);
        }
    }
}
