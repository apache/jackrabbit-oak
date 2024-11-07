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

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.ActiveDeletedBlobCollectorImpl;

import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class ActiveDeletedBlobCollectionIT extends AbstractActiveDeletedBlobTest {

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    private MongoConnection mongoConnection = null;

    private final DataStoreType dataStoreType;

    private FailOnDemandValidatorProvider failOnDemandValidatorProvider;

    @BeforeClass
    public static void assumeMongo() {
        assumeTrue(MongoUtils.isAvailable());
    }

    enum DataStoreType {
        WITH_FDS,
        WITHOUT_FDS
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> fixtures() {
        List<Object[]> result = new ArrayList<>();
        result.add(new Object[]{DataStoreType.WITHOUT_FDS});
        result.add(new Object[]{DataStoreType.WITH_FDS});
        return result;
    }

    public ActiveDeletedBlobCollectionIT(DataStoreType dataStoreType) {
        this.dataStoreType = dataStoreType;
    }

    @Override
    protected ContentRepository createRepository() {
        File deletedBlobsDir = new File(blobCollectionRoot.getRoot(), "deleted-blobs");
        assertTrue(deletedBlobsDir.mkdirs());
        adbc = new ActiveDeletedBlobCollectorImpl(clock, deletedBlobsDir, executorService);

        IndexCopier copier = createIndexCopier();
        editorProvider = new LuceneIndexEditorProvider(copier, null,
                new ExtractedTextCache(10* FileUtils.ONE_MB,100),
                null, Mounts.defaultMountInfoProvider(), adbc, null, null);
        provider = new LuceneIndexProvider(copier);
        mongoConnection = connectionFactory.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDatabase());
        if (dataStoreType == DataStoreType.WITHOUT_FDS) {
            MongoBlobStore blobStore = new MongoBlobStore(mongoConnection.getDatabase());
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
                .setMongoDB(mongoConnection.getMongoClient(), mongoConnection.getDBName())
                .setBlobStore(this.blobStore)
                .getNodeStore();

        failOnDemandValidatorProvider = new FailOnDemandValidatorProvider();
        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, editorProvider);
        asyncIndexUpdate.setValidatorProviders(Collections.singletonList(failOnDemandValidatorProvider));

        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .createContentRepository();
    }

    @After
    public void dispose() {
        String dbName = mongoConnection.getDBName();
        ((DocumentNodeStore) nodeStore).dispose();
        MongoUtils.dropCollections(dbName);
    }

    @Test
    public void simpleAsyncIndexUpdateBasedBlobCollection() throws Exception {
        createIndex("test1", Set.of("propa"));
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

        assertTrue("First commit must create some chunks", firstCommitNumChunks > initialNumChunks);
        assertTrue("First commit must create some chunks", secondCommitNumChunks > firstCommitNumChunks);
        assertTrue("First GC should delete some chunks", firstGCNumChunks < secondCommitNumChunks);
        assertTrue("Second GC should delete some chunks too", secondGCNumChunks < firstGCNumChunks);
    }

    @Test
    public void dontDeleteIfIndexingFailed() throws Exception {
        createIndex("test1", Set.of("propa"));
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

        failOnDemandValidatorProvider.shouldFail = true;

        root.getTree("/").addChild("test").setProperty("propa", "foo1");
        root.commit();
        asyncIndexUpdate.run();
        assertTrue("Indexing must have failed", asyncIndexUpdate.isFailing());
        long secondCommitNumChunks = blobStore.numChunks;
        adbc.purgeBlobsDeleted(0, blobStore);//hack to purge file
        hackPurgeNumChunks = blobStore.numChunks;
        Assert.assertEquals("Hack purge must not purge any blob (second commit)",
                secondCommitNumChunks, hackPurgeNumChunks);

        adbc.purgeBlobsDeleted(time, blobStore);
        long firstGCNumChunks = blobStore.numChunks;
        adbc.purgeBlobsDeleted(clock.getTimeIncreasing(), blobStore);
        long secondGCNumChunks = blobStore.numChunks;

        assertTrue("First commit must create some chunks", firstCommitNumChunks > initialNumChunks);
        assertTrue("Second commit must create some chunks", secondCommitNumChunks > firstCommitNumChunks);
        assertTrue("First GC should delete some chunks", firstGCNumChunks < secondCommitNumChunks);
        assertEquals("Second GC must not delete chunks as commit failed", firstGCNumChunks, secondGCNumChunks);
    }

    private static class FailOnDemandValidatorProvider extends ValidatorProvider {
        boolean shouldFail;

        @Override
        protected @Nullable Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
            return new DefaultValidator() {
                @Override
                public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
                    if (shouldFail && FulltextIndexConstants.INDEX_DATA_CHILD_NAME.equals(name)) {
                        throw new CommitFailedException("failing-validator", 1, "Failed commit as requested");
                    }

                    return this;
                }
            };
        }
    }

}
