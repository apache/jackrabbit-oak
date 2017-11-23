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

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.ActiveDeletedBlobCollectorImpl;

import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Collection;
import java.util.List;

import static com.google.common.collect.ImmutableSet.of;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class ActiveDeletedBlobCollectionIT extends AbstractActiveDeletedBlobTest {

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    private MongoConnection mongoConnection = null;

    private final DataStoreType dataStoreType;

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
        List<Object[]> result = Lists.newArrayList();
        result.add(new Object[]{DataStoreType.WITHOUT_FDS});
        result.add(new Object[]{DataStoreType.WITH_FDS});
        return result;
    }

    public ActiveDeletedBlobCollectionIT(DataStoreType dataStoreType) {
        this.dataStoreType = dataStoreType;
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

}
