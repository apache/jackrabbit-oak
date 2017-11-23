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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.blob.BlobTrackingStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.BlobIdTracker;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory
    .ActiveDeletedBlobCollectorImpl;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.intersection;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo.getOrCreateId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ActiveDeletedBlobSyncTrackerTest extends AbstractActiveDeletedBlobTest {
    @Rule
    public TemporaryFolder blobTrackerRoot = new TemporaryFolder(new File("target"));

    @Override
    protected ContentRepository createRepository() {
        try {
            File blobCollectorDeleted = new File(blobCollectionRoot.getRoot(), "deleted-blobs");
            blobCollectorDeleted.mkdirs();
            adbc = new ActiveDeletedBlobCollectorImpl(clock, new File(blobCollectionRoot.getRoot(), "deleted-blobs"),
                executorService);

            IndexCopier copier = createIndexCopier();
            editorProvider =
                new LuceneIndexEditorProvider(copier, null, new ExtractedTextCache(10 * FileUtils.ONE_MB,
                    100), null,
                    Mounts.defaultMountInfoProvider(), adbc);
            provider = new LuceneIndexProvider(copier);

            OakFileDataStore ds = new OakFileDataStore();
            ds.setMinRecordLength(10);
            ds.init(fileDataStoreRoot.getRoot().getAbsolutePath());
            DataStoreBlobStore dsbs = new DataStoreBlobStore(ds);
            this.blobStore = new AbstractActiveDeletedBlobTest.CountingBlobStore(dsbs);

            FileStore store = FileStoreBuilder.fileStoreBuilder(temporaryFolder.getRoot()).withMemoryMapping(false)
                .withBlobStore(blobStore).build();
            nodeStore = SegmentNodeStoreBuilders.builder(store).build();
            BlobTrackingStore trackingStore = (BlobTrackingStore) blobStore;
            trackingStore.addTracker(
                new BlobIdTracker(blobTrackerRoot.getRoot().getAbsolutePath(), getOrCreateId(nodeStore), 600,
                    dsbs));
            // set the blob store to skip writing blobs through the node store
            editorProvider.setBlobStore(blobStore);

            asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, editorProvider);
            return new Oak(nodeStore).with(new InitialContent()).with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider).with((Observer) provider).with(editorProvider)
                .createContentRepository();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // OAK-6504
    @Test
    public void syncActiveDeletionWithBlobTracker() throws Exception {
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
        assertEquals("Hack purge must not purge any blob (first commit)",
                firstCommitNumChunks, hackPurgeNumChunks);

        root.getTree("/").addChild("test").setProperty("propa", "foo1");
        root.commit();
        asyncIndexUpdate.run();
        long secondCommitNumChunks = blobStore.numChunks;
        adbc.purgeBlobsDeleted(0, blobStore);//hack to purge file
        hackPurgeNumChunks = blobStore.numChunks;
        assertEquals("Hack purge must not purge any blob (second commit)",
            secondCommitNumChunks, hackPurgeNumChunks);

        adbc.purgeBlobsDeleted(time, blobStore);
        adbc.getBlobDeletionCallback();
        long firstGCNumChunks = blobStore.numChunks;

        assertTrue("First commit must create some chunks", firstCommitNumChunks > initialNumChunks);
        assertTrue("First commit must create some chunks", secondCommitNumChunks > firstCommitNumChunks);
        assertTrue("First GC should delete some chunks", firstGCNumChunks < secondCommitNumChunks);

        assertTrackedDeleted(blobStore.getAllChunkIds(-1), blobStore);
    }

    private static void assertTrackedDeleted(Iterator<String> afterDeletions,
        GarbageCollectableBlobStore blobStore) throws IOException {

        List<String> afterDeletionIds = newArrayList(afterDeletions);
        // get the currently tracked ones
        ArrayList<String> trackedIds = newArrayList(((BlobTrackingStore) blobStore).getTracker().get());
        assertEquals("Tracked ids length different from current blob list",
            trackedIds.size(), afterDeletionIds.size());
        assertTrue("Tracked ids different from current blob list",
            newHashSet(trackedIds).equals(newHashSet(afterDeletionIds)));
    }
}
