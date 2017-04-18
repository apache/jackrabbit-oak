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

package org.apache.jackrabbit.oak.plugins.document;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.BlobGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.GarbageCollectionRepoStats;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for gc in a shared data store among hetrogeneous oak node stores.
 */
public class SharedBlobStoreGCTest {
    private static final Logger log = LoggerFactory.getLogger(SharedBlobStoreGCTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    protected Cluster cluster1;
    protected Cluster cluster2;
    private Clock clock;

    @Before
    public void setUp() throws Exception {
        log.debug("In setUp()");

        clock = new Clock.Virtual();
        clock.waitUntil(Revision.getCurrentTimestamp());
        DataStoreUtils.time = clock.getTime();

        File rootFolder = folder.newFolder();
        BlobStore blobeStore1 = getBlobStore(rootFolder);
        DocumentNodeStore ds1 = new DocumentMK.Builder()
                .setAsyncDelay(0)
                .setDocumentStore(new MemoryDocumentStore())
                .setBlobStore(blobeStore1)
                .clock(clock)
                .getNodeStore();
        String repoId1 = ClusterRepositoryInfo.getOrCreateId(ds1);
        // Register the unique repository id in the data store
        ((SharedDataStore) blobeStore1).addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.REPOSITORY.getNameFromId(repoId1));

        BlobStore blobeStore2 = getBlobStore(rootFolder);
        DocumentNodeStore ds2 = new DocumentMK.Builder()
                .setAsyncDelay(0)
                .setDocumentStore(new MemoryDocumentStore())
                .setBlobStore(blobeStore2)
                .clock(clock)
                .getNodeStore();
        String repoId2 = ClusterRepositoryInfo.getOrCreateId(ds2);
        // Register the unique repository id in the data store
        ((SharedDataStore) blobeStore2).addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.REPOSITORY.getNameFromId(repoId2));

        cluster1 = new Cluster(ds1, repoId1, 20);
        cluster1.init();
        log.debug("Initialized {}", cluster1);

        cluster2 = new Cluster(ds2, repoId2, 100);
        cluster2.init();
        log.debug("Initialized {}", cluster2);
    }

    static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    @Test
    public void testGC() throws Exception {
        log.debug("Running testGC()");
        // Only run the mark phase on both the clusters
        cluster1.gc.collectGarbage(true);
        cluster2.gc.collectGarbage(true);

        // Execute the gc with sweep
        cluster1.gc.collectGarbage(false);

        Assert.assertEquals(true, Sets.symmetricDifference(Sets.union(cluster1.getInitBlobs(), cluster2.getInitBlobs()),
            cluster1.getExistingBlobIds()).isEmpty());
    }

    @Test
    public void testGCWithNodeSpecialChars() throws Exception {
        log.debug("Running testGC()");
        // Only run the mark phase on both the clusters
        cluster1.initBlobs.addAll(cluster1.addNodeSpecialChars());
        cluster2.initBlobs.addAll(cluster1.addNodeSpecialChars());
        cluster1.gc.collectGarbage(true);
        cluster2.gc.collectGarbage(true);

        // Execute the gc with sweep
        cluster1.gc.collectGarbage(false);

        Assert.assertEquals(true, Sets.symmetricDifference(Sets.union(cluster1.getInitBlobs(), cluster2.getInitBlobs()),
            cluster1.getExistingBlobIds()).isEmpty());
    }

    @Test
    public void testGCStats() throws Exception {
        log.debug("Running testGCStats()");
        // Only run the mark phase on both the clusters to get the stats
        cluster1.gc.collectGarbage(true);
        cluster2.gc.collectGarbage(true);
    
        Set<String> actualRepoIds = Sets.newHashSet();
        actualRepoIds.add(cluster1.repoId);
        actualRepoIds.add(cluster2.repoId);
    
        Set<Integer> actualNumBlobs = Sets.newHashSet();
        actualNumBlobs.add(cluster1.initBlobs.size());
        actualNumBlobs.add(cluster2.initBlobs.size());
    
        List<GarbageCollectionRepoStats> statsList = cluster1.gc.getStats();
        Set<Integer> observedNumBlobs = Sets.newHashSet();
        Set<String> observedRepoIds = Sets.newHashSet();
        for (GarbageCollectionRepoStats stat : statsList) {
            observedNumBlobs.add(stat.getNumLines());
            observedRepoIds.add(stat.getRepositoryId());
            Assert.assertTrue(stat.getStartTime() <= stat.getEndTime());
            if (stat.getRepositoryId().equals(cluster1.repoId)) {
                Assert.assertTrue(stat.isLocal());
            }
        }
    
        Assert.assertTrue(Sets.difference(actualNumBlobs, observedNumBlobs).isEmpty());
        Assert.assertTrue(Sets.difference(actualRepoIds, observedRepoIds).isEmpty());
    }

    @Test
    // GC should fail
    public void testOnly1ClusterMark() throws Exception {
        log.debug("Running testOnly1ClusterMark()");

        // Only run the mark phase on one cluster
        cluster1.gc.collectGarbage(true);

        // Execute the gc with sweep
        cluster1.gc.collectGarbage(false);

        Set<String> existing = cluster1.getExistingBlobIds();
        log.debug("Existing blobs {}", existing);
        Assert.assertTrue((cluster1.getInitBlobs().size() + cluster2.getInitBlobs().size()) <= existing.size());
        Assert.assertTrue(existing.containsAll(cluster2.getInitBlobs()));
        Assert.assertTrue(existing.containsAll(cluster1.getInitBlobs()));
    }

    @Test
    public void testRepeatedMarkWithSweep() throws Exception {
        log.debug("Running testRepeatedMarkWithSweep()");

        // Only run the mark phase on one cluster
        cluster1.gc.collectGarbage(true);
        cluster2.gc.collectGarbage(true);
        cluster2.gc.collectGarbage(true);

        // Execute the gc with sweep
        cluster2.gc.collectGarbage(false);

        Assert.assertTrue(Sets.symmetricDifference(
            Sets.union(cluster1.getInitBlobs(), cluster2.getInitBlobs()),
            cluster1.getExistingBlobIds()).isEmpty());
    }

    @After
    public void tearDown() throws Exception {
        DataStoreUtils.time = -1;
        cluster1.getDocumentNodeStore().dispose();
        cluster2.getDocumentNodeStore().dispose();
    }

    protected DataStoreBlobStore getBlobStore(File root) throws Exception {
        return DataStoreUtils.getBlobStore(root);
    }

    public class Cluster {
        private DocumentNodeStore ds;
        private int seed;
        private BlobGarbageCollector gc;
        private Date startDate;
        private String repoId;
        private Set<String> initBlobs = new HashSet<String>();

        protected Set<String> getInitBlobs() {
            return initBlobs;
        }

        public Cluster(final DocumentNodeStore ds, final String repoId, int seed)
                throws IOException {
            this.ds = ds;
            this.gc = new MarkSweepGarbageCollector(
                            new DocumentBlobReferenceRetriever(ds),
                            (GarbageCollectableBlobStore) ds.getBlobStore(),
                            MoreExecutors.sameThreadExecutor(),
                            "./target", 5, 0, repoId);
            this.startDate = new Date();
            this.seed = seed;
            this.repoId = repoId;
        }

        /**
         * Creates the setup load with deletions.
         * 
         * @throws Exception
         */
        public void init() throws Exception {
            NodeBuilder a = ds.getRoot().builder();

            int number = 10;
            // track the number of the assets to be deleted
            List<Integer> deletes = Lists.newArrayList();
            Random rand = new Random(47);
            for (int i = 0; i < 5; i++) {
                int n = rand.nextInt(number);
                if (!deletes.contains(n)) {
                    deletes.add(n);
                }
            }
            for (int i = 0; i < number; i++) {
                Blob b = ds.createBlob(randomStream(i + seed, 16516));
                if (!deletes.contains(i)) {
                    Iterator<String> idIter =
                            ((GarbageCollectableBlobStore) ds.getBlobStore())
                                    .resolveChunks(b.toString());
                    while (idIter.hasNext()) {
                        initBlobs.add(idIter.next());
                    }
                }
                a.child("c" + i).setProperty("x", b);
            }
            ds.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            a = ds.getRoot().builder();
            for (int id : deletes) {
                a.child("c" + id).remove();
                ds.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            }
            long maxAge = 10; // hours
            // 1. Go past GC age and check no GC done as nothing deleted
            clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(maxAge));

            VersionGarbageCollector vGC = ds.getVersionGarbageCollector();
            VersionGCStats stats = vGC.gc(0, TimeUnit.MILLISECONDS);
            Assert.assertEquals(deletes.size(), stats.deletedDocGCCount);
            sleep();
        }

        private HashSet<String> addNodeSpecialChars() throws Exception {
            List<String> specialCharSets =
                Lists.newArrayList("q\\%22afdg\\%22", "a\nbcd", "a\n\rabcd", "012\\efg" );
            HashSet<String> set = new HashSet<String>();
            NodeBuilder a = ds.getRoot().builder();
            for (int i = 0; i < specialCharSets.size(); i++) {
                Blob b = ds.createBlob(randomStream(i, 18432));
                NodeBuilder n = a.child("cspecial");
                n.child(specialCharSets.get(i)).setProperty("x", b);
                Iterator<String> idIter =
                    ((GarbageCollectableBlobStore) ds.getBlobStore())
                        .resolveChunks(b.toString());
                set.addAll(Lists.newArrayList(idIter));
            }
            ds.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            return set;
        }


        public Set<String> getExistingBlobIds() throws Exception {
            GarbageCollectableBlobStore store = (GarbageCollectableBlobStore) ds.getBlobStore();
            Iterator<String> cur = store.getAllChunkIds(0);

            Set<String> existing = Sets.newHashSet();
            while (cur.hasNext()) {
                existing.add(cur.next());
            }
            return existing;
        }

        public DataStore getDataStore() {
            return ((DataStoreBlobStore) ds.getBlobStore()).getDataStore();
        }

        public Date getDate() {
            return startDate;
        }
        
        public DocumentNodeStore getDocumentNodeStore() {
            return ds;
        }
    }

    protected void sleep() throws InterruptedException {
    }
}

