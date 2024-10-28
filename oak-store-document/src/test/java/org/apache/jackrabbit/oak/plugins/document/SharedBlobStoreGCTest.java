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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.guava.common.collect.Sets;
import org.apache.jackrabbit.guava.common.util.concurrent.MoreExecutors;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.blob.BlobGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.GarbageCollectionRepoStats;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
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

import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.randomStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for gc in a shared data store among heterogeneous oak node stores.
 */
public class SharedBlobStoreGCTest {
    private static final Logger log = LoggerFactory.getLogger(SharedBlobStoreGCTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    protected Cluster cluster1;
    protected Cluster cluster2;
    private Clock clock;
    protected boolean retryCreation = false;
    private File rootFolder;

    @Before
    public void setUp() throws Exception {
        log.debug("In setUp()");

        clock = new Clock.Virtual();
        clock.waitUntil(Revision.getCurrentTimestamp());
        DataStoreUtils.time = clock.getTime();

        rootFolder = folder.newFolder();
        BlobStore blobeStore1 = getBlobStore(rootFolder);
        DocumentNodeStore ds1 = new DocumentMK.Builder()
                .setAsyncDelay(0)
                .setDocumentStore(new MemoryDocumentStore())
                .setBlobStore(blobeStore1)
                .clock(clock)
                .getNodeStore();
        String repoId1 = ClusterRepositoryInfo.getOrCreateId(ds1);
        // Register the unique repository id in the data store
        ((SharedDataStore) blobeStore1).setRepositoryId(repoId1);

        BlobStore blobeStore2 = getBlobStore(rootFolder);
        DocumentNodeStore ds2 = new DocumentMK.Builder()
                .setAsyncDelay(0)
                .setDocumentStore(new MemoryDocumentStore())
                .setBlobStore(blobeStore2)
                .clock(clock)
                .getNodeStore();
        String repoId2 = ClusterRepositoryInfo.getOrCreateId(ds2);
        // Register the unique repository id in the data store
        ((SharedDataStore) blobeStore2).setRepositoryId(repoId2);

        cluster1 = new Cluster(ds1, repoId1, 20);
        cluster1.init();
        log.debug("Initialized {}", cluster1);

        cluster2 = new Cluster(ds2, repoId2, 100);
        cluster2.init();
        log.debug("Initialized {}", cluster2);
    }

    @Test
    public void testGC() throws Exception {
        log.debug("Running testGC()");
        // Only run the mark phase on both the clusters
        cluster1.gc.collectGarbage(true);
        cluster2.gc.collectGarbage(true);

        // Execute the gc with sweep
        cluster1.gc.collectGarbage(false);

        assertTrue(Sets.symmetricDifference(
                Sets.union(cluster1.getInitBlobs(), cluster2.getInitBlobs()),
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

        assertTrue(Sets.symmetricDifference(
                Sets.union(cluster1.getInitBlobs(), cluster2.getInitBlobs()),
                cluster1.getExistingBlobIds()).isEmpty());
    }

    @Test
    public void testGCStats() throws Exception {
        log.debug("Running testGCStats()");
        // Only run the mark phase on both the clusters to get the stats
        cluster1.gc.collectGarbage(true);
        cluster2.gc.collectGarbage(true);
    
        Set<String> actualRepoIds = new HashSet<>();
        actualRepoIds.add(cluster1.repoId);
        actualRepoIds.add(cluster2.repoId);
    
        Set<Integer> actualNumBlobs = new HashSet<>();
        actualNumBlobs.add(cluster1.initBlobs.size());
        actualNumBlobs.add(cluster2.initBlobs.size());
    
        List<GarbageCollectionRepoStats> statsList = cluster1.gc.getStats();
        Set<Integer> observedNumBlobs = new HashSet<>();
        Set<String> observedRepoIds = new HashSet<>();
        for (GarbageCollectionRepoStats stat : statsList) {
            observedNumBlobs.add(stat.getNumLines());
            observedRepoIds.add(stat.getRepositoryId());
            assertTrue(stat.getStartTime() <= stat.getEndTime());
            if (stat.getRepositoryId().equals(cluster1.repoId)) {
                assertTrue(stat.isLocal());
            }
        }
    
        assertTrue(Sets.difference(actualNumBlobs, observedNumBlobs).isEmpty());
        assertTrue(Sets.difference(actualRepoIds, observedRepoIds).isEmpty());
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
        assertTrue((cluster1.getInitBlobs().size() + cluster2.getInitBlobs().size()) <= existing.size());
        assertTrue(existing.containsAll(cluster2.getInitBlobs()));
        assertTrue(existing.containsAll(cluster1.getInitBlobs()));
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

        assertTrue(Sets.symmetricDifference(
            Sets.union(cluster1.getInitBlobs(), cluster2.getInitBlobs()),
            cluster1.getExistingBlobIds()).isEmpty());
    }

    @Test
    public void testMarkOnCloned() throws Exception {
        log.debug("Running testMarkOnCloned()");

        BlobStore blobeStore3 = getBlobStore(rootFolder);
        DocumentNodeStore ds3 = new DocumentMK.Builder()
            .setAsyncDelay(0)
            .setDocumentStore(new MemoryDocumentStore())
            .setBlobStore(blobeStore3)
            .clock(clock)
            .getNodeStore();
        NodeBuilder a = ds3.getRoot().builder();
        a.child(":clusterConfig").setProperty(":clusterId", cluster2.repoId);

        Cluster cluster3 = new Cluster(ds3, cluster2.repoId, 120);
        cluster3.init();
        log.debug("Initialized {}", cluster3);

        // run the mark phase on other repositories
        cluster1.gc.collectGarbage(true);
        cluster2.gc.collectGarbage(true);

        // Execute the gc with sweep
        cluster3.gc.collectGarbage(false);

        Set<String> existing = cluster1.getExistingBlobIds();
        log.debug("Existing blobs {}", existing);
        assertTrue(existing.containsAll(cluster2.getInitBlobs()));
        assertTrue(existing.containsAll(cluster1.getInitBlobs()));
        assertTrue(existing.containsAll(cluster3.getInitBlobs()));
    }

    @Test
    public void testGCStatsOnCloned() throws Exception {
        log.debug("Running testGCStatsOnCloned()");
        BlobStore blobeStore3 = getBlobStore(rootFolder);
        DocumentNodeStore ds3 = new DocumentMK.Builder()
            .setAsyncDelay(0)
            .setDocumentStore(new MemoryDocumentStore())
            .setBlobStore(blobeStore3)
            .clock(clock)
            .getNodeStore();
        NodeBuilder a = ds3.getRoot().builder();
        a.child(":clusterConfig").setProperty(":clusterId", cluster2.repoId);
        Cluster cluster3 = new Cluster(ds3, cluster2.repoId, 120);
        cluster3.init();

        Set<String> actualRepoIds = new HashSet<>();
        actualRepoIds.add(cluster1.repoId);
        actualRepoIds.add(cluster2.repoId);

        log.debug("Initialized {}", cluster3);

        Set<String> observedRepoIds = new HashSet<>();
        List<GarbageCollectionRepoStats> statsList = cluster1.gc.getStats();
        for (GarbageCollectionRepoStats stat : statsList) {
            assertEquals(0, stat.getNumLines());
            observedRepoIds.add(stat.getRepositoryId());
            if (stat.getRepositoryId().equals(cluster1.repoId)) {
                assertTrue(stat.isLocal());
            }
        }

        assertTrue(Sets.difference(actualRepoIds, observedRepoIds).isEmpty());

        // Only run the mark phase on all the nodes to get the stats
        cluster1.gc.collectGarbage(true);
        cluster2.gc.collectGarbage(true);
        cluster3.gc.collectGarbage(true);


        Set<Integer> actualNumBlobs = new HashSet<>();
        actualNumBlobs.add(cluster1.initBlobs.size());
        actualNumBlobs.add(cluster2.initBlobs.size());
        actualNumBlobs.add(cluster3.initBlobs.size());

        statsList = cluster1.gc.getStats();
        Set<Integer> observedNumBlobs = new HashSet<>();
        observedRepoIds = new HashSet<>();
        for (GarbageCollectionRepoStats stat : statsList) {
            observedNumBlobs.add(stat.getNumLines());
            observedRepoIds.add(stat.getRepositoryId());
            assertTrue(stat.getStartTime() <= stat.getEndTime());
            if (stat.getRepositoryId().equals(cluster1.repoId)) {
                assertTrue(stat.isLocal());
            }
        }

        assertTrue(Sets.difference(actualNumBlobs, observedNumBlobs).isEmpty());
        assertTrue(Sets.difference(actualRepoIds, observedRepoIds).isEmpty());
    }

    @After
    public void tearDown() throws Exception {
        DataStoreUtils.time = -1;
        if (cluster1 != null) {
            cluster1.getDocumentNodeStore().dispose();
        }
        if (cluster2 != null) {
            cluster2.getDocumentNodeStore().dispose();
        }
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
                            MoreExecutors.newDirectExecutorService(),
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
            List<Integer> deletes = new ArrayList<>();
            Random rand = new Random(47);
            for (int i = 0; i < 5; i++) {
                int n = rand.nextInt(number);
                if (!deletes.contains(n)) {
                    deletes.add(n);
                }
            }
            for (int i = 0; i < number; i++) {
                Blob b = null;
                // Simple retry
                if (retryCreation) {
                    for (int retry = 0; retry < 5; retry++) {
                        try {
                            b = ds.createBlob(randomStream(i + seed, 16516));
                            if (b != null) {
                                break;
                            }
                        } catch (Exception e) {
                            if (retry >= 5) {
                                log.warn("Error in writing record", e);
                                throw e;
                            } else {
                                log.warn("Error in writing record...retrying", e);
                                Thread.sleep(100);
                            }
                        }
                    }
                } else {
                    b = ds.createBlob(randomStream(i + seed, 16516));
                }

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
            assertEquals(deletes.size(), stats.deletedDocGCCount);
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
                set.addAll(CollectionUtils.toList(idIter));
            }
            ds.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            return set;
        }


        public Set<String> getExistingBlobIds() throws Exception {
            GarbageCollectableBlobStore store = (GarbageCollectableBlobStore) ds.getBlobStore();
            Iterator<String> cur = store.getAllChunkIds(0);

            Set<String> existing = new HashSet<>();
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

