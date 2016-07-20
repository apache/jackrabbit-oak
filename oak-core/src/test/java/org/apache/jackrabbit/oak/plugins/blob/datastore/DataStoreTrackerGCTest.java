/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.BlobTrackingStore;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.identifier.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.union;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.io.FileUtils.forceDelete;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.getBlobStore;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils
    .SharedStoreRecordType.REPOSITORY;
import static org.apache.jackrabbit.oak.plugins.document.Revision.getCurrentTimestamp;
import static org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY;
import static org.apache.jackrabbit.oak.spi.commit.EmptyHook.INSTANCE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeThat;

/**
 */
public class DataStoreTrackerGCTest {
    private Clock clock;
    private File blobStoreRoot;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @BeforeClass
    public static void check() {
        File root = new File("./target/blobstore");
        try {
            BlobStore store = getBlobStore(root);
            assumeThat(store, instanceOf(BlobTrackingStore.class));
        } catch (Exception e) {
            assumeNoException(e);
        } finally {
            try {
                forceDelete(root);
            } catch (IOException e) {
            }
        }
    }

    @Before
    public void setup() throws IOException, InterruptedException {
        this.clock = getTestClock();
        this.blobStoreRoot = folder.newFolder("blobstore");
    }

    @Test
    public void gc() throws Exception {
        Cluster cluster = new Cluster("cluster1");
        BlobStore s = cluster.blobStore;
        BlobIdTracker tracker = (BlobIdTracker) ((BlobTrackingStore) s).getTracker();
        DataStoreState state = init(cluster.nodeStore, 0);
        ScheduledFuture<?> scheduledFuture = newSingleThreadScheduledExecutor()
            .schedule(tracker.new SnapshotJob(), 0, MILLISECONDS);
        scheduledFuture.get();
        // All blobs added should be tracked now
        assertEquals(state.blobsAdded, retrieveTracked(tracker));

        cluster.gc.collectGarbage(false);
        Set<String> existingAfterGC = iterate(s);
        // Check the state of the blob store after gc
        assertEquals(state.blobsPresent, existingAfterGC);
        // Tracked blobs should reflect deletions after gc
        assertEquals(state.blobsPresent, retrieveTracked(tracker));
    }

    @Test
    public void gcColdStart() throws Exception {
        Cluster cluster = new Cluster("cluster1");
        BlobStore s = cluster.blobStore;
        BlobIdTracker tracker = (BlobIdTracker) ((BlobTrackingStore) s).getTracker();
        DataStoreState state = init(cluster.nodeStore, 0);

        // No blobs should be found now as snapshot not done
        assertNotEquals(state.blobsAdded, retrieveTracked(tracker));

        cluster.gc.collectGarbage(false);
        Set<String> existingAfterGC = iterate(s);
        // Check the state of the blob store after gc
        assertEquals(state.blobsPresent, existingAfterGC);
        // Tracked blobs should reflect deletions after gc
        assertEquals(state.blobsPresent, retrieveTracked(tracker));
    }

    private HashSet<String> addNodeSpecialChars(DocumentNodeStore ds) throws Exception {
        List<String> specialCharSets =
            Lists.newArrayList("q\\%22afdg\\%22", "a\nbcd", "a\n\rabcd", "012\\efg" );
        HashSet<String> set = new HashSet<String>();
        NodeBuilder a = ds.getRoot().builder();
        int toBeDeleted = 0;
        for (int i = 0; i < specialCharSets.size(); i++) {
            Blob b = ds.createBlob(randomStream(i, 18432));
            NodeBuilder n = a.child("cspecial" + i);
            n.child(specialCharSets.get(i)).setProperty("x", b);
            Iterator<String> idIter =
                ((GarbageCollectableBlobStore) ds.getBlobStore())
                    .resolveChunks(b.toString());
            List<String> ids = Lists.newArrayList(idIter);
            if (toBeDeleted != i) {
                set.addAll(ids);
            }
        }
        ds.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Delete one node again
        a = ds.getRoot().builder();
        a.child("cspecial" + 0).remove();
        ds.merge(a, INSTANCE, EMPTY);
        long maxAge = 10; // hours
        // 1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(clock.getTime() + MINUTES.toMillis(maxAge));

        VersionGarbageCollector vGC = ds.getVersionGarbageCollector();
        VersionGarbageCollector.VersionGCStats stats = vGC.gc(0, MILLISECONDS);
        return set;
    }

    @Test
    public void gcForcedRetrieve() throws Exception {
        Cluster cluster = new Cluster("cluster1");
        BlobStore s = cluster.blobStore;
        BlobIdTracker tracker = (BlobIdTracker) ((BlobTrackingStore) s).getTracker();
        DataStoreState state = init(cluster.nodeStore, 0);
        ScheduledFuture<?> scheduledFuture = newSingleThreadScheduledExecutor()
            .schedule(tracker.new SnapshotJob(), 0, MILLISECONDS);
        scheduledFuture.get();
        // All blobs added should be tracked now
        assertEquals(state.blobsAdded, retrieveTracked(tracker));

        // Do addition and deletion which would not have been tracked as yet
        Set<String> newBlobs = addNodeSpecialChars(cluster.nodeStore);
        state.blobsAdded.addAll(newBlobs);
        state.blobsPresent.addAll(newBlobs);

        // The new blobs should not be found now as new snapshot not done
        assertEquals(Sets.difference(state.blobsAdded, retrieveTracked(tracker)), newBlobs);

        //force gc to retrieve blob ids from datastore
        cluster.gc.collectGarbage(false, true);
        Set<String> existingAfterGC = iterate(s);

        // Check the state of the blob store after gc
        assertEquals(state.blobsPresent, existingAfterGC);
        // Tracked blobs should reflect deletions after gc and also the additions after
        assertEquals(state.blobsPresent, retrieveTracked(tracker));
    }

    @Test
    public void gcWithInlined() throws Exception {
        Cluster cluster = new Cluster("cluster1");
        BlobStore s = cluster.blobStore;
        BlobIdTracker tracker = (BlobIdTracker) ((BlobTrackingStore) s).getTracker();
        DataStoreState state = init(cluster.nodeStore, 0);
        addInlined(cluster.nodeStore);
        ScheduledFuture<?> scheduledFuture = newSingleThreadScheduledExecutor()
            .schedule(tracker.new SnapshotJob(), 0, MILLISECONDS);
        scheduledFuture.get();
        // All blobs added should be tracked now
        assertEquals(state.blobsAdded, retrieveTracked(tracker));

        cluster.gc.collectGarbage(false);
        Set<String> existingAfterGC = iterate(s);
        // Check the state of the blob store after gc
        assertEquals(state.blobsPresent, existingAfterGC);
        // Tracked blobs should reflect deletions after gc
        assertEquals(state.blobsPresent, retrieveTracked(tracker));
    }

    private HashSet<String> addInlined(NodeStore nodeStore) throws Exception {
        HashSet<String> set = new HashSet<String>();
        NodeBuilder a = nodeStore.getRoot().builder();
        int number = 4;
        for (int i = 0; i < number; i++) {
            Blob b = nodeStore.createBlob(randomStream(i, 90));
            a.child("cinline" + i).setProperty("x", b);
        }
        nodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return set;
    }

    @Test
    public void differentCluster() throws Exception {
        // Add blobs to cluster1
        Cluster cluster1 = new Cluster("cluster1");
        BlobStore s1 = cluster1.blobStore;
        BlobIdTracker tracker1 = (BlobIdTracker) ((BlobTrackingStore) s1).getTracker();
        DataStoreState state1 = init(cluster1.nodeStore, 0);
        ScheduledFuture<?> scheduledFuture1 = newSingleThreadScheduledExecutor()
            .schedule(tracker1.new SnapshotJob(), 0, MILLISECONDS);
        scheduledFuture1.get();
        // All blobs added should be tracked now
        assertEquals(state1.blobsAdded, retrieveTracked(tracker1));

        // Add blobs to cluster1
        Cluster cluster2 = new Cluster("cluster2");
        BlobStore s2 = cluster2.blobStore;
        BlobIdTracker tracker2 = (BlobIdTracker) ((BlobTrackingStore) s2).getTracker();
        DataStoreState state2 = init(cluster2.nodeStore, 0);
        ScheduledFuture<?> scheduledFuture2 = newSingleThreadScheduledExecutor()
            .schedule(tracker2.new SnapshotJob(), 0, MILLISECONDS);
        scheduledFuture2.get();

        // All blobs added should be tracked now
        assertEquals(state2.blobsAdded, retrieveTracked(tracker2));
        cluster2.gc.collectGarbage(true);

        // do a gc on cluster1 with sweep
        cluster1.gc.collectGarbage(false);
        Set<String> existingAfterGC = iterate(s1);

        // Check the state of the blob store after gc
        assertEquals(
            union(state1.blobsPresent, state2.blobsPresent), existingAfterGC);
        // Tracked blobs should reflect deletions after gc
        assertEquals(
            union(state1.blobsPresent, state2.blobsPresent),
            retrieveTracked(tracker1));

    }

    private Set<String> iterate(BlobStore blobStore) throws Exception {
        Iterator<String> cur = ((GarbageCollectableBlobStore) blobStore).getAllChunkIds(0);

        Set<String> existing = newHashSet();
        while (cur.hasNext()) {
            existing.add(cur.next());
        }
        return existing;
    }

    private static Set<String> retrieveTracked(BlobTracker tracker) throws IOException {
        Set<String> retrieved = newHashSet();
        Iterator<String> iter = tracker.get();
        while(iter.hasNext()) {
            retrieved.add(iter.next());
        }
        closeQuietly((Closeable)iter);
        return retrieved;
    }

    public DataStoreState init(DocumentNodeStore s, int idStart) throws Exception {
        NodeBuilder a = s.getRoot().builder();

        int number = 10;
        int maxDeleted = 5;
        // track the number of the assets to be deleted
        List<Integer> processed = Lists.newArrayList();
        Random rand = new Random(47);
        for (int i = idStart; i < idStart + maxDeleted; i++) {
            int n = rand.nextInt(number);
            if (!processed.contains(n)) {
                processed.add(n);
            }
        }
        DataStoreState state = new DataStoreState();
        for (int i = idStart; i < idStart + number; i++) {
            Blob b = s.createBlob(randomStream(i, 16516));
            Iterator<String> idIter =
                ((GarbageCollectableBlobStore) s.getBlobStore())
                    .resolveChunks(b.toString());
            while (idIter.hasNext()) {
                String chunk = idIter.next();
                state.blobsAdded.add(chunk);
                if (!processed.contains(i)) {
                    state.blobsPresent.add(chunk);
                }
            }
            a.child("c" + i).setProperty("x", b);
            // Add a duplicated entry
            if (i == idStart) {
                a.child("cdup").setProperty("x", b);
            }
        }
        s.merge(a, INSTANCE, EMPTY);


        a = s.getRoot().builder();
        for (int id : processed) {
            a.child("c" + id).remove();
            s.merge(a, INSTANCE, EMPTY);
        }
        long maxAge = 10; // hours
        // 1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(clock.getTime() + MINUTES.toMillis(maxAge));

        VersionGarbageCollector vGC = s.getVersionGarbageCollector();
        VersionGarbageCollector.VersionGCStats stats = vGC.gc(0, MILLISECONDS);

        return state;
    }

    class Cluster implements Closeable {
        DocumentNodeStore nodeStore;
        BlobStore blobStore;
        MarkSweepGarbageCollector gc;
        String repoId;
        BlobIdTracker tracker;

        public Cluster(String clusterId) throws Exception {
            blobStore = getBlobStore(blobStoreRoot);
            nodeStore = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setDocumentStore(new MemoryDocumentStore())
                .setBlobStore(blobStore)
                .getNodeStore();
            repoId = ClusterRepositoryInfo.getOrCreateId(nodeStore);
            nodeStore.runBackgroundOperations();

            ((SharedDataStore) blobStore).addMetadataRecord(
                new ByteArrayInputStream(new byte[0]),
                REPOSITORY.getNameFromId(repoId));

            String trackerRoot = folder.newFolder(clusterId).getAbsolutePath();
            tracker = new BlobIdTracker(trackerRoot,
                repoId, 86400, (SharedDataStore) blobStore);
            // add the tracker to the blobStore
            ((BlobTrackingStore) blobStore).addTracker(tracker);

            // initialized the GC
            gc = new MarkSweepGarbageCollector(
                new DocumentBlobReferenceRetriever(nodeStore),
                (GarbageCollectableBlobStore) blobStore, newSingleThreadExecutor(),
                folder.newFolder("gc" + clusterId).getAbsolutePath(), 5, 0, repoId);
        }

        public void close() throws IOException {
            nodeStore.dispose();
        }

    }

    protected Clock getTestClock() throws InterruptedException {
        Clock clock = new Clock.Virtual();
        clock.waitUntil(getCurrentTimestamp());
        return clock;
    }

    static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    private class DataStoreState {
        Set<String> blobsAdded = newHashSet();
        Set<String> blobsPresent = newHashSet();
    }
}
