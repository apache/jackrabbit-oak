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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import ch.qos.logback.classic.Level;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.blob.BlobTrackingStore;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.TestUtils;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.base.StandardSystemProperty.JAVA_IO_TMPDIR;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.union;
import static java.lang.String.valueOf;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.io.FileUtils.forceDelete;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.readStringsAsSet;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeStrings;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.createFDS;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.getBlobStore;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils
    .SharedStoreRecordType.REPOSITORY;
import static org.apache.jackrabbit.oak.plugins.document.Revision.getCurrentTimestamp;
import static org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY;
import static org.apache.jackrabbit.oak.spi.commit.EmptyHook.INSTANCE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
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
        TestUtils.setRevisionClock(clock);
        this.blobStoreRoot = folder.newFolder("blobstore");
    }

    @AfterClass
    public static void resetClock() {
        TestUtils.resetRevisionClockToDefault();
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
    public void gcReconcileActiveDeletion() throws Exception {
        Cluster cluster = new Cluster("cluster1");
        BlobStore s = cluster.blobStore;
        BlobIdTracker tracker = (BlobIdTracker) ((BlobTrackingStore) s).getTracker();
        DataStoreState state = init(cluster.nodeStore, 0);

        // Simulate creation and active deletion after init without version gc to enable references to hang around
        List<String> addlAdded = doActiveDelete(cluster.nodeStore,
            (DataStoreBlobStore) cluster.blobStore, tracker, folder,0, 2);
        List<String> addlPresent = Lists.newArrayList(addlAdded.get(2), addlAdded.get(3));
        List<String> activeDeleted = Lists.newArrayList(addlAdded.get(0), addlAdded.get(1));
        state.blobsPresent.addAll(addlPresent);
        state.blobsAdded.addAll(addlPresent);

        cluster.gc.collectGarbage(false);

        Set<String> existingAfterGC = iterate(s);
        // Check the state of the blob store after gc
        assertEquals(state.blobsPresent, existingAfterGC);
        // Tracked blobs should reflect deletions after gc
        assertEquals(state.blobsPresent, retrieveTracked(tracker));
        // Check that the delete tracker is refreshed
        assertEquals(Sets.newHashSet(activeDeleted), retrieveActiveDeleteTracked(tracker, folder));
    }

    @Test
    public void gcReconcileActiveDeletionMarkCleared() throws Exception {
        Cluster cluster = new Cluster("cluster1");
        BlobStore s = cluster.blobStore;
        BlobIdTracker tracker = (BlobIdTracker) ((BlobTrackingStore) s).getTracker();
        // Simulate active deletion before the init to ensure that the references also cleared
        List<String> addlAdded = doActiveDelete(cluster.nodeStore,
            (DataStoreBlobStore) cluster.blobStore, tracker, folder,0, 2);
        DataStoreState state = init(cluster.nodeStore, 0);

        // Force a snapshot of the tracker to refresh
        File f = folder.newFile();
        tracker.remove(f, BlobTracker.Options.ACTIVE_DELETION);

        List<String> addlPresent = Lists.newArrayList(addlAdded.get(2), addlAdded.get(3));
        List<String> activeDeleted = Lists.newArrayList(addlAdded.get(0), addlAdded.get(1));
        state.blobsPresent.addAll(addlPresent);
        state.blobsAdded.addAll(addlPresent);

        cluster.gc.collectGarbage(false);
        Set<String> existingAfterGC = iterate(s);
        // Check the state of the blob store after gc
        assertEquals(state.blobsPresent, existingAfterGC);
        // Tracked blobs should reflect deletions after gc
        assertEquals(state.blobsPresent, retrieveTracked(tracker));
        // Check that the delete tracker is refreshed
        assertEquals(Sets.newHashSet(), retrieveActiveDeleteTracked(tracker, folder));
    }

    @Test
    public void consistencyCheckNoActiveDeletion() throws Exception {
        File tmpFolder = folder.newFolder();
        String previousTmp = System.setProperty(JAVA_IO_TMPDIR.key(), tmpFolder.getAbsolutePath());

        try {
            Cluster cluster = new Cluster("cluster1");
            BlobStore s = cluster.blobStore;
            BlobIdTracker tracker = (BlobIdTracker) ((BlobTrackingStore) s).getTracker();
            DataStoreState state = init(cluster.nodeStore, 0);

            // Since datastore in consistent state and only active deletions the missing list should be empty
            assertEquals(0, cluster.gc.checkConsistency());
            assertTrue(FileUtils.listFiles(tmpFolder, null, true).size() == 0);
        } finally {
            if (previousTmp != null) {
                System.setProperty(JAVA_IO_TMPDIR.key(), previousTmp);
            } else {
                System.clearProperty(JAVA_IO_TMPDIR.key());
            }
        }
    }

    @Test
    public void consistencyCheckOnlyActiveDeletion() throws Exception {
        Cluster cluster = new Cluster("cluster1");
        BlobStore s = cluster.blobStore;
        BlobIdTracker tracker = (BlobIdTracker) ((BlobTrackingStore) s).getTracker();
        DataStoreState state = init(cluster.nodeStore, 0);

        List<String> addlAdded = doActiveDelete(cluster.nodeStore,
            (DataStoreBlobStore) cluster.blobStore, tracker, folder,0, 2);
        List<String> addlPresent = Lists.newArrayList(addlAdded.get(2), addlAdded.get(3));
        List<String> activeDeleted = Lists.newArrayList(addlAdded.get(0), addlAdded.get(1));
        state.blobsPresent.addAll(addlPresent);
        state.blobsAdded.addAll(addlPresent);

        // Since datastore in consistent state and only active deletions the missing list should be empty
        assertEquals(0, cluster.gc.checkConsistency());
    }

    @Test
    public void consistencyCheckDeletedWithActiveDeletion() throws Exception {
        Cluster cluster = new Cluster("cluster1");
        BlobStore s = cluster.blobStore;
        BlobIdTracker tracker = (BlobIdTracker) ((BlobTrackingStore) s).getTracker();
        DataStoreState state = init(cluster.nodeStore, 0);

        // Directly delete from blobstore
        ArrayList<String> blobs = Lists.newArrayList(state.blobsPresent);
        String removedId = blobs.remove(0);
        ((DataStoreBlobStore) s).deleteChunks(Lists.newArrayList(removedId), 0);
        state.blobsPresent = Sets.newHashSet(blobs);
        File f = folder.newFile();
        writeStrings(Lists.newArrayList(removedId).iterator(), f, false);
        tracker.remove(f);

        List<String> addlAdded = doActiveDelete(cluster.nodeStore,
            (DataStoreBlobStore) cluster.blobStore, tracker, folder,0, 2);
        List<String> addlPresent = Lists.newArrayList(addlAdded.get(2), addlAdded.get(3));
        state.blobsPresent.addAll(addlPresent);
        state.blobsAdded.addAll(addlPresent);

        // Only the missing blob should be reported and not the active deletions
        assertEquals(1, cluster.gc.checkConsistency());
    }

    private List<String> doActiveDelete(NodeStore nodeStore, DataStoreBlobStore blobStore, BlobIdTracker tracker,
        TemporaryFolder folder, int delIdx, int num) throws Exception {
        List<String> set = Lists.newArrayList();
        NodeBuilder a = nodeStore.getRoot().builder();
        int number = 4;
        for (int i = 0; i < number; i++) {
            Blob b = nodeStore.createBlob(randomStream(i, 90));
            a.child("cactive" + i).setProperty("x", b);
            set.add(b.getContentIdentity());
        }
        nodeStore.merge(a, INSTANCE, EMPTY);

        List<String> deleted = Lists.newArrayList();

        //a = nodeStore.getRoot().builder();
        for(int idx = delIdx; idx < delIdx + num; idx++) {
            blobStore.deleteChunks(Lists.newArrayList(set.get(idx)), 0);
            deleted.add(set.get(idx));
            a.child("cactive" + idx).remove();
        }
        nodeStore.merge(a, INSTANCE, EMPTY);

        File f = folder.newFile();
        writeStrings(deleted.iterator(), f, false);

        tracker.remove(f, BlobTracker.Options.ACTIVE_DELETION);
        return set;
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

        // Create a snapshot
        ScheduledFuture<?> scheduledFuture = newSingleThreadScheduledExecutor()
            .schedule(tracker.new SnapshotJob(), 0, MILLISECONDS);
        scheduledFuture.get();
        // Tracked blobs should reflect deletions after gc and the deleted should not get resurrected
        assertEquals(state.blobsPresent, retrieveTracked(tracker));
    }

    private static Set<String> retrieveActiveDeleteTracked(BlobIdTracker tracker, TemporaryFolder folder) throws IOException {
        File f = folder.newFile();
        Set<String> retrieved = readStringsAsSet(
            new FileInputStream(tracker.getDeleteTracker().retrieve(f.getAbsolutePath())), false);
        return retrieved;
    }

    private static List<String> range(int min, int max) {
        List<String> list = newArrayList();
        for (int i = min; i <= max; i++) {
            list.add(valueOf(i));
        }
        return list;
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
        long maxAge = 10; // minutes
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

        // Create a snapshot
        scheduledFuture = newSingleThreadScheduledExecutor()
            .schedule(tracker.new SnapshotJob(), 0, MILLISECONDS);
        scheduledFuture.get();
        // Tracked blobs should reflect deletions after gc and the deleted should not get resurrected
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
            Blob b = nodeStore.createBlob(randomStream(i, 40));
            a.child("cinline" + i).setProperty("x", b);
        }
        nodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return set;
    }

    private void clusterGCInternal(Cluster cluster1, Cluster cluster2, boolean same) throws Exception {
        BlobStore s1 = cluster1.blobStore;
        BlobIdTracker tracker1 = (BlobIdTracker) ((BlobTrackingStore) s1).getTracker();
        DataStoreState state1 = init(cluster1.nodeStore, 0);
        cluster1.nodeStore.runBackgroundOperations();
        ScheduledFuture<?> scheduledFuture1 = newSingleThreadScheduledExecutor()
            .schedule(tracker1.new SnapshotJob(), 0, MILLISECONDS);
        scheduledFuture1.get();

        // Add blobs to cluster1
        BlobStore s2 = cluster2.blobStore;
        BlobIdTracker tracker2 = (BlobIdTracker) ((BlobTrackingStore) s2).getTracker();
        cluster2.nodeStore.runBackgroundOperations();
        DataStoreState state2 = init(cluster2.nodeStore, 20);
        cluster2.nodeStore.runBackgroundOperations();
        cluster1.nodeStore.runBackgroundOperations();

        ScheduledFuture<?> scheduledFuture2 = newSingleThreadScheduledExecutor()
            .schedule(tracker2.new SnapshotJob(), 0, MILLISECONDS);
        scheduledFuture2.get();

        // Run first round of GC
        // If not same cluster need to mark references on other repositories
        if (!same) {
            cluster2.gc.collectGarbage(true);
        }
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

        // Again create snapshots at both cluster nodes to synchronize the latest state of
        // local references with datastore at each node
        scheduledFuture1 = newSingleThreadScheduledExecutor()
            .schedule(tracker1.new SnapshotJob(), 0, MILLISECONDS);
        scheduledFuture1.get();
        scheduledFuture2 = newSingleThreadScheduledExecutor()
            .schedule(tracker2.new SnapshotJob(), 0, MILLISECONDS);
        scheduledFuture2.get();


        // Capture logs for the second round of gc
        LogCustomizer customLogs = LogCustomizer
            .forLogger(MarkSweepGarbageCollector.class.getName())
            .enable(Level.WARN)
            .filter(Level.WARN)
            .contains("Error occurred while deleting blob with id")
            .create();
        customLogs.starting();

        if (!same) {
            cluster2.gc.collectGarbage(true);
        }
        cluster1.gc.collectGarbage(false);

        existingAfterGC = iterate(s1);
        assertEquals(0, customLogs.getLogs().size());

        customLogs.finished();
        // Check the state of the blob store after gc
        assertEquals(
            union(state1.blobsPresent, state2.blobsPresent), existingAfterGC);
    }

    /**
     * Tests GC twice on a 2 node shared datastore setup.
     * @throws Exception
     */
    @Test
    public void differentClusterGC() throws Exception {
        Cluster cluster1 = new Cluster("cluster1");
        Cluster cluster2 = new Cluster("cluster2");

        clusterGCInternal(cluster1, cluster2, false);
    }

    /**
     * Tests GC twice on 2 node cluster setup.
     * @throws Exception
     */
    @Test
    public void sameClusterGC() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        Cluster cluster1 = new Cluster("cluster1-1", 1, store);
        Cluster cluster2 = new Cluster("cluster1-2", 2, store);

        clusterGCInternal(cluster1, cluster2, true);
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
            if (!processed.contains(idStart + n)) {
                processed.add(idStart + n);
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
        long maxAge = 10; // minutes
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

        public Cluster(String clusterName) throws Exception {
            this(clusterName, 1, new MemoryDocumentStore());
        }

        public Cluster(String clusterName, int clusterId, MemoryDocumentStore store) throws Exception {
            blobStore = new DataStoreBlobStore(createFDS(blobStoreRoot, 50));
            nodeStore = builderProvider.newBuilder()
                .setClusterId(clusterId)
                .clock(clock)
                .setAsyncDelay(0)
                .setDocumentStore(store)
                .setBlobStore(blobStore)
                .getNodeStore();
            repoId = ClusterRepositoryInfo.getOrCreateId(nodeStore);
            nodeStore.runBackgroundOperations();

            ((SharedDataStore) blobStore).addMetadataRecord(
                new ByteArrayInputStream(new byte[0]),
                REPOSITORY.getNameFromId(repoId));

            String trackerRoot = folder.newFolder(clusterName).getAbsolutePath();
            tracker = new BlobIdTracker(trackerRoot,
                repoId, 86400, (SharedDataStore) blobStore);
            // add the tracker to the blobStore
            ((BlobTrackingStore) blobStore).addTracker(tracker);

            // initialized the GC
            gc = new MarkSweepGarbageCollector(
                new DocumentBlobReferenceRetriever(nodeStore),
                (GarbageCollectableBlobStore) blobStore, newSingleThreadExecutor(),
                folder.newFolder("gc" + clusterName).getAbsolutePath(), 5, 0, repoId);
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
