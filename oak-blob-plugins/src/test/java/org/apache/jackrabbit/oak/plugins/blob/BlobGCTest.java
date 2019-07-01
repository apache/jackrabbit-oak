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

package org.apache.jackrabbit.oak.plugins.blob;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.RepositoryException;

import ch.qos.logback.classic.Level;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.MultiDataStoreAware;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.apache.jackrabbit.oak.api.blob.BlobUpload;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector.GarbageCollectionOperationStats.CONSISTENCY_NAME;
import static org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector.GarbageCollectionOperationStats.FINISH_FAILURE;
import static org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector.GarbageCollectionOperationStats.NAME;
import static org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector.GarbageCollectionOperationStats.NUM_BLOBS_DELETED;
import static org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector.GarbageCollectionOperationStats.NUM_CANDIDATES;
import static org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector.GarbageCollectionOperationStats.START;
import static org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector.GarbageCollectionOperationStats.TOTAL_SIZE_DELETED;
import static org.apache.jackrabbit.oak.plugins.blob.OperationsStatsMBean.TYPE;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.randomStream;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY;
import static org.apache.jackrabbit.oak.stats.StatsOptions.METRICS_ONLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Generic class for BlobGC tests which uses custom MemoryNodeStore as well as a memory NodeStore.
 */
public class BlobGCTest {
    protected static final Logger log = LoggerFactory.getLogger(BlobGCTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    protected Whiteboard wb;

    protected Closer closer;

    protected Cluster cluster;

    protected Clock clock;

    @Before
    public void before() throws Exception {
        closer = Closer.create();
        clock = getClock();

        // add whiteboard
        final AtomicReference<Map<?, ?>> props = new AtomicReference<Map<?, ?>>();
        wb = new DefaultWhiteboard(){
            @Override
            public <T> Registration register(Class<T> type, T service, Map<?, ?> properties) {
                props.set(properties);
                return super.register(type, service, properties);
            }
        };

        TimeLapsedDataStore dataStore = new TimeLapsedDataStore();
        DataStoreBlobStore blobStore = new DataStoreBlobStore(dataStore);
        MemoryBlobStoreNodeStore nodeStore = new MemoryBlobStoreNodeStore(blobStore);
        cluster = new Cluster(folder.newFolder(), blobStore, nodeStore, 0);
        closer.register(cluster);
    }

    @After
    public void after() {
        try {
            closer.close();
        } catch (IOException e) {
            log.error("Error closing cluster instances", e);
        }
    }

    protected Clock getClock() {
        return new Clock.Virtual();
    }

    class Cluster implements Closeable {
        protected final BlobStoreState blobStoreState;
        private final File root;
        String repoId;
        protected final TimeLapsedDataStore dataStore;
        protected final GarbageCollectableBlobStore blobStore;
        protected final NodeStore nodeStore;
        private MarkSweepGarbageCollector collector;
        protected BlobReferenceRetriever referenceRetriever;
        protected ScheduledExecutorService scheduledExecutor;
        protected ThreadPoolExecutor executor;
        protected DefaultStatisticsProvider statsProvider;
        protected long startReferenceTime;

        public Cluster(File root, GarbageCollectableBlobStore blobStore, NodeStore nodeStore, int seed) throws Exception {
            this.root = root;
            this.nodeStore = nodeStore;
            this.dataStore = (TimeLapsedDataStore) ((DataStoreBlobStore) blobStore).getDataStore();
            this.blobStore = blobStore;
            if (SharedDataStoreUtils.isShared(blobStore)) {
                repoId = ClusterRepositoryInfo.getOrCreateId(nodeStore);
                ((SharedDataStore) blobStore).addMetadataRecord(
                    new ByteArrayInputStream(new byte[0]),
                    REPOSITORY.getNameFromId(repoId));
            }
            referenceRetriever = ((MemoryBlobStoreNodeStore) nodeStore).getBlobReferenceRetriever();
            startReferenceTime = clock.getTime();
            log.info("Reference time {}", startReferenceTime);
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
            statsProvider = new DefaultStatisticsProvider(scheduledExecutor);

            blobStoreState = setUp(nodeStore, blobStore, 10, 5, 100, seed);
        }

        public void setRepoId(String id) {
            this.repoId = id;
        }

        public MarkSweepGarbageCollector getCollector(long blobGcMaxAgeInSecs) throws Exception {
            return getCollector(blobGcMaxAgeInSecs, false);
        }

        public MarkSweepGarbageCollector getCollector(long blobGcMaxAgeInSecs, boolean checkConsistency) throws Exception {
            collector =
                new MarkSweepGarbageCollector(referenceRetriever, blobStore, executor, root.getAbsolutePath(), 2048,
                    blobGcMaxAgeInSecs, checkConsistency, repoId, wb, statsProvider);
            return collector;
        }

        @Override public void close() throws IOException {
            new ExecutorCloser(scheduledExecutor).close();
            new ExecutorCloser(executor).close();
        }
    }

    @Test
    public void sharedGC() throws Exception {
        log.info("Staring sharedGC()");

        // Setup a different cluster/repository sharing the blob store
        MemoryBlobStoreNodeStore secondClusterNodeStore = new MemoryBlobStoreNodeStore(cluster.blobStore);
        Cluster secondCluster = new Cluster(folder.newFolder(), cluster.blobStore, secondClusterNodeStore, 100);
        closer.register(secondCluster);

        Sets.SetView<String> totalPresent =
            Sets.union(cluster.blobStoreState.blobsPresent, secondCluster.blobStoreState.blobsPresent);
        Sets.SetView<String> totalAdded =
            Sets.union(cluster.blobStoreState.blobsAdded, secondCluster.blobStoreState.blobsAdded);

        // Execute mark on the default cluster
        executeGarbageCollection(cluster, cluster.getCollector(0), true);
        Set<String> existingAfterGC = executeGarbageCollection(secondCluster, secondCluster.getCollector(0), false);

        assertTrue(Sets.symmetricDifference(totalPresent, existingAfterGC).isEmpty());
        assertStats(secondCluster.statsProvider, 1, 0, totalAdded.size() - totalPresent.size(),
            totalAdded.size() - totalPresent.size(), NAME);
    }

    @Test
    public void noSharedGC() throws Exception {
        log.info("Starting noSharedGC()");

        // Setup a different cluster/repository sharing the blob store
        MemoryBlobStoreNodeStore secondClusterNodeStore = new MemoryBlobStoreNodeStore(cluster.blobStore);
        Cluster secondCluster = new Cluster(folder.newFolder(), cluster.blobStore, secondClusterNodeStore, 100);
        closer.register(secondCluster);

        Sets.SetView<String> totalAdded =
            Sets.union(cluster.blobStoreState.blobsAdded, secondCluster.blobStoreState.blobsAdded);

        Set<String> existingAfterGC = executeGarbageCollection(secondCluster, secondCluster.getCollector(0), false);

        assertEquals(totalAdded, existingAfterGC);
        assertStats(secondCluster.statsProvider, 1, 1, 0, 0, NAME);
    }

    @Test
    public void sharedGCRepositoryCloned() throws Exception {
        log.debug("Starting sharedGCRepoCloned()");

        // Setup a different cluster/repository sharing the blob store and the repository id
        MemoryBlobStoreNodeStore secondClusterNodeStore = new MemoryBlobStoreNodeStore(cluster.blobStore);
        Cluster secondCluster = new Cluster(folder.newFolder(), cluster.blobStore, secondClusterNodeStore, 100);
        closer.register(secondCluster);

        ((SharedDataStore) secondCluster.blobStore).deleteMetadataRecord(REPOSITORY.getNameFromId(secondCluster.repoId));
        secondCluster.setRepoId(cluster.repoId);

        Sets.SetView<String> totalPresent =
            Sets.union(cluster.blobStoreState.blobsPresent, secondCluster.blobStoreState.blobsPresent);

        // Execute mark on the default cluster
        executeGarbageCollection(cluster, cluster.getCollector(0), true);
        Set<String> existingAfterGC = executeGarbageCollection(secondCluster, secondCluster.getCollector(0), false);

        assertTrue(Sets.symmetricDifference(totalPresent, existingAfterGC).isEmpty());
    }

    @Test
    public void gc() throws Exception {
        log.info("Starting gc()");

        Set<String> existingAfterGC = executeGarbageCollection(cluster, cluster.getCollector(0), false);
        assertTrue(Sets.symmetricDifference(cluster.blobStoreState.blobsPresent, existingAfterGC).isEmpty());
        assertStats(cluster.statsProvider, 1, 0,
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(),
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(), NAME);
    }

    @Test
    public void gcWithConsistencyCheck() throws Exception {
        log.info("Starting gcWithConsistencyCheck()");
        ((MemoryBlobStoreNodeStore) cluster.nodeStore).getReferencedBlobs().add("SPURIOUS");

        MarkSweepGarbageCollector collector = cluster.getCollector(0, true);
        Set<String> existingAfterGC = executeGarbageCollection(cluster, collector, false);
        assertFalse(Sets.symmetricDifference(cluster.blobStoreState.blobsPresent, existingAfterGC).isEmpty());
        assertStats(cluster.statsProvider, 1, 0,
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size() + 1,
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size() + 1, NAME);
        assertStatsBean(collector.getConsistencyOperationStats(), 1, 1, 1);
    }

    @Test
    public void gcWithNoDeleteDirectBinary() throws Exception {
        log.info("Starting gcWithNoDeleteDirectBinary()");

        setupDirectBinary(1, 0);
        Set<String> existingAfterGC = executeGarbageCollection(cluster, cluster.getCollector(0), false);
        assertTrue(Sets.symmetricDifference(cluster.blobStoreState.blobsPresent, existingAfterGC).isEmpty());
        assertStats(cluster.statsProvider, 1, 0,
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(),
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(), NAME);
    }

    @Test
    public void gcWithDeleteDirectBinary() throws Exception {
        log.info("Starting gcWithNoDeleteDirectBinary()");

        setupDirectBinary(5, 2);
        Set<String> existingAfterGC = executeGarbageCollection(cluster, cluster.getCollector(0), false);
        assertTrue(Sets.symmetricDifference(cluster.blobStoreState.blobsPresent, existingAfterGC).isEmpty());
        assertStats(cluster.statsProvider, 1, 0,
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(),
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(), NAME);
    }

    @Test
    public void noGc() throws Exception {
        log.info("Starting noGc()");

        long afterSetupTime = clock.getTime();
        log.info("after setup time {}", afterSetupTime);

        Set<String> existingAfterGC =
            executeGarbageCollection(cluster, cluster.getCollector(afterSetupTime - cluster.startReferenceTime + 2),
                false);
        assertTrue(Sets.symmetricDifference(cluster.blobStoreState.blobsAdded, existingAfterGC).isEmpty());
        assertStats(cluster.statsProvider, 1, 0, 0,
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(), NAME);
    }

    @Test
    public void checkConsistency() throws Exception {
        log.info("Starting checkConsistency()");

        long afterSetupTime = clock.getTime();
        log.info("after setup time {}", afterSetupTime);

        MarkSweepGarbageCollector collector = cluster.getCollector(0);
        long missing = collector.checkConsistency();

        assertEquals(0, missing);
        assertStats(cluster.statsProvider, 1, 0, 0, 0, CONSISTENCY_NAME);
        assertStatsBean(collector.getConsistencyOperationStats(), 1, 0, 0);
    }

    @Test
    public void checkConsistencyFailure() throws Exception {
        log.info("Starting checkConsistencyFailure()");

        long afterSetupTime = clock.getTime();
        log.info("after setup time {}", afterSetupTime);

        cluster.blobStore
            .countDeleteChunks(Lists.newArrayList(Iterators.getLast(cluster.blobStoreState.blobsPresent.iterator())),
                0);
        MarkSweepGarbageCollector collector = cluster.getCollector(0);
        long missing = collector.checkConsistency();

        assertEquals(1, missing);
        assertStats(cluster.statsProvider, 1, 1, 1, 0, CONSISTENCY_NAME);
        assertStatsBean(collector.getConsistencyOperationStats(), 1, 1, 1);
    }

    @Test
    public void gcCheckDeletedSize() throws Exception {
        log.info("Starting gcCheckDeletedSize()");

        // Capture logs for the second round of gc
        LogCustomizer customLogs = LogCustomizer
            .forLogger(MarkSweepGarbageCollector.class.getName())
            .enable(Level.INFO)
            .filter(Level.INFO)
            .contains("Estimated size recovered for")
            .create();
        customLogs.starting();

        Set<String> existingAfterGC =
            executeGarbageCollection(cluster, cluster.getCollector(0),false);
        assertEquals(1, customLogs.getLogs().size());
        long deletedSize = (cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size()) * 100;
        assertTrue(customLogs.getLogs().get(0).contains(String.valueOf(deletedSize)));
        assertStats(cluster.statsProvider, 1, 0,
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(),
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(), NAME);
        assertEquals(deletedSize, getStatCount(cluster.statsProvider, NAME, TOTAL_SIZE_DELETED));
        customLogs.finished();
        assertTrue(Sets.symmetricDifference(cluster.blobStoreState.blobsPresent, existingAfterGC).isEmpty());
    }

    @Test
    public void gcMarkOnly() throws Exception {
        log.info("Starting gcMarkOnly()");

        Set<String> existingAfterGC =
            executeGarbageCollection(cluster, cluster.getCollector(0),true);
        assertTrue(Sets.symmetricDifference(cluster.blobStoreState.blobsAdded, existingAfterGC).isEmpty());
        assertStats(cluster.statsProvider, 1, 0, 0, 0, NAME);
    }

    protected Set<String> executeGarbageCollection(Cluster cluster, MarkSweepGarbageCollector collector, boolean markOnly)
        throws Exception {
        collector.collectGarbage(markOnly);

        assertEquals(0, cluster.executor.getTaskCount());
        Set<String> existingAfterGC = iterate(cluster.blobStore);
        log.info("{} blobs existing after gc : {}", existingAfterGC.size(), existingAfterGC);

        return existingAfterGC;
    }

    private void assertStats(StatisticsProvider statsProvider, int start, int failure, long deleted, long candidates,
        String typeName) {

        assertEquals("Start counter mismatch", start, getStatCount(statsProvider, typeName, START));
        assertEquals("Finish error mismatch", failure, getStatCount(statsProvider, typeName, FINISH_FAILURE));
        assertEquals("Num deleted mismatch", deleted, getStatCount(statsProvider, typeName, NUM_BLOBS_DELETED));
        assertEquals("Num candidates mismatch", candidates, getStatCount(statsProvider, typeName, NUM_CANDIDATES));
    }


    private void assertStatsBean(OperationsStatsMBean mbean, int start, int failure, long deleted) {
        assertEquals("Start counter mismatch", start, mbean.getStartCount());
        assertEquals("Finish error mismatch", failure, mbean.getFailureCount());
        assertEquals("Num deleted mismatch", deleted, mbean.numDeleted());
    }

    private long getStatCount(StatisticsProvider statsProvider, String typeName, String name) {
        return statsProvider.getCounterStats(
            TYPE + "." + typeName + "." + name, METRICS_ONLY).getCount();
    }

    protected Set<String> iterate(GarbageCollectableBlobStore blobStore) throws Exception {
        Iterator<String> cur = blobStore.getAllChunkIds(0);

        Set<String> existing = Sets.newHashSet();
        while (cur.hasNext()) {
            existing.add(cur.next());
        }
        return existing;
    }

    public BlobStoreState setUp (NodeStore nodeStore,
        GarbageCollectableBlobStore blobStore,
        int count,
        int deletions,
        int blobSize,
        int seed) throws Exception {

        preSetup();

        NodeBuilder a = nodeStore.getRoot().builder();
        /* Create and delete nodes with blobs stored in DS*/
        int maxDeleted  = deletions;
        int numBlobs = count;
        List<Integer> toBeDeleted = Lists.newArrayList();
        Random rand = new Random();
        for (int i = 0; i < maxDeleted; i++) {
            int n = rand.nextInt(numBlobs);
            if (!toBeDeleted.contains(n)) {
                toBeDeleted.add(n);
            }
        }

        BlobStoreState state = new BlobStoreState();
        for (int i = 0; i < numBlobs; i++) {
            Blob b = nodeStore.createBlob(
                randomStream(Integer.parseInt(String.valueOf(seed) + String.valueOf(i)), blobSize));
            Iterator<String> idIter = blobStore.resolveChunks(b.getContentIdentity());
            while (idIter.hasNext()) {
                String chunk = idIter.next();
                state.blobsAdded.add(chunk);
                if (!toBeDeleted.contains(i)) {
                    state.blobsPresent.add(chunk);
                }
            }
            a.child("c" + i).setProperty("x", b);
        }

        nodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        log.info("Created blobs : {}", state.blobsAdded.size());

        for (int id : toBeDeleted) {
            delete("c" + id, nodeStore);
        }
        log.info("Deleted nodes : {}", toBeDeleted.size());

        // Sleep a little to make eligible for cleanup
        clock.waitUntil(5);

        postSetup(nodeStore, state);

        log.info("{} blobs added : {}", state.blobsAdded.size(), state.blobsAdded);
        log.info("{} blobs remaining : {}", state.blobsPresent.size(), state.blobsPresent);

        return state;
    }

    protected void setupDirectBinary(int numCreate, int numDelete) throws CommitFailedException {
        for (int i = 0; i < numCreate; i++) {
            BlobUpload blobUpload = ((BlobAccessProvider) cluster.blobStore).initiateBlobUpload(100, 1);
            Blob blob = ((BlobAccessProvider) cluster.blobStore).completeBlobUpload(blobUpload.getUploadToken());

            cluster.blobStoreState.blobsAdded.add(blob.getContentIdentity());
            cluster.blobStoreState.blobsPresent.add(blob.getContentIdentity());
            NodeBuilder builder = cluster.nodeStore.getRoot().builder();
            builder.child("dbu" + i).setProperty("x", blob);
            cluster.nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            PropertyState property = cluster.nodeStore.getRoot().getChildNode("dbu" + i).getProperty("x");
            Blob blobReturned = property.getValue(Type.BINARY);
            ((MemoryBlobStoreNodeStore) cluster.nodeStore).getReferencedBlobs().add(blobReturned.getContentIdentity());
        }

        for (int i = 0; i < Math.min(numCreate, numDelete); i++) {
            PropertyState property = cluster.nodeStore.getRoot().getChildNode("dbu" + i).getProperty("x");
            String blobId = property.getValue(Type.BINARY).getContentIdentity();

            delete("dbu" + i, cluster.nodeStore);
            ((MemoryBlobStoreNodeStore) cluster.nodeStore).getReferencedBlobs().remove(blobId);
            cluster.blobStoreState.blobsPresent.remove(blobId);
        }
    }

    protected Set<String> createBlobs(GarbageCollectableBlobStore blobStore, int count, int size) throws Exception {
        HashSet<String> blobSet = new HashSet<String>();
        for  (int i = 0; i < count; i++) {
            String id = blobStore.writeBlob(randomStream(10 + i, size));
            Iterator<String> idIter = blobStore.resolveChunks(id);
            while (idIter.hasNext()) {
                String chunk = idIter.next();
                blobSet.add(chunk);
            }
        }
        log.info("{} Additional created {}", blobSet.size(), blobSet);
        return blobSet;
    }

    void preSetup() {}

    protected void postSetup(NodeStore nodeStore, BlobStoreState state) {
        ((MemoryBlobStoreNodeStore) nodeStore).setReferencedBlobs(state.blobsPresent);
    }

    protected void delete(String nodeId, NodeStore nodeStore) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.child(nodeId).remove();

        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    /**
     * Represents state of the blobs after setup
     */
    class BlobStoreState {
        Set<String> blobsAdded = Sets.newHashSet();
        Set<String> blobsPresent = Sets.newHashSet();
    }

    /**
     * MemoryNodeStore extension which created blobs in the in-memory blob store
     */
    public static class MemoryBlobStoreNodeStore extends MemoryNodeStore {
        private final BlobStore blobStore;
        Set<String> referencedBlobs;

        public MemoryBlobStoreNodeStore(BlobStore blobStore) {
            this.blobStore = blobStore;
        }

        public void setReferencedBlobs(Set<String> referencedBlobs) {
            this.referencedBlobs = referencedBlobs;
        }

        public Set<String> getReferencedBlobs() {
            return this.referencedBlobs;
        }

        @Override
        public ArrayBasedBlob createBlob(InputStream in) {
            try {
                String id = blobStore.writeBlob(in);
                return new TestBlob(id, blobStore);
            } catch(Exception e) {
                log.error("Error in createBlobs", e);
            }
            return null;
        }

        public BlobReferenceRetriever getBlobReferenceRetriever() {
            return collector -> {
                for(String id : referencedBlobs) {
                    collector.addReference(id, null);
                }
            };
        }

        static class TestBlob extends ArrayBasedBlob {
            private String id;
            private BlobStore blobStore;

            public TestBlob(String id, BlobStore blobStore) {
                super(new byte[0]);
                this.id = id;
                this.blobStore = blobStore;
            }

            @Override
            public String getContentIdentity() {
                return id;
            }
            @NotNull
            @Override
            public InputStream getNewStream() {
                try {
                    return blobStore.getInputStream(id);
                } catch (IOException e) {
                    log.error("Error in getNewStream", e);
                }
                return null;
            }

            @Override
            public long length() {
                try {
                    return blobStore.getBlobLength(id);
                } catch (IOException e) {
                    log.error("Error in length", e);
                }
                return 0;
            }
        }
    }

    /**
     * Test in memory DS to store the contents with an increasing time
     */
    class TimeLapsedDataStore implements DataStore, MultiDataStoreAware, SharedDataStore, DataRecordAccessProvider {
        public static final int MIN_RECORD_LENGTH = 50;

        private final long startTime;
        Map<String, DataRecord> store;
        Map<String, DataRecord> metadata;
        Map<String, String> uploadTokens;

        public TimeLapsedDataStore() {
            this.startTime = clock.getTime();
            store = Maps.newHashMap();
            metadata = Maps.newHashMap();
            uploadTokens = Maps.newHashMap();
        }

        @Override public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
            if (store.containsKey(identifier.toString())) {
                return getRecord(identifier);
            }
            return null;
        }

        @Override public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
            return store.get(identifier.toString());
        }

        @Override public DataRecord getRecordFromReference(String reference) throws DataStoreException {
            return getRecord(new DataIdentifier(reference));
        }

        @Override public DataRecord addRecord(InputStream stream) throws DataStoreException {
            try {
                byte[] data = IOUtils.toByteArray(stream);
                String id = getIdForInputStream(new ByteArrayInputStream(data));
                TestRecord rec = new TestRecord(id, data, clock.getTime());
                store.put(id, rec);
                log.info("Blob created {} with timestamp {}", rec.id, rec.lastModified);
                return rec;
            } catch (Exception e) {
                throw new DataStoreException(e);
            }

        }

        @Override public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
            return  Iterators.transform(store.keySet().iterator(), input -> new DataIdentifier(input));
        }

        @Override public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
            store.remove(identifier.toString());
        }

        /***************************************** SharedDataStore ***************************************/

        @Override public void addMetadataRecord(InputStream stream, String name) throws DataStoreException {
            try {
                byte[] data = IOUtils.toByteArray(stream);
                TestRecord rec = new TestRecord(name, data, clock.getTime());
                metadata.put(name, rec);
                log.info("Metadata created {} with timestamp {}", rec.id, rec.lastModified);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override public void addMetadataRecord(File f, String name) throws DataStoreException {
            FileInputStream fstream = null;
            try {
                fstream = new FileInputStream(f);
                addMetadataRecord(fstream, name);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeQuietly(fstream);
            }
        }

        @Override public DataRecord getMetadataRecord(String name) {
            return metadata.get(name);
        }

        @Override public boolean metadataRecordExists(String name) {
            return metadata.containsKey(name);
        }

        @Override public List<DataRecord> getAllMetadataRecords(String prefix) {
            List<DataRecord> recs = Lists.newArrayList();
            Iterator<Map.Entry<String, DataRecord>> iter = metadata.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, DataRecord> entry = iter.next();
                if (entry.getKey().startsWith(prefix)) {
                    recs.add(entry.getValue());
                }
            }
            return recs;
        }

        @Override public boolean deleteMetadataRecord(String name) {
            metadata.remove(name);
            if (!metadata.containsKey(name)) {
                return true;
            }
            return false;
        }

        @Override public void deleteAllMetadataRecords(String prefix) {
            List<String> recs = Lists.newArrayList();
            Iterator<Map.Entry<String, DataRecord>> iter = metadata.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, DataRecord> entry = iter.next();
                if (entry.getKey().startsWith(prefix)) {
                    recs.add(entry.getKey());
                }
            }

            for(String key: recs) {
                metadata.remove(key);
            }
        }

        @Override public Iterator<DataRecord> getAllRecords() throws DataStoreException {
            return store.values().iterator();
        }

        @Override public DataRecord getRecordForId(DataIdentifier id) throws DataStoreException {
            return store.get(id.toString());
        }

        @Override public SharedDataStore.Type getType() {
            return SharedDataStore.Type.SHARED;
        }

        /**************************** DataRecordAccessProvider *************************/

        @Override public @Nullable URI getDownloadURI(@NotNull DataIdentifier identifier,
            @NotNull DataRecordDownloadOptions downloadOptions) {
            return null;
        }

        @Override
        public @Nullable DataRecordUpload initiateDataRecordUpload(long maxUploadSizeInBytes, int maxNumberOfURIs)
            throws IllegalArgumentException, DataRecordUploadException {
            String upToken = UUID.randomUUID().toString();
            Random rand = new Random();
            InputStream stream = randomStream(rand.nextInt(1000), 100);
            byte[] data = new byte[0];
            try {
                data = IOUtils.toByteArray(stream);
            } catch (IOException e) {
                throw new DataRecordUploadException(e);
            }
            TestRecord rec = new TestRecord(upToken, data, clock.getTime());
            store.put(upToken, rec);

            DataRecordUpload uploadRec = new DataRecordUpload() {
                @Override public @NotNull String getUploadToken() {
                    return upToken;
                }

                @Override public long getMinPartSize() {
                    return maxUploadSizeInBytes;
                }

                @Override public long getMaxPartSize() {
                    return maxUploadSizeInBytes;
                }

                @Override public @NotNull Collection<URI> getUploadURIs() {
                    return Collections.EMPTY_LIST;
                }
            };
            return uploadRec;
        }

        @Override public @NotNull DataRecord completeDataRecordUpload(@NotNull String uploadToken)
            throws IllegalArgumentException, DataRecordUploadException, DataStoreException {
            return store.get(uploadToken);
        }

        class TestRecord implements DataRecord {
            String id;
            byte[] data;
            long lastModified;

            public TestRecord(String id, byte[] data, long lastModified) {
                this.id = id;
                this.data = data;
                this.lastModified = lastModified;
            }

            @Override public DataIdentifier getIdentifier() {
                return new DataIdentifier(id);
            }

            @Override public String getReference() {
                return id;
            }

            @Override public long getLength() throws DataStoreException {
                return data.length;
            }

            @Override public InputStream getStream() throws DataStoreException {
                return new ByteArrayInputStream(data);
            }

            @Override public long getLastModified() {
                return lastModified;
            }
        }

        private String getIdForInputStream(final InputStream in)
            throws Exception {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            OutputStream output = new DigestOutputStream(new NullOutputStream(), digest);
            try {
                IOUtils.copyLarge(in, output);
            } finally {
                IOUtils.closeQuietly(output);
                IOUtils.closeQuietly(in);
            }
            return encodeHexString(digest.digest());
        }

        /*************************************** No Op ***********************/
        @Override public void init(String homeDir) throws RepositoryException {
        }

        @Override public void updateModifiedDateOnAccess(long before) {
        }

        @Override public int deleteAllOlderThan(long min) throws DataStoreException {
            return 0;
        }

        @Override public int getMinRecordLength() {
            return MIN_RECORD_LENGTH;
        }

        @Override public void close() throws DataStoreException {
        }

        @Override public void clearInUse() {
        }
    }
}
