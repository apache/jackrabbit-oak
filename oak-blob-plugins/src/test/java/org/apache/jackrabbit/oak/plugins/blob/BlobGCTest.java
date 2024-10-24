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

import static org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector.GarbageCollectionOperationStats.BLOB_REFERENCES_SIZE;
import static org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector.GarbageCollectionOperationStats.CONSISTENCY_NAME;
import static org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector.GarbageCollectionOperationStats.FINISH_FAILURE;
import static org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector.GarbageCollectionOperationStats.NAME;
import static org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector.GarbageCollectionOperationStats.NUM_BLOBS_DELETED;
import static org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector.GarbageCollectionOperationStats.NUM_BLOB_REFERENCES;
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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import ch.qos.logback.classic.Level;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.guava.common.collect.Sets;
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.apache.jackrabbit.oak.api.blob.BlobUpload;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector.NotAllRepositoryMarkedException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.internal.util.collections.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic class for BlobGC tests which uses custom MemoryNodeStore as well as a memory NodeStore.
 */
public class BlobGCTest {
    protected static final Logger log = LoggerFactory.getLogger(BlobGCTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));
    
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

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

        TimeLapsedDataStore dataStore = new TimeLapsedDataStore(clock);
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
        private final Clock clock;
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
        
        protected int blobSize = 100;
        
        public Cluster(File root, GarbageCollectableBlobStore blobStore, NodeStore nodeStore, int seed) throws Exception {
            this.root = root;
            this.nodeStore = nodeStore;
            this.dataStore = (TimeLapsedDataStore) ((DataStoreBlobStore) blobStore).getDataStore();
            this.blobStore = blobStore;
            this.clock = dataStore.getClock();

            if (SharedDataStoreUtils.isShared(blobStore)) {
                repoId = ClusterRepositoryInfo.getOrCreateId(nodeStore);
                ((SharedDataStore) blobStore).setRepositoryId(repoId);
            }
            referenceRetriever = ((MemoryBlobStoreNodeStore) nodeStore).getBlobReferenceRetriever();
            startReferenceTime = clock.getTime();
            log.info("Reference time {}", startReferenceTime);
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

            blobStoreState = setUp(nodeStore, blobStore, 10, 5, blobSize, seed);
        }

        public void setRepoId(String id) {
            this.repoId = id;
        }

        public MarkSweepGarbageCollector getCollector(long blobGcMaxAgeInSecs) throws Exception {
            return getCollector(blobGcMaxAgeInSecs, false, false);
        }

        public MarkSweepGarbageCollector getCollector(long blobGcMaxAgeInSecs, boolean checkConsistency,
            boolean sweepIfRefsPastRetention) throws Exception {
            statsProvider = new DefaultStatisticsProvider(scheduledExecutor);

            collector =
                new MarkSweepGarbageCollector(referenceRetriever, blobStore, executor, root.getAbsolutePath(), 2048,
                    blobGcMaxAgeInSecs, checkConsistency, sweepIfRefsPastRetention, repoId, wb, statsProvider);
            collector.setClock(clock);
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
            totalAdded.size() - totalPresent.size(), secondCluster.blobStoreState.blobsPresent.size(), 
            cluster.blobSize, NAME);
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
        assertStats(secondCluster.statsProvider, 1, 1, 0, 0, secondCluster.blobStoreState.blobsPresent.size(),
            secondCluster.blobSize, NAME);
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
    public void sharedGCRefsOld() throws Exception {
        log.info("Staring sharedGCRefsOld()");

        // Setup a different cluster/repository sharing the blob store
        MemoryBlobStoreNodeStore secondClusterNodeStore = new MemoryBlobStoreNodeStore(cluster.blobStore);
        Cluster secondCluster = new Cluster(folder.newFolder(), cluster.blobStore, secondClusterNodeStore, 100);
        closer.register(secondCluster);

        Sets.SetView<String> totalPresent =
            Sets.union(cluster.blobStoreState.blobsPresent, secondCluster.blobStoreState.blobsPresent);
        Sets.SetView<String> totalAdded =
            Sets.union(cluster.blobStoreState.blobsAdded, secondCluster.blobStoreState.blobsAdded);

        clock.waitUntil(clock.getTime() + 5);

        // Execute mark on the default cluster
        executeGarbageCollection(cluster, cluster.getCollector(5), true);
        executeGarbageCollection(secondCluster, secondCluster.getCollector(5), true);

        clock.waitUntil(clock.getTime() + 5);

        Set<String> existingAfterGC = executeGarbageCollection(secondCluster, secondCluster.getCollector(5, false, true), false);

        assertTrue(Sets.symmetricDifference(totalPresent, existingAfterGC).isEmpty());
        assertStats(secondCluster.statsProvider, 1, 0, totalAdded.size() - totalPresent.size(),
            totalAdded.size() - totalPresent.size(), secondCluster.blobStoreState.blobsPresent.size(),
            secondCluster.blobSize, NAME);
    }

    @Test
    public void sharedGCRefsNotOld() throws Exception {
        log.info("Staring sharedGCRefsNotOld()");

        // Setup a different cluster/repository sharing the blob store
        MemoryBlobStoreNodeStore secondClusterNodeStore = new MemoryBlobStoreNodeStore(cluster.blobStore);
        Cluster secondCluster = new Cluster(folder.newFolder(), cluster.blobStore, secondClusterNodeStore, 100);
        closer.register(secondCluster);

        Sets.SetView<String> totalPresent =
            Sets.union(cluster.blobStoreState.blobsPresent, secondCluster.blobStoreState.blobsPresent);
        Sets.SetView<String> totalAdded =
            Sets.union(cluster.blobStoreState.blobsAdded, secondCluster.blobStoreState.blobsAdded);

        // Execute mark on the default cluster
        executeGarbageCollection(cluster, cluster.getCollector(5), true);

        // Let the second cluster one not pass retention old time
        clock.waitUntil(clock.getTime() + 5);

        executeGarbageCollection(secondCluster, secondCluster.getCollector(5), true);

        Set<String> existingAfterGC = executeGarbageCollection(secondCluster, secondCluster.getCollector(6, false, true), false);

        assertTrue(Sets.symmetricDifference(totalAdded, existingAfterGC).isEmpty());
        assertStats(secondCluster.statsProvider, 1, 1, 0,0, secondCluster.blobStoreState.blobsPresent.size(),
            secondCluster.blobSize, NAME);
    }

    @Test
    public void gc() throws Exception {
        log.info("Starting gc()");

        Set<String> existingAfterGC = executeGarbageCollection(cluster, cluster.getCollector(0), false);
        assertTrue(Sets.symmetricDifference(cluster.blobStoreState.blobsPresent, existingAfterGC).isEmpty());
        assertStats(cluster.statsProvider, 1, 0,
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(),
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(),
            cluster.blobStoreState.blobsPresent.size(), cluster.blobSize, NAME);
    }

    @Test
    public void gcWithConsistencyCheck() throws Exception {
        log.info("Starting gcWithConsistencyCheck()");
        ((MemoryBlobStoreNodeStore) cluster.nodeStore).getReferencedBlobs().add("SPURIOUS#100");

        MarkSweepGarbageCollector collector = cluster.getCollector(0, true, false);
        Set<String> existingAfterGC = executeGarbageCollection(cluster, collector, false);
        assertFalse(Sets.symmetricDifference(cluster.blobStoreState.blobsPresent, existingAfterGC).isEmpty());
        assertStats(cluster.statsProvider, 1, 0,
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size() + 1,
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size() + 1,
            cluster.blobStoreState.blobsPresent.size(), cluster.blobSize, NAME);
        assertStats(cluster.statsProvider, 1, 1, 1, 0,
            cluster.blobStoreState.blobsPresent.size(), cluster.blobSize, CONSISTENCY_NAME);
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
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(),
            cluster.blobStoreState.blobsPresent.size(), cluster.blobSize, NAME);
    }

    @Test
    public void gcWithDeleteDirectBinary() throws Exception {
        log.info("Starting gcWithNoDeleteDirectBinary()");

        setupDirectBinary(5, 2);
        Set<String> existingAfterGC = executeGarbageCollection(cluster, cluster.getCollector(0), false);
        assertTrue(Sets.symmetricDifference(cluster.blobStoreState.blobsPresent, existingAfterGC).isEmpty());
        assertStats(cluster.statsProvider, 1, 0,
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(),
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(),
            cluster.blobStoreState.blobsPresent.size(), cluster.blobSize, NAME);
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
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(),
            cluster.blobStoreState.blobsPresent.size(), cluster.blobSize, NAME);
    }

    @Test
    public void checkConsistency() throws Exception {
        log.info("Starting checkConsistency()");

        long afterSetupTime = clock.getTime();
        log.info("after setup time {}", afterSetupTime);

        MarkSweepGarbageCollector collector = cluster.getCollector(0);
        long missing = collector.checkConsistency();

        assertEquals(0, missing);
        assertStats(cluster.statsProvider, 1, 0, 0, 0, cluster.blobStoreState.blobsPresent.size(), cluster.blobSize,
            CONSISTENCY_NAME);
        assertStatsBean(collector.getConsistencyOperationStats(), 1, 0, 0);
    }

    @Test
    public void checkConsistencyMarkOnly() throws Exception {
        log.info("Starting checkConsistencyMarkOnly()");

        long afterSetupTime = clock.getTime();
        log.info("after setup time {}", afterSetupTime);

        MarkSweepGarbageCollector collector = cluster.getCollector(0);
        long missing = collector.checkConsistency(true);

        assertEquals(0, missing);
        assertStats(cluster.statsProvider, 1, 0, 0, 0, cluster.blobStoreState.blobsPresent.size(), cluster.blobSize,
                CONSISTENCY_NAME);
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
        assertStats(cluster.statsProvider, 1, 1, 1, 0, cluster.blobStoreState.blobsPresent.size(), cluster.blobSize,
            CONSISTENCY_NAME);
        assertStatsBean(collector.getConsistencyOperationStats(), 1, 1, 1);
    }

    @Test
    public void checkConsistencyGlobal() throws Exception {
        log.info("Staring checkConsistencyGlobal()");

        // Setup a different cluster/repository sharing the blob store
        MemoryBlobStoreNodeStore secondClusterNodeStore = new MemoryBlobStoreNodeStore(cluster.blobStore, true);
        Cluster secondCluster = new Cluster(folder.newFolder(), cluster.blobStore, secondClusterNodeStore, 100);
        closer.register(secondCluster);
        
        int totalPresent = secondCluster.blobStoreState.blobsPresent.size() + cluster.blobStoreState.blobsPresent.size();
        secondCluster.blobStoreState.blobsPresent.add(Iterables.firstOf(cluster.blobStoreState.blobsPresent));
        // Execute mark on the default cluster
        executeGarbageCollection(cluster, cluster.getCollector(0), true);
        MarkSweepGarbageCollector globalCollector = secondCluster.getCollector(0, true, false);
        long missing = globalCollector.checkConsistency();
        assertEquals(0, missing);
        assertStats(secondCluster.statsProvider, 1, 0, 0, 0, totalPresent,
            cluster.blobSize, CONSISTENCY_NAME);
        assertStatsBean(globalCollector.getConsistencyOperationStats(), 1, 0, 0);
    }

    @Test
    public void checkConsistencyGlobalFailureOther() throws Exception {
        log.info("Staring checkConsistencyGlobalFailureOther()");

        // Setup a different cluster/repository sharing the blob store
        MemoryBlobStoreNodeStore secondClusterNodeStore = new MemoryBlobStoreNodeStore(cluster.blobStore);
        Cluster secondCluster = new Cluster(folder.newFolder(), cluster.blobStore, secondClusterNodeStore, 100);
        closer.register(secondCluster);

        cluster.blobStore
            .countDeleteChunks(Lists.newArrayList(Iterators.getLast(cluster.blobStoreState.blobsPresent.iterator())),
                0);

        // Execute mark on the default cluster
        executeGarbageCollection(cluster, cluster.getCollector(0), true);
        MarkSweepGarbageCollector globalCollector = secondCluster.getCollector(0, true, false);
        long missing = globalCollector.checkConsistency();
        assertEquals(1, missing);
        int totalPresent =
            secondCluster.blobStoreState.blobsPresent.size() + cluster.blobStoreState.blobsPresent.size();
        assertStats(secondCluster.statsProvider, 1, 1, 1, 0, totalPresent,
            cluster.blobSize, CONSISTENCY_NAME);
        assertStatsBean(globalCollector.getConsistencyOperationStats(), 1, 1, 1);
    }

    @Test
    public void checkConsistencyGlobalFailure() throws Exception {
        log.info("Staring checkConsistencyGlobalFailureOther()");

        // Setup a different cluster/repository sharing the blob store
        MemoryBlobStoreNodeStore secondClusterNodeStore = new MemoryBlobStoreNodeStore(cluster.blobStore);
        Cluster secondCluster = new Cluster(folder.newFolder(), cluster.blobStore, secondClusterNodeStore, 100);
        closer.register(secondCluster);

        secondCluster.blobStore
            .countDeleteChunks(Lists.newArrayList(Iterators.getLast(secondCluster.blobStoreState.blobsPresent.iterator())),
                0);

        // Execute mark on the default cluster
        executeGarbageCollection(cluster, cluster.getCollector(0), true);
        MarkSweepGarbageCollector globalCollector = secondCluster.getCollector(0, true, false);
        long missing = globalCollector.checkConsistency();
        assertEquals(1, missing);
        int totalPresent =
            secondCluster.blobStoreState.blobsPresent.size() + cluster.blobStoreState.blobsPresent.size();
        assertStats(secondCluster.statsProvider, 1, 1, 1, 0, totalPresent,
            cluster.blobSize, CONSISTENCY_NAME);
        assertStatsBean(globalCollector.getConsistencyOperationStats(), 1, 1, 1);
    }

    @Test
    public void checkConsistencyFailureNotAllMarked() throws Exception {
        log.info("Staring checkConsistencyFailureNotAllMarked()");
        expectedEx.expect(NotAllRepositoryMarkedException.class);

        // Setup a different cluster/repository sharing the blob store
        MemoryBlobStoreNodeStore secondClusterNodeStore = new MemoryBlobStoreNodeStore(cluster.blobStore);
        Cluster secondCluster = new Cluster(folder.newFolder(), cluster.blobStore, secondClusterNodeStore, 100);
        closer.register(secondCluster);

        // Execute mark on the default cluster
        MarkSweepGarbageCollector globalCollector = cluster.getCollector(0, true, false);
        globalCollector.checkConsistency();
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
            cluster.blobStoreState.blobsAdded.size() - cluster.blobStoreState.blobsPresent.size(),
            cluster.blobStoreState.blobsPresent.size(), cluster.blobSize, NAME);
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
        assertStats(cluster.statsProvider, 1, 0, 0, 0, cluster.blobStoreState.blobsPresent.size(), cluster.blobSize, 
            NAME);
        assertEquals(cluster.blobStoreState.blobsPresent.size(), getStatCount(cluster.statsProvider, NAME,
            NUM_BLOB_REFERENCES));
        assertEquals(cluster.blobStoreState.blobsPresent.size() * cluster.blobSize, getStatCount(cluster.statsProvider, NAME,
            BLOB_REFERENCES_SIZE));
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
        long blobsPresent, long blobsPresentSize, String typeName) {

        assertEquals("Start counter mismatch", start, getStatCount(statsProvider, typeName, START));
        assertEquals("Finish error mismatch", failure, getStatCount(statsProvider, typeName, FINISH_FAILURE));
        assertEquals("Num deleted mismatch", deleted, getStatCount(statsProvider, typeName, NUM_BLOBS_DELETED));
        assertEquals("Num candidates mismatch", candidates, getStatCount(statsProvider, typeName, NUM_CANDIDATES));
        assertEquals("Num references mismatch", blobsPresent, getStatCount(statsProvider, typeName,
            NUM_BLOB_REFERENCES));
        assertEquals("Blob reference size mismatch", blobsPresent * blobsPresentSize, getStatCount(statsProvider,
            typeName,
            BLOB_REFERENCES_SIZE));
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

        Set<String> existing = new HashSet<>();
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
        List<Integer> toBeDeleted = new ArrayList<>();
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
        Set<String> blobsAdded = new HashSet<>();
        Set<String> blobsPresent = new HashSet<>();
    }

}
