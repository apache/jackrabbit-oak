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

package org.apache.jackrabbit.oak.segment;

import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import ch.qos.logback.classic.Level;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.blob.GarbageCollectorFileState;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for SegmentNodeStore DataStore GC
 */
public class SegmentDataStoreBlobGCIT {

    private static final Logger log = LoggerFactory.getLogger(SegmentDataStoreBlobGCIT.class);

    private static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    private SegmentNodeStore nodeStore;

    private FileStore store;

    private DataStoreBlobStore blobStore;

    private SegmentGCOptions gcOptions = defaultGCOptions();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @After
    public void closeFileStore() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    @After
    public void closeBlobStore() throws Exception {
        if (blobStore != null) {
            blobStore.close();
        }
    }

    private SegmentNodeStore getNodeStore(BlobStore blobStore) throws Exception {
        if (nodeStore == null) {
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            FileStoreBuilder builder = fileStoreBuilder(getWorkDir())
                    .withNodeDeduplicationCacheSize(16384)
                    .withBlobStore(blobStore)
                    .withMaxFileSize(256)
                    .withMemoryMapping(false)
                    .withStatisticsProvider(new DefaultStatisticsProvider(executor))
                    .withGCOptions(gcOptions);
            store = builder.build();
            nodeStore = SegmentNodeStoreBuilders.builder(store).build();
        }
        return nodeStore;
    }

    private File getWorkDir() {
        return folder.getRoot();
    }

    public DataStoreState setUp() throws Exception {
        return setUp(10);
    }

    protected DataStoreBlobStore getBlobStore(File folder) throws Exception {
        return DataStoreUtils.getBlobStore(folder);
    }

    public DataStoreState setUp(int count) throws Exception {
        if (blobStore == null) {
            blobStore = getBlobStore(folder.newFolder());
        }
        nodeStore = getNodeStore(blobStore);

        NodeBuilder a = nodeStore.getRoot().builder();

        /* Create garbage by creating in-lined blobs (size < 16KB) */
        int number = 500;
        NodeBuilder content = a.child("content");
        for (int i = 0; i < number; i++) {
            NodeBuilder c = content.child("x" + i);
            for (int j = 0; j < 5; j++) {
                c.setProperty("p" + j, nodeStore.createBlob(randomStream(j, 16384)));
            }
        }
        nodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        final long dataSize = store.getStats().getApproximateSize();
        log.info("File store dataSize {}", byteCountToDisplaySize(dataSize));

        // 2. Now remove the nodes to generate garbage
        content = a.child("content");
        for (int i = 0; i < 100; i++) {
            NodeBuilder c = content.child("x" + i);
            for (int j = 0; j < 5; j++) {
                c.removeProperty("p" + j);
            }
        }
        nodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        /* Create and delete nodes with blobs stored in DS*/
        int maxDeleted  = 5;
        int numBlobs = count;
        List<Integer> processed = Lists.newArrayList();
        Random rand = new Random();
        for (int i = 0; i < maxDeleted; i++) {
            int n = rand.nextInt(numBlobs);
            if (!processed.contains(n)) {
                processed.add(n);
            }
        }
    
        DataStoreState state = new DataStoreState();
        for (int i = 0; i < numBlobs; i++) {
            SegmentBlob b = (SegmentBlob) nodeStore.createBlob(randomStream(i, 18342));
            Iterator<String> idIter = blobStore.resolveChunks(b.getBlobId());
            while (idIter.hasNext()) {
                String chunk = idIter.next();
                state.blobsAdded.add(chunk);
                if (!processed.contains(i)) {
                    state.blobsPresent.add(chunk);
                }
            }
            a.child("c" + i).setProperty("x", b);
        }
        
        nodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        log.info("Created blobs : {}", state.blobsAdded.size());

        for (int id : processed) {
            delete("c" + id);
        }
        log.info("Deleted nodes : {}", processed.size());

        // Sleep a little to make eligible for cleanup
        TimeUnit.MILLISECONDS.sleep(5);

        // Ensure cleanup is efficient by surpassing the number of
        // retained generations
        for (int k = 0; k < gcOptions.getRetainedGenerations(); k++) {
            store.compactFull();
        }
        store.cleanup();

        return state;
    }

    private HashSet<String> addInlined() throws Exception {
        HashSet<String> set = new HashSet<String>();
        NodeBuilder a = nodeStore.getRoot().builder();
        int number = 4;
        for (int i = 0; i < number; i++) {
            Blob b = nodeStore.createBlob(randomStream(i, 16514));
            a.child("cinline" + i).setProperty("x", b);
        }
        nodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return set;
    }

    private HashSet<String> addNodeSpecialChars() throws Exception {
        List<String> specialCharSets =
            Lists.newArrayList("q\\%22afdg\\%22", "a\nbcd", "a\n\rabcd", "012\\efg" );
        HashSet<String> set = new HashSet<String>();
        NodeBuilder a = nodeStore.getRoot().builder();
        for (int i = 0; i < specialCharSets.size(); i++) {
            SegmentBlob b = (SegmentBlob) nodeStore.createBlob(randomStream(i, 18432));
            NodeBuilder n = a.child("cspecial");
            n.child(specialCharSets.get(i)).setProperty("x", b);
            Iterator<String> idIter = blobStore.resolveChunks(b.getBlobId());
            set.addAll(Lists.newArrayList(idIter));
        }
        nodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return set;
    }

    private class DataStoreState {
        Set<String> blobsAdded = Sets.newHashSet();
        Set<String> blobsPresent = Sets.newHashSet();
    }
    
    private void delete(String nodeId) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.child(nodeId).remove();

        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @Test
    public void gc() throws Exception {
        DataStoreState state = setUp();
        log.info("{} blobs that should remain after gc : {}", state.blobsPresent.size(), state.blobsPresent);
        log.info("{} blobs for nodes which are deleted : {}", state.blobsPresent.size(), state.blobsPresent);
        Set<String> existingAfterGC = gcInternal(0);
        assertTrue(Sets.symmetricDifference(state.blobsPresent, existingAfterGC).isEmpty());
    }

    @Test
    public void checkMark() throws Exception {
        LogCustomizer customLogs = LogCustomizer
            .forLogger(MarkSweepGarbageCollector.class.getName())
            .enable(Level.TRACE)
            .filter(Level.TRACE)
            .create();

        DataStoreState state = setUp(10);
        log.info("{} blobs available : {}", state.blobsPresent.size(), state.blobsPresent);
        customLogs.starting();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        String rootFolder = folder.newFolder().getAbsolutePath();
        MarkSweepGarbageCollector gcObj = init(0, executor, rootFolder);
        gcObj.collectGarbage(true);
        customLogs.finished();

        assertBlobReferenceRecords(state.blobsPresent, rootFolder);
    }

    @Test
    public void noGc() throws Exception {
        DataStoreState state = setUp();
        log.info("{} blobs that should remain after gc : {}", state.blobsAdded.size(), state.blobsAdded);
        log.info("{} blobs for nodes which are deleted : {}", state.blobsPresent.size(), state.blobsPresent);
        Set<String> existingAfterGC = gcInternal(86400);
        assertTrue(Sets.symmetricDifference(state.blobsAdded, existingAfterGC).isEmpty());
    }

    @Test
    public void gcSpecialChar() throws Exception {
        DataStoreState state = setUp();
        Set<String> specialCharNodeBlobs = addNodeSpecialChars();
        state.blobsAdded.addAll(specialCharNodeBlobs);
        state.blobsPresent.addAll(specialCharNodeBlobs);
        Set<String> existingAfterGC = gcInternal(0);
        assertTrue(Sets.symmetricDifference(state.blobsPresent, existingAfterGC).isEmpty());
    }

    @Test
    public void consistencyCheckInit() throws Exception {
        DataStoreState state = setUp();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gcObj = init(86400, executor);
        long candidates = gcObj.checkConsistency();
        assertEquals(1, executor.getTaskCount());
        assertEquals(0, candidates);
    }

    @Test
    public void consistencyCheckWithGc() throws Exception {
        DataStoreState state = setUp();
        Set<String> existingAfterGC = gcInternal(0);
        assertTrue(Sets.symmetricDifference(state.blobsPresent, existingAfterGC).isEmpty());
        
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gcObj = init(86400, executor);
        long candidates = gcObj.checkConsistency();
        assertEquals(1, executor.getTaskCount());
        assertEquals(0, candidates);
    }
    
    @Test
    public void consistencyCheckWithRenegadeDelete() throws Exception {
        DataStoreState state = setUp();
        
        // Simulate faulty state by deleting some blobs directly
        Random rand = new Random(87);
        List<String> existing = Lists.newArrayList(state.blobsPresent);
        
        long count = blobStore.countDeleteChunks(ImmutableList.of(existing.get(rand.nextInt(existing.size()))), 0);
        
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gcObj = init(86400, executor);
        long candidates = gcObj.checkConsistency();
        assertEquals(1, executor.getTaskCount());
        assertEquals(count, candidates);
    }

    @Test
    public void gcLongRunningBlobCollection() throws Exception {
        DataStoreState state = setUp();
        log.info("{} Blobs added {}", state.blobsAdded.size(), state.blobsAdded);
        log.info("{} Blobs should be present {}", state.blobsPresent.size(), state.blobsPresent);
        
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        String repoId = null;
        if (SharedDataStoreUtils.isShared(store.getBlobStore())) {
            repoId = ClusterRepositoryInfo.getOrCreateId(nodeStore);
            ((SharedDataStore) store.getBlobStore()).addMetadataRecord(
                new ByteArrayInputStream(new byte[0]),
                REPOSITORY.getNameFromId(repoId));
        }
        TestGarbageCollector gc = new TestGarbageCollector(
            new SegmentBlobReferenceRetriever(store),
            (GarbageCollectableBlobStore) store.getBlobStore(), executor, folder.newFolder().getAbsolutePath(), 5, 5000, repoId);
        gc.collectGarbage(false);
        Set<String> existingAfterGC = iterate();
        log.info("{} Blobs existing after gc {}", existingAfterGC.size(), existingAfterGC);
        
        assertTrue(Sets.difference(state.blobsPresent, existingAfterGC).isEmpty());
        assertEquals(gc.additionalBlobs, Sets.symmetricDifference(state.blobsPresent, existingAfterGC));
    }

    @Test
    public void gcWithInlined() throws Exception {
        blobStore = new DataStoreBlobStore(DataStoreUtils.createFDS(new File(getWorkDir(), "datastore"), 16516));
        DataStoreState state = setUp();
        addInlined();
        log.info("{} blobs that should remain after gc : {}", state.blobsAdded.size(), state.blobsAdded);
        log.info("{} blobs for nodes which are deleted : {}", state.blobsPresent.size(), state.blobsPresent);
        Set<String> existingAfterGC = gcInternal(0);
        assertTrue(Sets.symmetricDifference(state.blobsPresent, existingAfterGC).isEmpty());
    }

    @Test
    public void consistencyCheckInlined() throws Exception {
        blobStore = new DataStoreBlobStore(DataStoreUtils.createFDS(new File(getWorkDir(), "datastore"), 16516));
        DataStoreState state = setUp();
        addInlined();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gcObj = init(86400, executor);
        long candidates = gcObj.checkConsistency();
        assertEquals(1, executor.getTaskCount());
        assertEquals(0, candidates);
    }

    private Set<String> gcInternal(long maxBlobGcInSecs) throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gc = init(maxBlobGcInSecs, executor);
        gc.collectGarbage(false);

        assertEquals(0, executor.getTaskCount());
        Set<String> existingAfterGC = iterate();
        log.info("{} blobs existing after gc : {}", existingAfterGC.size(), existingAfterGC);
        return existingAfterGC;
    }

    private static void assertBlobReferenceRecords(Set<String> expected, String rootFolder) throws IOException {
        // Read the marked files to check if paths logged or not
        File root = new File(rootFolder);
        List<File> rootFile = FileFilterUtils.filterList(
            FileFilterUtils.prefixFileFilter("gcworkdir-"),
            root.listFiles());
        List<File> markedFiles = FileFilterUtils.filterList(
            FileFilterUtils.prefixFileFilter("marked-"),
            rootFile.get(0).listFiles());
        try (InputStream is = new FileInputStream(markedFiles.get(0))) {
                Set<String> records = FileIOUtils.readStringsAsSet(is, true);
                assertEquals(expected.size(), records.size());
                assertEquals(expected, records);
        } finally {
            FileUtils.forceDelete(rootFile.get(0));
        }
    }

    private MarkSweepGarbageCollector init(long blobGcMaxAgeInSecs, ThreadPoolExecutor executor)
        throws Exception {
        return init(blobGcMaxAgeInSecs, executor, folder.newFolder().getAbsolutePath());
    }

    private MarkSweepGarbageCollector init(long blobGcMaxAgeInSecs, ThreadPoolExecutor executor,
        String root) throws Exception {
        String repoId = null;
        if (SharedDataStoreUtils.isShared(store.getBlobStore())) {
            repoId = ClusterRepositoryInfo.getOrCreateId(nodeStore);
            ((SharedDataStore) store.getBlobStore()).addMetadataRecord(
                new ByteArrayInputStream(new byte[0]), 
                REPOSITORY.getNameFromId(repoId));
        }
        MarkSweepGarbageCollector gc =
            new MarkSweepGarbageCollector(new SegmentBlobReferenceRetriever(store),
                (GarbageCollectableBlobStore) store.getBlobStore(), executor,
                root, 2048, blobGcMaxAgeInSecs, repoId);
        return gc;
    }    

    private Set<String> iterate() throws Exception {
        Iterator<String> cur = blobStore.getAllChunkIds(0);

        Set<String> existing = Sets.newHashSet();
        while (cur.hasNext()) {
            existing.add(cur.next());
        }
        return existing;
    }

    /**
    * Waits for some time and adds additional blobs after blob referenced identified to simulate
    * long running blob id collection phase.
     */
    private class TestGarbageCollector extends MarkSweepGarbageCollector {

        private long maxLastModifiedInterval;

        private String root;

        private GarbageCollectableBlobStore blobStore;

        private Set<String> additionalBlobs;
        
        TestGarbageCollector(BlobReferenceRetriever marker, GarbageCollectableBlobStore blobStore,
                                    Executor executor, String root, int batchCount, long maxLastModifiedInterval,
                                    @Nullable String repositoryId) throws IOException {
            super(marker, blobStore, executor, root, batchCount, maxLastModifiedInterval, repositoryId);
            this.root = root;
            this.blobStore = blobStore;
            this.maxLastModifiedInterval = maxLastModifiedInterval;
            this.additionalBlobs = Sets.newHashSet();
        }

        @Override
        protected void markAndSweep(boolean markOnly, boolean forceBlobRetrieve) throws Exception {
            try (GarbageCollectorFileState fs = new GarbageCollectorFileState(root)) {
                Stopwatch sw = Stopwatch.createStarted();
                LOG.info("Starting Test Blob garbage collection");
                
                // Sleep a little more than the max interval to get over the interval for valid blobs
                Thread.sleep(maxLastModifiedInterval + 100);
                LOG.info("Slept {} to make blobs old", maxLastModifiedInterval + 100);
                
                long markStart = System.currentTimeMillis();
                mark(fs);
                LOG.info("Mark finished");
                
                additionalBlobs = createAdditional();
                
                if (!markOnly) {
                    Thread.sleep(maxLastModifiedInterval + 100);
                    LOG.info("Slept {} to make additional blobs old", maxLastModifiedInterval + 100);

                    long deleteCount = sweep(fs, markStart, forceBlobRetrieve);

                    LOG.info("Blob garbage collection completed in {}. Number of blobs deleted [{}]", sw.toString(),
                        deleteCount, maxLastModifiedInterval);
                }
            }
        }
        
        private HashSet<String> createAdditional() throws Exception {
            HashSet<String> blobSet = new HashSet<String>();
            NodeBuilder a = nodeStore.getRoot().builder();
            int number = 5;
            for (int i = 0; i < number; i++) {
                SegmentBlob b = (SegmentBlob) nodeStore.createBlob(randomStream(100 + i, 16516));
                a.child("cafter" + i).setProperty("x", b);
                Iterator<String> idIter =
                    ((GarbageCollectableBlobStore) blobStore).resolveChunks(b.getBlobId());
                while (idIter.hasNext()) {
                    String chunk = idIter.next();
                    blobSet.add(chunk);
                }
            }
            log.info("{} Additional created {}", blobSet.size(), blobSet);
            
            nodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            return blobSet;
        }
    }

}

