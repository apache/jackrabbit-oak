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
package org.apache.jackrabbit.oak.plugins.segment;

import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

/**
 * Tests for SegmentNodeStore DataStore GC
 */
public class SegmentDataStoreBlobGCTest {
    private static final Logger log = LoggerFactory.getLogger(SegmentDataStoreBlobGCTest.class);
    
    SegmentNodeStore nodeStore;
    FileStore store;
    DataStoreBlobStore blobStore;

    protected SegmentNodeStore getNodeStore(BlobStore blobStore) throws IOException {
        if (nodeStore == null) {
            FileStore.Builder builder = FileStore.newFileStore(getWorkDir())
                                            .withBlobStore(blobStore).withMaxFileSize(256)
                                            .withCacheSize(64).withMemoryMapping(false);
            store = builder.create();
            CompactionStrategy compactionStrategy =
                new CompactionStrategy(false, true,
                    CompactionStrategy.CleanupType.CLEAN_OLD, 0, CompactionStrategy.MEMORY_THRESHOLD_DEFAULT) {
                    @Override
                    public boolean compacted(@Nonnull Callable<Boolean> setHead) throws Exception {
                        return setHead.call();
                    }
                };
            store.setCompactionStrategy(compactionStrategy);
            nodeStore = new SegmentNodeStore(store);
        }
        return nodeStore;

    }

    private static File getWorkDir() {
        return new File("target", "DataStoreBlobGCTest");
    }

    public DataStoreState setUp() throws Exception {
        FileDataStore fds = new FileDataStore();
        fds.setMinRecordLength(4092);
        fds.init(getWorkDir().getAbsolutePath());
        blobStore = new DataStoreBlobStore(fds);
        nodeStore = getNodeStore(blobStore);
        
        NodeBuilder a = nodeStore.getRoot().builder();

        /* Create garbage by creating in-lined blobs (size < 16KB) */
        int number = 4000;
        NodeBuilder content = a.child("content");
        for (int i = 0; i < number; i++) {
            NodeBuilder c = content.child("x" + i);
            for (int j = 0; j < 5; j++) {
                c.setProperty("p" + j, nodeStore.createBlob(randomStream(j, 16384)));
            }
        }
        nodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    
        final long dataSize = store.size();
        log.info("File store dataSize {}", byteCountToDisplaySize(dataSize));
    
        // 2. Now remove the nodes to generate garbage
        content = a.child("content");
        for (int i = 0; i < 2000; i++) {
            NodeBuilder c = content.child("x" + i);
            for (int j = 0; j < 5; j++) {
                c.removeProperty("p" + j);
            }
        }
        nodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        /* Create and delete nodes with blobs stored in DS*/
        int maxDeleted  = 5;
        int numBlobs = 10;
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
            SegmentBlob b = (SegmentBlob) nodeStore.createBlob(randomStream(i, 16516));
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

        // Sleep a little to make eligible for cleanup
        TimeUnit.MILLISECONDS.sleep(5);
        store.maybeCompact(false);
        store.cleanup();

        return state;
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
    public void noGc() throws Exception {
        DataStoreState state = setUp();
        log.info("{} blobs that should remain after gc : {}", state.blobsAdded.size(), state.blobsAdded);
        log.info("{} blobs for nodes which are deleted : {}", state.blobsPresent.size(), state.blobsPresent);
        Set<String> existingAfterGC = gcInternal(86400);
        assertTrue(Sets.symmetricDifference(state.blobsAdded, existingAfterGC).isEmpty());
    }
    
    @Test
    public void gcLongRunningBlobCollection() throws Exception {
        DataStoreState state = setUp();
        log.info("{} Blobs added {}", state.blobsAdded.size(), state.blobsAdded);
        log.info("{} Blobs should be present {}", state.blobsPresent.size(), state.blobsPresent);
        
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        TestGarbageCollector gc = new TestGarbageCollector(
            new SegmentBlobReferenceRetriever(store.getTracker()),
            (GarbageCollectableBlobStore) store.getBlobStore(), executor, "./target", 5, 5000);
        gc.collectGarbage();
        Set<String> existingAfterGC = iterate();
        log.info("{} Blobs existing after gc {}", existingAfterGC.size(), existingAfterGC);
        
        assertTrue(Sets.difference(state.blobsPresent, existingAfterGC).isEmpty());
        assertEquals(gc.additionalBlobs, Sets.symmetricDifference(state.blobsPresent, existingAfterGC));
    }
    
    private Set<String> gcInternal(long maxBlobGcInSecs) throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gc = new MarkSweepGarbageCollector(
                new SegmentBlobReferenceRetriever(store.getTracker()),
                    (GarbageCollectableBlobStore) store.getBlobStore(), executor, 
                    "./target", 2048, true, maxBlobGcInSecs);
        gc.collectGarbage();

        assertEquals(1, executor.getTaskCount());
        Set<String> existingAfterGC = iterate();
        log.info("{} blobs existing after gc : {}", existingAfterGC.size(), existingAfterGC);
        return existingAfterGC;
    }

    protected Set<String> iterate() throws Exception {
        Iterator<String> cur = blobStore.getAllChunkIds(0);

        Set<String> existing = Sets.newHashSet();
        while (cur.hasNext()) {
            existing.add(cur.next());
        }
        return existing;
    }

    @After
    public void close() throws IOException {
        if (store != null) {
            store.close();
        }
        FileUtils.cleanDirectory(getWorkDir());
    }

    static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }
    
    /**
     * Waits for some time and adds additional blobs after blob referenced identified to simulate
     * long running blob id collection phase.
     */
    class TestGarbageCollector extends MarkSweepGarbageCollector {
        long maxLastModifiedInterval;
        String root;
        GarbageCollectableBlobStore blobStore;
        Set<String> additionalBlobs;
        
        public TestGarbageCollector(BlobReferenceRetriever marker, GarbageCollectableBlobStore blobStore,
                                    Executor executor, String root, int batchCount, long maxLastModifiedInterval) 
        throws IOException {
            super(marker, blobStore, executor, root, batchCount, true, maxLastModifiedInterval);
            this.root = root;
            this.blobStore = blobStore;
            this.maxLastModifiedInterval = maxLastModifiedInterval;
            this.additionalBlobs = Sets.newHashSet();
        }
    
        @Override
        protected void markAndSweep() throws IOException, InterruptedException {
            boolean threw = true;
            try {
                Stopwatch sw = Stopwatch.createStarted();
                LOG.info("Starting Blob garbage collection");
            
                // Sleep a little more than the max interval to get over the interval for valid blobs
                Thread.sleep(maxLastModifiedInterval + 1);
                LOG.info("Slept {} to make blobs old", maxLastModifiedInterval + 1);
            
                long markStart = System.currentTimeMillis();
                mark();
                int deleteCount = sweep(markStart);
                threw = false;
            
                LOG.info("Blob garbage collection completed in {}. Number of blobs identified for deletion [{}] (This "
                        + "includes blobs newer than configured interval [{}] which are ignored for deletion)",
                    sw.toString(), deleteCount, markStart - maxLastModifiedInterval);
            } finally {
                if (!LOG.isTraceEnabled()) {
                    Closeables.close(fs, threw);
                }
            }
        }
    
        @Override
        protected void mark() throws IOException, InterruptedException {
            LOG.debug("Starting mark phase of the garbage collector");
        
            // Find all blob references after iterating over the whole repository
            iterateNodeTree();
        
            try {
                additionalBlobs = createAdditional();
            } catch (Exception e) {
                LOG.warn("Error in creating additional blobs", e);
            }
        
            Thread.sleep(maxLastModifiedInterval + 100);
            LOG.info("Slept {} to make additional blobs old", maxLastModifiedInterval + 100);
        
            // Find all blobs available in the blob store
            try {
                (new BlobIdRetriever()).call();
            } catch (Exception e) {
                LOG.warn("Error occurred while fetching all the blobIds from the BlobStore. GC would " +
                    "continue with the blobIds retrieved so far", e.getCause());
            }
        
            difference();
            LOG.debug("Ending mark phase of the garbage collector");
        }
        
        public HashSet<String> createAdditional() throws Exception {
            HashSet<String> blobSet = new HashSet<String>();
            NodeBuilder a = nodeStore.getRoot().builder();
            int number = 5;
            for (int i = 0; i < number; i++) {
                SegmentBlob b = (SegmentBlob) nodeStore.createBlob(randomStream(100 + i, 16516));
                a.child("cafter" + i).setProperty("x", b);
                Iterator<String> idIter = blobStore.resolveChunks(b.getBlobId());
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

