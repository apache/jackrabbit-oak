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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import junit.framework.Assert;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Tests for MongoMK GC
 */
public class MongoBlobGCTest extends AbstractMongoConnectionTest {
    private Clock clock;
    private static final Logger log = LoggerFactory.getLogger(MongoBlobGCTest.class);

    public DataStoreState setUp(boolean deleteDirect) throws Exception {
        DocumentNodeStore s = mk.getNodeStore();
        NodeBuilder a = s.getRoot().builder();

        int number = 10;
        // track the number of the assets to be deleted
        List<Integer> processed = Lists.newArrayList();
        Random rand = new Random(47);
        for (int i = 0; i < 5; i++) {
            int n = rand.nextInt(number);
            if (!processed.contains(n)) {
                processed.add(n);
            }
        }
    
        DataStoreState state = new DataStoreState();
        for (int i = 0; i < number; i++) {
            Blob b = s.createBlob(randomStream(i, 4160));
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
        }
        s.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        if (deleteDirect) {
            for (int id : processed) {
                deleteFromMongo("c" + id);
            }
        } else {
            a = s.getRoot().builder();
            for (int id : processed) {
                a.child("c" + id).remove();
                s.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            }
            long maxAge = 10; // hours
            // 1. Go past GC age and check no GC done as nothing deleted
            clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(maxAge));

            VersionGarbageCollector vGC = s.getVersionGarbageCollector();
            VersionGCStats stats = vGC.gc(0, TimeUnit.MILLISECONDS);
            Assert.assertEquals(processed.size(), stats.deletedDocGCCount);
        }

        return state;
    }
    
    private class DataStoreState {
        Set<String> blobsAdded = Sets.newHashSet();
        Set<String> blobsPresent = Sets.newHashSet();
    }
    
    private HashSet<String> addInlined() throws Exception {
        HashSet<String> set = new HashSet<String>();
        DocumentNodeStore s = mk.getNodeStore();
        NodeBuilder a = s.getRoot().builder();
        int number = 12;
        for (int i = 0; i < number; i++) {
            Blob b = s.createBlob(randomStream(i, 50));
            a.child("cinline" + i).setProperty("x", b);
        }
        s.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return set;
    }

    private void deleteFromMongo(String nodeId) {
        DBCollection coll = mongoConnection.getDB().getCollection("nodes");
        BasicDBObject blobNodeObj = new BasicDBObject();
        blobNodeObj.put("_id", "1:/" + nodeId);
        coll.remove(blobNodeObj);
    }

    @Test
    public void gcDirectMongoDelete() throws Exception {
        DataStoreState state = setUp(true);
        Set<String> existingAfterGC = gc(0);
        assertTrue(Sets.symmetricDifference(state.blobsPresent, existingAfterGC).isEmpty());
    }
    
    @Test
    public void noGc() throws Exception {
        DataStoreState state = setUp(true);
        Set<String> existingAfterGC = gc(86400);
        assertTrue(Sets.symmetricDifference(state.blobsAdded, existingAfterGC).isEmpty());
    }    

    @Test
    public void gcVersionDelete() throws Exception {
        DataStoreState state = setUp(false);
        Set<String> existingAfterGC = gc(0);
        assertTrue(Sets.symmetricDifference(state.blobsPresent, existingAfterGC).isEmpty());
    }

    @Test
    public void gcDirectMongoDeleteWithInlined() throws Exception {
        DataStoreState state = setUp(true);
        addInlined();
        Set<String> existingAfterGC = gc(0);
        assertTrue(Sets.symmetricDifference(state.blobsPresent, existingAfterGC).isEmpty());
    }
    @Test
    public void gcVersionDeleteWithInlined() throws Exception {
        DataStoreState state = setUp(false);
        addInlined();
        Set<String> existingAfterGC = gc(0);
        assertTrue(Sets.symmetricDifference(state.blobsPresent, existingAfterGC).isEmpty());
    }
    
    @Test
    public void gcLongRunningBlobCollection() throws Exception {
        DataStoreState state = setUp(true);
        log.info("{} Blobs added {}", state.blobsAdded.size(), state.blobsAdded);
        log.info("{} Blobs should be present {}", state.blobsPresent.size(), state.blobsPresent);
        
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        DocumentNodeStore store = mk.getNodeStore();

        TestGarbageCollector gc = new TestGarbageCollector(
            new DocumentBlobReferenceRetriever(store),
            (GarbageCollectableBlobStore) store.getBlobStore(), executor, "./target", 5, 5000);
        gc.collectGarbage();
        
        Set<String> existingAfterGC = iterate();
        log.info("{} Blobs existing after gc {}", existingAfterGC.size(), existingAfterGC);
        
        assertTrue(Sets.difference(state.blobsPresent, existingAfterGC).isEmpty());
        assertEquals(gc.additionalBlobs, Sets.symmetricDifference(state.blobsPresent, existingAfterGC));
    }

    private Set<String> gc(int blobGcMaxAgeInSecs) throws Exception {
        DocumentNodeStore store = mk.getNodeStore();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gc = new MarkSweepGarbageCollector(
                new DocumentBlobReferenceRetriever(store),
                (GarbageCollectableBlobStore) store.getBlobStore(),
                executor,
                "./target", 5, true, blobGcMaxAgeInSecs);
        gc.collectGarbage();
        
        assertEquals(1, executor.getTaskCount());
        return iterate();
    }

    protected Set<String> iterate() throws Exception {
        GarbageCollectableBlobStore store = (GarbageCollectableBlobStore)
                mk.getNodeStore().getBlobStore();
        Iterator<String> cur = store.getAllChunkIds(0);

        Set<String> existing = Sets.newHashSet();
        while (cur.hasNext()) {
            existing.add(cur.next());
        }
        return existing;
    }

    static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    @Override
    protected Clock getTestClock() throws InterruptedException {
        clock = new Clock.Virtual();
        clock.waitUntil(Revision.getCurrentTimestamp());
        return clock;
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
            DocumentNodeStore s = mk.getNodeStore();
            NodeBuilder a = s.getRoot().builder();
            int number = 5;
            for (int i = 0; i < number; i++) {
                Blob b = s.createBlob(randomStream(100 + i, 16516));
                a.child("cafter" + i).setProperty("x", b);
                Iterator<String> idIter =
                    ((GarbageCollectableBlobStore) s.getBlobStore())
                        .resolveChunks(b.toString());
                while (idIter.hasNext()) {
                    String chunk = idIter.next();
                    blobSet.add(chunk);
                }
            }
            log.info("{} Additional created {}", blobSet.size(), blobSet);
            
            s.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            return blobSet;
        }
    }
}
