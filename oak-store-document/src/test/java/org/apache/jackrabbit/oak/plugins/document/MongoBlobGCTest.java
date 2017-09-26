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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import ch.qos.logback.classic.Level;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import junit.framework.Assert;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.blob.GarbageCollectorFileState;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobReferenceIterator;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils
    .SharedStoreRecordType.REPOSITORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for MongoMK GC
 */
public class MongoBlobGCTest extends AbstractMongoConnectionTest {
    private Clock clock;
    private static final Logger log = LoggerFactory.getLogger(MongoBlobGCTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    public DataStoreState setUp(boolean deleteDirect) throws Exception {
        return setUp(deleteDirect, 10);
    }

    public DataStoreState setUp(boolean deleteDirect, int count) throws Exception {
        DocumentNodeStore s = mk.getNodeStore();
        NodeBuilder a = s.getRoot().builder();

        int number = count;
        int maxDeleted = 5;
        // track the number of the assets to be deleted
        List<Integer> processed = Lists.newArrayList();
        Random rand = new Random(47);
        for (int i = 0; i < maxDeleted; i++) {
            int n = rand.nextInt(number);
            if (!processed.contains(n)) {
                processed.add(n);
            }
        }
    
        DataStoreState state = new DataStoreState();
        for (int i = 0; i < number; i++) {
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
            if (i == 0) {
                a.child("cdup").setProperty("x", b);
            }
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

    private HashSet<String> addNodeSpecialChars() throws Exception {
        List<String> specialCharSets =
            Lists.newArrayList("q\\%22afdg\\%22", "a\nbcd", "a\n\rabcd", "012\\efg" );
        DocumentNodeStore ds = mk.getNodeStore();
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
    public void checkMark() throws Exception {
        String rootFolder = folder.newFolder().getAbsolutePath();
        LogCustomizer customLogs = LogCustomizer
            .forLogger(MarkSweepGarbageCollector.class.getName())
            .enable(Level.TRACE)
            .filter(Level.TRACE)
            .create();

        DataStoreState state = setUp(true, 10);
        log.info("{} blobs available : {}", state.blobsPresent.size(), state.blobsPresent);
        customLogs.starting();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gcObj = init(0, executor, rootFolder);
        gcObj.collectGarbage(true);
        customLogs.finished();

        assertBlobReferences(state.blobsPresent, rootFolder);
    }

    @Test
    public void gcSpecialChar() throws Exception {
        DataStoreState state = setUp(true);
        Set<String> specialCharNodeBlobs = addNodeSpecialChars();
        state.blobsAdded.addAll(specialCharNodeBlobs);
        state.blobsPresent.addAll(specialCharNodeBlobs);
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
    public void consistencyCheckInlined() throws Exception {
        DataStoreState state = setUp(true);
        addInlined();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gcObj = init(86400, executor);
        long candidates = gcObj.checkConsistency();
        assertEquals(1, executor.getTaskCount());
        assertEquals(0, candidates);        
    }
    
    @Test
    public void consistencyCheckInit() throws Exception {
        DataStoreState state = setUp(true);
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gcObj = init(86400, executor);
        long candidates = gcObj.checkConsistency();
        assertEquals(1, executor.getTaskCount());
        assertEquals(0, candidates);
    }

    @Test
    public void consistencyCheckWithGc() throws Exception {
        DataStoreState state = setUp(true);
        Set<String> existingAfterGC = gc(0);
        assertTrue(Sets.symmetricDifference(state.blobsPresent, existingAfterGC).isEmpty());
        
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gcObj = init(86400, executor);
        long candidates = gcObj.checkConsistency();
        assertEquals(1, executor.getTaskCount());
        assertEquals(0, candidates);
    }
    
    @Test
    public void consistencyCheckWithRenegadeDelete() throws Exception {
        DataStoreState state = setUp(true);
        
        // Simulate faulty state by deleting some blobs directly
        Random rand = new Random(87);
        List<String> existing = Lists.newArrayList(state.blobsPresent);

        GarbageCollectableBlobStore store = (GarbageCollectableBlobStore)
                                                mk.getNodeStore().getBlobStore();
        long count = store.countDeleteChunks(ImmutableList.of(existing.get(rand.nextInt(existing.size()))), 0);
    
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gcObj = init(86400, executor);
        long candidates = gcObj.checkConsistency();
        assertEquals(1, executor.getTaskCount());
        assertEquals(count, candidates);
    }

    // OAK-3390
    @Test
    public void referencedBlobs() throws Exception {
        Iterator<ReferencedBlob> blobs = mk.getNodeStore().getReferencedBlobsIterator();
        assertTrue(blobs instanceof MongoBlobReferenceIterator);
    }
    
    @Test
    public void gcLongRunningBlobCollection() throws Exception {
        DataStoreState state = setUp(true);
        log.info("{} Blobs added {}", state.blobsAdded.size(), state.blobsAdded);
        log.info("{} Blobs should be present {}", state.blobsPresent.size(), state.blobsPresent);
        
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        DocumentNodeStore store = mk.getNodeStore();
        String repoId = null;
        if (SharedDataStoreUtils.isShared(store.getBlobStore())) {
            repoId = ClusterRepositoryInfo.getOrCreateId(store);
            ((SharedDataStore) store.getBlobStore()).addMetadataRecord(
                new ByteArrayInputStream(new byte[0]),
                REPOSITORY.getNameFromId(repoId));
        }
        TestGarbageCollector gc =
            new TestGarbageCollector(new DocumentBlobReferenceRetriever(store),
                (GarbageCollectableBlobStore) store.getBlobStore(), executor,
                folder.newFolder().getAbsolutePath(), 5, 5000, repoId);
        gc.collectGarbage(false);
        Set<String> existingAfterGC = iterate();
        log.info("{} Blobs existing after gc {}", existingAfterGC.size(), existingAfterGC);
    
        assertTrue(Sets.difference(state.blobsPresent, existingAfterGC).isEmpty());
        assertEquals(gc.additionalBlobs, Sets.symmetricDifference(state.blobsPresent, existingAfterGC));
    }

    @Test
    public void checkGcPathLogging() throws Exception {
        String rootFolder = folder.newFolder().getAbsolutePath();
        LogCustomizer customLogs = LogCustomizer
            .forLogger(MarkSweepGarbageCollector.class.getName())
            .enable(Level.TRACE)
            .filter(Level.TRACE)
            .create();

        setUp(false);
        customLogs.starting();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gcObj = init(0, executor, rootFolder);
        gcObj.collectGarbage(true);
        customLogs.finished();

        assertBlobReferenceRecords(1, rootFolder);
    }

    @Test
    public void checkConsistencyPathLogging() throws Exception {
        String rootFolder = folder.newFolder().getAbsolutePath();
        LogCustomizer customLogs = LogCustomizer
            .forLogger(MarkSweepGarbageCollector.class.getName())
            .enable(Level.TRACE)
            .filter(Level.TRACE)
            .create();

        setUp(false);
        customLogs.starting();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gcObj = init(86400, executor, rootFolder);
        gcObj.checkConsistency();
        customLogs.finished();

        assertBlobReferenceRecords(2, rootFolder);
    }

    private static void assertBlobReferences(Set<String> expected, String rootFolder) throws IOException {
        InputStream is = null;
        try {
            is = new FileInputStream(getMarkedFile(rootFolder));
            Set<String> records = FileIOUtils.readStringsAsSet(is, true);
            assertEquals(expected, records);
        } finally {
            Closeables.close(is, false);
        }
    }

    private static void assertBlobReferenceRecords(int expected, String rootFolder) throws IOException {
        InputStream is = null;
        try {
            is = new FileInputStream(getMarkedFile(rootFolder));
            Set<String> records = FileIOUtils.readStringsAsSet(is, true);
            for (String rec : records) {
                assertEquals(expected, Splitter.on(",").omitEmptyStrings().splitToList(rec).size());
            }
        } finally {
            Closeables.close(is, false);
        }
    }

    private static File getMarkedFile(String rootFolder) {
        // Read the marked files to check if paths logged or not
        File root = new File(rootFolder);
        List<File> rootFile = FileFilterUtils.filterList(
            FileFilterUtils.prefixFileFilter("gcworkdir-"),
            root.listFiles());
        List<File> markedFiles = FileFilterUtils.filterList(
            FileFilterUtils.prefixFileFilter("marked-"),
            rootFile.get(0).listFiles());
        return markedFiles.get(0);
    }

    private Set<String> gc(int blobGcMaxAgeInSecs) throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gc = init(blobGcMaxAgeInSecs, executor);
        gc.collectGarbage(false);
        
        assertEquals(0, executor.getTaskCount());
        return iterate();
    }

    private MarkSweepGarbageCollector init(int blobGcMaxAgeInSecs, ThreadPoolExecutor executor) throws Exception {
        return init(blobGcMaxAgeInSecs, executor, null);
    }

    private MarkSweepGarbageCollector init(int blobGcMaxAgeInSecs, ThreadPoolExecutor executor,
        String root) throws Exception {
        DocumentNodeStore store = mk.getNodeStore();
        String repoId = null;
        if (SharedDataStoreUtils.isShared(store.getBlobStore())) {
            repoId = ClusterRepositoryInfo.getOrCreateId(store);
            ((SharedDataStore) store.getBlobStore()).addMetadataRecord(
                new ByteArrayInputStream(new byte[0]),
                REPOSITORY.getNameFromId(repoId));
        }
        if (Strings.isNullOrEmpty(root)) {
            root = folder.newFolder().getAbsolutePath();
        }

        MarkSweepGarbageCollector gc = new MarkSweepGarbageCollector(
                new DocumentBlobReferenceRetriever(store),
                (GarbageCollectableBlobStore) store.getBlobStore(), executor, root, 5, blobGcMaxAgeInSecs, repoId);
        return gc;
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
            boolean threw = true;
            GarbageCollectorFileState fs = new GarbageCollectorFileState(root);
            try {
                Stopwatch sw = Stopwatch.createStarted();
                LOG.info("Starting Test Blob garbage collection");
                
                // Sleep a little more than the max interval to get over the interval for valid blobs
                Thread.sleep(maxLastModifiedInterval + 1000);
                LOG.info("Slept {} to make blobs old", maxLastModifiedInterval + 1000);
                
                long markStart = System.currentTimeMillis();
                mark(fs);
                LOG.info("Mark finished");
                
                additionalBlobs = createAdditional();
    
                if (!markOnly) {
                    Thread.sleep(maxLastModifiedInterval + 100);
                    LOG.info("Slept {} to make additional blobs old", maxLastModifiedInterval + 100);
    
                    long deleteCount = sweep(fs, markStart, forceBlobRetrieve);
                    threw = false;
            
                    LOG.info("Blob garbage collection completed in {}. Number of blobs deleted [{}]", sw.toString(),
                        deleteCount, maxLastModifiedInterval);
                }
            } finally {
                if (!LOG.isTraceEnabled()) {
                    Closeables.close(fs, threw);
                }
            }
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
