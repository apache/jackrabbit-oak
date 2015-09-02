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
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.plugins.identifier.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for SegmentNodeStore DataStore GC
 */
@RunWith(Parameterized.class)
public class SegmentDataStoreBlobGCTest {
    private static final Logger log = LoggerFactory.getLogger(SegmentDataStoreBlobGCTest.class);

    private final boolean usePersistedMap;

    SegmentNodeStore nodeStore;
    FileStore store;
    DataStoreBlobStore blobStore;
    Date startDate;

    @Parameterized.Parameters
    public static List<Boolean[]> fixtures() {
        return ImmutableList.of(new Boolean[] {true}, new Boolean[] {false});
    }

    public SegmentDataStoreBlobGCTest(boolean usePersistedMap) {
        this.usePersistedMap = usePersistedMap;
    }

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
            compactionStrategy.setPersistCompactionMap(usePersistedMap);
            store.setCompactionStrategy(compactionStrategy);
            nodeStore = new SegmentNodeStore(store);
        }
        return nodeStore;
    }

    private static File getWorkDir() {
        return new File("target", "DataStoreBlobGCTest");
    }

    public DataStoreState setUp() throws Exception {
        blobStore = DataStoreUtils.getBlobStore();
        nodeStore = getNodeStore(blobStore);
        startDate = new Date();
        
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
        log.info("Deleted nodes : {}", processed.size());

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
    
    private Set<String> gcInternal(long maxBlobGcInSecs) throws Exception {
        String repoId = null;
        if (SharedDataStoreUtils.isShared(store.getBlobStore())) {
            repoId = ClusterRepositoryInfo.createId(nodeStore);
            ((SharedDataStore) store.getBlobStore()).addMetadataRecord(
                new ByteArrayInputStream(new byte[0]),
                REPOSITORY.getNameFromId(repoId));
        }
        
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gc = new MarkSweepGarbageCollector(
                new SegmentBlobReferenceRetriever(store.getTracker()),
                    (GarbageCollectableBlobStore) store.getBlobStore(), executor, "./target", 2048, maxBlobGcInSecs, 
                                                                        repoId);
        gc.collectGarbage(false);

        assertEquals(0, executor.getTaskCount());
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
    public void close() throws Exception {
        if (store != null) {
            store.close();
        }
        DataStoreUtils.cleanup(blobStore.getDataStore(), startDate);
        FileUtils.deleteDirectory(getWorkDir());
        FileUtils.deleteDirectory(new File(DataStoreUtils.getHomeDir()));
    }

    static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }
}

