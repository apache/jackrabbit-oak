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

import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.google.common.util.concurrent.MoreExecutors;
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

import javax.annotation.Nonnull;

/**
 * Tests for SegmentNodeStore DataStore GC
 */
public class SegmentDataStoreBlobGCTest {
    SegmentNodeStore nodeStore;
    FileStore store;
    DataStoreBlobStore blobStore;

    protected SegmentNodeStore getNodeStore(BlobStore blobStore) throws IOException {
        if (nodeStore == null) {
            store = new FileStore(blobStore, getWorkDir(), 256, false);
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

    public HashSet<String> setUp() throws Exception {
        blobStore = DataStoreUtils.getBlobStore();
        nodeStore = getNodeStore(blobStore);

        HashSet<String> set = new HashSet<String>();

        NodeBuilder a = nodeStore.getRoot().builder();

        int number = 10;
        int maxDeleted  = 5;

        // track the number of the assets to be deleted
        List<Integer> processed = Lists.newArrayList();
        Random rand = new Random();
        for (int i = 0; i < maxDeleted; i++) {
            int n = rand.nextInt(number);
            if (!processed.contains(n)) {
                processed.add(n);
            }
        }
        for (int i = 0; i < number; i++) {
            SegmentBlob b = (SegmentBlob) nodeStore.createBlob(randomStream(i, 16516));
            if (!processed.contains(i)) {
                Iterator<String> idIter = blobStore
                        .resolveChunks(b.getBlobId());
                while (idIter.hasNext()) {
                    set.add(idIter.next());
                }
            }
            a.child("c" + i).setProperty("x", b);
        }
        nodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        for (int id : processed) {
            delete("c" + id);
        }
        store.compact();

        return set;
    }

    private void delete(String nodeId) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.child(nodeId).remove();

        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @Test
    public void gc() throws Exception {
        HashSet<String> remaining = setUp();
        String repoId = null;
        if (SharedDataStoreUtils.isShared(store.getBlobStore())) {
            repoId = ClusterRepositoryInfo.createId(nodeStore);
            ((SharedDataStore) store.getBlobStore()).addMetadataRecord(
                new ByteArrayInputStream(new byte[0]),
                REPOSITORY.getNameFromId(repoId));
        }

        MarkSweepGarbageCollector gc = new MarkSweepGarbageCollector(
                new SegmentBlobReferenceRetriever(store.getTracker()),
                    (GarbageCollectableBlobStore) store.getBlobStore(),
                    MoreExecutors.sameThreadExecutor(),
                    "./target", 2048, 0, repoId);
        gc.collectGarbage(false);

        Set<String> existingAfterGC = iterate();
        assertTrue(Sets.symmetricDifference(remaining, existingAfterGC).isEmpty());
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
        FileUtils.cleanDirectory(new File(DataStoreUtils.getHomeDir()));
    }

    static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }
}

