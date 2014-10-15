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

package org.apache.jackrabbit.oak.plugins.segment.file;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.Compactor;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class CompactionAndCleanupTest {

    private File directory;

    @Before
    public void setUp() throws IOException {
        directory = File.createTempFile(
                "FileStoreTest", "dir", new File("target"));
        directory.delete();
        directory.mkdir();
    }

    @Test
    @Ignore("OAK-2045")
    public void compactionAndWeakReferenceMagic() throws Exception{
        final int MB = 1024 * 1024;
        final int blobSize = 5 * MB;

        FileStore fileStore = new FileStore(directory, 1, 1, false);
        SegmentNodeStore nodeStore = new SegmentNodeStore(fileStore);

        //1. Create a property with 5 MB blob
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.setProperty("a1", createBlob(nodeStore, blobSize));
        builder.setProperty("b", "foo");

        //Keep a reference to this nodeState to simulate long
        //running session
        NodeState ns1 = nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        System.out.printf("File store pre removal %d%n", mb(fileStore.size()));
        assertEquals(mb(fileStore.size()), mb(blobSize));


        //2. Now remove the property
        builder = nodeStore.getRoot().builder();
        builder.removeProperty("a1");
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Size remains same
        System.out.printf("File store pre compaction %d%n", mb(fileStore.size()));
        assertEquals(mb(fileStore.size()), mb(blobSize));

        //3. Compact
        fileStore.compact();

        //Size still remains same
        System.out.printf("File store post compaction %d%n", mb(fileStore.size()));
        assertEquals(mb(fileStore.size()), mb(blobSize));

        //4. Add some more property to flush the current TarWriter
        builder = nodeStore.getRoot().builder();
        builder.setProperty("a2", createBlob(nodeStore, blobSize));
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Size is double
        System.out.printf("File store pre cleanup %d%n", mb(fileStore.size()));
        assertEquals(mb(fileStore.size()), 2 * mb(blobSize));

        //5. Cleanup
        cleanup(fileStore);

        //Size is still double. Deleted space not reclaimed
        System.out.printf("File store post cleanup %d%n", mb(fileStore.size()));
        assertEquals(mb(fileStore.size()), 2*mb(blobSize));

        //6. Null out any hard reference
        ns1 = null;
        builder = null;
        cleanup(fileStore);

        //Size should not come back to 5 and deleted data
        //space reclaimed
        System.out.printf("File store post cleanup and nullification %d%n", mb(fileStore.size()));
        assertEquals(mb(fileStore.size()), mb(blobSize));
    }

    @After
    public void cleanDir() throws IOException {
        FileUtils.deleteDirectory(directory);
    }

    private static void cleanup(FileStore fileStore) throws IOException {
        fileStore.getTracker().setCompactionMap(new Compactor(null).getCompactionMap());
        fileStore.getTracker().getWriter().dropCache();

        fileStore.cleanup();
    }

    private static Blob createBlob(NodeStore nodeStore, int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

    private static long mb(long size){
        return size / (1024 * 1024);
    }

    @Test
    public void testGainEstimator() throws Exception {
        final int MB = 1024 * 1024;
        final int blobSize = 2 * MB;

        FileStore fileStore = new FileStore(directory, 2, false);
        SegmentNodeStore nodeStore = new SegmentNodeStore(fileStore);

        // 1. Create some blob properties
        NodeBuilder builder = nodeStore.getRoot().builder();

        NodeBuilder c1 = builder.child("c1");
        c1.setProperty("a", createBlob(nodeStore, blobSize));
        c1.setProperty("b", "foo");

        NodeBuilder c2 = builder.child("c2");
        c2.setProperty("a", createBlob(nodeStore, blobSize));
        c2.setProperty("b", "foo");

        NodeBuilder c3 = builder.child("c3");
        c3.setProperty("a", createBlob(nodeStore, blobSize));
        c3.setProperty("b", "foo");
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // 2. Now remove the property
        builder = nodeStore.getRoot().builder();
        builder.child("c1").remove();
        builder.child("c2").remove();
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        fileStore.flush();
        try {
            // should be at 66%
            assertTrue(fileStore.estimateCompactionGain()
                    .estimateCompactionGain() > 60);
        } finally {
            fileStore.close();
        }
    }

    @Ignore("OAK-2192")  // FIXME OAK-2192
    @Test
    public void testMixedSegments() throws Exception {
        FileStore store = new FileStore(directory, 2, false);
        final SegmentNodeStore nodeStore = new SegmentNodeStore(store);

        NodeBuilder root = nodeStore.getRoot().builder();
        createNodes(root.setChildNode("test"), 10, 3);
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        final Set<UUID> beforeSegments = new HashSet<UUID>();
        collectSegments(store.getHead(), beforeSegments);

        final AtomicReference<Boolean> run = new AtomicReference<Boolean>(true);
        final List<Integer> failedCommits = newArrayList();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int k = 0; run.get(); k++) {
                    try {
                        NodeBuilder root = nodeStore.getRoot().builder();
                        root.setChildNode("b" + k);
                        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                        Thread.sleep(5);
                    } catch (CommitFailedException e) {
                        failedCommits.add(k);
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                        break;
                    }
                }
            }
        });
        t.start();

        store.compact();
        run.set(false);
        t.join();

        assertTrue(failedCommits.isEmpty());

        Set<UUID> afterSegments = new HashSet<UUID>();
        collectSegments(store.getHead(), afterSegments);
        try {
            for (UUID u : beforeSegments) {
                assertFalse("Mixed segments found: " + u, afterSegments.contains(u));
            }
        } finally {
            store.close();
        }
    }

    private static void collectSegments(SegmentNodeState s, Set<UUID> segmentIds) {
        SegmentId sid = s.getRecordId().getSegmentId();
        UUID id = new UUID(sid.getMostSignificantBits(),
                sid.getLeastSignificantBits());
        segmentIds.add(id);
        for (ChildNodeEntry cne : s.getChildNodeEntries()) {
            collectSegments((SegmentNodeState) cne.getNodeState(), segmentIds);
        }
    }

    private static void createNodes(NodeBuilder builder, int count, int depth) {
        if (depth > 0) {
            for (int k = 0; k < count; k++) {
                NodeBuilder child = builder.setChildNode("node" + k);
                createProperties(child, count);
                createNodes(child, count, depth - 1);
            }
        }
    }

    private static void createProperties(NodeBuilder builder, int count) {
        for (int k = 0; k < count; k++) {
            builder.setProperty("property-" + UUID.randomUUID().toString(), "value-" + UUID.randomUUID().toString());
        }
    }


}
