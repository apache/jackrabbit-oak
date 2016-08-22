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

import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType.CLEAN_NONE;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType.CLEAN_OLD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

public class CompactionAndCleanupTest {

    private static final Logger log = LoggerFactory
            .getLogger(CompactionAndCleanupTest.class);

    private File directory;

    @Before
    public void setUp() throws IOException {
        directory = File.createTempFile(
                "FileStoreTest", "dir", new File("target"));
        directory.delete();
        directory.mkdir();
    }

    @Test
    public void compactionNoBinaryClone() throws Exception {
        // 2MB data, 5MB blob
        final int blobSize = 5 * 1024 * 1024;
        final int dataNodes = 10000;

        // really long time span, no binary cloning

        FileStore fileStore = new FileStore(directory, 1);
        final SegmentNodeStore nodeStore = new SegmentNodeStore(fileStore);
        CompactionStrategy custom = new CompactionStrategy(false, false,
                CLEAN_OLD, TimeUnit.HOURS.toMillis(1), (byte) 0) {
            @Override
            public boolean compacted(@Nonnull Callable<Boolean> setHead)
                    throws Exception {
                return nodeStore.locked(setHead);
            }
        };
        fileStore.setCompactionStrategy(custom);

        // 1a. Create a bunch of data
        NodeBuilder extra = nodeStore.getRoot().builder();
        NodeBuilder content = extra.child("content");
        for (int i = 0; i < dataNodes; i++) {
            NodeBuilder c = content.child("c" + i);
            for (int j = 0; j < 1000; j++) {
                c.setProperty("p" + i, "v" + i);
            }
        }
        nodeStore.merge(extra, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // ----

        final long dataSize = fileStore.size();
        log.debug("File store dataSize {}", byteCountToDisplaySize(dataSize));

        try {
            // 1. Create a property with 5 MB blob
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.setProperty("a1", createBlob(nodeStore, blobSize));
            builder.setProperty("b", "foo");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            log.debug("File store pre removal {}, expecting {}",
                    byteCountToDisplaySize(fileStore.size()),
                    byteCountToDisplaySize(blobSize + dataSize));
            assertEquals(mb(blobSize + dataSize), mb(fileStore.size()));

            // 2. Now remove the property
            builder = nodeStore.getRoot().builder();
            builder.removeProperty("a1");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // Size remains same, no cleanup happened yet
            log.debug("File store pre compaction {}, expecting {}",
                    byteCountToDisplaySize(fileStore.size()),
                    byteCountToDisplaySize(blobSize + dataSize));
            assertEquals(mb(blobSize + dataSize), mb(fileStore.size()));

            // 3. Compact
            assertTrue(fileStore.maybeCompact(false));

            // Size doesn't shrink: ran compaction with a '1 Hour' cleanup
            // strategy
            assertSize("post compaction", fileStore.size(),
                    blobSize + dataSize, blobSize + 2 * dataSize);

            // 4. Add some more property to flush the current TarWriter
            builder = nodeStore.getRoot().builder();
            builder.setProperty("a2", createBlob(nodeStore, blobSize));
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // Size is double
            assertSize("pre cleanup", fileStore.size(), 2 * blobSize
                    + dataSize, 2 * blobSize + 2 * dataSize);

            // 5. Cleanup, expecting store size:
            // no data content =>
            // fileStore.size() == blobSize
            // some data content =>
            // fileStore.size() in [blobSize + dataSize, blobSize + 2xdataSize]
            assertTrue(fileStore.maybeCompact(false));
            fileStore.cleanup();
            assertSize("post cleanup", fileStore.size(), 0, blobSize + 2 * dataSize);

            // refresh the ts ref, to simulate a long wait time
            custom.setOlderThan(0);
            TimeUnit.MILLISECONDS.sleep(5);

            boolean needsCompaction = true;
            for (int i = 0; i < 3 && needsCompaction; i++) {
                needsCompaction = fileStore.maybeCompact(false);
                fileStore.cleanup();
            }

            // gain is finally 0%
            assertFalse(fileStore.maybeCompact(false));

            // no data loss happened
            byte[] blob = ByteStreams.toByteArray(nodeStore.getRoot()
                    .getProperty("a2").getValue(Type.BINARY).getNewStream());
            assertEquals(blobSize, blob.length);
        } finally {
            fileStore.close();
        }
    }

    private static void assertSize(String info, long size, long lower,
            long upper) {
        log.debug("File Store {} size {}, expected in interval [{},{}]", info,
                byteCountToDisplaySize(size), byteCountToDisplaySize(lower),
                byteCountToDisplaySize(upper));
        assertTrue("File Store " + log + " size expected in interval ["
                + mb(lower) + "," + mb(upper) + "] but was: " + mb(size),
                mb(size) >= mb(lower) && mb(size) <= mb(upper));
    }

    @After
    public void cleanDir() {
        try {
            deleteDirectory(directory);
        } catch (IOException e) {
            log.error("Error cleaning directory", e);
        }
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
    public void testMixedSegments() throws Exception {
        FileStore store = new FileStore(directory, 2, false);
        final SegmentNodeStore nodeStore = new SegmentNodeStore(store);
        store.setCompactionStrategy(new CompactionStrategy(true, false, CLEAN_NONE, 0, (byte) 5) {
            @Override
            public boolean compacted(Callable<Boolean> setHead) throws Exception {
                return nodeStore.locked(setHead);
            }
        });

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
        for (PropertyState propertyState : s.getProperties()) {
            sid = ((SegmentPropertyState) propertyState).getRecordId().getSegmentId();
            id = new UUID(sid.getMostSignificantBits(),
                    sid.getLeastSignificantBits());
            segmentIds.add(id);
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

    @Test
    public void randomAccessFileConcurrentReadAndLength() throws Exception {
        final FileStore fileStore = FileStore.newFileStore(directory)
                .withMaxFileSize(1)
                .withMemoryMapping(false)
                .create();

        final SegmentNodeStore nodeStore = new SegmentNodeStore(fileStore);
        fileStore.setCompactionStrategy(new CompactionStrategy(false, false, CLEAN_NONE, 0, (byte) 0) {
            @Override
            public boolean compacted(@Nonnull Callable<Boolean> setHead)
            throws Exception {
                return nodeStore.locked(setHead);
            }
        });

        ExecutorService executorService = newFixedThreadPool(300);
        final AtomicInteger counter = new AtomicInteger();
        final ReferenceCollector dummyCollector = new ReferenceCollector() {

            @Override
            public void addReference(String reference) {
                // do nothing
            }
        };

        try {
            Callable<Void> concurrentWriteTask = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    NodeBuilder builder = nodeStore.getRoot().builder();
                    builder.setProperty("blob-" + counter.getAndIncrement(), createBlob(nodeStore, 25 * 25));
                    nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                    fileStore.flush();
                    return null;
                }
            };

            Callable<Void> concurrentCleanupTask = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    fileStore.cleanup();
                    return null;
                }
            };

            Callable<Void> concurrentReferenceCollector = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    fileStore.getTracker().collectBlobReferences(dummyCollector);
                    return null;
                }
            };

            List<Future<?>> results = newArrayList();
            results.add(executorService.submit(concurrentCleanupTask));

            for (int i = 0; i < 100; i++) {
                results.add(executorService.submit(concurrentWriteTask));
                results.add(executorService.submit(concurrentReferenceCollector));
            }

            for (Future<?> result : results) {
                assertNull(result.get());
            }

        } finally {
            executorService.shutdown();
            executorService.awaitTermination(10, SECONDS);
            fileStore.close();
        }
    }

}
