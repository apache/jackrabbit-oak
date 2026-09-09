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
package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.LONGS;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.segment.file.CompactedNodeState;
import org.apache.jackrabbit.oak.segment.file.CompactionWriter;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.GCIncrement;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RecordRepackerTest {

    /**
     * Directory of a sample segment store to run the {@link #repackSampleStore}
     * test against.
     */
    private static final String SAMPLE_STORE = System.getProperty("oak.repack.sampleStore");

    private MemoryStore store;

    private SegmentWriter writer;

    @BeforeEach
    void setup() throws IOException {
        store = new MemoryStore();
        writer = defaultSegmentWriterBuilder("test").build(store);
    }

    private RecordId write(NodeState state) throws IOException {
        RecordId id = writer.writeNode(state);
        writer.flush();
        return id;
    }

    private RecordId repack(RecordId rootId) throws IOException {
        GCGeneration target = rootId.getSegmentId().getGcGeneration().nextFull();
        RecordRepacker repacker = new RecordRepacker(store, store.getReader(),
                store.getSegmentIdProvider(), store.getBlobStore(), Segment.MEDIUM_LIMIT, target);
        RecordId newRoot = repacker.repack(rootId);
        assertEquals(target, newRoot.getSegmentId().getGcGeneration(),
                "repacked root must be in the target generation");
        return newRoot;
    }

    /**
     * Repack {@code before}, then assert that the repacked tree is content-equal
     * to the original, has an identical stable id, and lives in a fresh generation.
     */
    private void assertRoundTrips(NodeState before) throws IOException {
        RecordId rootId = write(before);
        SegmentNodeState beforeNode = store.getReader().readNode(rootId);

        RecordId newRootId = repack(rootId);
        SegmentNodeState afterNode = store.getReader().readNode(newRootId);

        assertNotEquals(rootId, newRootId, "repacking must move the root to a new record");
        // Structural (deep) comparison against the in-memory state: the SegmentNodeState
        // fast-path on stable id is bypassed because 'before' is not a SegmentNodeState.
        assertEquals(before, afterNode);
        assertEquals(beforeNode.getStableId(), afterNode.getStableId(),
                "stable id must be preserved across record-level compaction");
    }

    @Test
    void emptyNode() throws IOException {
        assertRoundTrips(EMPTY_NODE);
    }

    @Test
    void simpleProperties() throws IOException {
        NodeState before = EMPTY_NODE.builder()
                .setProperty("string", "abc")
                .setProperty("long", 123L)
                .setProperty("double", Math.PI)
                .setProperty("boolean", true)
                .getNodeState();
        assertRoundTrips(before);
    }

    @Test
    void primaryTypeAndMixins() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("jcr:primaryType", "nt:unstructured", NAME);
        builder.setProperty("jcr:mixinTypes", List.of("mix:versionable", "mix:lockable"), NAMES);
        builder.setProperty("prop", "value");
        assertRoundTrips(builder.getNodeState());
    }

    @Test
    void singleChild() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child("only").setProperty("p", "v");
        assertRoundTrips(builder.getNodeState());
    }

    @Test
    void deeplyNested() throws IOException {
        NodeBuilder root = EMPTY_NODE.builder();
        NodeBuilder builder = root;
        for (int i = 0; i < 500; i++) {
            builder = builder.child("level" + i);
            builder.setProperty("depth", (long) i);
        }
        assertRoundTrips(root.getNodeState());
    }

    @Test
    void manyChildrenMap() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        for (int i = 0; i < 400; i++) {
            builder.child("child" + i).setProperty("index", (long) i);
        }
        assertRoundTrips(builder.getNodeState());
    }

    @Test
    void multiValuedProperties() throws IOException {
        List<Long> longs = new ArrayList<>();
        List<String> strings = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            longs.add((long) i);
            strings.add("value-" + i);
        }
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("longs", longs, LONGS);
        builder.setProperty("strings", strings, STRINGS);
        builder.setProperty("empty", List.<String>of(), STRINGS);
        assertRoundTrips(builder.getNodeState());
    }

    @Test
    void longStringProperty() throws IOException {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < Segment.MEDIUM_LIMIT * 4) {
            sb.append("The quick brown fox jumps over the lazy dog. ");
        }
        NodeState before = EMPTY_NODE.builder()
                .setProperty("long-string", sb.toString())
                .getNodeState();
        assertRoundTrips(before);
    }

    @Test
    void binaryProperties() throws IOException {
        byte[] small = randomBytes(0x40);
        byte[] medium = randomBytes(Segment.MEDIUM_LIMIT + 100);
        byte[] large = randomBytes(Segment.MAX_SEGMENT_SIZE + 4096);

        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("small", blob(small), BINARY);
        builder.setProperty("medium", blob(medium), BINARY);
        builder.setProperty("large", blob(large), BINARY);
        assertRoundTrips(builder.getNodeState());
    }

    @Test
    void sharedSubtreeIsDeduplicated() throws IOException {
        // The same subtree referenced under two names must be repacked once and
        // remain shared (same record id) in the output.
        NodeBuilder shared = EMPTY_NODE.builder();
        for (int i = 0; i < 20; i++) {
            shared.child("c" + i).setProperty("i", (long) i);
        }
        RecordId sharedId = write(shared.getNodeState());
        SegmentNodeState sharedNode = store.getReader().readNode(sharedId);

        NodeBuilder root = EMPTY_NODE.builder();
        root.setChildNode("a", sharedNode);
        root.setChildNode("b", sharedNode);
        RecordId rootId = write(root.getNodeState());

        RecordId newRootId = repack(rootId);
        SegmentNodeState afterRoot = store.getReader().readNode(newRootId);
        SegmentNodeState a = (SegmentNodeState) afterRoot.getChildNode("a");
        SegmentNodeState b = (SegmentNodeState) afterRoot.getChildNode("b");
        assertEquals(a.getRecordId(), b.getRecordId(),
                "shared subtree must stay shared after repacking");
        assertEquals(root.getNodeState(), afterRoot);
    }

    @Test
    void nodeDataModeDeduplicatesContentEqualNodes() throws IOException {
        // Two content-identical subtrees written independently get distinct (default)
        // stable ids. NODE_DATA mode collapses them (their translated record data is
        // equal), while STABLE_ID mode keeps them apart (distinct stable ids).
        RecordId id1 = write(contentEqualSubtree());
        RecordId id2 = write(contentEqualSubtree());
        SegmentNodeState n1 = store.getReader().readNode(id1);
        SegmentNodeState n2 = store.getReader().readNode(id2);
        assertNotEquals(n1.getStableId(), n2.getStableId(),
                "independently written nodes must have distinct stable ids");

        NodeBuilder root = EMPTY_NODE.builder();
        root.setChildNode("a", n1);
        root.setChildNode("b", n2);
        RecordId rootId = write(root.getNodeState());

        SegmentNodeState byData = repackHead(rootId, RecordRepacker.Mode.NODE_DATA);
        assertEquals(root.getNodeState(), byData);
        assertEquals(childRecordId(byData, "a"), childRecordId(byData, "b"),
                "NODE_DATA must collapse content-equal nodes with distinct stable ids");

        SegmentNodeState byStableId = repackHead(rootId, RecordRepacker.Mode.STABLE_ID);
        assertEquals(root.getNodeState(), byStableId);
        assertNotEquals(childRecordId(byStableId, "a"), childRecordId(byStableId, "b"),
                "STABLE_ID must keep nodes with distinct stable ids apart");
    }

    @Test
    void noDedupModeKeepsContentEqualNodesSeparate() throws IOException {
        // A faithful, un-deduplicated repack preserves only existing physical sharing, so two
        // independently written (content-equal) subtrees stay two separate records.
        NodeBuilder root = EMPTY_NODE.builder();
        root.setChildNode("a", store.getReader().readNode(write(contentEqualSubtree())));
        root.setChildNode("b", store.getReader().readNode(write(contentEqualSubtree())));
        RecordId rootId = write(root.getNodeState());

        SegmentNodeState repacked = repackHead(rootId, RecordRepacker.Mode.NO_DEDUP);
        assertEquals(root.getNodeState(), repacked);
        assertNotEquals(childRecordId(repacked, "a"), childRecordId(repacked, "b"),
                "NO_DEDUP must not collapse content-equal nodes");
    }

    @Test
    void deepDedupModeCollapsesAcrossDistinctStableIds() throws IOException {
        // Two independently written, content-equal subtrees with distinct stable ids.
        NodeBuilder root = EMPTY_NODE.builder();
        root.setChildNode("a", store.getReader().readNode(write(contentEqualSubtree())));
        root.setChildNode("b", store.getReader().readNode(write(contentEqualSubtree())));
        RecordId rootId = write(root.getNodeState());

        // STABLE_ID materialises the (distinct) stable ids as explicit blocks and preserves them,
        // producing a tree where a and b carry distinct explicit stable ids.
        RecordId explicitRoot = repackHead(rootId, RecordRepacker.Mode.STABLE_ID).getRecordId();

        // NODE_DATA keys nodes by data and their stable-id treatment, so the distinct explicit
        // ids keep a and b apart.
        SegmentNodeState byNodeData = repackHead(explicitRoot, RecordRepacker.Mode.NODE_DATA);
        assertEquals(store.getReader().readNode(explicitRoot), byNodeData);
        assertNotEquals(childRecordId(byNodeData, "a"), childRecordId(byNodeData, "b"),
                "NODE_DATA must keep nodes with distinct explicit stable ids apart");

        // DEEP_DEDUP ignores stable ids, so the content-equal a and b collapse to one record.
        SegmentNodeState byDeep = repackHead(explicitRoot, RecordRepacker.Mode.DEEP_DEDUP);
        assertEquals(store.getReader().readNode(explicitRoot), byDeep);
        assertEquals(childRecordId(byDeep, "a"), childRecordId(byDeep, "b"),
                "DEEP_DEDUP must collapse content-equal nodes across distinct stable ids");
    }

    private static NodeState contentEqualSubtree() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("p", "v");
        builder.child("x").setProperty("i", 1L);
        return builder.getNodeState();
    }

    private SegmentNodeState repackHead(RecordId rootId, RecordRepacker.Mode mode) throws IOException {
        return repackHead(rootId, mode, 1);
    }

    private SegmentNodeState repackHead(RecordId rootId, RecordRepacker.Mode mode, int concurrency)
            throws IOException {
        return repackHead(rootId, mode, concurrency, false);
    }

    private SegmentNodeState repackHead(RecordId rootId, RecordRepacker.Mode mode, int concurrency,
            boolean singleWriter) throws IOException {
        GCGeneration target = rootId.getSegmentId().getGcGeneration().nextFull();
        RecordRepacker repacker = new RecordRepacker(store, store.getReader(),
                store.getSegmentIdProvider(), store.getBlobStore(), Segment.MEDIUM_LIMIT, target, mode,
                0, concurrency).withSingleWriter(singleWriter);
        RecordId newRoot = repacker.repack(rootId);
        assertEquals(target, newRoot.getSegmentId().getGcGeneration(),
                "repacked root must be in the target generation");
        return store.getReader().readNode(newRoot);
    }

    @Test
    void parallelRepackPreservesContent() throws IOException {
        // A store with enough branches to split into many parallel subtrees, whose leaves share
        // content across branches so cross-thread deduplication is exercised too.
        NodeBuilder root = EMPTY_NODE.builder();
        for (int i = 0; i < 200; i++) {
            NodeBuilder branch = root.child("branch" + i);
            branch.setProperty("i", (long) i);
            for (int j = 0; j < 20; j++) {
                branch.child("leaf" + j).setProperty("v", "value-" + (j % 5));
            }
        }
        RecordId rootId = write(root.getNodeState());
        NodeState expected = root.getNodeState();

        SegmentNodeState serial = repackHead(rootId, RecordRepacker.Mode.NODE_DATA, 1);
        SegmentNodeState parallel = repackHead(rootId, RecordRepacker.Mode.NODE_DATA, 4);
        SegmentNodeState parallelSingleWriter =
                repackHead(rootId, RecordRepacker.Mode.NODE_DATA, 4, true);

        assertSameContent("/", expected, serial);
        assertSameContent("/", expected, parallel);
        assertSameContent("/", expected, parallelSingleWriter);
    }

    @Test
    void indexFirstRepackPreservesContent() throws IOException {
        // A tree with an "oak:index"-like subtree (its leaves share content with the rest of the
        // tree, so the two-stage repack exercises cross-stage deduplication) plus other branches.
        NodeBuilder root = EMPTY_NODE.builder();
        NodeBuilder index = root.child("oak:index");
        for (int i = 0; i < 50; i++) {
            index.child("idx" + i).setProperty("match", "value-" + (i % 5));
        }
        for (int i = 0; i < 100; i++) {
            NodeBuilder branch = root.child("branch" + i);
            for (int j = 0; j < 10; j++) {
                branch.child("leaf" + j).setProperty("match", "value-" + (j % 5));
            }
        }
        RecordId rootId = write(root.getNodeState());
        NodeState expected = root.getNodeState();
        RecordId indexId = ((SegmentNodeState) store.getReader().readNode(rootId)
                .getChildNode("oak:index")).getRecordId();
        GCGeneration target = rootId.getSegmentId().getGcGeneration().nextFull();

        // Two-stage index-first (/oak:index repacked first, then the rest), serial and parallel;
        // both must round-trip content.
        for (int concurrency : new int[] {1, 4}) {
            RecordRepacker repacker = new RecordRepacker(store, store.getReader(),
                    store.getSegmentIdProvider(), store.getBlobStore(), Segment.MEDIUM_LIMIT, target,
                    RecordRepacker.Mode.NODE_DATA, 0, concurrency).withIndexFirst(indexId);
            SegmentNodeState repacked = store.getReader().readNode(repacker.repack(rootId));
            assertSameContent("/", expected, repacked);
        }
    }

    @Test
    void multiStageRepackPreservesContent() throws IOException {
        // Nested stages mirroring the real superroot: / -> root -> oak:index. Repacking with the ordered
        // pre-stages [oak:index, root] (each a barrier) before the whole superroot must round-trip.
        NodeBuilder superRoot = EMPTY_NODE.builder();
        NodeBuilder contentRoot = superRoot.child("root");
        NodeBuilder index = contentRoot.child("oak:index");
        for (int i = 0; i < 50; i++) {
            index.child("idx" + i).setProperty("match", "value-" + (i % 5));
        }
        for (int i = 0; i < 100; i++) {
            NodeBuilder branch = contentRoot.child("branch" + i);
            for (int j = 0; j < 10; j++) {
                branch.child("leaf" + j).setProperty("match", "value-" + (j % 5));
            }
        }
        RecordId superRootId = write(superRoot.getNodeState());
        NodeState expected = superRoot.getNodeState();
        SegmentNodeState superHead = store.getReader().readNode(superRootId);
        RecordId contentRootId = ((SegmentNodeState) superHead.getChildNode("root")).getRecordId();
        RecordId indexId = ((SegmentNodeState) superHead.getChildNode("root")
                .getChildNode("oak:index")).getRecordId();
        GCGeneration target = superRootId.getSegmentId().getGcGeneration().nextFull();

        for (int concurrency : new int[] {1, 4}) {
            RecordRepacker repacker = new RecordRepacker(store, store.getReader(),
                    store.getSegmentIdProvider(), store.getBlobStore(), Segment.MEDIUM_LIMIT, target,
                    RecordRepacker.Mode.NODE_DATA, 0, concurrency)
                    .withStages(List.of(indexId, contentRootId));
            SegmentNodeState repacked = store.getReader().readNode(repacker.repack(superRootId));
            assertSameContent("/", expected, repacked);
        }
    }

    @Test
    void windowDedupPreservesContent() throws IOException {
        // The bounded recency window (withDedupWindow) deduplicates only content still in the window and
        // emits the rest fresh; it must round-trip content at any window size and concurrency - including
        // a window of 1 (almost everything a miss) and a window larger than the store (≈ exact dedup).
        NodeBuilder root = EMPTY_NODE.builder();
        for (int i = 0; i < 200; i++) {
            NodeBuilder branch = root.child("branch" + i);
            branch.setProperty("i", (long) i);
            for (int j = 0; j < 20; j++) {
                // Repeated shared strings/subtrees (dedup targets) plus per-branch unique ones.
                branch.child("leaf" + j).setProperty("v", "shared-" + (j % 5));
                branch.child("leaf" + j).setProperty("u", "unique-" + i + "-" + j);
            }
        }
        RecordId rootId = write(root.getNodeState());
        NodeState expected = root.getNodeState();
        GCGeneration target = rootId.getSegmentId().getGcGeneration().nextFull();

        for (int window : new int[] {1, 16, 1_000_000}) {
            for (int concurrency : new int[] {1, 4}) {
                RecordRepacker repacker = new RecordRepacker(store, store.getReader(),
                        store.getSegmentIdProvider(), store.getBlobStore(), Segment.MEDIUM_LIMIT, target,
                        RecordRepacker.Mode.NODE_DATA, 0, concurrency).withDedupWindow(window);
                SegmentNodeState repacked = store.getReader().readNode(repacker.repack(rootId));
                assertSameContent("/", expected, repacked);
                assertTrue(repacker.getDedupWindowMisses() > 0,
                        "window cache must record emitted (missed) records");
            }
        }
    }

    @Test
    void dedupCacheImplsPreserveContent() throws IOException {
        // Every bounded-dedup implementation - the off-heap memory-mapped window (MMAP_WINDOW) and the
        // in-heap window (HEAP_WINDOW) - must round-trip content at a small size and any concurrency.
        NodeBuilder root = EMPTY_NODE.builder();
        for (int i = 0; i < 200; i++) {
            NodeBuilder branch = root.child("branch" + i);
            branch.setProperty("i", (long) i);
            for (int j = 0; j < 20; j++) {
                branch.child("leaf" + j).setProperty("v", "shared-" + (j % 5));
                branch.child("leaf" + j).setProperty("u", "unique-" + i + "-" + j);
            }
        }
        RecordId rootId = write(root.getNodeState());
        NodeState expected = root.getNodeState();
        GCGeneration target = rootId.getSegmentId().getGcGeneration().nextFull();

        for (RecordRepacker.DedupCacheImpl impl : RecordRepacker.DedupCacheImpl.values()) {
            for (int concurrency : new int[] {1, 4}) {
                RecordRepacker repacker = new RecordRepacker(store, store.getReader(),
                        store.getSegmentIdProvider(), store.getBlobStore(), Segment.MEDIUM_LIMIT, target,
                        RecordRepacker.Mode.NODE_DATA, 0, concurrency)
                        .withDedupWindow(16)        // force the bounded path (expectedRecords is 0 here)
                        .withDedupCacheImpl(impl);
                SegmentNodeState repacked = store.getReader().readNode(repacker.repack(rootId));
                assertSameContent("/", expected, repacked);
                assertTrue(repacker.getDedupWindowMisses() > 0,
                        impl + " cache must record emitted (missed) records");
            }
        }
    }

    private static RecordId childRecordId(SegmentNodeState node, String name) {
        return ((SegmentNodeState) node.getChildNode(name)).getRecordId();
    }

    /**
     * Repack a copy of a real, on-disk sample segment store and assert that the
     * repacked head is content-identical to the original head. Skipped when the
     * sample store is not present (e.g. on CI).
     */
    @Test
    void repackSampleStore(@TempDir Path tempDir) throws Exception {
        File sample = getSampleStore();

        File copy = tempDir.resolve("segmentstore").toFile();
        copySegmentStore(sample, copy);

        try (FileStore fileStore = fileStoreBuilder(copy).build()) {
            SegmentReader reader = fileStore.getReader();
            SegmentNodeState originalHead = fileStore.getHead();
            RecordId originalRoot = originalHead.getRecordId();

            GCGeneration target = originalRoot.getSegmentId().getGcGeneration().nextFull();
            RecordRepacker repacker = new RecordRepacker(fileStore, reader,
                    fileStore.getSegmentIdProvider(), fileStore.getBlobStore(),
                    fileStore.getBinariesInlineThreshold(), target);

            RecordId repackedRoot = repacker.repack(originalRoot);

            assertNotEquals(originalRoot, repackedRoot, "repacking must move the root");
            assertEquals(target, repackedRoot.getSegmentId().getGcGeneration(),
                    "repacked root must be in the target generation");

            SegmentNodeState repackedHead = reader.readNode(repackedRoot);
            assertEquals(originalHead.getStableId(), repackedHead.getStableId(),
                    "stable id of the head must be preserved");
            // Deep content comparison that bypasses SegmentNodeState's stable-id fast path.
            assertSameContent("/", originalHead, repackedHead);
        }
    }

    private static @NotNull File getSampleStore() {
        assumeFalse(SAMPLE_STORE == null, "no sample segment store path configured");
        File sample = new File(SAMPLE_STORE);
        assumeTrue(sample.isDirectory(), "sample segment store not present at " + sample.getAbsolutePath());
        return sample;
    }

    /**
     * Analyse which nodes the record-level content deduplication ({@code NODE_DATA} /
     * {@code DEEP_DEDUP}) collapses. Every node in the sample store's content tree is grouped by
     * its <em>content signature</em>: recursively, its property names/types/values plus its child
     * names and their content signatures. Two nodes share a signature iff they translate to the
     * same repacked record (ignoring stable ids) &mdash; exactly the key those modes dedupe on. The
     * largest clusters are reported with a few example repository paths and the content rendered as
     * JSON, so we can see what kind of content is duplicated. Skipped when the sample store is
     * absent.
     */
    @Test
    void analyzeDedupClusters() throws Exception {
        File sample = getSampleStore();

        try (ReadOnlyFileStore store = fileStoreBuilder(sample).buildReadOnly()) {
            SegmentNodeState superRoot = store.getReader().readNode(store.getRevisions().getHead());
            NodeState root = superRoot.getChildNode("root");
            assertTrue(root.exists(), "content root not found under the superroot");

            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            // Pass 1: count nodes per content signature.
            Map<String, long[]> counts = new HashMap<>();
            walk(root, "/", digest, (sig, node, path) ->
                    counts.computeIfAbsent(sig, k -> new long[1])[0]++);

            long totalNodes = 0;
            long duplicatedNodes = 0;
            long recordsSaved = 0;
            for (long[] c : counts.values()) {
                totalNodes += c[0];
                if (c[0] > 1) {
                    duplicatedNodes += c[0];
                    recordsSaved += c[0] - 1;
                }
            }

            // Top clusters by node records saved (copies - 1).
            List<Map.Entry<String, long[]>> top = new ArrayList<>();
            for (Map.Entry<String, long[]> e : counts.entrySet()) {
                if (e.getValue()[0] > 1) {
                    top.add(e);
                }
            }
            top.sort(Comparator.comparingLong(
                    (Map.Entry<String, long[]> e) -> e.getValue()[0]).reversed());
            int topN = Math.min(25, top.size());
            Set<String> topHashes = new HashSet<>();
            for (int i = 0; i < topN; i++) {
                topHashes.add(top.get(i).getKey());
            }

            // Pass 2: collect a few example paths and a representative node per top cluster.
            Map<String, List<String>> examples = new HashMap<>();
            Map<String, NodeState> repr = new HashMap<>();
            walk(root, "/", digest, (sig, node, path) -> {
                if (topHashes.contains(sig)) {
                    List<String> paths = examples.computeIfAbsent(sig, k -> new ArrayList<>());
                    if (paths.size() < 5) {
                        paths.add(path);
                    }
                    repr.putIfAbsent(sig, node);
                }
            });

            System.out.printf("%n=== Node content-dedup analysis (%s) ===%n", sample.getAbsolutePath());
            System.out.printf("content-tree nodes:           %,d%n", totalNodes);
            System.out.printf("distinct content shapes:      %,d%n", counts.size());
            System.out.printf("nodes in duplicated clusters: %,d (%.1f%%)%n",
                    duplicatedNodes, pct(duplicatedNodes, totalNodes));
            System.out.printf("node records saved by dedup:  %,d (%.1f%%)%n",
                    recordsSaved, pct(recordsSaved, totalNodes));

            System.out.printf("%n--- top %d content clusters (by node records saved) ---%n", topN);
            for (int i = 0; i < topN; i++) {
                Map.Entry<String, long[]> e = top.get(i);
                long copies = e.getValue()[0];
                NodeState example = repr.get(e.getKey());
                System.out.printf("%n[%d] copies=%,d  saved=%,d  subtreeHeight=%d%n",
                        i + 1, copies, copies - 1, subtreeHeight(example, 32));
                System.out.println("    example paths:");
                for (String p : examples.getOrDefault(e.getKey(), List.of())) {
                    System.out.println("      " + p);
                }
                System.out.println("    content:");
                System.out.println(indent(toJson(example, 4, 20), "      "));
            }
        }
    }

    /**
     * Dump the full record tree of a few representative nodes from the sample store so the
     * NodeState &rarr; segment-record mapping is visible: the node record's id slots (stable id,
     * template, child/child-map, property-values list), the shared template record (primary type /
     * mixins / child name / property-name list) and the string/value/list/map records they
     * reference. Skipped when the sample store is absent.
     */
    @Test
    void dumpRecordTree() throws Exception {
        File sample = getSampleStore();

        try (ReadOnlyFileStore store = fileStoreBuilder(sample).buildReadOnly()) {
            SegmentReader reader = store.getReader();
            NodeState root = reader.readNode(store.getRevisions().getHead()).getChildNode("root");

            dumpNode(reader, "empty rep:versionStorage bucket (leaf, no properties/children)",
                    root, "jcr:system", "jcr:versionStorage", "00", "00");
            dumpNode(reader, "rep:system (many children -> child map)",
                    root, "jcr:system");
            dumpNode(reader, "rep:Permissions entry (leaf with a single-valued and a multi-valued property)",
                    root, "jcr:system", "rep:permissionStore", "crx.default",
                    "analytics-administrators", "941565749", "0");
        }
    }

    private static void dumpNode(SegmentReader reader, String label, NodeState root, String... path) {
        NodeState node = root;
        for (String name : path) {
            node = node.getChildNode(name);
        }
        String p = "/" + String.join("/", path);
        if (!node.exists() || !(node instanceof SegmentNodeState)) {
            System.out.printf("%n### %s%n#   path %s not found in this sample store%n", label, p);
            return;
        }
        RecordId id = ((SegmentNodeState) node).getRecordId();
        System.out.printf("%n### %s%n#   path:        %s%n#   root record: %s%n%s",
                label, p, id, new RecordTreePrinter(reader).dump(id));
    }

    /**
     * Count how many records pertain to the {@code /oak:index} subtree, with a per-index breakdown
     * (one row per direct child of {@code /oak:index}). "Record" here means a distinct data-segment
     * record reachable by reference: node + stable-id block, template, map (leaf/branch/diff), list,
     * bucket, string and inlined blob-header records; bulk binary block data is shared and excluded,
     * matching the "records" metric used elsewhere.
     * <p>
     * Three views are reported, because records are shared:
     * <ul>
     *   <li><b>per-child reachable</b> — distinct records reachable from that index child (fresh
     *       dedup per child). Shared records (templates, strings, …) are counted in every child that
     *       reaches them, so these rows overlap and sum to more than the union.</li>
     *   <li><b>/oak:index union</b> — distinct records reachable from {@code /oak:index} as a whole.</li>
     *   <li><b>/oak:index exclusive</b> — records reachable from {@code /oak:index} but from no other
     *       part of {@code /root}; i.e. what would become unreferenced if {@code /oak:index} were
     *       removed. The truest "records that pertain to the index".</li>
     * </ul>
     * The denominator is the record count reachable from {@code /root} (the live content tree).
     * Skipped when the sample store is absent.
     */
    @Test
    void analyzeIndexRecordCounts() throws Exception {
        File sample = getSampleStore();

        try (ReadOnlyFileStore store = fileStoreBuilder(sample).buildReadOnly()) {
            SegmentReader reader = store.getReader();
            SegmentNodeState superRoot = reader.readNode(store.getRevisions().getHead());
            NodeState root = superRoot.getChildNode("root");
            assertTrue(root.exists(), "content root not found under the superroot");
            NodeState index = root.getChildNode("oak:index");
            assertTrue(index.exists(), "/oak:index not found under /root");
            RecordId rootId = ((SegmentNodeState) root).getRecordId();
            RecordId indexId = ((SegmentNodeState) index).getRecordId();

            List<String> names = new ArrayList<>();
            for (ChildNodeEntry e : index.getChildNodeEntries()) {
                names.add(e.getName());
            }
            Collections.sort(names);

            // (1) Per-child reachable (fresh dedup each) — the requested breakdown.
            System.out.printf("%n=== records per direct child of /oak:index (reachable, deduped per child) ===%n");
            System.out.printf("%-48s %14s%n", "index (direct child of /oak:index)", "records");
            System.out.printf("%-48s %14s%n",
                    "------------------------------------------------", "--------------");
            long sumPerChild = 0;
            for (String name : names) {
                RecordId childId = ((SegmentNodeState) index.getChildNode(name)).getRecordId();
                RecordCounter c = new RecordCounter(reader);
                c.count(childId);
                sumPerChild += c.total();
                System.out.printf("%-48s %,14d%n", name, c.total());
            }
            System.out.printf("%-48s %,14d%n",
                    "(sum of rows — overlaps due to shared records)", sumPerChild);

            // (2) /oak:index union reachable, with a record-type breakdown.
            RecordCounter unionCounter = new RecordCounter(reader);
            unionCounter.count(indexId);
            long indexUnion = unionCounter.total();

            // (3) /oak:index exclusive + /root denominator, in one shared walk (non-index first, so
            //     any record shared with the rest of /root is seen before the /oak:index phase).
            RecordCounter all = new RecordCounter(reader);
            for (ChildNodeEntry e : root.getChildNodeEntries()) {
                if (!"oak:index".equals(e.getName())) {
                    all.count(((SegmentNodeState) e.getNodeState()).getRecordId());
                }
            }
            long nonIndexReachable = all.total();
            all.count(indexId);
            long indexExclusive = all.total() - nonIndexReachable;
            all.count(rootId); // fold in /root's own node/template/child-map records
            long rootReachable = all.total();

            System.out.printf("%n=== /oak:index aggregate ===%n");
            System.out.printf("%-48s %,14d%n", "/oak:index union (reachable)", indexUnion);
            System.out.printf("    %s%n", unionCounter.breakdown());
            System.out.printf("%-48s %,14d%n", "/oak:index exclusive (only via /oak:index)", indexExclusive);
            System.out.printf("%-48s %,14d%n", "/root reachable (denominator)", rootReachable);
            System.out.printf("%-48s %13.1f%%%n", "union / root", 100.0 * indexUnion / rootReachable);
            System.out.printf("%-48s %13.1f%%%n", "exclusive / root", 100.0 * indexExclusive / rootReachable);
        }
    }

    /**
     * Estimate how much larger the store would be if template records were not shared. Oak writes
     * one {@link RecordType#TEMPLATE template} record per distinct node "shape" (primary type,
     * mixins, child-node arity and the set of property names/types), and every node of that shape
     * references it. This walks the whole reachable record graph (content root plus checkpoints),
     * measures each distinct template's own record size and counts how many node records reference
     * it, then reports the extra bytes that would be written if each node instead carried a private
     * copy of its template. Only the template record payload is counted: the strings and
     * property-name list a template references have their own deduplication and would stay shared.
     * The figure is a lower bound on the on-disk increase (record alignment and record-table
     * overhead are not counted). Skipped when the sample store is absent.
     */
    @Test
    void analyzeTemplateReuse() throws Exception {
        File sample = getSampleStore();

        try (ReadOnlyFileStore store = fileStoreBuilder(sample).buildReadOnly()) {
            SegmentReader reader = store.getReader();
            RecordId head = store.getRevisions().getHead();

            TemplateReuseAnalyser analyser = new TemplateReuseAnalyser(reader);
            analyser.analyseNode(head);

            Map<RecordId, long[]> templates = analyser.templates;
            long distinctTemplates = templates.size();
            long nodeRecords = analyser.getNodeCount();
            long currentTemplateBytes = 0;
            long noDedupTemplateBytes = 0;
            long refSum = 0;
            for (long[] t : templates.values()) {
                currentTemplateBytes += t[0];
                noDedupTemplateBytes += t[0] * t[1];
                refSum += t[1];
            }
            long extraBytes = noDedupTemplateBytes - currentTemplateBytes;

            // Cross-checks against the RecordUsageAnalyser accounting.
            assertEquals(nodeRecords, refSum, "every node record references exactly one template");
            assertEquals(analyser.getTemplateSize(), currentTemplateBytes,
                    "per-template sizes must sum to the total template bytes");
            assertTrue(extraBytes > 0, "templates are shared, so removing sharing must add bytes");

            long contentRecordBytes = analyser.getMapSize() + analyser.getListSize()
                    + analyser.getValueSize() + analyser.getTemplateSize() + analyser.getNodeSize();
            long storeOnDisk = directorySize(sample);

            System.out.printf("%n=== Template reuse analysis (%s) ===%n", sample.getAbsolutePath());
            System.out.printf("reachable node records:             %,d%n", nodeRecords);
            System.out.printf("distinct templates:                 %,d%n", distinctTemplates);
            System.out.printf("avg node records per template:      %.1f%n",
                    distinctTemplates == 0 ? 0.0 : (double) nodeRecords / distinctTemplates);
            System.out.printf("current template bytes (shared):    %s%n", mb(currentTemplateBytes));
            System.out.printf("template bytes if not deduplicated: %s%n", mb(noDedupTemplateBytes));
            System.out.printf("extra bytes from duplication:       %s%n", mb(extraBytes));
            System.out.printf("  = %.0fx the current template bytes%n",
                    currentTemplateBytes == 0 ? 0.0 : (double) noDedupTemplateBytes / currentTemplateBytes);
            System.out.printf("  = +%.1f%% of content-record bytes (%s of node/template/map/list/value records)%n",
                    pct(extraBytes, contentRecordBytes), mb(contentRecordBytes));
            System.out.printf("  = +%.1f%% of the on-disk store (%s, incl. bulk binaries)%n",
                    pct(extraBytes, storeOnDisk), mb(storeOnDisk));

            // Top templates by the bytes their duplication would add (size x (refs - 1)).
            List<Map.Entry<RecordId, long[]>> top = new ArrayList<>(templates.entrySet());
            top.sort(Comparator.comparingLong(
                    (Map.Entry<RecordId, long[]> e) -> e.getValue()[0] * (e.getValue()[1] - 1)).reversed());
            int topN = Math.min(15, top.size());
            System.out.printf("%n--- top %d templates by duplication cost ---%n", topN);
            for (int i = 0; i < topN; i++) {
                Map.Entry<RecordId, long[]> e = top.get(i);
                long size = e.getValue()[0];
                long refs = e.getValue()[1];
                String shape = reader.readTemplate(e.getKey()).toString();
                if (shape.length() > 160) {
                    shape = shape.substring(0, 160) + "…";
                }
                System.out.printf("%n[%d] template size=%dB  nodes=%,d  extra=%s%n",
                        i + 1, size, refs, mb(size * (refs - 1)));
                System.out.println("    " + shape);
            }
        }
    }

    /**
     * Measure how much content-deduplicating child-node maps would save. Child maps are the one
     * structural record type the repacker does not yet content-deduplicate; because a fresh (not
     * shared) map id also prevents the multi-child node that references it from deduplicating,
     * closing this gap can save both the duplicate map records and the multi-child nodes (and their
     * ancestors) whose deduplication the distinct map ids blocked. The same self-marker store is
     * repacked in {@code DEEP_DEDUP} mode with map deduplication off and on, and the deduplicated
     * output is analysed per record type with {@link RecordUsageAnalyser}. Skipped when the sample
     * store is absent. Run with a generous heap (e.g. {@code -Dtest.opts.memory=-Xmx4g}).
     */
    @Test
    void measureMapDedup(@TempDir Path tempDir) throws Exception {
        File sample = getSampleStore();

        File prepared = tempDir.resolve("prepared").toFile();
        prepareStoreWithGarbage(sample, prepared);

        Usage off = repackAndMeasure(tempDir.resolve("map-off").toFile(), prepared, false);
        Usage on = repackAndMeasure(tempDir.resolve("map-on").toFile(), prepared, true);

        System.out.printf("%n=== Child-map deduplication effect (DEEP_DEDUP, %s) ===%n",
                prepared.getAbsolutePath());
        printUsageRow("nodes", off.nodeCount, off.nodeSize, on.nodeCount, on.nodeSize);
        printUsageRow("templates", off.templateCount, off.templateSize, on.templateCount, on.templateSize);
        printUsageRow("maps", off.mapCount, off.mapSize, on.mapCount, on.mapSize);
        printUsageRow("lists", off.listCount, off.listSize, on.listCount, on.listSize);
        printUsageRow("values", -1, off.valueSize, -1, on.valueSize);

        long offTotal = off.total();
        long onTotal = on.total();
        System.out.printf("%nTOTAL content bytes: off=%s  on=%s  saved=%s (%.1f%%)%n",
                mb(offTotal), mb(onTotal), mb(offTotal - onTotal), pct(offTotal - onTotal, offTotal));
        System.out.printf("structural records (node+template+map+list): off=%,d  on=%,d  saved=%,d (%.1f%%)%n",
                off.records(), on.records(), off.records() - on.records(),
                pct(off.records() - on.records(), off.records()));

        assertTrue(onTotal <= offTotal, "map dedup must not increase the content-record bytes");
        assertTrue(on.mapCount < off.mapCount, "map dedup must reduce the number of map records");
        assertTrue(on.nodeCount < off.nodeCount,
                "map dedup should unlock deduplication of the multi-child nodes referencing the maps");
    }

    /** Per-record-type usage of a repacked (deduplicated) output, from {@link RecordUsageAnalyser}. */
    private static final class Usage {
        final long nodeCount, nodeSize, templateCount, templateSize, mapCount, mapSize,
                listCount, listSize, valueSize;

        Usage(RecordUsageAnalyser a) {
            nodeCount = a.getNodeCount();
            nodeSize = a.getNodeSize();
            templateCount = a.getTemplateCount();
            templateSize = a.getTemplateSize();
            mapCount = a.getMapCount();
            mapSize = a.getMapSize();
            listCount = a.getListCount();
            listSize = a.getListSize();
            valueSize = a.getValueSize();
        }

        long total() {
            return nodeSize + templateSize + mapSize + listSize + valueSize;
        }

        long records() {
            return nodeCount + templateCount + mapCount + listCount;
        }
    }

    /** Repack a fresh copy of {@code prepared} in DEEP_DEDUP mode and analyse the deduplicated output. */
    private Usage repackAndMeasure(File dest, File prepared, boolean mapDedup) throws Exception {
        copySegmentStore(prepared, dest);
        try (FileStore fs = fileStoreBuilder(dest).build()) {
            SegmentNodeState originalHead = fs.getHead();
            RecordId root = originalHead.getRecordId();
            GCGeneration target = root.getSegmentId().getGcGeneration().nextFull();
            RecordRepacker repacker = new RecordRepacker(fs, fs.getReader(),
                    fs.getSegmentIdProvider(), fs.getBlobStore(),
                    fs.getBinariesInlineThreshold(), target, RecordRepacker.Mode.DEEP_DEDUP)
                    .withMapDeduplication(mapDedup);
            RecordId repacked = repacker.repack(root);
            fs.flush();
            // Content must be preserved regardless of map deduplication (bypasses the stable-id
            // fast path, which DEEP_DEDUP does not preserve).
            assertSameContent("/", originalHead, fs.getReader().readNode(repacked));
            RecordUsageAnalyser usage = new RecordUsageAnalyser(fs.getReader());
            usage.analyseNode(repacked);
            return new Usage(usage);
        }
    }

    private static void printUsageRow(String type, long offCount, long offBytes, long onCount, long onBytes) {
        String off = offCount < 0 ? "" : String.format("%,d recs, ", offCount);
        String on = onCount < 0 ? "" : String.format("%,d recs, ", onCount);
        System.out.printf("  %-10s off: %s%-16s  on: %s%-16s  saved %s%n",
                type, off, mb(offBytes), on, mb(onBytes), mb(offBytes - onBytes));
    }

    /** Visitor invoked once per node (post-order) with its content signature and repository path. */
    @FunctionalInterface
    private interface NodeVisitor {
        void visit(String signature, NodeState node, String path);
    }

    /**
     * Post-order walk that computes each node's content signature (hashing property
     * names/types/values and, recursively, child names and their signatures) and invokes
     * {@code visitor}. A node's own name is deliberately excluded &mdash; it is stored in the parent,
     * not the node record &mdash; so content-equal nodes under different names share a signature,
     * matching record-level dedup. Returns the visited node's signature.
     */
    private static String walk(NodeState node, String path, MessageDigest digest, NodeVisitor visitor) {
        StringBuilder sb = new StringBuilder();
        List<PropertyState> props = new ArrayList<>();
        node.getProperties().forEach(props::add);
        props.sort(Comparator.comparing(PropertyState::getName));
        for (PropertyState p : props) {
            sb.append('P').append(p.getName()).append('#').append(p.getType().tag()).append('=');
            appendSignatureValues(sb, p);
            sb.append('\u0002');
        }
        List<ChildNodeEntry> children = new ArrayList<>();
        node.getChildNodeEntries().forEach(children::add);
        children.sort(Comparator.comparing(ChildNodeEntry::getName));
        for (ChildNodeEntry c : children) {
            String childPath = path.endsWith("/") ? path + c.getName() : path + "/" + c.getName();
            String childSig = walk(c.getNodeState(), childPath, digest, visitor);
            sb.append('C').append(c.getName()).append('=').append(childSig).append('\u0002');
        }
        String sig = toHex(digest.digest(sb.toString().getBytes(StandardCharsets.UTF_8)), 16);
        visitor.visit(sig, node, path);
        return sig;
    }

    private static void appendSignatureValues(StringBuilder sb, PropertyState p) {
        Type<?> base = p.getType().isArray() ? p.getType().getBaseType() : p.getType();
        for (int i = 0; i < p.count(); i++) {
            if (base == BINARY) {
                sb.append("bin:").append(p.getValue(BINARY, i).length());
            } else {
                sb.append(p.getValue(Type.STRING, i));
            }
            sb.append('\u0001');
        }
    }

    private static String toHex(byte[] bytes, int len) {
        StringBuilder sb = new StringBuilder(len * 2);
        for (int i = 0; i < len; i++) {
            sb.append(Character.forDigit((bytes[i] >> 4) & 0xf, 16));
            sb.append(Character.forDigit(bytes[i] & 0xf, 16));
        }
        return sb.toString();
    }

    private static double pct(long part, long total) {
        return total == 0 ? 0.0 : 100.0 * part / total;
    }

    private static String mb(long bytes) {
        return String.format("%,d B (%.1f MB)", bytes, bytes / (1024.0 * 1024.0));
    }

    private static int subtreeHeight(NodeState node, int limit) {
        if (limit <= 0) {
            return 0;
        }
        int h = 0;
        for (ChildNodeEntry c : node.getChildNodeEntries()) {
            h = Math.max(h, 1 + subtreeHeight(c.getNodeState(), limit - 1));
        }
        return h;
    }

    /** Render a node's content as indented JSON, bounded to {@code maxDepth} and {@code maxChildren}. */
    private static String toJson(NodeState node, int maxDepth, int maxChildren) {
        StringBuilder sb = new StringBuilder();
        appendJson(node, sb, 0, maxDepth, maxChildren);
        return sb.toString();
    }

    private static void appendJson(NodeState node, StringBuilder sb, int depth, int maxDepth, int maxChildren) {
        String pad = "  ".repeat(depth + 1);
        String closePad = "  ".repeat(depth);
        sb.append('{');
        boolean first = true;
        List<PropertyState> props = new ArrayList<>();
        node.getProperties().forEach(props::add);
        props.sort(Comparator.comparing(PropertyState::getName));
        for (PropertyState p : props) {
            sb.append(first ? "\n" : ",\n").append(pad).append(jsonString(p.getName())).append(": ");
            appendJsonValue(sb, p);
            first = false;
        }
        List<ChildNodeEntry> children = new ArrayList<>();
        node.getChildNodeEntries().forEach(children::add);
        children.sort(Comparator.comparing(ChildNodeEntry::getName));
        int shown = 0;
        for (ChildNodeEntry c : children) {
            if (shown >= maxChildren) {
                sb.append(",\n").append(pad).append(jsonString("…"))
                        .append(": ").append(jsonString("(" + (children.size() - shown) + " more children)"));
                break;
            }
            sb.append(first ? "\n" : ",\n").append(pad).append(jsonString(c.getName())).append(": ");
            if (depth + 1 >= maxDepth) {
                sb.append(jsonString("{…}"));
            } else {
                appendJson(c.getNodeState(), sb, depth + 1, maxDepth, maxChildren);
            }
            first = false;
            shown++;
        }
        if (!first) {
            sb.append('\n').append(closePad);
        }
        sb.append('}');
    }

    private static void appendJsonValue(StringBuilder sb, PropertyState p) {
        if (p.isArray()) {
            sb.append('[');
            for (int i = 0; i < p.count(); i++) {
                sb.append(i == 0 ? "" : ", ").append(jsonScalar(p, i));
            }
            sb.append(']');
        } else {
            sb.append(jsonScalar(p, 0));
        }
    }

    private static String jsonScalar(PropertyState p, int i) {
        Type<?> base = p.getType().isArray() ? p.getType().getBaseType() : p.getType();
        if (base == BINARY) {
            return jsonString("<binary " + p.getValue(BINARY, i).length() + " bytes>");
        }
        String v = p.getValue(Type.STRING, i);
        if (v.length() > 200) {
            v = v.substring(0, 200) + "…(" + v.length() + " chars)";
        }
        return jsonString(v);
    }

    private static String jsonString(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (ch < 0x20) {
                        sb.append(String.format("\\u%04x", (int) ch));
                    } else {
                        sb.append(ch);
                    }
            }
        }
        sb.append('"');
        return sb.toString();
    }

    private static String indent(String text, String prefix) {
        return prefix + text.replace("\n", "\n" + prefix);
    }

    /**
     * Prints the record tree rooted at a node using {@link SegmentParser}'s type-aware, root-anchored
     * walk. Child nodes are not expanded (they are listed by their parent's child-map entries as
     * {@code name -> NODE <id>}), so the output shows one node's own records: the node record's id
     * slots, its (shared) template, and the string/value/list/map records they reference.
     */
    private static final class RecordTreePrinter extends SegmentParser {
        private final SegmentReader reader;
        private final StringBuilder out = new StringBuilder();
        private int depth;
        private int lines;

        RecordTreePrinter(SegmentReader reader) {
            super(reader);
            this.reader = reader;
        }

        String dump(RecordId nodeId) {
            onNode(null, nodeId);
            return out.toString();
        }

        private void line(String s) {
            if (lines++ > 200) {
                return;
            }
            for (int i = 0; i < depth; i++) {
                out.append("  ");
            }
            out.append(s).append('\n');
        }

        @Override
        protected void onNode(RecordId parentId, RecordId nodeId) {
            if (parentId != null) {
                // A child node: already listed by its parent's child-map entry; don't expand it.
                return;
            }
            SegmentNodeState node = reader.readNode(nodeId);
            RecordId stableRec = nodeId.getSegment().readRecordId(nodeId.getRecordNumber(), 0, 0);
            line(String.format("NODE %s  (childNodes=%d, properties=%d)",
                    nodeId, node.getChildNodeCount(Long.MAX_VALUE), node.getPropertyCount()));
            depth++;
            if (stableRec.equals(nodeId)) {
                line("[id 0] stableId = SELF (self-reference default; no separate record)");
            } else {
                line(String.format("[id 0] stableId -> BLOCK %s  value=%s", stableRec, node.getStableId()));
            }
            parseNode(nodeId);
            depth--;
        }

        @Override
        protected void onTemplate(RecordId parentId, RecordId templateId) {
            Segment seg = templateId.getSegment();
            int head = seg.readInt(templateId.getRecordNumber(), 0);
            boolean hasPrimaryType = (head & (1 << 31)) != 0;
            int mixins = (head >> 18) & ((1 << 10) - 1);
            int props = head & ((1 << 18) - 1);
            String children = (head & (1 << 29)) != 0 ? "zero"
                    : (head & (1 << 28)) != 0 ? "many" : "single";
            line(String.format(
                    "[id 1] template -> TEMPLATE %s  {primaryType=%b, mixins=%d, children=%s, properties=%d}",
                    templateId, hasPrimaryType, mixins, children, props));
            depth++;
            parseTemplate(templateId);
            depth--;
        }

        @Override
        protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) {
            line(String.format("property \"%s\" (%s) -> %s",
                    template.getName(), template.getType(), propertyId));
            depth++;
            parseProperty(parentId, propertyId, template);
            depth--;
        }

        @Override
        protected void onString(RecordId parentId, RecordId stringId) {
            String v = reader.readString(stringId);
            if (v.length() > 80) {
                v = v.substring(0, 80) + "…(" + v.length() + " chars)";
            }
            line(String.format("STRING %s = \"%s\"", stringId, v));
        }

        @Override
        protected void onBlob(RecordId parentId, RecordId blobId) {
            BlobInfo info = parseBlob(blobId);
            line(String.format("BLOB %s  type=%s size=%d", blobId, info.blobType, info.size));
        }

        @Override
        protected void onList(RecordId parentId, RecordId listId, int count) {
            line(String.format("LIST %s  (count=%d)", listId, count));
        }

        @Override
        protected void onMapLeaf(RecordId parentId, RecordId mapId, MapRecord map) {
            line(String.format("MAP-LEAF %s  (entries=%d)  [name -> child node record]", mapId, map.size()));
            depth++;
            int shown = 0;
            for (MapEntry e : map.getEntries()) {
                if (shown++ >= 12) {
                    line(String.format("… (%d more entries)", map.size() - 12));
                    break;
                }
                line(String.format("\"%s\" -> NODE %s", e.getName(), e.getValue()));
            }
            depth--;
        }

        @Override
        protected void onMapBranch(RecordId parentId, RecordId mapId, MapRecord map) {
            line(String.format("MAP-BRANCH %s", mapId));
            depth++;
            parseMapBranch(mapId, map);
            depth--;
        }
    }

    /**
     * Extends {@link RecordUsageAnalyser} to additionally record, per distinct template record, its
     * own size ({@link SegmentParser.TemplateInfo#size}: head + reference slots + per-property type
     * bytes, excluding the shared strings / property-name list it points at) and the number of
     * distinct node records that reference it. The base class deduplicates records by id while
     * walking, so each node is parsed once and {@link #onTemplate} fires exactly once per node
     * record; the size is measured on first sight and subsequent references only bump the count.
     */
    private static final class TemplateReuseAnalyser extends RecordUsageAnalyser {
        /** template record id -> [size in bytes, number of referencing node records]. */
        private final Map<RecordId, long[]> templates = new HashMap<>();

        TemplateReuseAnalyser(SegmentReader reader) {
            super(reader);
        }

        @Override
        protected void onTemplate(RecordId parentId, RecordId templateId) {
            long[] entry = templates.get(templateId);
            if (entry == null) {
                entry = new long[] { parseTemplate(templateId).size, 0 };
                templates.put(templateId, entry);
            }
            entry[1]++;
            super.onTemplate(parentId, templateId);
        }
    }

    /**
     * Benchmark helper: compact a copy of the sample store with the record-level
     * {@link RecordRepacker} and the two semantic compactors ({@link ClassicCompactor} via
     * {@link CheckpointCompactor}, and {@link ParallelCompactor}) and print a side-by-side
     * comparison of wall-clock time, peak heap and the size of the written (compacted)
     * generation. Skipped when the sample store is absent.
     * <p>
     * This is a rough, single-JVM measurement meant for local benchmarking. Run it with a
     * generous heap (e.g. {@code -Dtest.opts.memory=-Xmx4g}), because the semantic compactors
     * build NodeState graphs and dedup caches that the record-level repacker avoids.
     */
    @Test
    void compareWithCheckpointCompactor(@TempDir Path tempDir) throws Exception {
        File sample = getSampleStore();

        // Build the benchmark input once by copying the sample's content tree through the
        // NodeStore API. Copying node-by-node re-writes every record from scratch, dropping
        // the source stable ids, and intermittently deleting and re-copying already-copied
        // branches creates garbage (an uncompacted store, as it would look before GC). All
        // arms then run on identical raw copies of this single prepared store.
        File prepared = tempDir.resolve("prepared").toFile();
        prepareStoreWithGarbage(sample, prepared);
        verifyStableIdsDropped(prepared);

        // The input (pre-compaction) store's own record count and non-bulk record bytes, for a
        // baseline row and to pre-size the repacker's alias/dedup tables so they do not rehash.
        long[] inputStats = countStoreContent(prepared);
        int expectedRecords = (int) Math.min(Integer.MAX_VALUE, inputStats[0]);

        int runs = Integer.getInteger("oak.repack.runs", 1);

        // The arm list can be driven from the command line via -Doak.repack.arms=<spec>[,<spec>...]
        // so a split-level / thread-count sweep can be bisected across runs without recompiling. Each
        // spec is one of:
        //   dedup:<threads>:<split>[:inexact][:full|:heap]  hot-dedup (NODE_DATA, auto off-heap mmap
        //                                        window) repack; :full = full-dedup (exact off-heap);
        //                                        :heap = the in-heap window instead of the mmap window
        //   none:<threads>:<split>[:inexact]    preserve (NO_DEDUP) repack (always inexact under >1 thread)
        //   checkpoint                          CheckpointCompactor (Classic, full up-compaction)
        //   parallel:<n>                        ParallelCompactor at concurrency n
        // where <threads> 1 means serial, <split> 0 means the coupled default (concurrency*32 roots).
        // With no property set, the default decoupling matrix below runs.
        List<Arm> arms = new ArrayList<>();
        String armsSpec = System.getProperty("oak.repack.arms");
        if (armsSpec != null && !armsSpec.isBlank()) {
            for (String spec : armsSpec.split(",")) {
                if (!spec.isBlank()) {
                    arms.add(buildArm(spec.trim(), expectedRecords));
                }
            }
        } else {
            int[] matrixThreads = { 4, 8 };
            int[] splitTargets = { 128, 512, 2048 };
            for (RecordRepacker.Mode mode : new RecordRepacker.Mode[] {
                    RecordRepacker.Mode.NO_DEDUP, RecordRepacker.Mode.NODE_DATA }) {
                String name = mode == RecordRepacker.Mode.NO_DEDUP ? "preserve" : "hot-dedup";
                String slug = mode == RecordRepacker.Mode.NO_DEDUP ? "none" : "dedup";
                arms.add(new Arm("RecordRepacker(" + name + ")", "repack-" + slug,
                        fs -> runRecordRepacker(fs, mode, expectedRecords)));
                for (int c : new int[] { 8, 16 }) {
                    final int cc = c;
                    arms.add(new Arm("RecordRepacker(" + name + "|c" + cc + ")", "repack-" + slug + "-c" + cc,
                            fs -> runRecordRepacker(fs, mode, expectedRecords, cc)));
                }
                for (int t : matrixThreads) {
                    for (int st : splitTargets) {
                        final int tt = t;
                        final int sst = st;
                        arms.add(new Arm("RecordRepacker(" + name + "|" + tt + "/" + sst + ")",
                                "repack-" + slug + "-" + tt + "-" + sst,
                                fs -> runRecordRepacker(fs, mode, expectedRecords, tt, sst)));
                    }
                }
            }
        }

        System.out.printf("%n=== Sample store compaction comparison (%s), %d runs each ===%n",
                prepared.getAbsolutePath(), runs);

        // Pass-major: one full pass over every arm before starting the next pass, so aborting
        // between passes still leaves a complete, balanced dataset (N completed passes = N runs per
        // arm). Incremental medians (over the passes completed so far) are printed after every pass.
        int nArms = arms.size();
        long[][] times = new long[nArms][runs];
        long[][] heaps = new long[nArms][runs];
        long[][] outputs = new long[nArms][runs];
        long[][] records = new long[nArms][runs];
        for (int i = 0; i < runs; i++) {
            System.out.printf("%n=== pass %d/%d ===%n", i + 1, runs);
            for (int a = 0; a < nArms; a++) {
                Arm arm = arms.get(a);
                File dest = tempDir.resolve(arm.dir + "-" + i).toFile();
                CompactionResult r = measureCompaction(arm.label, dest, prepared, arm.compaction);
                assertTrue(r.outputBytes > 0, arm.label + " wrote no output");
                times[a][i] = r.elapsedMillis;
                heaps[a][i] = r.peakHeapBytes / (1024 * 1024);
                outputs[a][i] = r.outputBytes;
                records[a][i] = r.recordCount;
                deleteRecursively(dest);
            }
            printMedians(arms, times, heaps, outputs, records, i + 1, inputStats);
        }

        // Full per-arm run breakdown once all passes have completed.
        for (int a = 0; a < nArms; a++) {
            printRuns(arms.get(a).label, times[a], heaps[a], outputs[a], records[a]);
        }
    }

    /**
     * Print the medians table over the first {@code completed} passes (incremental, abort-safe). Also
     * appended to {@code -Doak.repack.resultFile} if set, flushed each pass, so an early abort (see
     * the pass-major loop) still leaves the completed passes' medians on disk - surefire buffers
     * System.out into the report file and loses it on kill.
     */
    private static void printMedians(List<Arm> arms, long[][] times, long[][] heaps,
            long[][] outputs, long[][] records, int completed, long[] inputStats) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%n--- medians over %d run(s) ---%n", completed));
        sb.append(String.format("%-40s %12s %16s %16s %14s%n",
                "compactor", "time (ms)", "peak heap (MB)", "output (bytes)", "records"));
        // Baseline: the input store before any compaction (non-bulk record bytes and record count,
        // measured the same way as each arm's output; includes the garbage the compactors remove).
        sb.append(String.format("%-40s %12s %16s %16d %14d%n",
                "(input, pre-compaction)", "-", "-", inputStats[1], inputStats[0]));
        for (int a = 0; a < arms.size(); a++) {
            sb.append(String.format("%-40s %12d %16d %16d %14d%n", arms.get(a).label,
                    median(times[a], completed), median(heaps[a], completed),
                    median(outputs[a], completed), median(records[a], completed)));
        }
        System.out.print(sb);
        appendResultFile(sb.toString());
    }

    /** Append {@code text} to {@code -Doak.repack.resultFile} (if set), flushed immediately so an early
     * abort still leaves completed rows on disk (surefire buffers System.out and loses it on kill). */
    private static void appendResultFile(String text) {
        String resultFile = System.getProperty("oak.repack.resultFile");
        if (resultFile == null || resultFile.isBlank()) {
            return;
        }
        try {
            Files.write(Path.of(resultFile), text.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("could not append to " + resultFile + ": " + e);
        }
    }

    /** Parse one {@code -Doak.repack.arms} spec into an {@link Arm}; see {@link #compareWithCheckpointCompactor}. */
    private Arm buildArm(String spec, int expectedRecords) {
        String[] p = spec.split(":");
        String kind = p[0];
        if (kind.equals("checkpoint")) {
            return new Arm("CheckpointCompactor", "checkpoint", this::runCheckpointCompactor);
        }
        if (kind.equals("parallel")) {
            int n = Integer.parseInt(p[1]);
            return new Arm("ParallelCompactor(" + n + ")", "parallel-" + n, fs -> runParallelCompactor(fs, n));
        }
        RecordRepacker.Mode mode = kind.equals("none")
                ? RecordRepacker.Mode.NO_DEDUP : RecordRepacker.Mode.NODE_DATA;
        int threads = p.length > 1 ? Integer.parseInt(p[1]) : 1;
        int split = p.length > 2 ? Integer.parseInt(p[2]) : 0;
        // Tokens after the split are flags: "inexact" (racy dedup), "single" (one synchronized writer),
        // "full" (the exact, unbounded off-heap dedup cache instead of the default recency window),
        // "heap" (in-heap window instead of the default off-heap mmap window), or "idx" (three-stage
        // index-first repack: /oak:index, then /root, then / - each a parallel barrier; see
        // RecordRepacker#withStages).
        boolean exact = true;
        boolean single = false;
        boolean full = false;
        int stages = 0;
        RecordRepacker.DedupCacheImpl cacheImpl = RecordRepacker.DedupCacheImpl.MMAP_WINDOW;
        for (int i = 3; i < p.length; i++) {
            if (p[i].equals("inexact")) {
                exact = false;
            } else if (p[i].equals("single")) {
                single = true;
            } else if (p[i].equals("full")) {
                full = true;
            } else if (p[i].equals("heap")) {
                cacheImpl = RecordRepacker.DedupCacheImpl.HEAP_WINDOW;
            } else if (p[i].equals("idx")) {
                stages = 2;
            }
        }
        // full-dedup pins the exact cache (window 0); otherwise the default auto window (hot-dedup).
        int window = full ? 0 : -1;
        String implTag = cacheImpl == RecordRepacker.DedupCacheImpl.HEAP_WINDOW ? "-heap" : "";
        String idxTag = stages == 2 ? "+idx" : "";
        String name = (mode == RecordRepacker.Mode.NO_DEDUP ? "preserve"
                : (full ? "full-dedup" : "hot-dedup" + implTag)) + idxTag;
        String flags = (exact ? "" : "|inexact") + (single ? "|1w" : "");
        String cfg = threads <= 1 ? ""
                : "|" + threads + "/" + (split > 0 ? Integer.toString(split) : "c") + flags;
        String label = "RecordRepacker(" + name + cfg + ")";
        String dir = "repack-" + kind + (full ? "-full" : "") + implTag
                + (stages == 2 ? "-idx" : "")
                + "-" + threads + "-" + split
                + (exact ? "" : "-inexact") + (single ? "-1w" : "");
        final boolean ex = exact;
        final boolean sw = single;
        final int win = window;
        final int st = stages;
        final RecordRepacker.DedupCacheImpl impl = cacheImpl;
        return new Arm(label, dir,
                fs -> runRecordRepacker(fs, mode, expectedRecords, threads, split, ex, sw, win, impl, st));
    }

    /** A named compaction to benchmark, run on a raw copy of the prepared store in {@code dir}. */
    private static final class Arm {
        final String label;
        final String dir;
        final Compaction compaction;

        Arm(String label, String dir, Compaction compaction) {
            this.label = label;
            this.dir = dir;
            this.compaction = compaction;
        }
    }

    private static void printRuns(String label, long[] times, long[] heaps, long[] outputs, long[] records) {
        System.out.printf("%n%s%n", label);
        System.out.printf("  %-5s %12s %16s %16s %14s%n",
                "run", "time (ms)", "peak heap (MB)", "output (bytes)", "records");
        for (int i = 0; i < times.length; i++) {
            System.out.printf("  %-5d %12d %16d %16d %14d%n", i + 1, times[i], heaps[i], outputs[i], records[i]);
        }
        System.out.printf("  %-5s %12d %16d %16d %14d%n", "min", min(times), min(heaps), min(outputs), min(records));
        System.out.printf("  %-5s %12d %16d %16d %14d%n", "med", median(times), median(heaps), median(outputs), median(records));
        System.out.printf("  %-5s %12d %16d %16d %14d%n", "mean", mean(times), mean(heaps), mean(outputs), mean(records));
        System.out.printf("  %-5s %12d %16d %16d %14d%n", "max", max(times), max(heaps), max(outputs), max(records));
    }

    private static long min(long[] a) {
        long m = a[0];
        for (long x : a) {
            m = Math.min(m, x);
        }
        return m;
    }

    private static long max(long[] a) {
        long m = a[0];
        for (long x : a) {
            m = Math.max(m, x);
        }
        return m;
    }

    private static long mean(long[] a) {
        long sum = 0;
        for (long x : a) {
            sum += x;
        }
        return sum / a.length;
    }

    private static long median(long[] a) {
        long[] sorted = a.clone();
        Arrays.sort(sorted);
        int n = sorted.length;
        return n % 2 == 1 ? sorted[n / 2] : (sorted[n / 2 - 1] + sorted[n / 2]) / 2;
    }

    /** Median over the first {@code n} elements of {@code a}. */
    private static long median(long[] a, int n) {
        long[] sorted = Arrays.copyOf(a, n);
        Arrays.sort(sorted);
        return n % 2 == 1 ? sorted[n / 2] : (sorted[n / 2 - 1] + sorted[n / 2]) / 2;
    }

    private static void deleteRecursively(File file) {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }

    /**
     * Build a fresh segment store at {@code prepared} by copying the content tree of
     * {@code sample} through the {@link org.apache.jackrabbit.oak.spi.state.NodeStore} API.
     * Copying node-by-node re-writes every record from scratch, so the copies carry
     * self-reference-default stable ids instead of the source's (verified by
     * {@link #verifyStableIdsDropped}). Garbage is introduced by intermittently deleting an
     * already-copied top-level branch and copying it again, so its first copy becomes
     * unreferenced, and a couple of intermediate states are retained as checkpoints so the
     * repacked graph has real checkpoint history.
     */
    private void prepareStoreWithGarbage(File sample, File prepared) throws Exception {
        Files.createDirectories(prepared.toPath());
        int garbageEvents = 0;
        int checkpoints = 0;
        List<String> names = new ArrayList<>();
        // Only stores with external binaries (blobs in a - here absent - DataStore) need a
        // BlobStore, and only to relink references by id; stores with inline binaries must be
        // copied without one, so the binaries stay inlined instead of being externalised.
        BlobStore blobStore = hasExternalBlobs(sample) ? new IdentityBlobStore() : null;
        FileStoreBuilder sampleBuilder = fileStoreBuilder(sample);
        FileStoreBuilder destBuilder = fileStoreBuilder(prepared);
        if (blobStore != null) {
            sampleBuilder.withBlobStore(blobStore);
            destBuilder.withBlobStore(blobStore);
        }
        try (FileStore sampleStore = sampleBuilder.build();
                FileStore destStore = destBuilder.build()) {
            SegmentNodeStore sampleNs = SegmentNodeStoreBuilders.builder(sampleStore).build();
            SegmentNodeStore destNs = SegmentNodeStoreBuilders.builder(destStore).build();
            NodeState src = sampleNs.getRoot();
            for (ChildNodeEntry entry : src.getChildNodeEntries()) {
                names.add(entry.getName());
            }

            NodeBuilder rootProps = destNs.getRoot().builder();
            for (PropertyState p : src.getProperties()) {
                rootProps.setProperty(p);
            }
            destNs.merge(rootProps, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            String previous = null;
            for (int i = 0; i < names.size(); i++) {
                String name = names.get(i);
                NodeBuilder root = destNs.getRoot().builder();
                copyTree(src.getChildNode(name), root.child(name));
                destNs.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

                // Intermittently turn an already-copied branch into garbage.
                if (previous != null && i % 3 == 2) {
                    recreateBranch(destNs, src, previous);
                    garbageEvents++;
                }
                // Retain a couple of intermediate states as checkpoints.
                if (i == names.size() / 3 || i == 2 * names.size() / 3) {
                    destNs.checkpoint(TimeUnit.DAYS.toMillis(1));
                    checkpoints++;
                }
                previous = name;
            }
            // Guarantee at least one garbage event even for stores with few top-level branches.
            if (garbageEvents == 0 && previous != null) {
                recreateBranch(destNs, src, previous);
                garbageEvents++;
            }
            destStore.flush();
        }
        System.out.printf(
                "prepared store from %d top-level branches, %d garbage delete/recopy events, %d checkpoints%n",
                names.size(), garbageEvents, checkpoints);
        assertTrue(garbageEvents > 0, "expected to create garbage while preparing the store");
        assertTrue(checkpoints > 0, "expected to create checkpoints while preparing the store");
    }

    /** Delete branch {@code name} from {@code destNs}, then copy it again from {@code src}. */
    private static void recreateBranch(SegmentNodeStore destNs, NodeState src, String name)
            throws CommitFailedException {
        NodeBuilder delete = destNs.getRoot().builder();
        delete.getChildNode(name).remove();
        destNs.merge(delete, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeBuilder recopy = destNs.getRoot().builder();
        copyTree(src.getChildNode(name), recopy.child(name));
        destNs.merge(recopy, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static void copyTree(NodeState src, NodeBuilder dest) {
        for (PropertyState p : src.getProperties()) {
            dest.setProperty(p);
        }
        for (ChildNodeEntry e : src.getChildNodeEntries()) {
            copyTree(e.getNodeState(), dest.child(e.getName()));
        }
    }

    /**
     * Verify that copying through the NodeStore API dropped the source stable ids: the
     * copied nodes should carry the self-reference-default stable id (the record id at index
     * 0 equals the node's own record id). Walks a bounded sample of the prepared store.
     */
    private void verifyStableIdsDropped(File prepared) throws Exception {
        try (FileStore fileStore = fileStoreBuilder(prepared).build()) {
            long self = 0;
            long explicit = 0;
            long cap = 50_000;
            Deque<SegmentNodeState> queue = new ArrayDeque<>();
            queue.add(fileStore.getHead());
            while (!queue.isEmpty() && self + explicit < cap) {
                SegmentNodeState node = queue.removeFirst();
                if (hasSelfMarkerStableId(node)) {
                    self++;
                } else {
                    explicit++;
                }
                for (ChildNodeEntry e : node.getChildNodeEntries()) {
                    NodeState child = e.getNodeState();
                    if (child instanceof SegmentNodeState) {
                        queue.add((SegmentNodeState) child);
                    }
                }
            }
            System.out.printf("prepared store stable ids (sampled %d nodes): self-marker=%d, explicit=%d%n",
                    self + explicit, self, explicit);
            assertTrue(self > explicit,
                    "NodeStore-level copy should drop stable ids (self-marker nodes should dominate)");
        }
    }

    private static boolean hasSelfMarkerStableId(SegmentNodeState node) {
        // Mirrors SegmentNodeState.getStableIdBytes: when the record id at index 0 equals the
        // node's own record id, the node carries no explicit (source) stable id.
        return node.getSegment().readRecordId(node.getRecordNumber()).equals(node.getRecordId());
    }

    /**
     * Whether {@code sample} stores any binary as an external blob reference (data in a
     * DataStore) rather than inline. Walks the content tree, stopping at the first external
     * binary, so it is cheap on external-blob stores and a full (read-only) scan on inline ones.
     */
    private static boolean hasExternalBlobs(File sample) throws Exception {
        try (FileStore store = fileStoreBuilder(sample).build()) {
            Deque<NodeState> queue = new ArrayDeque<>();
            queue.add(SegmentNodeStoreBuilders.builder(store).build().getRoot());
            while (!queue.isEmpty()) {
                NodeState node = queue.removeFirst();
                for (PropertyState p : node.getProperties()) {
                    if (p.getType() == BINARY) {
                        if (isExternalBlob(p.getValue(BINARY))) {
                            return true;
                        }
                    } else if (p.getType() == BINARIES) {
                        for (Blob b : p.getValue(BINARIES)) {
                            if (isExternalBlob(b)) {
                                return true;
                            }
                        }
                    }
                }
                for (ChildNodeEntry e : node.getChildNodeEntries()) {
                    queue.add(e.getNodeState());
                }
            }
        }
        return false;
    }

    private static boolean isExternalBlob(Blob blob) {
        return blob instanceof SegmentBlob && ((SegmentBlob) blob).isExternal();
    }

    /**
     * Identity {@link BlobStore} used only to copy external blob <em>references</em> across
     * stores in {@link #prepareStoreWithGarbage}: a blob reference equals its blob id, so an
     * external binary is relinked by id without its data (which lives in an absent DataStore).
     * Compaction never reads blob data, so this faithfully preserves external references.
     * <p>
     * The blob <em>length</em> is derivable from the blob id (which ends in {@code #<length>}, a
     * DataStore convention), so {@link #getBlobLength} answers without the data - unlike the
     * genuinely data-backed methods, which are unreachable on this path and therefore unsupported.
     */
    private static final class IdentityBlobStore implements BlobStore {

        private final Map<String, Long> referenceToLength = new ConcurrentHashMap<>();

        @Override
        public String getReference(String blobId) {
            DataStoreBlobStore.BlobId id = DataStoreBlobStore.BlobId.of(blobId);
            String reference = id.getBlobId();
            referenceToLength.putIfAbsent(reference, id.getLength());
            return reference;
        }

        @Override
        public String getBlobId(String reference) {
            Long length = referenceToLength.get(reference);
            if (length != null) {
                return reference + "#" + length;
            }
            return reference;
        }

        @Override
        public long getBlobLength(String blobId) {
            int separator = blobId.lastIndexOf('#');
            if (separator >= 0) {
                return Long.parseLong(blobId.substring(separator + 1));
            }
            throw new UnsupportedOperationException("blob id carries no length: " + blobId);
        }

        @Override
        public String writeBlob(InputStream in) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String writeBlob(InputStream in, BlobOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readBlob(String blobId, long pos, byte[] buff, int off, int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream getInputStream(String blobId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private void runRecordRepacker(FileStore fileStore, RecordRepacker.Mode mode, int expectedRecords)
            throws Exception {
        runRecordRepacker(fileStore, mode, expectedRecords, 1);
    }

    private void runRecordRepacker(FileStore fileStore, RecordRepacker.Mode mode, int expectedRecords,
            int concurrency) throws Exception {
        runRecordRepacker(fileStore, mode, expectedRecords, concurrency, 0);
    }

    private void runRecordRepacker(FileStore fileStore, RecordRepacker.Mode mode, int expectedRecords,
            int concurrency, int splitTarget) throws Exception {
        runRecordRepacker(fileStore, mode, expectedRecords, concurrency, splitTarget, true);
    }

    private void runRecordRepacker(FileStore fileStore, RecordRepacker.Mode mode, int expectedRecords,
            int concurrency, int splitTarget, boolean exactDedup) throws Exception {
        runRecordRepacker(fileStore, mode, expectedRecords, concurrency, splitTarget, exactDedup, false);
    }

    private void runRecordRepacker(FileStore fileStore, RecordRepacker.Mode mode, int expectedRecords,
            int concurrency, int splitTarget, boolean exactDedup, boolean singleWriter)
            throws Exception {
        // -1 = the default auto-sized recency window (hot-dedup).
        runRecordRepacker(fileStore, mode, expectedRecords, concurrency, splitTarget, exactDedup,
                singleWriter, -1);
    }

    private void runRecordRepacker(FileStore fileStore, RecordRepacker.Mode mode, int expectedRecords,
            int concurrency, int splitTarget, boolean exactDedup, boolean singleWriter, int dedupWindow)
            throws Exception {
        runRecordRepacker(fileStore, mode, expectedRecords, concurrency, splitTarget, exactDedup,
                singleWriter, dedupWindow, RecordRepacker.DedupCacheImpl.MMAP_WINDOW, 0);
    }

    private void runRecordRepacker(FileStore fileStore, RecordRepacker.Mode mode, int expectedRecords,
            int concurrency, int splitTarget, boolean exactDedup, boolean singleWriter, int dedupWindow,
            RecordRepacker.DedupCacheImpl cacheImpl, int stages) throws Exception {
        SegmentNodeState head = fileStore.getHead();
        RecordId root = head.getRecordId();
        GCGeneration target = root.getSegmentId().getGcGeneration().nextFull();
        RecordRepacker repacker = new RecordRepacker(fileStore, fileStore.getReader(),
                fileStore.getSegmentIdProvider(), fileStore.getBlobStore(),
                fileStore.getBinariesInlineThreshold(), target, mode, expectedRecords, concurrency)
                .withSplitTarget(splitTarget)
                .withExactDedup(exactDedup)
                .withSingleWriter(singleWriter)
                .withDedupWindow(dedupWindow)
                .withDedupCacheImpl(cacheImpl)
                .withStages(stageRoots(head, stages));
        RecordId repacked = repacker.repack(root);
        assertEquals(target, repacked.getSegmentId().getGcGeneration(),
                "repacked root must be in the target generation");
    }

    /**
     * Record id of {@code /oak:index} under the superroot {@code head}, or {@code null} if absent.
     * Drives {@link RecordRepacker#withIndexFirst} for the two-stage index-first repack.
     */
    private static RecordId indexRoot(SegmentNodeState head) {
        NodeState index = head.getChildNode("root").getChildNode("oak:index");
        return index instanceof SegmentNodeState ? ((SegmentNodeState) index).getRecordId() : null;
    }

    /**
     * Ordered pre-stage roots for {@link RecordRepacker#withStages}, given the superroot {@code head}:
     * {@code stages<=0} none (single-stage repack); {@code 1} {@code [/root/oak:index]} (index-first);
     * {@code 2} {@code [/root/oak:index, /root]} (the whole superroot {@code /} is always the final
     * stage, so this yields three parallel barriers: {@code /root/oak:index}, {@code /root}, {@code /}).
     */
    private static List<RecordId> stageRoots(SegmentNodeState head, int stages) {
        if (stages <= 0) {
            return List.of();
        }
        RecordId indexId = indexRoot(head);
        if (indexId == null) {
            return List.of();
        }
        if (stages == 1) {
            return List.of(indexId);
        }
        return List.of(indexId, ((SegmentNodeState) head.getChildNode("root")).getRecordId());
    }

    private void runCheckpointCompactor(FileStore fileStore) throws Exception {
        SegmentNodeState head = fileStore.getHead();
        GCGeneration base = head.getGcGeneration();
        GCGeneration target = base.nextFull();
        GCIncrement increment = new GCIncrement(base, base.nextPartial(), target);
        SegmentWriterFactory writerFactory = generation -> defaultSegmentWriterBuilder("c")
                .withGeneration(generation).build(fileStore);
        CompactionWriter compactionWriter = new CompactionWriter(fileStore.getReader(),
                fileStore.getBlobStore(), increment, writerFactory);
        GCNodeWriteMonitor monitor = new GCNodeWriteMonitor(-1, GCMonitor.EMPTY);
        CheckpointCompactor compactor = new CheckpointCompactor(GCMonitor.EMPTY,
                new ClassicCompactor(compactionWriter, monitor));
        CompactedNodeState compacted = compactor.compactUp(head, Canceller.newCanceller());
        assertNotNull(compacted, "checkpoint compaction must not be cancelled");
        assertEquals(target, compacted.getGcGeneration(),
                "compacted head must be in the target generation");
        compactionWriter.flush();
    }

    private void runParallelCompactor(FileStore fileStore, int concurrency) throws Exception {
        SegmentNodeState head = fileStore.getHead();
        GCGeneration base = head.getGcGeneration();
        GCGeneration target = base.nextFull();
        GCIncrement increment = new GCIncrement(base, base.nextPartial(), target);
        SegmentWriterFactory writerFactory = generation -> defaultSegmentWriterBuilder("c")
                .withGeneration(generation)
                .withWriterPool(SegmentBufferWriterPool.PoolType.THREAD_SPECIFIC)
                .build(fileStore);
        CompactionWriter compactionWriter = new CompactionWriter(fileStore.getReader(),
                fileStore.getBlobStore(), increment, writerFactory);
        GCNodeWriteMonitor monitor = new GCNodeWriteMonitor(-1, GCMonitor.EMPTY);
        CheckpointCompactor compactor = new CheckpointCompactor(GCMonitor.EMPTY,
                new ParallelCompactor(GCMonitor.EMPTY, compactionWriter, monitor, concurrency));
        CompactedNodeState compacted = compactor.compactUp(head, Canceller.newCanceller());
        assertNotNull(compacted, "parallel compaction must not be cancelled");
        assertEquals(target, compacted.getGcGeneration(),
                "compacted head must be in the target generation");
        compactionWriter.flush();
    }

    @FunctionalInterface
    private interface Compaction {
        void run(FileStore fileStore) throws Exception;
    }

    /**
     * Run {@code compaction} on a fresh copy of {@code sample}, measuring the wall-clock
     * time, the peak heap used (sampled on a background thread, relative to a settled
     * baseline) and the size of the compacted generation (growth of the store directory
     * after flushing). Time includes the final flush so both compactors are measured
     * end-to-end.
     */
    private CompactionResult measureCompaction(String label, File dest, File sample,
            Compaction compaction) throws Exception {
        copySegmentStore(sample, dest);
        long elapsedMillis;
        long peakHeapBytes;
        long outputBytes;
        GCGeneration target;
        try (FileStore fileStore = fileStoreBuilder(dest).build()) {
            fileStore.flush();
            long sizeBefore = directorySize(dest);
            // Each compaction writes the next full generation on top of the head; count the
            // records it produced once it has been flushed.
            target = fileStore.getHead().getGcGeneration().nextFull();

            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
            settle();
            long baseHeap = memoryBean.getHeapMemoryUsage().getUsed();
            AtomicLong peak = new AtomicLong(baseHeap);
            AtomicBoolean sampling = new AtomicBoolean(true);
            Thread sampler = new Thread(() -> {
                while (sampling.get()) {
                    peak.accumulateAndGet(memoryBean.getHeapMemoryUsage().getUsed(), Math::max);
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            });
            sampler.setDaemon(true);
            sampler.start();

            long start = System.nanoTime();
            compaction.run(fileStore);
            fileStore.flush();
            elapsedMillis = (System.nanoTime() - start) / 1_000_000;

            sampling.set(false);
            sampler.join();

            peakHeapBytes = Math.max(0, peak.get() - baseHeap);
            outputBytes = directorySize(dest) - sizeBefore;
        }
        long records = countRecordsInGeneration(dest, target);
        return new CompactionResult(label, elapsedMillis, peakHeapBytes, outputBytes, records);
    }

    /**
     * Count the records the compaction wrote into the {@code target} generation of the store
     * at {@code dir}. Reopens the store read-only and sums the record counts of all data
     * segments in that generation; bulk binary segments are shared and carry no generation,
     * so they are excluded (matching the {@code output (bytes)} column, which measures the
     * growth of the store directory).
     */
    private static long countRecordsInGeneration(File dir, GCGeneration target) throws Exception {
        long[] records = {0};
        try (ReadOnlyFileStore store = fileStoreBuilder(dir).buildReadOnly()) {
            for (SegmentId id : store.getSegmentIds()) {
                if (id.isDataSegmentId() && target.equals(id.getGcGeneration())) {
                    id.getSegment().forEachRecord((number, type, offset) -> records[0]++);
                }
            }
        }
        return records[0];
    }

    /**
     * Total record count and non-bulk (data-segment) byte size of the whole store at {@code dir},
     * summed over every data segment regardless of generation. Bulk binary segments are excluded, so
     * the byte figure is comparable to each arm's {@code output (bytes)}. Unlike
     * {@link #countRecordsInGeneration}, this includes unreferenced (garbage) records still present
     * on disk, giving the true pre-compaction size.
     *
     * @return {@code [recordCount, dataSegmentBytes]}.
     */
    private static long[] countStoreContent(File dir) throws Exception {
        long[] stats = {0, 0};
        try (ReadOnlyFileStore store = fileStoreBuilder(dir).buildReadOnly()) {
            for (SegmentId id : store.getSegmentIds()) {
                if (id.isDataSegmentId()) {
                    Segment segment = id.getSegment();
                    segment.forEachRecord((number, type, offset) -> stats[0]++);
                    stats[1] += segment.size();
                }
            }
        }
        return stats;
    }

    private static void settle() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(100);
        }
    }

    private static long directorySize(File dir) {
        long total = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                total += file.isDirectory() ? directorySize(file) : file.length();
            }
        }
        return total;
    }

    private static final class CompactionResult {
        final String label;
        final long elapsedMillis;
        final long peakHeapBytes;
        final long outputBytes;
        final long recordCount;

        CompactionResult(String label, long elapsedMillis, long peakHeapBytes, long outputBytes,
                long recordCount) {
            this.label = label;
            this.elapsedMillis = elapsedMillis;
            this.peakHeapBytes = peakHeapBytes;
            this.outputBytes = outputBytes;
            this.recordCount = recordCount;
        }
    }

    /**
     * Recursively assert that {@code actual} has exactly the same properties and
     * children (by content) as {@code expected}. Deliberately avoids
     * {@link SegmentNodeState#equals}, which short-circuits on the (preserved)
     * stable id and would not detect a corrupted repack.
     */
    private static void assertSameContent(String path, NodeState expected, NodeState actual) {
        assertEquals(expected.getPropertyCount(), actual.getPropertyCount(),
                "property count at " + path);
        for (PropertyState p : expected.getProperties()) {
            PropertyState q = actual.getProperty(p.getName());
            assertNotNull(q, "missing property " + p.getName() + " at " + path);
            assertSameProperty(path, p, q);
        }

        assertEquals(expected.getChildNodeCount(Long.MAX_VALUE),
                actual.getChildNodeCount(Long.MAX_VALUE), "child count at " + path);
        for (ChildNodeEntry entry : expected.getChildNodeEntries()) {
            NodeState child = actual.getChildNode(entry.getName());
            assertTrue(child.exists(), "missing child " + entry.getName() + " at " + path);
            assertSameContent(path + entry.getName() + "/", entry.getNodeState(), child);
        }
    }

    private static void assertSameProperty(String path, PropertyState p, PropertyState q) {
        assertEquals(p.getType(), q.getType(), "type of " + p.getName() + " at " + path);
        assertEquals(p.count(), q.count(), "value count of " + p.getName() + " at " + path);
        Type<?> type = p.getType();
        if (type == BINARY || type == BINARIES) {
            // Binaries are relinked (bulk blocks are shared): comparing lengths is a
            // cheap and sufficient check that avoids streaming large binary content.
            for (int i = 0; i < p.count(); i++) {
                assertEquals(p.getValue(BINARY, i).length(), q.getValue(BINARY, i).length(),
                        "binary length of " + p.getName() + "[" + i + "] at " + path);
            }
        } else {
            assertEquals(p, q, "property " + p.getName() + " at " + path);
        }
    }

    private static void copySegmentStore(File from, File to) throws IOException {
        Files.createDirectories(to.toPath());
        File[] files = from.listFiles();
        assertNotNull(files, "cannot list " + from);
        for (File file : files) {
            if (file.isFile() && !"repo.lock".equals(file.getName())) {
                Files.copy(file.toPath(), to.toPath().resolve(file.getName()));
            }
        }
    }

    private byte[] randomBytes(int size) {
        byte[] data = new byte[size];
        new java.util.Random(size).nextBytes(data);
        return data;
    }

    private Blob blob(byte[] data) throws IOException {
        return new SegmentBlob(store.getBlobStore(), writer.writeStream(new ByteArrayInputStream(data)));
    }

    // ------------------------------------------------------------------------
    // Read locality benchmark
    //
    // The repacker writes the record graph in post-order DFS, so a node's whole subtree is emitted
    // as one contiguous run of records (and hence into a small set of consecutive segments). This
    // benchmark measures how well that on-disk layout serves reads: it lays out the same content
    // three ways (serial repack, parallel repack, semantic CheckpointCompactor), then traverses the
    // content tree of each with a deliberately small segment cache (so the working set does not fit
    // and locality shows up as re-fetches) in both depth-first and breadth-first read order. The
    // metric is the number of segment fetches (archive reads = segment-cache misses), counted via
    // an {@link IOMonitor} on the read path - a structural figure independent of how fast the local
    // TAR store is. A "simulated ms" column applies a configurable per-fetch/per-byte latency to
    // model a network-backed store (Azure/S3), where each fetch is a round-trip.
    // ------------------------------------------------------------------------

    @Test
    void compareReadLocality(@TempDir Path tempDir) throws Exception {
        File sample = getSampleStore();

        File prepared = tempDir.resolve("prepared").toFile();
        prepareStoreWithGarbage(sample, prepared);
        int expectedRecords = (int) Math.min(Integer.MAX_VALUE, countStoreContent(prepared)[0]);

        // Small cache relative to the compacted tree, so a full traversal must re-fetch under
        // eviction and the layout's locality is exposed. Latency is applied arithmetically to the
        // fetch/byte counts (no real sleeping) to keep the run fast and deterministic.
        int[] cacheSizes = parseCacheSizes(System.getProperty("oak.repack.readCacheMB", "32"));
        long fetchLatencyNanos = Long.getLong("oak.repack.readLatencyMicros", 1000) * 1000L;
        long byteLatencyNanos = Long.getLong("oak.repack.readBandwidthNanosPerKiB", 0) / 1024L;
        int threads = Integer.getInteger("oak.repack.readThreads", 16);
        int splitTarget = Integer.getInteger("oak.repack.readSplitTarget", 2048);

        String header = "--- read locality (caches " + Arrays.toString(cacheSizes) + " MB, sim latency "
                + (fetchLatencyNanos / 1000) + " us/fetch) ---" + System.lineSeparator()
                + String.format("%-30s %-14s %8s %12s %12s %12s %12s%n",
                "layout", "workload", "cacheMB", "fetches", "distinct", "MB fetched", "sim ms");
        System.out.print(header);
        appendResultFile(header);

        // Produce and measure one layout at a time, deleting its store before the next, so peak disk
        // stays bounded even for a large sample. Scenarios are grouped so each one's serial and
        // parallel (‖threads) rows are adjacent, serial first.
        //
        // preserve: no content deduplication (only physical source sharing via the alias). Serially
        // this writes every record fresh and contiguous; in parallel each worker keeps its own copy of
        // shared records (no cross-worker dedup). Isolates whether dedup - pointing a node's children
        // at records another writer emitted elsewhere - is what fragments reads.
        measureLayout("repack(preserve)", tempDir.resolve("serial-nodedup").toFile(), prepared,
                dir -> produceRepackLayout(prepared, dir, RecordRepacker.Mode.NO_DEDUP, 1, 0,
                        expectedRecords),
                cacheSizes, fetchLatencyNanos, byteLatencyNanos);
        measureLayout("repack(preserve‖" + threads + ")", tempDir.resolve("parallel-nodedup").toFile(),
                prepared,
                dir -> produceRepackLayout(prepared, dir, RecordRepacker.Mode.NO_DEDUP, threads,
                        splitTarget, expectedRecords),
                cacheSizes, fetchLatencyNanos, byteLatencyNanos);
        // full-dedup (exact off-heap cache) vs hot-dedup (default auto-sized recency window): the
        // window layout must read as well as the exact one to confirm the window is a safe default.
        measureLayout("repack(full-dedup)", tempDir.resolve("serial-exact").toFile(), prepared,
                dir -> produceRepackLayout(prepared, dir, RecordRepacker.Mode.NODE_DATA, 1, 0,
                        expectedRecords, 0),
                cacheSizes, fetchLatencyNanos, byteLatencyNanos);
        measureLayout("repack(full-dedup‖" + threads + ")", tempDir.resolve("parallel-exact").toFile(),
                prepared,
                dir -> produceRepackLayout(prepared, dir, RecordRepacker.Mode.NODE_DATA, threads,
                        splitTarget, expectedRecords, 0),
                cacheSizes, fetchLatencyNanos, byteLatencyNanos);
        measureLayout("repack(hot-dedup)", tempDir.resolve("serial").toFile(), prepared,
                dir -> produceRepackLayout(prepared, dir, RecordRepacker.Mode.NODE_DATA, 1, 0,
                        expectedRecords, -1),
                cacheSizes, fetchLatencyNanos, byteLatencyNanos);
        measureLayout("repack(hot-dedup‖" + threads + ")", tempDir.resolve("parallel").toFile(),
                prepared,
                dir -> produceRepackLayout(prepared, dir, RecordRepacker.Mode.NODE_DATA, threads,
                        splitTarget, expectedRecords, -1),
                cacheSizes, fetchLatencyNanos, byteLatencyNanos);
        // hot-dedup+idx: the three-stage index-first repack - /oak:index, then /root, then / - each a
        // parallel barrier, keeping the wide index child sets in one contiguous run. The parallel row
        // is the point of the mode; the serial row is the staged ordering applied single-threaded.
        measureLayout("repack(hot-dedup+idx)", tempDir.resolve("serial-idx").toFile(), prepared,
                dir -> produceRepackLayout(prepared, dir, RecordRepacker.Mode.NODE_DATA, 1, 0,
                        expectedRecords, -1, 2),
                cacheSizes, fetchLatencyNanos, byteLatencyNanos);
        measureLayout("repack(hot-dedup+idx‖" + threads + ")", tempDir.resolve("parallel-idx").toFile(),
                prepared,
                dir -> produceRepackLayout(prepared, dir, RecordRepacker.Mode.NODE_DATA, threads,
                        splitTarget, expectedRecords, -1, 2),
                cacheSizes, fetchLatencyNanos, byteLatencyNanos);
        measureLayout("semantic(checkpoint)", tempDir.resolve("semantic").toFile(), prepared,
                dir -> produceSemanticLayout(prepared, dir),
                cacheSizes, fetchLatencyNanos, byteLatencyNanos);
        measureLayout("semantic(parallel‖" + threads + ")", tempDir.resolve("semantic-parallel").toFile(),
                prepared,
                dir -> produceParallelSemanticLayout(prepared, dir, threads),
                cacheSizes, fetchLatencyNanos, byteLatencyNanos);
    }

    /**
     * SIZE decomposition of the bounded recency-window content-dedup ({@link RecordRepacker#withDedupWindow})
     * that replaces the exact off-heap-spilled dedup cache. Contrasts the exact record-dedup floor against
     * a sweep of window sizes (default {@code 65536,262144,1048576,4194304}, override with
     * {@code -Doak.repack.dedupWindows=...}), serial and parallel, reporting records + bytes + per-type +
     * the window hit/miss counts. A window that recovers the exact floor's size proves the hot duplicates
     * (index mirrors, version buckets, type names) recur within the window, so the filesystem spill for
     * duplicates can be dropped. Additive and test-only.
     */
    @Test
    void measureDedupWindow(@TempDir Path tempDir) throws Exception {
        File sample = getSampleStore();

        File prepared = tempDir.resolve("prepared").toFile();
        prepareStoreWithGarbage(sample, prepared);
        long[] content = countStoreContent(prepared);
        int expectedRecords = (int) Math.min(Integer.MAX_VALUE, content[0]);
        int threads = Integer.getInteger("oak.repack.readThreads", 16);
        int splitTarget = Integer.getInteger("oak.repack.readSplitTarget", 2048);
        boolean analyzeTypes = Boolean.parseBoolean(System.getProperty("oak.repack.analyzeTypes", "true"));
        int[] windows = parseThresholds(
                System.getProperty("oak.repack.dedupWindows", "65536,262144,1048576,4194304"));

        System.out.println("--- recency-window content-dedup SIZE decomposition ---");
        System.out.printf("%-32s %14s %10s | %12s %10s %10s %10s %12s | %s%n",
                "arm", "records", "bytes", "nodes", "templates", "maps", "lists", "strings",
                "win hits/misses");
        System.out.printf("%-32s %,14d %10s%n", "(input, pre-compaction)", content[0], mb(content[1]));

        windowArm("full-dedup(serial)", prepared, tempDir, 0, 1, 0, expectedRecords, analyzeTypes);
        for (int w : windows) {
            windowArm("window " + w + "(serial)", prepared, tempDir, w, 1, 0, expectedRecords, analyzeTypes);
        }
        windowArm("full-dedup(‖" + threads + ")", prepared, tempDir, 0, threads, splitTarget,
                expectedRecords, analyzeTypes);
        for (int w : windows) {
            windowArm("window " + w + "(‖" + threads + ")", prepared, tempDir, w, threads, splitTarget,
                    expectedRecords, analyzeTypes);
        }
    }

    /**
     * One recency-window repack arm: repack the prepared store in {@link RecordRepacker.Mode#NODE_DATA}
     * with the given content-dedup window ({@code 0} = the exact off-heap cache), print records + bytes +
     * per-type breakdown + window hit/miss counts, then delete it.
     */
    private void windowArm(String label, File prepared, Path tempDir, int window, int concurrency,
            int splitTarget, int expectedRecords, boolean analyzeTypes) throws Exception {
        File dir = tempDir.resolve("win-" + label.replaceAll("[^a-zA-Z0-9]", "_")).toFile();
        copySegmentStore(prepared, dir);
        long sizeBefore = directorySize(dir);
        GCGeneration target;
        long[] ref;
        long hits;
        long misses;
        try (FileStore fs = fileStoreBuilder(dir).build()) {
            SegmentNodeState head = fs.getHead();
            RecordId root = head.getRecordId();
            target = root.getSegmentId().getGcGeneration().nextFull();
            RecordRepacker repacker = new RecordRepacker(fs, fs.getReader(), fs.getSegmentIdProvider(),
                    fs.getBlobStore(), fs.getBinariesInlineThreshold(), target,
                    RecordRepacker.Mode.NODE_DATA, expectedRecords, concurrency).withSplitTarget(splitTarget);
            // 0 pins the exact off-heap cache; > 0 a fixed window. Unconditional so the "exact" arm is
            // exact even though the repacker's default is now an auto-sized window.
            repacker.withDedupWindow(window);
            RecordId repacked = repacker.repack(root);
            fs.flush();
            hits = repacker.getDedupWindowHits();
            misses = repacker.getDedupWindowMisses();
            ref = toRef(repacked);
        }
        long bytes = directorySize(dir) - sizeBefore;
        long records = countRecordsInGeneration(dir, target);

        long nodes = -1, templates = -1, maps = -1, lists = -1, strings = -1;
        if (analyzeTypes) {
            try (ReadOnlyFileStore ro = fileStoreBuilder(dir).buildReadOnly()) {
                SegmentId sid = ro.getSegmentIdProvider().newSegmentId(ref[0], ref[1]);
                RecordUsageAnalyser a = new RecordUsageAnalyser(ro.getReader());
                a.analyseNode(new RecordId(sid, (int) ref[2]));
                nodes = a.getNodeCount();
                templates = a.getTemplateCount();
                maps = a.getMapCount();
                lists = a.getListCount();
                strings = a.getSmallStringCount() + a.getMediumStringCount() + a.getLongStringCount();
            } catch (Exception | OutOfMemoryError e) {
                System.out.println("  (per-type analysis skipped: " + e + ")");
            }
        }
        String hm = window <= 0 ? "-" : String.format("%,d / %,d", hits, misses);
        System.out.printf("%-32s %,14d %10s | %12s %10s %10s %10s %12s | %s%n",
                label, records, mb(bytes),
                col(nodes), col(templates), col(maps), col(lists), col(strings), hm);
        deleteDir(dir);
    }

    private static String col(long v) {
        return v < 0 ? "-" : String.format("%,d", v);
    }

    /**
     * Decompose <em>why</em> the list-widest fetch count differs across layouts (in particular why
     * parallel record-dedup reads so much worse than serial). For each layout it reads exactly the
     * widest node's immediate children from a cold store and reports, in one pass:
     * <ul>
     *   <li><b>fetches</b> - archive reads (cache misses), i.e. the headline number;</li>
     *   <li><b>distinct</b> - distinct segments the workload touches (the true footprint; the
     *       {@link SegmentFetchTracker} set is cache-independent);</li>
     *   <li><b>refetch</b> = fetches - distinct - segments re-read because the footprint exceeds the
     *       {@code cacheMB} cache and eviction unloads them (thrash), <em>not</em> footprint;</li>
     *   <li><b>nodeSegs</b> / <b>tmplSegs</b> - distinct segments holding the children's own NODE
     *       records resp. their TEMPLATE records, read straight from the record slots. This splits
     *       the footprint into "the children themselves are scattered" (worker/frontier split) vs
     *       "the records the children point at are scattered" (dedup: one canonical copy lives in
     *       whichever worker emitted it first).</li>
     * </ul>
     * Additive and test-only; leaves {@link #compareReadLocality} untouched.
     */
    @Test
    void analyzeWideListingLocality(@TempDir Path tempDir) throws Exception {
        File sample = getSampleStore();

        File prepared = tempDir.resolve("prepared").toFile();
        prepareStoreWithGarbage(sample, prepared);
        int expectedRecords = (int) Math.min(Integer.MAX_VALUE, countStoreContent(prepared)[0]);

        int cacheMB = Integer.getInteger("oak.repack.readCacheMB", 32);
        int threads = Integer.getInteger("oak.repack.readThreads", 16);
        int splitTarget = Integer.getInteger("oak.repack.readSplitTarget", 2048);

        System.out.println("--- wide-listing locality decomposition (cache " + cacheMB + " MB) ---");
        System.out.printf("%-24s %10s %10s %10s %10s %10s %10s%n",
                "layout", "children", "fetches", "distinct", "refetch", "nodeSegs", "tmplSegs");

        analyzeOneLayout("record-dedup(serial)", tempDir, prepared,
                RecordRepacker.Mode.NODE_DATA, 1, 0, expectedRecords, cacheMB);
        analyzeOneLayout("record-dedup(‖" + threads + ")", tempDir, prepared,
                RecordRepacker.Mode.NODE_DATA, threads, splitTarget, expectedRecords, cacheMB);
        analyzeOneLayout("no-dedup(‖" + threads + ")", tempDir, prepared,
                RecordRepacker.Mode.NO_DEDUP, threads, splitTarget, expectedRecords, cacheMB);
        analyzeOneLayout("no-dedup(serial)", tempDir, prepared,
                RecordRepacker.Mode.NO_DEDUP, 1, 0, expectedRecords, cacheMB);
    }

    private void analyzeOneLayout(String label, Path tempDir, File prepared,
            RecordRepacker.Mode mode, int concurrency, int splitTarget, int expectedRecords, int cacheMB)
            throws Exception {
        File dir = tempDir.resolve("layout-" + label.replaceAll("[^a-zA-Z0-9]", "_")).toFile();
        long[] ref = produceRepackLayout(prepared, dir, mode, concurrency, splitTarget, expectedRecords);
        try {
            if (widestPath == null) {
                discoverTargets(dir, ref);
            }
            List<String> widest = widestPath;

            // (A) cold read of exactly list-widest, tracking total + distinct segment fetches.
            SegmentFetchTracker tracker = new SegmentFetchTracker();
            try (ReadOnlyFileStore store = fileStoreBuilder(dir)
                    .withSegmentCacheSize(cacheMB).withIOMonitor(tracker).buildReadOnly()) {
                listImmediateChildren(resolvePath(readContentRoot(store, ref), widest));
            }
            long total = tracker.fetches.get();
            long distinct = tracker.distinct.size();

            // (B) warm pass: distinct segments of the children's own NODE and TEMPLATE records
            //     (slot 1 of a node record is its template; slot 0 is the stable id).
            Set<String> nodeSegs = new HashSet<>();
            Set<String> tmplSegs = new HashSet<>();
            try (ReadOnlyFileStore store = fileStoreBuilder(dir).buildReadOnly()) {
                NodeState widestNode = resolvePath(readContentRoot(store, ref), widest);
                for (ChildNodeEntry e : widestNode.getChildNodeEntries()) {
                    RecordId nid = ((SegmentNodeState) e.getNodeState()).getRecordId();
                    nodeSegs.add(segKey(nid.getSegmentId()));
                    RecordId tmpl = nid.getSegment().readRecordId(nid.getRecordNumber(), 0, 1);
                    tmplSegs.add(segKey(tmpl.getSegmentId()));
                }
            }
            System.out.printf("%-24s %10d %10d %10d %10d %10d %10d%n",
                    label, widestChildCount, total, distinct, total - distinct,
                    nodeSegs.size(), tmplSegs.size());

            // (C) per-record-type segment attribution of the same list-widest footprint, so the
            //     scatter can be pinned to a specific layer (node / template / value / string / ...).
            try (ReadOnlyFileStore store = fileStoreBuilder(dir).buildReadOnly()) {
                RecordId widestId = ((SegmentNodeState) resolvePath(readContentRoot(store, ref), widest))
                        .getRecordId();
                ListingSegCollector collector = new ListingSegCollector(store.getReader());
                collector.collect(widestId);
                System.out.printf("    by type (segs): node=%d template=%d map=%d value=%d "
                                + "string=%d list=%d blob=%d | union=%d%n",
                        collector.count("node"), collector.count("template"), collector.count("map"),
                        collector.count("value"), collector.count("string"), collector.count("list"),
                        collector.count("blob"), collector.all.size());
                System.out.printf("    records:        node=%d template=%d map=%d property=%d "
                                + "string=%d list=%d%n",
                        collector.records("node"), collector.records("template"),
                        collector.records("map"), collector.records("property"),
                        collector.records("string"), collector.records("list"));
                System.out.printf("    bytes (MB):     node=%.1f template=%.1f map=%.1f property=%.1f "
                                + "string=%.1f list=%.1f | union=%.1f MB%n",
                        collector.mb("node"), collector.mb("template"), collector.mb("map"),
                        collector.mb("property"), collector.mb("string"), collector.mb("list"),
                        collector.unionMB());
                double segMB = 256.0 / 1024.0; // 256 KB per data segment
                double valueLayerMB = collector.mb("property") + collector.mb("string")
                        + collector.mb("list");
                System.out.printf("    ideal segs (bytes/256KB): value-layer=%.0f (%.1f MB) "
                                + "union=%.0f (%.1f MB) | ACTUAL union segs=%d%n",
                        Math.ceil(valueLayerMB / segMB), valueLayerMB,
                        Math.ceil(collector.unionMB() / segMB), collector.unionMB(),
                        collector.all.size());
            }
        } finally {
            deleteDir(dir);
        }
    }

    /**
     * Same wide-listing segment attribution as {@link #analyzeWideListingLocality}, but for the
     * <b>top-K widest nodes</b> (default 5, {@code -Doak.repack.topWidest}) and comparing only
     * <b>hot-dedup serial vs hot-dedup &#8214;threads</b>. Each layout is produced once and every
     * target node is attributed against it. Rows are labelled by the node's path so the per-node
     * scatter can be read off directly. The record counts / bytes touched are layout-independent
     * (parallelism only relocates records, it does not change what is reachable), so they are printed
     * once per node from the serial pass. Additive and test-only.
     */
    @Test
    void analyzeTopWideListingLocality(@TempDir Path tempDir) throws Exception {
        File sample = getSampleStore();

        File prepared = tempDir.resolve("prepared").toFile();
        prepareStoreWithGarbage(sample, prepared);
        int expectedRecords = (int) Math.min(Integer.MAX_VALUE, countStoreContent(prepared)[0]);

        int cacheMB = Integer.getInteger("oak.repack.readCacheMB", 32);
        int threads = Integer.getInteger("oak.repack.readThreads", 16);
        int splitTarget = Integer.getInteger("oak.repack.readSplitTarget", 2048);
        int topK = Integer.getInteger("oak.repack.topWidest", 5);

        File serialDir = tempDir.resolve("hot-serial").toFile();
        long[] serialRef = produceRepackLayout(prepared, serialDir, RecordRepacker.Mode.NODE_DATA,
                1, 0, expectedRecords);
        File parDir = tempDir.resolve("hot-par").toFile();
        long[] parRef = produceRepackLayout(prepared, parDir, RecordRepacker.Mode.NODE_DATA,
                threads, splitTarget, expectedRecords);
        try {
            List<WideNode> targets = discoverTopWidest(serialDir, serialRef, topK);
            System.out.println("--- top-" + topK + " widest-node listing locality (hot-dedup, cache "
                    + cacheMB + " MB); serial vs ‖" + threads + " ---");
            long[] sStat = generationStats(serialDir, serialRef);
            long[] pStat = generationStats(parDir, parRef);
            System.out.printf("new-gen size  serial=%,d recs / %.1f MB   ‖%d=%,d / %.1f MB%n",
                    sStat[0], sStat[1] / 1048576.0, threads, pStat[0], pStat[1] / 1048576.0);
            for (WideNode t : targets) {
                System.out.println();
                System.out.println("=== " + t.display() + "  (" + t.children + " children) ===");
                System.out.printf("%-24s %8s %9s %6s %5s %5s %6s %7s %6s %7s %7s%n",
                        "layout", "fetches", "distinct", "node", "tmpl", "map", "value", "string",
                        "list", "ideal", "union");
                analyzeNode(t, "hot-dedup(serial)", serialDir, serialRef, cacheMB, true);
                analyzeNode(t, "hot-dedup(‖" + threads + ")", parDir, parRef, cacheMB, false);
            }
        } finally {
            deleteDir(serialDir);
            deleteDir(parDir);
        }
    }

    /**
     * Read/size Pareto frontier as a function of the dedup <b>window size</b> (the existing
     * {@link RecordRepacker#withDedupWindow(int)} knob), on the parallel layout. A smaller window
     * forgets shared records sooner, so a wide/bucketed node's aggregates re-emit their strings
     * locally (denser reads) at the cost of a larger store. Reports, per window, the store-wide new-gen
     * size and the top-K widest nodes' list-widest fetches - so we can see how much plain windowing
     * recovers before deciding whether a selective (look-ahead) aggregate duplication is needed.
     * Windows via {@code -Doak.repack.windowSweep} (comma-separated; -1 auto, 0 exact-unbounded,
     * &gt;0 fixed). Additive and test-only.
     */
    @Test
    void analyzeDedupWindowSweep(@TempDir Path tempDir) throws Exception {
        File sample = getSampleStore();

        File prepared = tempDir.resolve("prepared").toFile();
        prepareStoreWithGarbage(sample, prepared);
        int expectedRecords = (int) Math.min(Integer.MAX_VALUE, countStoreContent(prepared)[0]);

        int cacheMB = Integer.getInteger("oak.repack.readCacheMB", 32);
        int threads = Integer.getInteger("oak.repack.readThreads", 16);
        int splitTarget = Integer.getInteger("oak.repack.readSplitTarget", 2048);
        int topK = Integer.getInteger("oak.repack.topWidest", 3);
        int[] windows = parseCacheSizes(System.getProperty("oak.repack.windowSweep",
                "-1,1000000,200000,50000"));

        // Serial reference (auto window) - the read locality target.
        File serialDir = tempDir.resolve("serial").toFile();
        long[] serialRef = produceRepackLayout(prepared, serialDir, RecordRepacker.Mode.NODE_DATA,
                1, 0, expectedRecords);
        List<WideNode> targets = discoverTopWidest(serialDir, serialRef, topK);
        long[] sStat = generationStats(serialDir, serialRef);
        System.out.println("--- dedup-window sweep (‖" + threads + ", cache " + cacheMB
                + " MB): read/size frontier ---");
        System.out.printf("serial(ref) new-gen %,d recs / %.1f MB%n", sStat[0], sStat[1] / 1048576.0);
        for (WideNode t : targets) {
            System.out.printf("  serial  %-40s fetches=%d%n", t.display(),
                    listWidestFetches(serialDir, serialRef, t, cacheMB));
        }
        deleteDir(serialDir);

        for (int window : windows) {
            File dir = tempDir.resolve("win-" + window).toFile();
            long[] ref = produceRepackLayout(prepared, dir, RecordRepacker.Mode.NODE_DATA,
                    threads, splitTarget, expectedRecords, window);
            try {
                long[] gStat = generationStats(dir, ref);
                System.out.printf("window=%-9s new-gen %,d recs / %.1f MB%n",
                        window == -1 ? "auto" : window == 0 ? "exact" : Integer.toString(window),
                        gStat[0], gStat[1] / 1048576.0);
                for (WideNode t : targets) {
                    System.out.printf("  ‖%d     %-40s fetches=%d%n", threads, t.display(),
                            listWidestFetches(dir, ref, t, cacheMB));
                }
            } finally {
                deleteDir(dir);
            }
        }
    }

    /** Cold list-widest fetch count for one node against one layout. */
    private long listWidestFetches(File dir, long[] ref, WideNode target, int cacheMB) throws Exception {
        SegmentFetchTracker tracker = new SegmentFetchTracker();
        try (ReadOnlyFileStore store = fileStoreBuilder(dir)
                .withSegmentCacheSize(cacheMB).withIOMonitor(tracker).buildReadOnly()) {
            listImmediateChildren(resolvePath(readContentRoot(store, ref), target.path));
        }
        return tracker.fetches.get();
    }

    /** A wide node found during discovery: its path under the content root and its child count. */
    private static final class WideNode {
        final List<String> path;
        final int children;

        WideNode(List<String> path, int children) {
            this.path = path;
            this.children = children;
        }

        String display() {
            return path.isEmpty() ? "/" : "/" + String.join("/", path);
        }
    }

    private List<WideNode> topWidest;

    /** One streaming O(depth) walk retaining the {@code k} nodes with the most immediate children. */
    private List<WideNode> discoverTopWidest(File dir, long[] ref, int k) throws Exception {
        topWidest = new ArrayList<>();
        try (ReadOnlyFileStore store = fileStoreBuilder(dir).buildReadOnly()) {
            discoverWide(readContentRoot(store, ref), new ArrayDeque<>(), k);
        }
        topWidest.sort((a, b) -> Integer.compare(b.children, a.children));
        return topWidest;
    }

    private void discoverWide(NodeState node, ArrayDeque<String> path, int k) {
        int children = (int) node.getChildNodeCount(Long.MAX_VALUE);
        if (topWidest.size() < k || children > topWidest.get(topWidest.size() - 1).children) {
            topWidest.add(new WideNode(new ArrayList<>(path), children));
            topWidest.sort((a, b) -> Integer.compare(b.children, a.children));
            if (topWidest.size() > k) {
                topWidest.remove(topWidest.size() - 1);
            }
        }
        for (ChildNodeEntry entry : node.getChildNodeEntries()) {
            path.addLast(entry.getName());
            discoverWide(entry.getNodeState(), path, k);
            path.removeLast();
        }
    }

    /** Attribute one wide node's list-widest footprint against one already-produced layout. */
    private void analyzeNode(WideNode target, String label, File dir, long[] ref, int cacheMB,
            boolean printRecords) throws Exception {
        SegmentFetchTracker tracker = new SegmentFetchTracker();
        try (ReadOnlyFileStore store = fileStoreBuilder(dir)
                .withSegmentCacheSize(cacheMB).withIOMonitor(tracker).buildReadOnly()) {
            listImmediateChildren(resolvePath(readContentRoot(store, ref), target.path));
        }
        try (ReadOnlyFileStore store = fileStoreBuilder(dir).buildReadOnly()) {
            RecordId nodeId = ((SegmentNodeState) resolvePath(readContentRoot(store, ref), target.path))
                    .getRecordId();
            ListingSegCollector c = new ListingSegCollector(store.getReader());
            c.collect(nodeId);
            int ideal = (int) Math.ceil(c.unionMB() / (256.0 / 1024.0));
            System.out.printf("%-24s %8d %9d %6d %5d %5d %6d %7d %6d %7d %7d%n",
                    label, tracker.fetches.get(), tracker.distinct.size(),
                    c.count("node"), c.count("template"), c.count("map"), c.count("value"),
                    c.count("string"), c.count("list"), ideal, c.all.size());
            if (printRecords) {
                System.out.printf("   records touched: node=%d template=%d map=%d property=%d "
                                + "string=%d list=%d ; union=%.1f MB (ideal %d segs)%n",
                        c.records("node"), c.records("template"), c.records("map"),
                        c.records("property"), c.records("string"), c.records("list"),
                        c.unionMB(), ideal);
            }
        }
    }

    private static NodeState readContentRoot(ReadOnlyFileStore store, long[] ref) {
        SegmentId sid = store.getSegmentIdProvider().newSegmentId(ref[0], ref[1]);
        return store.getReader().readNode(new RecordId(sid, (int) ref[2])).getChildNode("root");
    }

    private static String segKey(SegmentId id) {
        return id.getMostSignificantBits() + "," + id.getLeastSignificantBits();
    }

    /** Like {@link LatencyIOMonitor} but also records the set of distinct segments fetched. */
    private static final class SegmentFetchTracker implements IOMonitor {
        final AtomicLong fetches = new AtomicLong();
        final Set<String> distinct = ConcurrentHashMap.newKeySet();

        @Override
        public void beforeSegmentRead(File file, long msb, long lsb, int length) {
            fetches.incrementAndGet();
            distinct.add(msb + "," + lsb);
        }

        @Override
        public void afterSegmentRead(File file, long msb, long lsb, int length, long elapsed) {
        }

        @Override
        public void beforeSegmentWrite(File file, long msb, long lsb, int length) {
        }

        @Override
        public void afterSegmentWrite(File file, long msb, long lsb, int length, long elapsed) {
        }
    }

    /**
     * Counts distinct data-segment records reachable from a node by reference: node + stable-id
     * block, template, map, list, bucket, string and inlined blob-header records. A single dedup set
     * shared across all record types means each physical record is counted exactly once, which also
     * folds a degenerate single-element list into its value record. Bulk binary block data is not
     * visited (shared, excluded), matching the "records" metric used by the repacker benchmark.
     */
    private static final class RecordCounter extends SegmentParser {
        private final Map<String, BitSet> seen = new HashMap<>();
        long nodes, stableIdBlocks, templates, maps, lists, buckets, strings, blobs;

        RecordCounter(SegmentReader reader) {
            super(reader);
        }

        void count(RecordId nodeId) {
            onNode(null, nodeId);
        }

        long total() {
            return nodes + stableIdBlocks + templates + maps + lists + buckets + strings + blobs;
        }

        String breakdown() {
            return String.format(
                    "nodes %,d, stable-id blocks %,d, templates %,d, maps %,d, lists %,d, "
                            + "buckets %,d, strings %,d, blobs %,d",
                    nodes, stableIdBlocks, templates, maps, lists, buckets, strings, blobs);
        }

        /** @return {@code true} if {@code id} was already seen; otherwise marks it seen and returns {@code false}. */
        private boolean seen(RecordId id) {
            BitSet bs = seen.computeIfAbsent(segKey(id.getSegmentId()), k -> new BitSet());
            int rn = id.getRecordNumber();
            if (bs.get(rn)) {
                return true;
            }
            bs.set(rn);
            return false;
        }

        @Override
        protected void onNode(RecordId parentId, RecordId nodeId) {
            if (seen(nodeId)) {
                return;
            }
            nodes++;
            // Stable-id block (node id-slot 0). A self-marker points at the node itself (no record).
            RecordId stableId = nodeId.getSegment().readRecordId(nodeId.getRecordNumber(), 0, 0);
            if (!stableId.equals(nodeId) && !seen(stableId)) {
                stableIdBlocks++;
            }
            parseNode(nodeId);
        }

        @Override
        protected void onTemplate(RecordId parentId, RecordId templateId) {
            if (seen(templateId)) {
                return;
            }
            templates++;
            parseTemplate(templateId);
        }

        @Override
        protected void onMapLeaf(RecordId parentId, RecordId mapId, MapRecord map) {
            if (seen(mapId)) {
                return;
            }
            maps++;
            parseMapLeaf(mapId, map);
        }

        @Override
        protected void onMapBranch(RecordId parentId, RecordId mapId, MapRecord map) {
            if (seen(mapId)) {
                return;
            }
            maps++;
            parseMapBranch(mapId, map);
        }

        @Override
        protected void onMapDiff(RecordId parentId, RecordId mapId, MapRecord map) {
            if (seen(mapId)) {
                return;
            }
            maps++;
            parseMapDiff(mapId, map);
        }

        @Override
        protected void onList(RecordId parentId, RecordId listId, int count) {
            if (seen(listId)) {
                return;
            }
            lists++;
            parseList(parentId, listId, count);
        }

        @Override
        protected void onListBucket(RecordId parentId, RecordId listId, int index, int count, int capacity) {
            // The top-level call passes the LIST record id (already counted in onList); only the
            // recursive sub-bucket calls carry genuine BUCKET record ids.
            if (seen(listId)) {
                return;
            }
            buckets++;
            parseListBucket(listId, index, count, capacity);
        }

        @Override
        protected void onString(RecordId parentId, RecordId stringId) {
            if (seen(stringId)) {
                return;
            }
            strings++;
            parseString(stringId);
        }

        @Override
        protected void onBlob(RecordId parentId, RecordId blobId) {
            if (seen(blobId)) {
                return;
            }
            blobs++;
            parseBlob(blobId);
        }
    }

    /**
     * Walks exactly the records a {@code list-widest} read touches - the widest node, its child map,
     * and each immediate child's template + properties + property values, but NOT the children's own
     * children - and buckets the distinct segments touched by record type. This attributes the
     * list-widest footprint (node vs template vs value/string/list/...) so the parallel-dedup
     * blow-up can be pinned to a specific record layer. Segment sets are cache-independent.
     */
    private static final class ListingSegCollector extends SegmentParser {
        private final Map<String, Set<String>> byType = new HashMap<>();
        private final Set<String> all = new HashSet<>();
        // distinct records (not segments) + their byte sizes, per type. Answers: if the records this
        // read touches were packed contiguously, how few segments could they occupy (bytes / 256 KB)?
        private final Map<String, Set<String>> recsByType = new HashMap<>();
        private final Map<String, Long> bytesByType = new HashMap<>();
        private final Set<String> seenRecords = new HashSet<>();
        private long unionBytes;
        private int depth;

        ListingSegCollector(SegmentReader reader) {
            super(reader);
        }

        void collect(RecordId widestNodeId) {
            onNode(null, widestNodeId);
        }

        int count(String type) {
            Set<String> s = byType.get(type);
            return s == null ? 0 : s.size();
        }

        int records(String type) {
            Set<String> s = recsByType.get(type);
            return s == null ? 0 : s.size();
        }

        double mb(String type) {
            return bytesByType.getOrDefault(type, 0L) / (1024.0 * 1024.0);
        }

        double unionMB() {
            return unionBytes / (1024.0 * 1024.0);
        }

        private void add(String type, RecordId id) {
            String key = segKey(id.getSegmentId());
            byType.computeIfAbsent(type, k -> new HashSet<>()).add(key);
            all.add(key);
        }

        /** Record one distinct record's byte size once (deduped by record id across the whole walk). */
        private void noteRecord(String type, RecordId id, long size) {
            String rk = segKey(id.getSegmentId()) + "#" + id.getRecordNumber();
            if (seenRecords.add(rk)) {
                recsByType.computeIfAbsent(type, k -> new HashSet<>()).add(rk);
                bytesByType.merge(type, size, Long::sum);
                unionBytes += size;
            }
        }

        @Override
        protected void onNode(RecordId parentId, RecordId nodeId) {
            add("node", nodeId);
            if (depth >= 2) {
                return; // parent (depth 0->1) and its children (1->2); grandchildren are not read
            }
            depth++;
            NodeInfo info = parseNode(nodeId);
            depth--;
            noteRecord("node", nodeId, info.size);
        }

        @Override
        protected void onTemplate(RecordId parentId, RecordId templateId) {
            add("template", templateId);
            TemplateInfo info = parseTemplate(templateId); // property-name / primary-type strings
            noteRecord("template", templateId, info.size);
        }

        @Override
        protected void onMap(RecordId parentId, RecordId mapId, MapRecord map) {
            if (depth >= 2) {
                return; // don't descend a child's own child map (grandchildren)
            }
            add("map", mapId);
            MapInfo info = parseMap(parentId, mapId, map);
            noteRecord("map", mapId, info.size);
        }

        @Override
        protected void onMapLeaf(RecordId parentId, RecordId mapId, MapRecord map) {
            add("map", mapId);
            MapInfo info = parseMapLeaf(mapId, map);
            noteRecord("map", mapId, info.size);
        }

        @Override
        protected void onMapBranch(RecordId parentId, RecordId mapId, MapRecord map) {
            add("map", mapId);
            MapInfo info = parseMapBranch(mapId, map);
            noteRecord("map", mapId, info.size);
        }

        @Override
        protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) {
            add("value", propertyId);
            PropertyInfo info = parseProperty(parentId, propertyId, template);
            noteRecord("property", propertyId, info.size);
        }

        @Override
        protected void onValue(RecordId parentId, RecordId valueId, Type<?> type) {
            add("value", valueId);
            parseValue(parentId, valueId, type);
        }

        @Override
        protected void onString(RecordId parentId, RecordId stringId) {
            add("string", stringId);
            BlobInfo info = parseString(stringId);
            noteRecord("string", stringId, info.size);
        }

        @Override
        protected void onBlob(RecordId parentId, RecordId blobId) {
            add("blob", blobId);
        }

        @Override
        protected void onList(RecordId parentId, RecordId listId, int count) {
            add("list", listId);
            ListInfo info = parseList(parentId, listId, count);
            noteRecord("list", listId, info.size);
        }

        @Override
        protected void onListBucket(RecordId parentId, RecordId listId, int index, int count,
                int capacity) {
            add("list", listId);
            ListBucketInfo info = parseListBucket(listId, index, count, capacity);
            noteRecord("list", listId, info.size);
        }
    }

    @FunctionalInterface
    private interface LayoutProducer {
        /** Copy the prepared store into {@code dir}, compact/repack it, and return the new root ref. */
        long[] produce(File dir) throws Exception;
    }

    private void measureLayout(String label, File dir, File prepared, LayoutProducer producer,
            int[] cacheSizes, long fetchLatencyNanos, long byteLatencyNanos) throws Exception {
        if (!layoutSelected(label)) {
            return;
        }
        long[] ref = producer.produce(dir);
        try {
            if (widestPath == null) {
                discoverTargets(dir, ref);
                System.out.println("  (widest node /root/" + String.join("/", widestPath) + " with "
                        + widestChildCount + " children; deepest path depth " + deepestPath.size() + ")");
            }
            List<String> widest = widestPath;
            List<String> deepest = deepestPath;
            printWorkload(label, "full-dfs", dir, ref, cacheSizes, fetchLatencyNanos, byteLatencyNanos,
                    RecordRepackerTest::traverseDepthFirst);
            printWorkload(label, "list-widest", dir, ref, cacheSizes, fetchLatencyNanos, byteLatencyNanos,
                    content -> listImmediateChildren(resolvePath(content, widest)));
            printWorkload(label, "subtree-widest", dir, ref, cacheSizes, fetchLatencyNanos, byteLatencyNanos,
                    content -> traverseDepthFirst(resolvePath(content, widest)));
            printWorkload(label, "deep-path", dir, ref, cacheSizes, fetchLatencyNanos, byteLatencyNanos,
                    content -> readAlongPath(content, deepest));
        } finally {
            deleteDir(dir);
        }
    }

    private void printWorkload(String label, String workload, File dir, long[] ref, int[] cacheSizes,
            long fetchLatencyNanos, long byteLatencyNanos, ReadWorkload workloadFn) throws Exception {
        for (int cacheMB : cacheSizes) {
            long[] counts = runReadWorkload(dir, ref, cacheMB, workloadFn);
            long fetches = counts[0];
            long bytes = counts[1];
            long distinct = counts[2];
            double simMillis = (fetches * fetchLatencyNanos + bytes * byteLatencyNanos) / 1_000_000.0;
            String row = String.format("%-30s %-14s %8d %12d %12d %12.1f %12.1f%n",
                    label, workload, cacheMB, fetches, distinct, bytes / (1024.0 * 1024.0), simMillis);
            System.out.print(row);
            appendResultFile(row);
        }
    }

    /**
     * Optional comma-separated substring filter ({@code -Doak.repack.readLayouts}) selecting which layouts
     * to produce and measure; all layouts when unset. Lets a run target e.g. only the semantic scenarios.
     */
    private static boolean layoutSelected(String label) {
        String filter = System.getProperty("oak.repack.readLayouts");
        if (filter == null || filter.isBlank()) {
            return true;
        }
        for (String token : filter.split(",")) {
            if (!token.isBlank() && label.contains(token.trim())) {
                return true;
            }
        }
        return false;
    }

    /** Parse a comma-separated list of segment-cache sizes (MB) for the read-locality cache sweep. */
    private static int[] parseCacheSizes(String spec) {
        String[] parts = spec.split(",");
        int[] sizes = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            sizes[i] = Integer.parseInt(parts[i].trim());
        }
        return sizes;
    }

    /**
     * Traverse the whole content tree ({@code /root}) of the store at {@code dir} from a cold
     * segment cache, forcing every node, template and value record to be read, and return
     * {@code [segmentFetches, bytesFetched]}. A fresh {@link ReadOnlyFileStore} with a small segment
     * cache and a counting {@link IOMonitor} is opened per call, so the counts reflect exactly the
     * archive reads (cache misses) this one traversal incurs.
     */
    private long[] runReadWorkload(File dir, long[] rootRef, int cacheMB, ReadWorkload workload)
            throws Exception {
        LatencyIOMonitor monitor = new LatencyIOMonitor();
        try (ReadOnlyFileStore store = fileStoreBuilder(dir)
                .withSegmentCacheSize(cacheMB)
                .withIOMonitor(monitor)
                .buildReadOnly()) {
            SegmentId sid = store.getSegmentIdProvider().newSegmentId(rootRef[0], rootRef[1]);
            NodeState superRoot = store.getReader().readNode(new RecordId(sid, (int) rootRef[2]));
            workload.read(superRoot.getChildNode("root"));
        }
        return new long[] {monitor.fetches.get(), monitor.bytes.get(), monitor.distinctSegs.size()};
    }

    @FunctionalInterface
    private interface ReadWorkload {
        void read(NodeState content) throws Exception;
    }

    private List<String> widestPath;
    private List<String> deepestPath;
    private int widestChildCount;

    /**
     * One warm pass over the content tree to locate the widest node (most immediate children) and a
     * deepest path. The structure is identical across layouts, so this runs once and the discovered
     * paths are reused (resolved by name) against every layout's cold store.
     */
    private void discoverTargets(File dir, long[] ref) throws Exception {
        try (ReadOnlyFileStore store = fileStoreBuilder(dir).buildReadOnly()) {
            SegmentId sid = store.getSegmentIdProvider().newSegmentId(ref[0], ref[1]);
            NodeState content = store.getReader().readNode(new RecordId(sid, (int) ref[2]))
                    .getChildNode("root");
            widestPath = new ArrayList<>();
            deepestPath = new ArrayList<>();
            widestChildCount = -1;
            // Streaming depth-first walk: only the current root-to-node path is retained (O(depth)),
            // so this scales to a many-million-node tree without holding the BFS frontier.
            discover(content, new ArrayDeque<>());
        }
    }

    private void discover(NodeState node, ArrayDeque<String> path) {
        long children = node.getChildNodeCount(Long.MAX_VALUE);
        if (children > widestChildCount) {
            widestChildCount = (int) children;
            widestPath = new ArrayList<>(path);
        }
        if (path.size() > deepestPath.size()) {
            deepestPath = new ArrayList<>(path);
        }
        for (ChildNodeEntry entry : node.getChildNodeEntries()) {
            path.addLast(entry.getName());
            discover(entry.getNodeState(), path);
            path.removeLast();
        }
    }

    private static NodeState resolvePath(NodeState content, List<String> path) {
        NodeState node = content;
        for (String name : path) {
            node = node.getChildNode(name);
        }
        return node;
    }

    /** Read a node and only its immediate children (no recursion) - the wide-listing pattern. */
    private static void listImmediateChildren(NodeState parent) {
        touchNode(parent);
        for (ChildNodeEntry entry : parent.getChildNodeEntries()) {
            touchNode(entry.getNodeState());
        }
    }

    /** Resolve a single deep path from the content root, reading each node along the way. */
    private static void readAlongPath(NodeState content, List<String> path) {
        NodeState node = content;
        touchNode(node);
        for (String name : path) {
            node = node.getChildNode(name);
            touchNode(node);
        }
    }

    private long[] produceRepackLayout(File prepared, File dir, RecordRepacker.Mode mode,
            int concurrency, int splitTarget, int expectedRecords) throws Exception {
        return produceRepackLayout(prepared, dir, mode, concurrency, splitTarget, expectedRecords, -1);
    }

    /**
     * As {@link #produceRepackLayout(File, File, RecordRepacker.Mode, int, int, int)}, but pin the
     * content-dedup cache: {@code window == -1} the default (auto-sized recency window), {@code 0} the
     * exact off-heap cache, {@code > 0} a fixed-size window. Lets the read-locality benchmark contrast
     * the default window layout against the exact one.
     */
    private long[] produceRepackLayout(File prepared, File dir, RecordRepacker.Mode mode,
            int concurrency, int splitTarget, int expectedRecords, int window) throws Exception {
        return produceRepackLayout(prepared, dir, mode, concurrency, splitTarget, expectedRecords,
                window, 0);
    }

    /**
     * As {@link #produceRepackLayout(File, File, RecordRepacker.Mode, int, int, int, int)}, but with
     * {@code stages} pre-stages driving the staged repack ({@link RecordRepacker#withStages} via
     * {@link #stageRoots}): {@code 1} = index-first ({@code /oak:index}, then the rest); {@code 2} =
     * three-stage ({@code /oak:index}, then {@code /root}, then {@code /}).
     */
    private long[] produceRepackLayout(File prepared, File dir, RecordRepacker.Mode mode,
            int concurrency, int splitTarget, int expectedRecords, int window, int stages)
            throws Exception {
        copySegmentStore(prepared, dir);
        try (FileStore fs = fileStoreBuilder(dir).build()) {
            SegmentNodeState head = fs.getHead();
            RecordId root = head.getRecordId();
            GCGeneration target = root.getSegmentId().getGcGeneration().nextFull();
            RecordRepacker repacker = new RecordRepacker(fs, fs.getReader(),
                    fs.getSegmentIdProvider(), fs.getBlobStore(), fs.getBinariesInlineThreshold(),
                    target, mode, expectedRecords, concurrency)
                    .withSplitTarget(splitTarget).withDedupWindow(window)
                    .withStages(stageRoots(head, stages));
            RecordId repacked = repacker.repack(root);
            fs.flush();
            return toRef(repacked);
        }
    }

    /** Records + non-bulk bytes written into the generation of {@code ref} (the repacked output). */
    private static long[] generationStats(File dir, long[] ref) throws Exception {
        long[] stats = {0, 0};
        try (ReadOnlyFileStore store = fileStoreBuilder(dir).buildReadOnly()) {
            GCGeneration target = store.getSegmentIdProvider()
                    .newSegmentId(ref[0], ref[1]).getGcGeneration();
            for (SegmentId id : store.getSegmentIds()) {
                if (id.isDataSegmentId() && target.equals(id.getGcGeneration())) {
                    Segment segment = id.getSegment();
                    segment.forEachRecord((number, type, offset) -> stats[0]++);
                    stats[1] += segment.size();
                }
            }
        }
        return stats;
    }

    private long[] produceSemanticLayout(File prepared, File dir) throws Exception {
        copySegmentStore(prepared, dir);
        try (FileStore fs = fileStoreBuilder(dir).build()) {
            SegmentNodeState head = fs.getHead();
            GCGeneration base = head.getGcGeneration();
            GCGeneration target = base.nextFull();
            GCIncrement increment = new GCIncrement(base, base.nextPartial(), target);
            SegmentWriterFactory writerFactory = generation -> defaultSegmentWriterBuilder("c")
                    .withGeneration(generation).build(fs);
            CompactionWriter compactionWriter = new CompactionWriter(fs.getReader(),
                    fs.getBlobStore(), increment, writerFactory);
            GCNodeWriteMonitor monitor = new GCNodeWriteMonitor(-1, GCMonitor.EMPTY);
            CheckpointCompactor compactor = new CheckpointCompactor(GCMonitor.EMPTY,
                    new ClassicCompactor(compactionWriter, monitor));
            CompactedNodeState compacted = compactor.compactUp(head, Canceller.newCanceller());
            assertNotNull(compacted, "checkpoint compaction must not be cancelled");
            compactionWriter.flush();
            fs.flush();
            return toRef(compacted.getRecordId());
        }
    }

    private long[] produceParallelSemanticLayout(File prepared, File dir, int concurrency)
            throws Exception {
        copySegmentStore(prepared, dir);
        try (FileStore fs = fileStoreBuilder(dir).build()) {
            SegmentNodeState head = fs.getHead();
            GCGeneration base = head.getGcGeneration();
            GCGeneration target = base.nextFull();
            GCIncrement increment = new GCIncrement(base, base.nextPartial(), target);
            SegmentWriterFactory writerFactory = generation -> defaultSegmentWriterBuilder("c")
                    .withGeneration(generation)
                    .withWriterPool(SegmentBufferWriterPool.PoolType.THREAD_SPECIFIC)
                    .build(fs);
            CompactionWriter compactionWriter = new CompactionWriter(fs.getReader(),
                    fs.getBlobStore(), increment, writerFactory);
            GCNodeWriteMonitor monitor = new GCNodeWriteMonitor(-1, GCMonitor.EMPTY);
            CheckpointCompactor compactor = new CheckpointCompactor(GCMonitor.EMPTY,
                    new ParallelCompactor(GCMonitor.EMPTY, compactionWriter, monitor, concurrency));
            CompactedNodeState compacted = compactor.compactUp(head, Canceller.newCanceller());
            assertNotNull(compacted, "parallel checkpoint compaction must not be cancelled");
            compactionWriter.flush();
            fs.flush();
            return toRef(compacted.getRecordId());
        }
    }

    private static double[] parseFractions(String spec) {
        String[] parts = spec.split(",");
        double[] out = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            out[i] = Double.parseDouble(parts[i].trim());
        }
        return out;
    }

    private static int[] parseThresholds(String spec) {
        String[] parts = spec.split(",");
        int[] out = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            out[i] = Integer.parseInt(parts[i].trim());
        }
        return out;
    }

    /**
     * Depth-first traversal reading every node (recursive, so only the current root-to-node path is
     * retained). An iterative stack would materialise and pin a wide node's whole child frontier
     * (165K children on the large store), retaining ~all segments at once and exhausting the heap; the
     * recursion holds O(depth) nodes, matching {@link #discover}.
     */
    private static void traverseDepthFirst(NodeState node) {
        touchNode(node);
        for (ChildNodeEntry entry : node.getChildNodeEntries()) {
            traverseDepthFirst(entry.getNodeState());
        }
    }

    /** Read every property value of {@code node}, forcing its template and value records to load. */
    private static void touchNode(NodeState node) {
        for (PropertyState p : node.getProperties()) {
            Type<?> type = p.getType();
            if (type == BINARY || type == BINARIES) {
                // Materialise the in-segment blob-id record but do NOT resolve the external blob
                // (length()/stream() needs a BlobStore and hits the DataStore, not the segment store,
                // which is irrelevant to segment-read locality and unavailable on the read-only store).
                for (int i = 0; i < p.count(); i++) {
                    p.getValue(BINARY, i);
                }
            } else {
                p.getValue(type);
            }
        }
    }

    private static long[] toRef(RecordId id) {
        return new long[] {id.getSegmentId().getMostSignificantBits(),
                id.getSegmentId().getLeastSignificantBits(), id.getRecordNumber()};
    }

    private static void deleteDir(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDir(f);
                } else {
                    f.delete();
                }
            }
        }
        dir.delete();
    }

    /** Counts segment fetches (archive reads = cache misses), distinct segments, and bytes read. */
    private static final class LatencyIOMonitor implements IOMonitor {
        final AtomicLong fetches = new AtomicLong();
        final AtomicLong bytes = new AtomicLong();
        // Distinct segments that missed at least once: fetches - distinctSegs.size() = re-reads
        // forced by cache eviction (thrash), so the two columns separate footprint from re-fetch.
        final Set<String> distinctSegs = ConcurrentHashMap.newKeySet();

        @Override
        public void beforeSegmentRead(File file, long msb, long lsb, int length) {
            fetches.incrementAndGet();
            bytes.addAndGet(length);
            distinctSegs.add(msb + "," + lsb);
        }

        @Override
        public void afterSegmentRead(File file, long msb, long lsb, int length, long elapsed) {
        }

        @Override
        public void beforeSegmentWrite(File file, long msb, long lsb, int length) {
        }

        @Override
        public void afterSegmentWrite(File file, long msb, long lsb, int length, long elapsed) {
        }
    }
}
