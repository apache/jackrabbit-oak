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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.segment.SegmentStream.BLOCK_SIZE;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Prototype of a <em>record-level</em> compactor. In contrast to the semantic
 * compactors ({@link ClassicCompactor}, {@code ParallelCompactor}) which rebuild
 * the content tree from {@link org.apache.jackrabbit.oak.spi.state.NodeState}
 * instances, this class repacks the raw records reachable from a root into new,
 * densely packed segments of a target {@link GCGeneration}.
 * <p>
 * The segment format guarantees that a record only ever references records that
 * were written before it, so the reachable record set forms a DAG with edges
 * pointing backwards in time. This class walks that DAG in <em>post-order</em>:
 * a record is only emitted once all the records it references have been emitted,
 * at which point their new {@link RecordId}s are known and can be substituted for
 * the old ones (a record cannot be copied verbatim, because moving it changes its
 * {@code RecordId}). The old&nbsp;&rarr;&nbsp;new mapping is kept in an
 * <em>alias table</em> ({@link #alias}) which also deduplicates records that were
 * already physically shared in the source (i.e. reachable via the same source
 * record id through multiple referrers).
 * <p>
 * In addition, a content-addressed cache ({@link #dedupCache}) performs
 * <em>record deduplication</em>: records that are byte-for-byte identical after
 * their references have been translated are emitted only once, even when they had
 * distinct source record ids (i.e. were <em>not</em> physically shared in the
 * source). Nodes are keyed by their stable id&nbsp;&mdash; which uniquely identifies
 * a node's content&nbsp;&mdash; so two records for the same logical node collapse to
 * one and their (identical) subtree is not walked twice; templates, values, strings,
 * blocks and lists are keyed by their translated content. This is the same kind of
 * new sharing the semantic compactors discover through content equality.
 * <p>
 * How nodes are keyed and how their stable ids are handled is selected by {@link Mode}:
 * {@link Mode#STABLE_ID} (the default) keys nodes by stable id and preserves every stable
 * id, {@link Mode#NODE_DATA} keys nodes by their translated record data and keeps stable ids
 * at their self-reference default unless already set, {@link Mode#DEEP_DEDUP} keys nodes by
 * their translated record data alone (ignoring stable ids) and {@link Mode#NO_DEDUP} disables
 * content deduplication altogether, relocating every record faithfully.
 * <p>
 * This first increment keeps the full alias table in memory (simplest, exact) so
 * that the transcoder itself can be validated; a reference-counted, memory-bounded
 * alias table is a follow-up. Only whole-graph (full) repacking is supported.
 * <p>
 * Byte payloads (value/string/block records) are copied verbatim without decoding
 * them into Java objects. Structural records (maps and lists) are rebuilt through
 * {@link DefaultSegmentWriter}, and binaries are relinked through
 * {@link DefaultSegmentWriter#writeBlob}, so bulk segments are never rewritten.
 */
public class RecordRepacker {

    /**
     * Strategy for handling a node's stable id and for deduplicating nodes.
     */
    public enum Mode {
        /**
         * Preserve every stable id by materialising it as an explicit block, and
         * deduplicate nodes by their stable id. A stable-id hit prunes the (identical)
         * subtree from a second walk, mirroring the semantic compactors' node cache.
         */
        STABLE_ID,

        /**
         * Keep a node's stable id at its self-reference default unless the source node
         * already has an explicit one, and deduplicate nodes by their translated record
         * data (the template, child and property references). Because references are
         * record ids, equal translated references mean byte-identical node records, so no
         * subtree hashing is needed. This can collapse content-equal nodes that carry
         * distinct <em>default</em> stable ids, at the cost of not preserving those default
         * stable ids and of walking a duplicate node's subtree before deduplicating it.
         */
        NODE_DATA,

        /**
         * Like {@link #NODE_DATA}, but the node dedup key ignores stable ids entirely and every
         * stable id is dropped to its self-reference default. Because deduplication runs
         * bottom-up in post-order, this collapses whole content-equal subtrees&nbsp;&mdash;
         * including nodes that carry <em>distinct explicit</em> stable ids, which {@link #NODE_DATA}
         * keeps apart. On a store whose nodes are all at their self-reference default this
         * coincides with {@link #NODE_DATA}; it only finds more when explicit stable ids are present.
         */
        DEEP_DEDUP,

        /**
         * Faithfully relocate every record without discovering any new sharing: keep only
         * the physical sharing already present in the source (via the alias table) and
         * disable the content-addressed {@link #dedupCache}. A node's stable id is kept
         * exactly as the source encodes it (a self-reference default stays a self-reference
         * default). This is the un-deduplicated baseline the other modes improve on.
         */
        NO_DEDUP
    }

    @NotNull
    private final SegmentStore store;

    @NotNull
    private final SegmentReader reader;

    @NotNull
    private final SegmentIdProvider idProvider;

    @NotNull
    private final GCGeneration targetGeneration;

    @NotNull
    private final Mode mode;

    /** Number of worker threads to repack with; {@code 1} means fully single-threaded. */
    private final int concurrency;

    @Nullable
    private final BlobStore blobStore;

    private final int binariesInlineThreshold;

    /**
     * Handles all writes to the target generation. Both the raw record emits of this class (see
     * {@link #emit}) and the {@link #structuralWriter} route through it, so all repacked records are
     * co-packed into the same segment stream. A plain {@link SegmentBufferWriter} single-threaded;
     * when repacking in parallel either a thread-specific {@link SegmentBufferWriterPool} (a writer
     * per worker thread, the default) or a single {@link SegmentBufferWriter} behind a synchronized
     * wrapper (all workers serialise on one writer) depending on {@link #singleWriter}. Rebuilt by
     * {@link #withSingleWriter(boolean)}, hence not {@code final}.
     */
    @NotNull
    private WriteOperationHandler writeHandler;

    /**
     * Used for rebuilding maps and lists and for relinking binaries. Shares
     * {@link #writeHandler} as its write operation handler, so it is thread-safe when the handler is.
     * Rebuilt together with {@link #writeHandler}, hence not {@code final}.
     */
    @NotNull
    private DefaultSegmentWriter structuralWriter;

    /**
     * Old {@link RecordId} to new {@link RecordId}, kept off the Java heap in a sharded, memory
     * mapped table (see {@link ShardedRecordIdMap}). Deduplicates records that were already
     * physically shared in the source.
     */
    @NotNull
    private final ShardedRecordIdMap alias;

    /**
     * Canonical record content (a {@value #KEY_HASH_WIDTH}-byte hash built by {@link HashKey})
     * to the {@link RecordId} of the (single) emitted copy. Enables record-level deduplication of
     * records that are content-identical after reference translation but had distinct source record
     * ids. Keys are tagged by record type (see the {@code T_*} constants); nodes are keyed by their
     * stable id or translated data (per {@link Mode}), everything else by its translated content.
     * {@code null} when the mode disables deduplication.
     * <p>
     * A bounded recency window by default (auto-sized to {@link #AUTO_WINDOW_FRACTION} of
     * {@link #expectedRecords}): off-heap in a bounded memory-mapped file ({@link MmapWindowDedupCache})
     * or, per {@link #withDedupCacheImpl}, in-heap ({@link WindowDedupCache}); either deduplicates only
     * records whose content recurs while still in the window and needs no whole-store spill. The exact,
     * unbounded, off-heap {@link ShardedRecordIdMap} is used when the window resolves to {@code 0} (see
     * {@link #withDedupWindow}). Built lazily in {@link #repack} (so the window vs exact choice can be
     * made after construction).
     */
    @Nullable
    private DedupCache dedupCache;

    /** Number of shards for the {@link #alias} / {@link #dedupCache} tables (matches concurrency). */
    private final int shardCount;

    /** Estimated record count, used to pre-size the exact tables; see the constructor. */
    private final int expectedRecords;

    /**
     * Selects the {@link #dedupCache} window size. {@code -1} (the default) auto-sizes a bounded recency
     * window (off-heap {@link MmapWindowDedupCache} by default) to {@link #AUTO_WINDOW_FRACTION} of
     * {@link #expectedRecords} (recovering nearly all of the exact deduplication at a small, bounded
     * footprint and no whole-store spill); {@code > 0} uses a window of exactly that many entries;
     * {@code 0} uses the exact, unbounded, off-heap {@link ShardedRecordIdMap}. Set via {@link #withDedupWindow}.
     */
    private int dedupWindow = -1;

    /**
     * Fraction of {@link #expectedRecords} used to auto-size the default recency window: a window of
     * ~10% of the input's record count recovers the exact deduplication within a few percent (the
     * evicted entries are one-off records that never recur), at a small fraction of the exact cache's
     * footprint. Below a known record count ({@code expectedRecords <= 0}) the exact cache is used.
     */
    private static final int AUTO_WINDOW_FRACTION = 10;

    /** Which bounded-dedup implementation to use for a windowed (non-exact) {@link #dedupCache}. */
    public enum DedupCacheImpl {
        /** Off-heap (memory-mapped) bounded recency window that forgets evicted entries ({@link MmapWindowDedupCache}). */
        MMAP_WINDOW,
        /** Hand-rolled in-heap sharded access-order LRU that forgets evicted entries ({@link WindowDedupCache}). */
        HEAP_WINDOW
    }

    /**
     * Bounded-dedup implementation; only used when {@link #resolveDedupWindow} resolves to a window.
     * The default {@link DedupCacheImpl#MMAP_WINDOW} keeps the window off the Java heap (in a bounded
     * memory-mapped file), which is lower-heap and faster than the in-heap {@link DedupCacheImpl#HEAP_WINDOW}
     * at a marginally larger output.
     */
    private DedupCacheImpl dedupCacheImpl = DedupCacheImpl.MMAP_WINDOW;

    /** Width in bytes of a {@link #dedupCache} content-hash key. */
    private static final int KEY_HASH_WIDTH = 16;

    /** Target number of parallel subtree roots to explore for, per worker thread. */
    private static final int SPLIT_POINTS_PER_THREAD = 32;

    /** Upper bound on the number of parallel subtree roots (bounds task count and exploration). */
    private static final int MAX_SPLIT_POINTS = 16384;

    /**
     * Maximum number of direct children a node may have and still be expanded into per-child split
     * roots during {@link #findSplitPoints}. A node wider than this is kept whole as a single split
     * root, so its entire child set is repacked by one worker into a contiguous run of segments
     * rather than being scattered across the workers' separate (thread-specific) segment streams.
     * Scattering fragments read locality: listing such a node then has to fetch a segment from every
     * worker instead of one dense run. Keeping wide nodes whole preserves the serial layout's read
     * density at the cost of repacking those (typically leaf-heavy) subtrees single-threaded.
     */
    private static final int SPLIT_MAX_FANOUT = 1024;

    /** Per-thread reused builder for {@link #dedupCache} keys; see {@link #hash(int)}. */
    @NotNull
    private final ThreadLocal<HashKey> hashKey = ThreadLocal.withInitial(HashKey::new);

    private static final int T_NODE = 0;
    private static final int T_TEMPLATE = 1;
    private static final int T_VALUE = 2;
    private static final int T_VALUE_LONG = 3;
    private static final int T_BLOCK = 4;
    private static final int T_LIST = 5;
    private static final int T_ARRAY = 6;
    private static final int T_EMPTY_ARRAY = 7;
    private static final int T_MAP = 8;

    /**
     * Whether to content-deduplicate child-node maps. On by default, so that whenever content
     * deduplication is enabled at all (any mode except {@link Mode#NO_DEDUP}) every structural
     * record type is deduplicated consistently. A child map whose translated
     * {@code name -> child id} entries match an already emitted map is not written again.
     * Because the entries are the (canonical) translated child ids, this also lets the
     * multi-child nodes referencing content-equal maps deduplicate. Can be turned off (e.g. to
     * measure its effect) via {@link #withMapDeduplication(boolean)}.
     */
    private boolean deduplicateMaps = true;

    /**
     * Target number of parallel subtree roots for {@link #findSplitPoints} (parallel repacking only).
     * {@code 0} (the default) derives the target from the worker count
     * ({@link #concurrency} &times; {@link #SPLIT_POINTS_PER_THREAD}), coupling the split depth to the
     * thread count. A positive value decouples the two: the frontier is explored to this many roots
     * regardless of {@link #concurrency}, so a deeper split (which pushes shared subtrees into the
     * exactly-deduplicated single-threaded spine, reducing cross-worker re-emission) can be measured
     * independently of the degree of parallelism. Set via {@link #withSplitTarget(int)}.
     */
    private int splitTarget = 0;

    /**
     * Whether concurrent content deduplication is <em>exact</em>. When {@code true} (the default),
     * the dedup-cache check and the record emit are performed as one atomic operation
     * ({@link ShardedRecordIdMap#computeIfAbsent}), so two workers reaching the same content never
     * both emit: the loser blocks, observes the winner's id and skips its write. When {@code false},
     * the check and emit race ({@code get} then {@code emit} then {@code putIfAbsent}) and a losing
     * worker's already-emitted record is left as unreferenced garbage - writes are not serialised
     * on the shard lock, at the cost of duplicated records under contention. Single-threaded the two
     * are equivalent. Applies only to the deduplicating modes; {@link Mode#NO_DEDUP} has no dedup
     * cache and so is always inexact under concurrency. Set via {@link #withExactDedup(boolean)}.
     */
    private boolean exactDedup = true;

    /**
     * When repacking in parallel ({@link #concurrency} &gt; 1), whether all worker threads serialise
     * their writes through a single {@link SegmentBufferWriter} (behind a synchronized wrapper) instead
     * of each writing to its own thread-specific writer. A single writer packs every record into one
     * segment stream (marginally denser, no partially-filled trailing segment per worker) but
     * serialises the physical write; the default (per-thread writers) writes in parallel. Only affects
     * the physical write path, not which records are emitted, so output is content-identical either
     * way. Set via {@link #withSingleWriter(boolean)}.
     */
    private boolean singleWriter = false;

    /**
     * Ordered list of subtree roots to repack first, each as a separate stage, before the rest of the
     * graph. Used to control emission order: repacking {@code /oak:index} first means its records - and
     * any records it shares with the rest of the repository - get their canonical copies emitted up
     * front, keeping the (typically wide) index subtrees packed into a contiguous run of segments
     * rather than scattered across the parallel workers' separate streams. Listing a wide index then
     * reads densely instead of fetching a segment per worker. Each stage runs at the configured
     * {@link #concurrency} and completes (a barrier) before the next begins; the final stage is always
     * the overall root passed to {@link #repack}, which alias-hits everything the earlier stages
     * already emitted. Stages may be nested (e.g. {@code /root/oak:index}, then {@code /root}): an inner
     * stage's records are simply alias hits when the enclosing stage runs. Empty (the default) disables
     * staging (a single stage over the whole graph). Set via {@link #withStages} / {@link #withIndexFirst}.
     */
    @NotNull
    private List<RecordId> stageRoots = List.of();

    public RecordRepacker(
            @NotNull SegmentStore store,
            @NotNull SegmentReader reader,
            @NotNull SegmentIdProvider idProvider,
            @Nullable BlobStore blobStore,
            int binariesInlineThreshold,
            @NotNull GCGeneration targetGeneration) throws IOException {
        this(store, reader, idProvider, blobStore, binariesInlineThreshold, targetGeneration,
                Mode.STABLE_ID);
    }

    public RecordRepacker(
            @NotNull SegmentStore store,
            @NotNull SegmentReader reader,
            @NotNull SegmentIdProvider idProvider,
            @Nullable BlobStore blobStore,
            int binariesInlineThreshold,
            @NotNull GCGeneration targetGeneration,
            @NotNull Mode mode) throws IOException {
        this(store, reader, idProvider, blobStore, binariesInlineThreshold, targetGeneration, mode, 0);
    }

    /**
     * @param expectedRecords an estimate of the number of records that will be repacked, used to
     *     pre-size the {@link #alias} and {@link #dedupCache} tables so they do not grow and rehash
     *     while repacking a large store. {@code 0} (or a negative value) falls back to a small
     *     default. Over-estimating is cheap - the tables are sparse memory-mapped files - and the
     *     value is capped at the tables' maximum size.
     */
    public RecordRepacker(
            @NotNull SegmentStore store,
            @NotNull SegmentReader reader,
            @NotNull SegmentIdProvider idProvider,
            @Nullable BlobStore blobStore,
            int binariesInlineThreshold,
            @NotNull GCGeneration targetGeneration,
            @NotNull Mode mode,
            int expectedRecords) throws IOException {
        this(store, reader, idProvider, blobStore, binariesInlineThreshold, targetGeneration, mode,
                expectedRecords, 1);
    }

    /**
     * @param concurrency number of worker threads to repack with. {@code 1} repacks single-threaded;
     *     a higher value partitions the record graph into subtrees repacked in parallel (only the
     *     deduplicating modes are safe to parallelize). The alias and dedup tables are sharded
     *     accordingly so concurrent workers rarely contend.
     */
    public RecordRepacker(
            @NotNull SegmentStore store,
            @NotNull SegmentReader reader,
            @NotNull SegmentIdProvider idProvider,
            @Nullable BlobStore blobStore,
            int binariesInlineThreshold,
            @NotNull GCGeneration targetGeneration,
            @NotNull Mode mode,
            int expectedRecords,
            int concurrency) throws IOException {
        this.store = requireNonNull(store);
        this.reader = requireNonNull(reader);
        this.idProvider = requireNonNull(idProvider);
        this.targetGeneration = requireNonNull(targetGeneration);
        this.mode = requireNonNull(mode);
        this.concurrency = Math.max(1, concurrency);
        this.blobStore = blobStore;
        this.binariesInlineThreshold = binariesInlineThreshold;
        this.writeHandler = newWriteHandler();
        this.structuralWriter = newStructuralWriter();
        this.shardCount = this.concurrency <= 1 ? 1 : Math.min(128, this.concurrency * 4);
        this.expectedRecords = expectedRecords;
        this.alias = new ShardedRecordIdMap(idProvider, RecordId.SERIALIZED_RECORD_ID_BYTES,
                expectedRecords, shardCount, "alias");
        // dedupCache is built lazily in repack(), so withDedupWindow() can pick window vs exact.
    }

    /**
     * Override the content-deduplication cache sizing. By default ({@link #dedupWindow} {@code == -1}) a
     * bounded in-heap recency window auto-sized to {@link #AUTO_WINDOW_FRACTION} of {@link #expectedRecords}
     * is used. Pass {@code maxEntries > 0} for a window of exactly that many entries, or {@code 0} for the
     * exact, unbounded, off-heap-spilled cache. Only records whose content recurs while still in the window
     * are deduplicated; the alias (physical-sharing memo) stays exact regardless. Package-private prototype
     * knob; takes effect for the deduplicating modes.
     *
     * @return {@code this}, for chaining.
     */
    @NotNull
    RecordRepacker withDedupWindow(int maxEntries) {
        this.dedupWindow = maxEntries;
        return this;
    }

    /**
     * Select the bounded-dedup implementation used when a window is in effect (see {@link DedupCacheImpl}).
     * The default {@link DedupCacheImpl#MMAP_WINDOW} keeps the window off-heap in a bounded memory-mapped
     * file; {@link DedupCacheImpl#HEAP_WINDOW} keeps it in the Java heap. No effect on the exact
     * ({@code window <= 0}) path. Package-private prototype knob.
     *
     * @return {@code this}, for chaining.
     */
    @NotNull
    RecordRepacker withDedupCacheImpl(@NotNull DedupCacheImpl impl) {
        this.dedupCacheImpl = impl;
        return this;
    }

    /**
     * Enable or disable content deduplication of child-node maps (see {@link #deduplicateMaps}).
     * Package-private prototype knob used to measure the effect of map deduplication.
     *
     * @return {@code this}, for chaining.
     */
    @NotNull
    RecordRepacker withMapDeduplication(boolean enabled) {
        this.deduplicateMaps = enabled;
        return this;
    }

    /**
     * Set the target number of parallel subtree roots, decoupling the split depth from the worker
     * count (see {@link #splitTarget}). {@code 0} restores the default (derive from concurrency).
     * Package-private prototype knob used to measure the split depth in isolation.
     *
     * @return {@code this}, for chaining.
     */
    @NotNull
    RecordRepacker withSplitTarget(int target) {
        this.splitTarget = Math.max(0, target);
        return this;
    }

    /**
     * Enable or disable exact concurrent content deduplication (see {@link #exactDedup}).
     * Package-private prototype knob used to measure the cost of serialising record emits on the
     * shard lock (exact) against the un-serialised racing emit (inexact, which leaves loser garbage).
     *
     * @return {@code this}, for chaining.
     */
    @NotNull
    RecordRepacker withExactDedup(boolean enabled) {
        this.exactDedup = enabled;
        return this;
    }

    /**
     * Switch the parallel write path between per-thread writers and a single serialised writer (see
     * {@link #singleWriter}) and rebuild {@link #writeHandler} / {@link #structuralWriter} accordingly.
     * Package-private prototype knob used to measure whether serialising all writes through one writer
     * (rather than one per worker thread) changes throughput - i.e. whether writing is a bottleneck.
     * Has no effect when repacking single-threaded. Must be called before {@link #repack(RecordId)}.
     *
     * @return {@code this}, for chaining.
     */
    @NotNull
    RecordRepacker withSingleWriter(boolean enabled) {
        this.singleWriter = enabled;
        this.writeHandler = newWriteHandler();
        this.structuralWriter = newStructuralWriter();
        return this;
    }

    /**
     * Repack {@code subtreeRoot} first, as a separate stage, before repacking the rest of the graph
     * (see {@link #stageRoots}). Intended for the {@code /oak:index} node, whose wide child sets read
     * best when kept together in one contiguous run rather than scattered across parallel workers.
     * Both stages run at the configured {@link #concurrency}. {@code null} (the default) disables the
     * two-stage ordering. Convenience for the single-stage case; see {@link #withStages}.
     * Package-private prototype knob.
     *
     * @return {@code this}, for chaining.
     */
    @NotNull
    RecordRepacker withIndexFirst(@Nullable RecordId subtreeRoot) {
        return withStages(subtreeRoot == null ? List.of() : List.of(subtreeRoot));
    }

    /**
     * Repack each of {@code stages} first, in order, each as a separate parallel stage separated by a
     * barrier, before repacking the rest of the graph (see {@link #stageRoots}). The overall root
     * passed to {@link #repack} is always repacked last. Stages may be nested and given innermost-first
     * (e.g. {@code [/root/oak:index, /root]}): an inner stage's records are alias hits when the
     * enclosing stage runs, so each stage only emits what the earlier stages did not. An empty list
     * (the default) disables staging. Package-private prototype knob.
     *
     * @return {@code this}, for chaining.
     */
    @NotNull
    RecordRepacker withStages(@NotNull List<RecordId> stages) {
        this.stageRoots = List.copyOf(stages);
        return this;
    }

    @NotNull
    private WriteOperationHandler newWriteHandler() {
        if (concurrency <= 1) {
            return new SegmentBufferWriter(idProvider, "repack", targetGeneration);
        }
        if (singleWriter) {
            return new SynchronizedWriteHandler(
                    new SegmentBufferWriter(idProvider, "repack", targetGeneration));
        }
        return SegmentBufferWriterPool.factory(idProvider, "repack", () -> targetGeneration)
                .newPool(SegmentBufferWriterPool.PoolType.THREAD_SPECIFIC);
    }

    @NotNull
    private DefaultSegmentWriter newStructuralWriter() {
        return new DefaultSegmentWriter(store, reader, idProvider, blobStore,
                WriterCacheManager.Empty.INSTANCE, writeHandler, binariesInlineThreshold);
    }

    /**
     * Repack the whole record graph reachable from {@code rootId} into the target
     * generation and return the {@link RecordId} of the repacked root node.
     */
    @NotNull
    public RecordId repack(@NotNull RecordId rootId) throws IOException {
        this.dedupCache = newDedupCache();
        try {
            RecordId newRoot = concurrency <= 1 ? repackSerial(rootId) : repackParallel(rootId);
            writeHandler.flush(store);
            return newRoot;
        } finally {
            alias.close();
            if (dedupCache != null) {
                dedupCache.close();
            }
        }
    }

    /**
     * Build the content-dedup cache for a repack run: {@code null} when the mode disables dedup, the exact
     * off-heap {@link ShardedRecordIdMap} when the window resolves to {@code 0} (full-dedup), otherwise the
     * bounded implementation selected by {@link #withDedupCacheImpl} (default {@link DedupCacheImpl#MMAP_WINDOW}).
     */
    @Nullable
    private DedupCache newDedupCache() throws IOException {
        if (!dedupEnabled()) {
            return null;
        }
        int window = resolveDedupWindow();
        if (window <= 0) {
            return new ShardedRecordIdMap(idProvider, KEY_HASH_WIDTH, expectedRecords, shardCount, "dedup");
        }
        switch (dedupCacheImpl) {
            case HEAP_WINDOW:
                return new WindowDedupCache(window, shardCount);
            case MMAP_WINDOW:
            default:
                return new MmapWindowDedupCache(idProvider, KEY_HASH_WIDTH, window, shardCount);
        }
    }

    /**
     * Resolve the effective recency-window size: an explicit {@link #dedupWindow} ({@code >= 0}) as set,
     * or - by default ({@code -1}) - {@link #AUTO_WINDOW_FRACTION} of {@link #expectedRecords}. Falls back
     * to {@code 0} (the exact cache) when the record count is unknown or too small to size a window.
     */
    private int resolveDedupWindow() {
        if (dedupWindow >= 0) {
            return dedupWindow;
        }
        return expectedRecords > 0 ? expectedRecords / AUTO_WINDOW_FRACTION : 0;
    }

    /**
     * Single-threaded repack. Each {@link #stageRoots} subtree is repacked first, in order, so its
     * records are emitted as one contiguous run up front; then the whole graph, which alias-hits them.
     */
    @NotNull
    private RecordId repackSerial(@NotNull RecordId rootId) throws IOException {
        for (RecordId stageRoot : stageRoots) {
            copyNode(stageRoot);
        }
        return copyNode(rootId);
    }

    /**
     * Repack the graph reachable from {@code rootId} using {@link #concurrency} worker threads. When
     * {@link #stageRoots} is non-empty this runs in stages, each parallel and separated by a barrier:
     * each stage subtree is repacked to completion in order, then the whole graph (whose records under
     * the earlier stages are now alias hits), so each stage's (typically wide) child sets are emitted
     * as a contiguous run before the next stage races. Otherwise a single stage repacks the whole graph.
     */
    @NotNull
    private RecordId repackParallel(@NotNull RecordId rootId) throws IOException {
        for (RecordId stageRoot : stageRoots) {
            // Repack this stage subtree first, in parallel, to completion (a barrier).
            parallelCopy(stageRoot);
        }
        // Final stage: repack the whole graph in parallel; the earlier stages are now aliased.
        return parallelCopy(rootId);
    }

    /**
     * Repack the graph reachable from {@code subtreeRoot} in parallel. The graph is explored top-down
     * to a frontier of subtree roots, each repacked concurrently ({@link #copyNode} is thread-safe:
     * the alias and dedup tables are sharded, the writers thread-specific). The frontier is chosen
     * locality-aware (see {@link #findSplitPoints}): a wide node's whole child set stays within one
     * worker, so its children are repacked into a contiguous segment run and reads of that node stay
     * dense. A final single-threaded walk from {@code subtreeRoot} emits the spine above the frontier;
     * each frontier subtree is already in the alias table, so descending into it is a hit that returns
     * the concurrently repacked result.
     */
    @NotNull
    private RecordId parallelCopy(@NotNull RecordId subtreeRoot) throws IOException {
        int target = splitTarget > 0 ? splitTarget : concurrency * SPLIT_POINTS_PER_THREAD;
        List<RecordId> splitRoots = findSplitPoints(subtreeRoot, target);
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        try {
            List<Future<RecordId>> futures = new ArrayList<>(splitRoots.size());
            for (RecordId splitRoot : splitRoots) {
                futures.add(executor.submit(() -> copyNode(splitRoot)));
            }
            for (Future<RecordId> future : futures) {
                future.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while repacking in parallel", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw cause instanceof IOException ? (IOException) cause
                    : new IOException("parallel repack failed", cause);
        } finally {
            executor.shutdown();
        }
        // Spine: the frontier subtrees are now aliased, so this only walks the nodes above them.
        return copyNode(subtreeRoot);
    }

    /**
     * Explore the node graph below {@code root} breadth-first (through child maps) until at least
     * {@code target} subtree roots are found or the frontier can no longer be expanded, and return
     * that (distinct) frontier. Growth is capped at {@value #MAX_SPLIT_POINTS} so a very wide node
     * does not produce an unbounded number of tasks.
     * <p>
     * The split is locality-aware: a node with more than {@link #SPLIT_MAX_FANOUT} children is
     * <em>not</em> expanded but kept whole as a single split root (see {@link #SPLIT_MAX_FANOUT}), so
     * one worker repacks its entire child set contiguously and reads of that node stay dense. Once a
     * node is kept whole (or is a leaf) it is not re-examined on later passes.
     */
    @NotNull
    private List<RecordId> findSplitPoints(@NotNull RecordId root, int target) {
        List<RecordId> frontier = new ArrayList<>();
        frontier.add(root);
        // Split roots that must not be expanded further: leaves, kept-as-is records and nodes kept
        // whole because they are wider than SPLIT_MAX_FANOUT. Tracked so they are not re-materialised.
        Set<RecordId> closed = new HashSet<>();
        while (frontier.size() < target) {
            List<RecordId> next = new ArrayList<>();
            boolean progressed = false;
            for (RecordId nodeId : frontier) {
                if (closed.contains(nodeId)) {
                    next.add(nodeId);
                    continue;
                }
                List<RecordId> children = next.size() < MAX_SPLIT_POINTS ? childNodeIds(nodeId) : List.of();
                if (children.isEmpty() || children.size() > SPLIT_MAX_FANOUT) {
                    // Leaf/kept-as-is, or too wide to scatter: keep this node as a single split root.
                    next.add(nodeId);
                    closed.add(nodeId);
                } else {
                    next.addAll(children);
                    progressed = true;
                }
            }
            if (!progressed) {
                break;
            }
            frontier = next;
            if (frontier.size() >= MAX_SPLIT_POINTS) {
                break;
            }
        }
        return new ArrayList<>(new LinkedHashSet<>(frontier));
    }

    /** The record ids of {@code nodeId}'s child nodes, or empty if it is a leaf or kept as-is. */
    @NotNull
    private List<RecordId> childNodeIds(@NotNull RecordId nodeId) {
        if (keepAsIs(nodeId)) {
            return List.of();
        }
        List<RecordId> children = new ArrayList<>();
        for (ChildNodeEntry entry : reader.readNode(nodeId).getChildNodeEntries()) {
            NodeState child = entry.getNodeState();
            if (child instanceof SegmentNodeState) {
                children.add(((SegmentNodeState) child).getRecordId());
            }
        }
        return children;
    }

    /**
     * @return {@code true} if the record must be kept as-is (never repacked):
     * records in bulk segments (binaries) and records that are already in the
     * target generation.
     */
    private boolean keepAsIs(@NotNull RecordId id) {
        SegmentId segmentId = id.getSegmentId();
        if (!segmentId.isDataSegmentId()) {
            return true;
        }
        return targetGeneration.equals(segmentId.getGcGeneration());
    }

    private RecordId emit(@NotNull RecordWriters.RecordWriter recordWriter) throws IOException {
        return writeHandler.execute(targetGeneration, writer -> recordWriter.write(writer, store));
    }

    /**
     * Emit the record produced by {@code recordWriter}, unless a content-identical
     * record (same {@code key}) has already been emitted, in which case its
     * {@link RecordId} is reused.
     */
    private RecordId dedup(@NotNull byte[] key, @NotNull RecordWriters.RecordWriter recordWriter)
            throws IOException {
        if (!dedupEnabled()) {
            return emit(recordWriter);
        }
        if (exactDedup) {
            return dedupCache.computeIfAbsent(key, () -> emit(recordWriter));
        }
        RecordId existing = dedupCache.get(key);
        if (existing != null) {
            return existing;
        }
        RecordId newId = emit(recordWriter);
        return dedupPut(key, newId);
    }

    /**
     * Record {@code newId} as the emitted copy for content {@code key} and return it, unless a
     * competing worker already emitted a copy of the same content - in which case that (winning)
     * {@link RecordId} is returned and {@code newId}'s record is left as unreferenced garbage.
     * Single-threaded this always returns {@code newId}.
     */
    private RecordId dedupPut(@NotNull byte[] key, @NotNull RecordId newId) throws IOException {
        RecordId won = dedupCache.putIfAbsent(key, newId);
        return won != null ? won : newId;
    }

    /**
     * Emit a node record deduplicated by {@code nodeKey}: reuse an already-emitted content-equal
     * node if present, otherwise emit via {@code emitNode} and record it, then map
     * {@code sourceId -> result} in the alias table. Exact under concurrency (see {@link #exactDedup}):
     * the check and the emit are atomic, so two workers with the same node content never both emit.
     * The caller must already have handled any pre-recursion dedup short-circuit (a stable-id hit that
     * prunes the subtree walk); this only guards the emit itself.
     */
    private RecordId dedupNode(@NotNull RecordId sourceId, @NotNull byte[] nodeKey,
            @NotNull ShardedRecordIdMap.RecordIdSupplier emitNode) throws IOException {
        RecordId newId;
        if (exactDedup) {
            newId = dedupCache.computeIfAbsent(nodeKey, emitNode);
        } else {
            RecordId existing = dedupCache.get(nodeKey);
            newId = existing != null ? existing : dedupPut(nodeKey, emitNode.get());
        }
        alias.putIfAbsent(sourceId, newId);
        return newId;
    }

    /**
     * Write a list record for the given (already translated) entries, deduplicating
     * lists that have an identical entry sequence.
     */
    private RecordId writeListDeduped(@NotNull List<RecordId> entries) throws IOException {
        if (!dedupEnabled()) {
            return structuralWriter.writeList(entries);
        }
        byte[] key = hash(T_LIST).putIds(entries).digest();
        if (exactDedup) {
            return dedupCache.computeIfAbsent(key, () -> structuralWriter.writeList(entries));
        }
        RecordId existing = dedupCache.get(key);
        if (existing != null) {
            return existing;
        }
        RecordId newId = structuralWriter.writeList(entries);
        return dedupPut(key, newId);
    }

    // -- per record type -----------------------------------------------------

    @Nullable
    private RecordId aliasGet(@NotNull RecordId id) {
        return alias.get(id);
    }

    private void aliasPut(@NotNull RecordId id, @NotNull RecordId newId) throws IOException {
        alias.putIfAbsent(id, newId);
    }

    private RecordId copyNode(@NotNull RecordId id) throws IOException {
        if (keepAsIs(id)) {
            return id;
        }
        RecordId cached = aliasGet(id);
        if (cached != null) {
            return cached;
        }
        return copyNodeCore(id);
    }

    private RecordId copyNodeCore(@NotNull RecordId id) throws IOException {
        Segment segment = id.getSegment();
        int recordNumber = id.getRecordNumber();
        RecordId stableSlot = segment.readRecordId(recordNumber);
        switch (mode) {
            case STABLE_ID:
                return copyNodeByStableId(id, segment, recordNumber, stableSlot);
            case DEEP_DEDUP:
                return copyNodeDeep(id, segment, recordNumber);
            default: // NODE_DATA and NO_DEDUP share the self-reference-default stable id handling
                return copyNodeByData(id, segment, recordNumber, stableSlot);
        }
    }

    /** @return {@code true} unless the mode disables content deduplication. */
    private boolean dedupEnabled() {
        return mode != Mode.NO_DEDUP;
    }

    /**
     * Deduplicate by stable id. A node's stable id uniquely identifies its content, so
     * two node records that share a stable id are the same logical node and can share a
     * single repacked record, pruning the (identical) subtree from a second walk. This
     * mirrors the stable-id keyed node cache of the semantic compactors, and preserves
     * every stable id by materialising it as an explicit block.
     */
    private RecordId copyNodeByStableId(@NotNull RecordId id, @NotNull Segment segment,
            int recordNumber, @NotNull RecordId stableSlot) throws IOException {
        byte[] stableId = readStableId(id, stableSlot);
        byte[] nodeKey = hash(T_NODE).putBytes(stableId).digest();
        RecordId deduped = dedupCache.get(nodeKey);
        if (deduped != null) {
            alias.putIfAbsent(id, deduped);
            return deduped;
        }

        RecordId newStableId = copyStableId(stableId);
        List<RecordId> ids = translateNodeBody(segment, recordNumber);

        return dedupNode(id, nodeKey, () -> emit(RecordWriters.newNodeStateWriter(newStableId, ids)));
    }

    /**
     * Handle a node with the self-reference-default stable id policy shared by
     * {@link Mode#NODE_DATA} and {@link Mode#NO_DEDUP}: the stable id is left at its
     * self-reference default (written from the new record id) unless the source node already
     * carries an explicit one, which is preserved. When deduplication is enabled
     * ({@link Mode#NODE_DATA}) the node is keyed by its translated record data and its
     * stable-id treatment, so two nodes with the same translated references collapse to one;
     * when it is disabled ({@link Mode#NO_DEDUP}) the node is emitted faithfully.
     */
    private RecordId copyNodeByData(@NotNull RecordId id, @NotNull Segment segment,
            int recordNumber, @NotNull RecordId stableSlot) throws IOException {
        boolean explicit = !stableSlot.equals(id);
        RecordId newStableId = explicit ? copyStableId(readStableId(id, stableSlot)) : null;
        List<RecordId> ids = translateNodeBody(segment, recordNumber);

        if (dedupEnabled()) {
            HashKey hk = hash(T_NODE);
            if (explicit) {
                hk.putByte(1).putId(newStableId);
            } else {
                hk.putByte(0);
            }
            byte[] nodeKey = hk.putIds(ids).digest();
            return dedupNode(id, nodeKey, () -> emit(RecordWriters.newNodeStateWriter(newStableId, ids)));
        }

        RecordId newId = emit(RecordWriters.newNodeStateWriter(newStableId, ids));
        aliasPut(id, newId);
        return newId;
    }

    /**
     * Deduplicate by the node's translated record data alone, ignoring stable ids, and drop
     * the stable id to its self-reference default. Because children are deduplicated first
     * (post-order), equal translated references mean the whole subtree is content-equal, so
     * this collapses content-equal nodes even when they carried distinct explicit stable ids
     * (which {@link #copyNodeByData} keeps apart).
     */
    private RecordId copyNodeDeep(@NotNull RecordId id, @NotNull Segment segment, int recordNumber)
            throws IOException {
        List<RecordId> ids = translateNodeBody(segment, recordNumber);

        byte[] nodeKey = hash(T_NODE).putIds(ids).digest();
        return dedupNode(id, nodeKey, () -> emit(RecordWriters.newNodeStateWriter(null, ids)));
    }

    /**
     * Translate a node's body records (template, children and property list) to the
     * target generation and return their new ids in node-record order.
     */
    private List<RecordId> translateNodeBody(@NotNull Segment segment, int recordNumber)
            throws IOException {
        RecordId templateId = segment.readRecordId(recordNumber, 0, 1);
        Template template = reader.readTemplate(templateId);

        List<RecordId> ids = new ArrayList<>();
        ids.add(copyTemplate(templateId));

        int propertyListIndex = 2;
        String childName = template.getChildName();
        if (childName == Template.MANY_CHILD_NODES) {
            ids.add(copyChildMap(segment.readRecordId(recordNumber, 0, 2)));
            propertyListIndex = 3;
        } else if (childName != Template.ZERO_CHILD_NODES) {
            ids.add(copyNode(segment.readRecordId(recordNumber, 0, 2)));
            propertyListIndex = 3;
        }

        PropertyTemplate[] propertyTemplates = template.getPropertyTemplates();
        if (propertyTemplates.length > 0) {
            RecordId propertyListId = segment.readRecordId(recordNumber, 0, propertyListIndex);
            ids.add(copyPropertyList(propertyListId, propertyTemplates));
        }
        return ids;
    }

    /**
     * Read the 20-byte stable id of a node. The first record id of a node points to
     * its stable id; when that slot is equal to the node's own record id the stable
     * id is not stored explicitly but is the serialised form of that record id.
     */
    private byte[] readStableId(@NotNull RecordId nodeId, @NotNull RecordId stableSlot) {
        byte[] data = new byte[RecordId.SERIALIZED_RECORD_ID_BYTES];
        if (stableSlot.equals(nodeId)) {
            nodeId.getBytes().get(data);
        } else {
            stableSlot.getSegment().readBytes(stableSlot.getRecordNumber(), 0, data, 0, data.length);
        }
        return data;
    }

    /**
     * Materialise the stable id as an explicit block record so that it survives the
     * change of record id caused by repacking (otherwise it would silently change).
     * Identical stable-id blocks are deduplicated.
     */
    private RecordId copyStableId(@NotNull byte[] stableId) throws IOException {
        return dedup(hash(T_BLOCK).putBytes(stableId).digest(),
                RecordWriters.newBlockWriter(stableId, 0, stableId.length));
    }

    private RecordId copyTemplate(@NotNull RecordId id) throws IOException {
        RecordId cached = alias.get(id);
        if (cached != null) {
            return cached;
        }

        Segment segment = id.getSegment();
        int recordNumber = id.getRecordNumber();

        int head = segment.readInt(recordNumber, 0);
        boolean hasPrimaryType = (head & (1 << 31)) != 0;
        boolean hasMixinTypes = (head & (1 << 30)) != 0;
        boolean zeroChildNodes = (head & (1 << 29)) != 0;
        boolean manyChildNodes = (head & (1 << 28)) != 0;
        int mixinCount = (head >> 18) & ((1 << 10) - 1);
        int propertyCount = head & ((1 << 18) - 1);

        int offset = 4;

        List<RecordId> ids = new ArrayList<>();

        RecordId primaryId = null;
        if (hasPrimaryType) {
            primaryId = copyString(segment.readRecordId(recordNumber, offset));
            ids.add(primaryId);
            offset += Segment.RECORD_ID_BYTES;
        }

        List<RecordId> mixinIds = null;
        if (hasMixinTypes) {
            mixinIds = new ArrayList<>(mixinCount);
            for (int i = 0; i < mixinCount; i++) {
                RecordId mixinId = copyString(segment.readRecordId(recordNumber, offset));
                mixinIds.add(mixinId);
                offset += Segment.RECORD_ID_BYTES;
            }
            ids.addAll(mixinIds);
        }

        RecordId childNameId = null;
        if (!zeroChildNodes && !manyChildNodes) {
            childNameId = copyString(segment.readRecordId(recordNumber, offset));
            ids.add(childNameId);
            offset += Segment.RECORD_ID_BYTES;
        }

        RecordId propNamesId = null;
        byte[] propertyTypes = new byte[propertyCount];
        if (propertyCount > 0) {
            RecordId propertyNamesListId = segment.readRecordId(recordNumber, offset);
            offset += Segment.RECORD_ID_BYTES;
            propNamesId = copyStringList(propertyNamesListId, propertyCount);
            ids.add(propNamesId);
            for (int i = 0; i < propertyCount; i++) {
                propertyTypes[i] = segment.readByte(recordNumber, offset + i);
            }
        }

        RecordId newId = dedup(
                hash(T_TEMPLATE).putInt(head).putIds(ids).putBytes(propertyTypes).digest(),
                RecordWriters.newTemplateWriter(ids, new RecordId[propertyCount],
                        propertyTypes, head, primaryId, mixinIds, childNameId, propNamesId));
        alias.putIfAbsent(id, newId);
        return newId;
    }

    private RecordId copyChildMap(@NotNull RecordId id) throws IOException {
        RecordId cached = aliasGet(id);
        if (cached != null) {
            return cached;
        }
        MapRecord map = reader.readMap(id);
        Map<String, RecordId> changes = new LinkedHashMap<>();
        for (MapEntry entry : map.getEntries()) {
            changes.put(entry.getName(), copyNode(entry.getValue()));
        }

        RecordId newId;
        if (dedupEnabled() && deduplicateMaps) {
            // Key on the translated (canonical) entries, order-independent, so content-equal
            // maps collapse to one record - which in turn lets their referencing nodes dedup.
            byte[] key = hashMap(changes);
            if (exactDedup) {
                newId = dedupCache.computeIfAbsent(key, () -> structuralWriter.writeMap(null, changes));
            } else {
                RecordId existing = dedupCache.get(key);
                if (existing != null) {
                    alias.putIfAbsent(id, existing);
                    return existing;
                }
                newId = dedupPut(key, structuralWriter.writeMap(null, changes));
            }
        } else {
            newId = structuralWriter.writeMap(null, changes);
        }
        aliasPut(id, newId);
        return newId;
    }

    private RecordId copyPropertyList(@NotNull RecordId id, @NotNull PropertyTemplate[] templates)
            throws IOException {
        RecordId cached = aliasGet(id);
        if (cached != null) {
            return cached;
        }
        List<RecordId> entries = new ListRecord(id, templates.length).getEntries();
        List<RecordId> newEntries = new ArrayList<>(templates.length);
        for (int i = 0; i < templates.length; i++) {
            newEntries.add(copyProperty(entries.get(i), templates[i]));
        }
        RecordId newId = writeListDeduped(newEntries);
        aliasPut(id, newId);
        return newId;
    }

    private RecordId copyProperty(@NotNull RecordId id, @NotNull PropertyTemplate template)
            throws IOException {
        Type<?> type = template.getType();
        if (!type.isArray()) {
            return type == Type.BINARY ? copyBinary(id) : copyString(id);
        }

        RecordId cached = aliasGet(id);
        if (cached != null) {
            return cached;
        }

        Segment segment = id.getSegment();
        int recordNumber = id.getRecordNumber();
        int count = segment.readInt(recordNumber);

        RecordId newId;
        if (count == 0) {
            newId = dedup(hash(T_EMPTY_ARRAY).digest(), RecordWriters.newListWriter());
        } else {
            RecordId listId = segment.readRecordId(recordNumber, 4);
            List<RecordId> entries = new ListRecord(listId, count).getEntries();
            Type<?> baseType = type.getBaseType();
            List<RecordId> newEntries = new ArrayList<>(count);
            for (RecordId entry : entries) {
                newEntries.add(baseType == Type.BINARY ? copyBinary(entry) : copyString(entry));
            }
            RecordId newListId = writeListDeduped(newEntries);
            newId = dedup(hash(T_ARRAY).putInt(count).putId(newListId).digest(),
                    RecordWriters.newListWriter(count, newListId));
        }
        aliasPut(id, newId);
        return newId;
    }

    private RecordId copyBinary(@NotNull RecordId id) throws IOException {
        RecordId cached = alias.get(id);
        if (cached != null) {
            return cached;
        }
        // Relink through the segment writer: external blobs re-emit just the blob
        // id, inline/long binaries re-link the existing bulk block records without
        // copying the bulk data.
        RecordId newId = structuralWriter.writeBlob(reader.readBlob(id));
        alias.putIfAbsent(id, newId);
        return newId;
    }

    private RecordId copyString(@NotNull RecordId id) throws IOException {
        if (keepAsIs(id)) {
            return id;
        }
        RecordId cached = aliasGet(id);
        if (cached != null) {
            return cached;
        }

        Segment segment = id.getSegment();
        int recordNumber = id.getRecordNumber();
        long length = segment.readLength(recordNumber);

        RecordId newId;
        if (length < Segment.SMALL_LIMIT) {
            byte[] data = readBytes(segment, recordNumber, 1, (int) length);
            newId = dedup(hash(T_VALUE).putBytes(data).digest(),
                    RecordWriters.newValueWriter((int) length, data));
        } else if (length < Segment.MEDIUM_LIMIT) {
            byte[] data = readBytes(segment, recordNumber, 2, (int) length);
            newId = dedup(hash(T_VALUE).putBytes(data).digest(),
                    RecordWriters.newValueWriter((int) length, data));
        } else {
            int count = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            RecordId listId = segment.readRecordId(recordNumber, 8);
            List<RecordId> blocks = new ListRecord(listId, count).getEntries();
            List<RecordId> newBlocks = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                RecordId block = blocks.get(i);
                if (keepAsIs(block)) {
                    newBlocks.add(block);
                } else {
                    int blockLength = (int) Math.min(BLOCK_SIZE, length - (long) i * BLOCK_SIZE);
                    newBlocks.add(copyBlock(block, blockLength));
                }
            }
            RecordId newListId = writeListDeduped(newBlocks);
            long len = (length - Segment.MEDIUM_LIMIT) | (0x3L << 62);
            newId = dedup(hash(T_VALUE_LONG).putLong(len).putId(newListId).digest(),
                    RecordWriters.newValueWriter(newListId, len));
        }
        aliasPut(id, newId);
        return newId;
    }

    /** Content-dedup hits when a bounded (windowed) cache is used, else {@code -1}. */
    long getDedupWindowHits() {
        return dedupCache != null ? dedupCache.getHits() : -1;
    }

    /** Content-dedup misses (fresh emits) when a bounded (windowed) cache is used, else {@code -1}. */
    long getDedupWindowMisses() {
        return dedupCache != null ? dedupCache.getMisses() : -1;
    }

    private RecordId copyBlock(@NotNull RecordId id, int length) throws IOException {
        RecordId cached = aliasGet(id);
        if (cached != null) {
            return cached;
        }
        byte[] data = readBytes(id.getSegment(), id.getRecordNumber(), 0, length);
        RecordId newId = dedup(hash(T_BLOCK).putBytes(data).digest(),
                RecordWriters.newBlockWriter(data, 0, length));
        aliasPut(id, newId);
        return newId;
    }

    private RecordId copyStringList(@NotNull RecordId id, int count) throws IOException {
        RecordId cached = aliasGet(id);
        if (cached != null) {
            return cached;
        }
        List<RecordId> entries = new ListRecord(id, count).getEntries();
        List<RecordId> newEntries = new ArrayList<>(count);
        for (RecordId entry : entries) {
            newEntries.add(copyString(entry));
        }
        RecordId newId = writeListDeduped(newEntries);
        aliasPut(id, newId);
        return newId;
    }

    private static byte[] readBytes(@NotNull Segment segment, int recordNumber, int position, int length) {
        byte[] data = new byte[length];
        segment.readBytes(recordNumber, position, data, 0, length);
        return data;
    }

    /**
     * Order-independent content hash of a child map's translated {@code name -> id} entries.
     */
    @NotNull
    private byte[] hashMap(@NotNull Map<String, RecordId> changes) {
        HashKey hk = hash(T_MAP).putInt(changes.size());
        for (Map.Entry<String, RecordId> entry : new TreeMap<>(changes).entrySet()) {
            hk.putBytes(entry.getKey().getBytes(StandardCharsets.UTF_8)).putId(entry.getValue());
        }
        return hk.digest();
    }

    /** Start a new {@link #dedupCache} key, tagged by record type, over the reused {@link HashKey}. */
    @NotNull
    private HashKey hash(int tag) {
        return hashKey.get().reset(tag);
    }

    /**
     * A {@link WriteOperationHandler} that serialises all writes onto a single (not thread-safe)
     * {@link SegmentBufferWriter} by synchronising every operation. Used by {@link #singleWriter} so
     * that parallel workers co-pack their records into one segment stream instead of one per thread.
     */
    private static final class SynchronizedWriteHandler implements WriteOperationHandler {
        @NotNull
        private final SegmentBufferWriter writer;

        SynchronizedWriteHandler(@NotNull SegmentBufferWriter writer) {
            this.writer = requireNonNull(writer);
        }

        @NotNull
        @Override
        public synchronized RecordId execute(@NotNull GCGeneration gcGeneration,
                @NotNull WriteOperation writeOperation) throws IOException {
            return writer.execute(gcGeneration, writeOperation);
        }

        @Override
        public synchronized void flush(@NotNull SegmentStore store) throws IOException {
            writer.flush(store);
        }

        @NotNull
        @Override
        public GCGeneration getGCGeneration() {
            return writer.getGCGeneration();
        }
    }

    /**
     * Builder for a fixed-width ({@value #KEY_HASH_WIDTH}-byte) content hash used as a
     * {@link #dedupCache} key. Fields are fed in a canonical, length-prefixed order so that
     * structurally different records cannot collide, then hashed with the 128-bit MurmurHash3
     * ({@link MurmurHash3#hash128x64}). A non-cryptographic hash is deliberate: the input is
     * trusted internal segment data (not adversarial), and 128 bits keeps the collision
     * probability negligible (~10^-26 at 15M records) at a fraction of a cryptographic hash's cost.
     * The builder reuses its byte buffer (cleared on {@link #reset}), so at most one key may be in
     * flight at a time - which holds because the repacker is single-threaded and each key is fully
     * built and digested before the next {@link #reset}.
     */
    private static final class HashKey {
        private byte[] buffer = new byte[256];
        private int length;

        @NotNull
        HashKey reset(int tag) {
            length = 0;
            return putInt(tag);
        }

        private void ensure(int extra) {
            if (length + extra > buffer.length) {
                buffer = Arrays.copyOf(buffer, Math.max(buffer.length * 2, length + extra));
            }
        }

        @NotNull
        HashKey putByte(int b) {
            ensure(1);
            buffer[length++] = (byte) b;
            return this;
        }

        @NotNull
        HashKey putInt(int value) {
            ensure(4);
            buffer[length++] = (byte) (value >>> 24);
            buffer[length++] = (byte) (value >>> 16);
            buffer[length++] = (byte) (value >>> 8);
            buffer[length++] = (byte) value;
            return this;
        }

        @NotNull
        HashKey putLong(long value) {
            ensure(8);
            for (int i = 0; i < 8; i++) {
                buffer[length++] = (byte) (value >>> (56 - 8 * i));
            }
            return this;
        }

        @NotNull
        HashKey putBytes(@NotNull byte[] bytes) {
            putInt(bytes.length);
            ensure(bytes.length);
            System.arraycopy(bytes, 0, buffer, length, bytes.length);
            length += bytes.length;
            return this;
        }

        @NotNull
        HashKey putId(@NotNull RecordId id) {
            SegmentId segmentId = id.getSegmentId();
            putLong(segmentId.getMostSignificantBits());
            putLong(segmentId.getLeastSignificantBits());
            putInt(id.getRecordNumber());
            return this;
        }

        @NotNull
        HashKey putIds(@NotNull List<RecordId> ids) {
            putInt(ids.size());
            for (RecordId id : ids) {
                putId(id);
            }
            return this;
        }

        @NotNull
        byte[] digest() {
            long[] hash = MurmurHash3.hash128x64(buffer, 0, length, 0);
            byte[] out = new byte[KEY_HASH_WIDTH];
            writeLong(out, 0, hash[0]);
            writeLong(out, 8, hash[1]);
            return out;
        }

        private static void writeLong(byte[] out, int offset, long value) {
            for (int i = 0; i < 8; i++) {
                out[offset + i] = (byte) (value >>> (56 - 8 * i));
            }
        }
    }
}
