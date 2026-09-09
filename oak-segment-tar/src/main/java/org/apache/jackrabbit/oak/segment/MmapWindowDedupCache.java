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

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.LongAdder;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An <em>off-heap</em>, bounded, sliding-window {@link DedupCache}: it has the same inexact recency
 * semantics as {@link WindowDedupCache} - a record is deduplicated only against recently emitted
 * records still resident in the window, older ones are forgotten and re-emitted - but the window
 * lives in a fixed-size memory-mapped file instead of the Java heap. This keeps the heap at the
 * exact cache's floor (the window entries do not count against it) while bounding the on-disk
 * footprint to the window size, unlike the exact {@link ShardedRecordIdMap}, whose spill grows with
 * the whole store.
 * <p>
 * The table is a fixed-capacity, never-growing, {@value #WAYS}-way set-associative hash table. A key
 * hashes to a set of {@value #WAYS} consecutive slots; a lookup returns the value of the slot whose
 * full key matches (comparing the whole content hash, so a false deduplication is impossible) and
 * marks it "recently used". An insert fills the first free slot in the set or, when the set is full,
 * evicts a not-recently-used slot chosen by a CLOCK second-chance scan (a spare bit in the occupancy
 * byte tracks reuse), approximating in-heap LRU without any per-entry recency list. Eviction never
 * clears a slot (occupied stays occupied), so the effective window is "a reused key survives, a
 * one-off ('cold') record falls out and is re-emitted" - exactly the recency semantics of
 * {@link WindowDedupCache}, at ~capacity most-recent entries.
 * <p>
 * Sharded and thread-safe: the slot array is partitioned into {@code shardCount} regions, each
 * guarded by its own monitor, and {@link #computeIfAbsent} emits while holding it, so concurrent
 * workers with the same content converge on one copy (for content still in the window). A single
 * mapping backs all shards; the backing descriptor is released right after mapping and the file is
 * deleted on {@link #close()}.
 */
final class MmapWindowDedupCache implements DedupCache {

    /** Slots probed per key (set associativity). Small, so every lookup/insert touches only a few slots. */
    private static final int WAYS = 8;

    /** Occupancy-byte flag: the slot holds an entry. */
    private static final byte OCCUPIED = 1;

    /** Occupancy-byte flag: the entry was reused since it was last considered for eviction (CLOCK). */
    private static final byte REFERENCED = 2;

    /** Serialised value width: msb (8) + lsb (8) + recordNumber (4). */
    private static final int VALUE_WIDTH = 20;

    @NotNull
    private final SegmentIdProvider idProvider;

    private final int keyWidth;

    private final int slotWidth;

    /** Slots per shard, a power of two (so the intra-shard home index is a mask). */
    private final int perShard;

    private final int shardCount;

    private final Object[] locks;

    private final LongAdder hits = new LongAdder();

    private final LongAdder misses = new LongAdder();

    private Path file;

    private MappedByteBuffer buffer;

    /**
     * @param window desired number of most-recent entries to retain (the window size); split evenly
     *     across shards and rounded up per shard to a power of two.
     * @param shardCount number of independently-locked shards (match the repacker's concurrency).
     */
    MmapWindowDedupCache(@NotNull SegmentIdProvider idProvider, int keyWidth, int window, int shardCount)
            throws IOException {
        this.idProvider = requireNonNull(idProvider);
        this.keyWidth = keyWidth;
        this.slotWidth = 1 + keyWidth + VALUE_WIDTH;
        this.shardCount = Math.max(1, shardCount);
        this.perShard = perShardSlots(window, this.shardCount, this.slotWidth);
        this.locks = new Object[this.shardCount];
        for (int i = 0; i < this.shardCount; i++) {
            locks[i] = new Object();
        }
        long bytes = (long) perShard * this.shardCount * slotWidth;
        this.file = Files.createTempFile("oak-repack-dedup-window-", ".map");
        this.file.toFile().deleteOnExit();
        this.buffer = mapFile(file, bytes);
    }

    @Override
    @Nullable
    public RecordId get(@NotNull byte[] key) {
        int shard = shardFor(key);
        synchronized (locks[shard]) {
            return getInShard(shard, key);
        }
    }

    @Override
    @Nullable
    public RecordId putIfAbsent(@NotNull byte[] key, @NotNull RecordId value) {
        int shard = shardFor(key);
        synchronized (locks[shard]) {
            RecordId existing = getInShard(shard, key);
            if (existing != null) {
                return existing;
            }
            putInShard(shard, key, value);
            return null;
        }
    }

    @Override
    @NotNull
    public RecordId computeIfAbsent(@NotNull byte[] key,
            @NotNull ShardedRecordIdMap.RecordIdSupplier writer) throws IOException {
        int shard = shardFor(key);
        synchronized (locks[shard]) {
            RecordId existing = getInShard(shard, key);
            if (existing != null) {
                hits.increment();
                return existing;
            }
            RecordId created = writer.get();
            misses.increment();
            putInShard(shard, key, created);
            return created;
        }
    }

    @Override
    public void close() {
        unmap(buffer);
        buffer = null;
        if (file != null) {
            file.toFile().delete();
            file = null;
        }
    }

    /** Deduplication hits (content found in the window). */
    @Override
    public long getHits() {
        return hits.sum();
    }

    /** Deduplication misses (content not in the window - emitted fresh). */
    @Override
    public long getMisses() {
        return misses.sum();
    }

    // -- internals -----------------------------------------------------------

    private int shardFor(@NotNull byte[] key) {
        // The key is a content hash; route by its leading eight bytes, disjoint from the slot bits.
        return (int) Math.floorMod(WindowDedupCache.readLong(key, 0), shardCount);
    }

    @Nullable
    private RecordId getInShard(int shard, @NotNull byte[] key) {
        int home = homeIndex(key);
        for (int w = 0; w < WAYS; w++) {
            int pos = slotPos(shard, (home + w) & (perShard - 1));
            byte occ = buffer.get(pos);
            if (occ != 0 && keyEquals(pos, key)) {
                if ((occ & REFERENCED) == 0) {
                    buffer.put(pos, (byte) (occ | REFERENCED)); // reused: give it a second chance
                }
                return readValue(pos);
            }
        }
        return null;
    }

    private void putInShard(int shard, @NotNull byte[] key, @NotNull RecordId value) {
        int home = homeIndex(key);
        int emptyPos = -1;
        for (int w = 0; w < WAYS; w++) {
            int pos = slotPos(shard, (home + w) & (perShard - 1));
            byte occ = buffer.get(pos);
            if (occ == 0) {
                if (emptyPos < 0) {
                    emptyPos = pos;
                }
            } else if (keyEquals(pos, key)) {
                buffer.put(pos, (byte) (occ | REFERENCED)); // present already: mark reused, refresh id
                writeValue(pos, value);
                return;
            }
        }
        // A new key: take the first free slot in the set, or, if the set is full, evict a
        // not-recently-used slot (CLOCK). Eviction never clears occupancy, so no probe hole is made.
        writeSlot(emptyPos >= 0 ? emptyPos : clockVictim(shard, home), key, value);
    }

    /**
     * CLOCK second-chance victim for a full set: return the first slot whose {@link #REFERENCED} bit
     * is clear (a cold, not-recently-reused entry), clearing (aging) the bit of each recently-used
     * slot it skips. If every slot was recently used they are all aged and the home slot is evicted.
     */
    private int clockVictim(int shard, int home) {
        for (int w = 0; w < WAYS; w++) {
            int pos = slotPos(shard, (home + w) & (perShard - 1));
            byte occ = buffer.get(pos);
            if ((occ & REFERENCED) == 0) {
                return pos;
            }
            buffer.put(pos, (byte) (occ & ~REFERENCED)); // second chance: age this slot
        }
        return slotPos(shard, home);
    }

    private int homeIndex(@NotNull byte[] key) {
        // Use the trailing eight bytes for the intra-shard index, decorrelated from the shard routing.
        return (int) (WindowDedupCache.readLong(key, 8) & (perShard - 1));
    }

    private int slotPos(int shard, int localIndex) {
        return (shard * perShard + localIndex) * slotWidth;
    }

    private boolean keyEquals(int pos, @NotNull byte[] key) {
        int base = pos + 1;
        for (int i = 0; i < keyWidth; i++) {
            if (buffer.get(base + i) != key[i]) {
                return false;
            }
        }
        return true;
    }

    private void writeSlot(int pos, @NotNull byte[] key, @NotNull RecordId value) {
        buffer.put(pos, OCCUPIED); // fresh entry starts un-referenced: a cold one-off is evicted first
        int base = pos + 1;
        for (int i = 0; i < keyWidth; i++) {
            buffer.put(base + i, key[i]);
        }
        writeValue(pos, value);
    }

    private void writeValue(int pos, @NotNull RecordId value) {
        SegmentId segmentId = value.getSegmentId();
        int vbase = pos + 1 + keyWidth;
        buffer.putLong(vbase, segmentId.getMostSignificantBits());
        buffer.putLong(vbase + 8, segmentId.getLeastSignificantBits());
        buffer.putInt(vbase + 16, value.getRecordNumber());
    }

    private RecordId readValue(int pos) {
        int vbase = pos + 1 + keyWidth;
        long msb = buffer.getLong(vbase);
        long lsb = buffer.getLong(vbase + 8);
        int recordNumber = buffer.getInt(vbase + 16);
        return new RecordId(idProvider.newSegmentId(msb, lsb), recordNumber);
    }

    /**
     * Slots per shard: a power of two of at least {@value #WAYS}, sized to hold ~{@code window} entries
     * across all shards, and capped so the whole table fits a single {@code int}-addressed mapping.
     */
    private static int perShardSlots(int window, int shardCount, int slotWidth) {
        int want = Math.max(WAYS, (Math.max(1, window) + shardCount - 1) / shardCount);
        int pow2 = Integer.highestOneBit(want - 1) << 1;
        long maxTotalSlots = Integer.MAX_VALUE / slotWidth;
        int maxPerShard = Integer.highestOneBit((int) Math.max(WAYS, maxTotalSlots / shardCount));
        return Math.min(pow2, maxPerShard);
    }

    private static MappedByteBuffer mapFile(Path file, long bytes) throws IOException {
        // The mapping outlives the channel/descriptor, so the backing file is only opened long enough
        // to size and map it. This keeps the descriptor count low regardless of shard count.
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(bytes);
            return raf.getChannel().map(READ_WRITE, 0, bytes);
        }
    }

    /** Best-effort unmap so the (deleted) file's pages are released promptly rather than at GC. */
    private static void unmap(MappedByteBuffer buffer) {
        if (buffer == null) {
            return;
        }
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            Object unsafe = theUnsafe.get(null);
            Method invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
            invokeCleaner.invoke(unsafe, buffer);
        } catch (Throwable t) {
            // The mapping will be released when the buffer is garbage collected.
        }
    }
}
