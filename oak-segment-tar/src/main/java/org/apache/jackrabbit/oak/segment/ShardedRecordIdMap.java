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

import java.io.IOException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A thread-safe {@link RecordId} map that partitions its entries across a fixed (power-of-two)
 * number of independently locked {@link SpillableRecordIdMap} shards. A key is routed to a shard by
 * a hash of the key, so concurrent operations on different shards proceed without contention; only
 * operations hitting the same shard serialise. Used by {@link RecordRepacker} for the alias and
 * content-deduplication tables when repacking with more than one thread.
 * <p>
 * With a single shard it degenerates to one {@link SpillableRecordIdMap} behind an (uncontended)
 * lock, matching single-threaded behaviour. Each shard keeps its entries off the Java heap in a
 * memory-mapped file (see {@link SpillableRecordIdMap}); the shards' backing descriptors are
 * released right after mapping, so a large shard count does not exhaust file descriptors.
 */
final class ShardedRecordIdMap implements DedupCache {

    private final SpillableRecordIdMap[] shards;

    private final Object[] locks;

    private final int mask;

    /**
     * @param expectedRecords estimate of the total number of entries, split evenly across shards to
     *     pre-size each shard; {@code 0} (or negative) uses a small default.
     * @param shardCount desired number of shards; rounded down to a power of two (at least one).
     */
    ShardedRecordIdMap(@NotNull SegmentIdProvider idProvider, int keyWidth, int expectedRecords,
            int shardCount, @NotNull String name) throws IOException {
        int count = Integer.highestOneBit(Math.max(1, shardCount));
        this.shards = new SpillableRecordIdMap[count];
        this.locks = new Object[count];
        this.mask = count - 1;
        int perShard = perShardCapacity(expectedRecords, count);
        for (int i = 0; i < count; i++) {
            shards[i] = new SpillableRecordIdMap(idProvider, keyWidth, perShard, name + "-" + i);
            locks[i] = new Object();
        }
    }

    @Nullable
    RecordId get(@NotNull RecordId key) {
        int i = shardOf(key);
        synchronized (locks[i]) {
            return shards[i].get(key);
        }
    }

    @Override
    @Nullable
    public RecordId get(@NotNull byte[] key) {
        int i = shardOf(key);
        synchronized (locks[i]) {
            return shards[i].get(key);
        }
    }

    /**
     * Insert {@code (key, value)} if the key is absent, and return the value already mapped (a
     * competing insert won) or {@code null} if this call inserted. The get and the conditional put
     * happen atomically under the shard lock, so concurrent callers converge on a single value.
     */
    @Nullable
    RecordId putIfAbsent(@NotNull RecordId key, @NotNull RecordId value) throws IOException {
        int i = shardOf(key);
        synchronized (locks[i]) {
            RecordId existing = shards[i].get(key);
            if (existing != null) {
                return existing;
            }
            shards[i].put(key, value);
            return null;
        }
    }

    @Override
    @Nullable
    public RecordId putIfAbsent(@NotNull byte[] key, @NotNull RecordId value) throws IOException {
        int i = shardOf(key);
        synchronized (locks[i]) {
            RecordId existing = shards[i].get(key);
            if (existing != null) {
                return existing;
            }
            shards[i].put(key, value);
            return null;
        }
    }

    /** A supplier of a {@link RecordId} that may fail with an {@link IOException}; see {@link #computeIfAbsent}. */
    @FunctionalInterface
    interface RecordIdSupplier {
        @NotNull
        RecordId get() throws IOException;
    }

    /**
     * Atomically return the value mapped to {@code key}, or, if absent, invoke {@code writer} to
     * produce a value, store it and return it - all while holding the shard lock. A concurrent
     * caller with the same key therefore blocks until this call completes and then observes the
     * stored value; it never invokes its own {@code writer}. Used to make record emission exactly
     * deduplicated under concurrency: the "is this content already emitted?" check and the emit
     * happen as one critical section, so racing workers never both emit (and so never leave a loser
     * copy as unreferenced garbage). The trade-off is that {@code writer} (the record emit) runs
     * under the shard lock, serialising emits whose keys land in the same shard.
     */
    @Override
    @NotNull
    public RecordId computeIfAbsent(@NotNull byte[] key, @NotNull RecordIdSupplier writer)
            throws IOException {
        int i = shardOf(key);
        synchronized (locks[i]) {
            RecordId existing = shards[i].get(key);
            if (existing != null) {
                return existing;
            }
            RecordId created = writer.get();
            shards[i].put(key, created);
            return created;
        }
    }

    @Override
    public void close() {
        for (SpillableRecordIdMap shard : shards) {
            shard.close();
        }
    }

    private int shardOf(@NotNull RecordId key) {
        long lsb = key.getSegmentId().getLeastSignificantBits();
        return (int) (lsb ^ (lsb >>> 32) ^ key.getRecordNumber()) & mask;
    }

    private int shardOf(@NotNull byte[] key) {
        // The keys are already uniformly distributed (serialised random segment ids, or a content
        // hash), so folding the leading four bytes into an int gives a well-spread shard index.
        int h = ((key[0] & 0xff) << 24) | ((key[1] & 0xff) << 16)
                | ((key[2] & 0xff) << 8) | (key[3] & 0xff);
        return (h ^ (h >>> 16)) & mask;
    }

    /** Initial slot count for each shard: ~twice the per-shard expected entries, or a small default. */
    private static int perShardCapacity(int expectedRecords, int shardCount) {
        if (expectedRecords <= 0) {
            return 1 << 12;
        }
        long perShard = ((long) expectedRecords / shardCount) * 2;
        return (int) Math.min(Integer.MAX_VALUE / 2, Math.max(16, perShard));
    }
}
