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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A bounded, in-heap, sharded LRU {@link DedupCache}: a record is deduplicated only against the most
 * recently emitted records still resident in the window, and older ones are forgotten. Unlike the exact
 * {@link ShardedRecordIdMap} it needs no off-heap spill and its footprint is capped at {@code maxEntries}
 * entries, trading a little size - duplicates whose recurrences are farther apart than the window are
 * re-emitted - for bounded heap and better read locality: a reused copy is always recent, so it sits near
 * its referrer rather than in a remote canonical segment.
 * <p>
 * This is the "sliding window" hot-record detector: content that recurs while still in the window is
 * "hot" (deduplicated); a one-off "cold" record falls out and is emitted fresh (the repacker's alias
 * still preserves the input's physical record sharing). It self-identifies hot vs cold for every record
 * type with no pre-pass, ranking or explicit hot set. Keys are the repacker's fixed-width (128-bit)
 * content hashes, compared in full, so there is no risk of a false deduplication.
 * <p>
 * Thread-safe: each shard is guarded by its own monitor and {@link #computeIfAbsent} emits while holding
 * it, so concurrent workers with the same content converge on one copy (for content still in the window).
 */
final class WindowDedupCache implements DedupCache {

    private final Shard[] shards;
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();

    /**
     * @param maxEntries total number of most-recent entries to retain across all shards (the window
     *     size); split evenly per shard.
     * @param shardCount number of independently-locked shards (match the repacker's concurrency).
     */
    WindowDedupCache(int maxEntries, int shardCount) {
        int n = Math.max(1, shardCount);
        int perShard = Math.max(1, maxEntries / n);
        shards = new Shard[n];
        for (int i = 0; i < n; i++) {
            shards[i] = new Shard(perShard);
        }
    }

    private Shard shardFor(@NotNull byte[] key) {
        // The key is a content hash; its bits are already well distributed.
        return shards[(int) Math.floorMod(readLong(key, 0), shards.length)];
    }

    @Override
    @Nullable
    public RecordId get(@NotNull byte[] key) {
        Shard s = shardFor(key);
        Key k = new Key(key);
        synchronized (s) {
            return s.map.get(k);
        }
    }

    @Override
    @Nullable
    public RecordId putIfAbsent(@NotNull byte[] key, @NotNull RecordId value) {
        Shard s = shardFor(key);
        Key k = new Key(key);
        synchronized (s) {
            RecordId existing = s.map.get(k);
            if (existing != null) {
                return existing;
            }
            s.map.put(k, value);
            return null;
        }
    }

    @Override
    @NotNull
    public RecordId computeIfAbsent(@NotNull byte[] key,
            @NotNull ShardedRecordIdMap.RecordIdSupplier writer) throws IOException {
        Shard s = shardFor(key);
        Key k = new Key(key);
        synchronized (s) {
            RecordId existing = s.map.get(k);
            if (existing != null) {
                hits.increment();
                return existing;
            }
            RecordId created = writer.get();
            s.map.put(k, created);
            misses.increment();
            return created;
        }
    }

    @Override
    public void close() {
        for (Shard s : shards) {
            synchronized (s) {
                s.map.clear();
            }
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

    /** Access-ordered LRU map bounded to {@code capacity} entries, guarded by its own monitor. */
    private static final class Shard {
        final LinkedHashMap<Key, RecordId> map;

        Shard(int capacity) {
            this.map = new LinkedHashMap<Key, RecordId>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Key, RecordId> eldest) {
                    return size() > capacity;
                }
            };
        }
    }

    /** Immutable 128-bit content-hash key (a copy, since callers reuse their hash buffer). */
    private static final class Key {
        private final long hi;
        private final long lo;

        Key(@NotNull byte[] b) {
            this.hi = readLong(b, 0);
            this.lo = b.length >= 16 ? readLong(b, 8) : 0L;
        }

        @Override
        public int hashCode() {
            long h = hi ^ lo;
            return (int) (h ^ (h >>> 32));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Key)) {
                return false;
            }
            Key k = (Key) o;
            return hi == k.hi && lo == k.lo;
        }
    }

    static long readLong(@NotNull byte[] b, int off) {
        return ((long) (b[off] & 0xff) << 56)
                | ((long) (b[off + 1] & 0xff) << 48)
                | ((long) (b[off + 2] & 0xff) << 40)
                | ((long) (b[off + 3] & 0xff) << 32)
                | ((long) (b[off + 4] & 0xff) << 24)
                | ((long) (b[off + 5] & 0xff) << 16)
                | ((long) (b[off + 6] & 0xff) << 8)
                | ((long) (b[off + 7] & 0xff));
    }
}
