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
package org.apache.jackrabbit.oak.segment.remote;

import java.util.ArrayList;
import java.util.List;

/**
 * Read-only, open-addressing map from a segment identifier (a pair of longs, {@code msb}/{@code lsb})
 * to its {@link RemoteSegmentArchiveEntry}, built once per archive and queried without boxing the
 * identifier into a {@link java.util.UUID} on every lookup.
 * <p>
 * {@link AbstractRemoteSegmentArchiveReader#containsSegment(long, long)} and
 * {@link AbstractRemoteSegmentArchiveReader#readSegment(long, long)} are invoked once per segment reference,
 * so they are on the hot path of many operations, in particular of online compaction. A naive implementation would
 * use {@code UUID} instances as keys in a map, which would require allocating a new UUID for every lookup in
 * every archive, paying for {@code UUID.hashCode()}/{@code equals()} through a generic {@code HashMap}. This class
 * stores the same entries in a flat array and computes the hash directly off the two longs.
 */
final class SegmentIndex {

    private static final SegmentIndex EMPTY = new SegmentIndex(new RemoteSegmentArchiveEntry[1], 0);

    private final RemoteSegmentArchiveEntry[] table;
    private final int mask;
    private final int size;

    private SegmentIndex(RemoteSegmentArchiveEntry[] table, int size) {
        this.table = table;
        this.mask = table.length - 1;
        this.size = size;
    }

    RemoteSegmentArchiveEntry get(long msb, long lsb) {
        for (int i = indexFor(msb, lsb, mask); ; i = (i + 1) & mask) {
            RemoteSegmentArchiveEntry e = table[i];
            if (e == null) {
                return null;
            }
            if (e.getMsb() == msb && e.getLsb() == lsb) {
                return e;
            }
        }
    }

    boolean containsKey(long msb, long lsb) {
        return get(msb, lsb) != null;
    }

    int size() {
        return size;
    }

    List<RemoteSegmentArchiveEntry> values() {
        List<RemoteSegmentArchiveEntry> out = new ArrayList<>(size);
        for (RemoteSegmentArchiveEntry e : table) {
            if (e != null) {
                out.add(e);
            }
        }
        return out;
    }

    private static int indexFor(long msb, long lsb, int mask) {
        long h = msb ^ Long.rotateLeft(lsb, 32);
        h ^= (h >>> 32);
        return (int) h & mask;
    }

    /**
     * Builds a {@link SegmentIndex}. Insertion is not on a hot path (it happens once per archive
     * open), so it favours simplicity over raw speed.
     */
    static final class Builder {

        private RemoteSegmentArchiveEntry[] table;
        private int size = 0;

        Builder(int expectedEntries) {
            table = new RemoteSegmentArchiveEntry[capacityFor(expectedEntries)];
        }

        /**
         * Adds {@code entry}. If an entry with the same {@code (msb, lsb)} is already present, the
         * one with the greatest {@link RemoteSegmentArchiveEntry#getPosition()} is kept, matching the
         * "latest copy wins" semantics of the archive index.
         */
        void put(RemoteSegmentArchiveEntry entry) {
            if (size >= table.length / 2) {
                grow();
            }
            insert(table, entry);
        }

        SegmentIndex build() {
            return size == 0 ? EMPTY : new SegmentIndex(table, size);
        }

        private void insert(RemoteSegmentArchiveEntry[] into, RemoteSegmentArchiveEntry entry) {
            int mask = into.length - 1;
            long msb = entry.getMsb();
            long lsb = entry.getLsb();
            for (int i = indexFor(msb, lsb, mask); ; i = (i + 1) & mask) {
                RemoteSegmentArchiveEntry existing = into[i];
                if (existing == null) {
                    into[i] = entry;
                    size++;
                    return;
                }
                if (existing.getMsb() == msb && existing.getLsb() == lsb) {
                    if (entry.getPosition() > existing.getPosition()) {
                        into[i] = entry;
                    }
                    return;
                }
            }
        }

        private void grow() {
            RemoteSegmentArchiveEntry[] old = table;
            table = new RemoteSegmentArchiveEntry[old.length * 2];
            size = 0;
            for (RemoteSegmentArchiveEntry e : old) {
                if (e != null) {
                    insert(table, e);
                }
            }
        }

        private static int capacityFor(int expectedEntries) {
            int minCapacity = Math.max(4, expectedEntries * 2);
            return Integer.highestOneBit(minCapacity - 1) << 1;
        }
    }
}
