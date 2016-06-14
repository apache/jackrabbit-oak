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
package org.apache.jackrabbit.oak.segment;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMapWithExpectedSize;
import static java.util.Collections.nCopies;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hash table of weak references to segment identifiers.
 */
public class SegmentIdTable {

    /**
     * The list of weak references to segment identifiers that are currently
     * being accessed. This represents a hash table that uses open addressing
     * with linear probing. It is not a hash map, to speed up read access.
     * <p>
     * The size of the table is always a power of two, so that we can use
     * bitwise "and" instead of modulo.
     * <p>
     * The table is indexed by the random identifier bits, which guarantees
     * uniform distribution of entries.
     * <p>
     * Open addressing with linear probing is used. Each table entry is either
     * null (when there are no matching identifiers), a weak references to the
     * matching identifier, or a weak reference to another identifier.
     * There are no tombstone entries as there is no explicit remove operation,
     * but a referent can become null if the entry is garbage collected.
     * <p>
     * The array is not sorted (we could; lookup might be faster, but adding
     * entries would be slower).
     */
    private final ArrayList<WeakReference<SegmentId>> references =
            newArrayList(nCopies(1024, (WeakReference<SegmentId>) null));

    private static final Logger LOG = LoggerFactory.getLogger(SegmentIdTable.class);

    
    /**
     * The refresh count (for diagnostics and testing).
     */
    private int rebuildCount;
    
    /**
     * The number of used entries (WeakReferences) in this table.
     */
    private int entryCount;

    /**
     * Get the segment id, and reference it in the weak references map. If the
     * pair of MSB/LSB is not tracked by this table, a new instance of {@link
     * SegmentId} is created using the provided {@link SegmentIdFactory} and
     * tracked by this table.
     *
     * @param msb   The most significant bits of the {@link SegmentId}.
     * @param lsb   The least significant bits of the {@link SegmentId}.
     * @param maker A non-{@code null} instance of {@link SegmentIdFactory}.
     * @return the segment id
     */
    @Nonnull
    synchronized SegmentId newSegmentId(long msb, long lsb, SegmentIdFactory maker) {
        int index = getIndex(lsb);
        boolean shouldRefresh = false;

        WeakReference<SegmentId> reference = references.get(index);
        while (reference != null) {
            SegmentId id = reference.get();
            if (id != null
                    && id.getMostSignificantBits() == msb
                    && id.getLeastSignificantBits() == lsb) {
                return id;
            }
            // shouldRefresh if we have a garbage collected entry
            shouldRefresh = shouldRefresh || id == null;
            // open addressing / linear probing
            index = (index + 1) % references.size();
            reference = references.get(index);
        }

        SegmentId id = maker.newSegmentId(msb, lsb);
        references.set(index, new WeakReference<SegmentId>(id));
        entryCount++;
        if (entryCount > references.size() * 0.75) {
            // more than 75% full            
            shouldRefresh = true;
        }
        if (shouldRefresh) {
            refresh();
        }
        return id;
    }

    /**
     * Returns all segment identifiers that are currently referenced in memory.
     *
     * @param ids referenced segment identifiers
     */
    void collectReferencedIds(Collection<SegmentId> ids) {
        ids.addAll(refresh());
    }

    private synchronized Collection<SegmentId> refresh() {
        int size = references.size();
        Map<SegmentId, WeakReference<SegmentId>> ids =
                newHashMapWithExpectedSize(size);

        boolean hashCollisions = false;
        boolean emptyReferences = false;
        for (int i = 0; i < size; i++) {
            WeakReference<SegmentId> reference = references.get(i);
            if (reference != null) {
                SegmentId id = reference.get();
                if (id != null) {
                    ids.put(id, reference);
                    hashCollisions = hashCollisions || (i != getIndex(id));
                } else {
                    references.set(i, null);
                    entryCount--;
                    emptyReferences = true;
                }
            }
        }
        
        if (entryCount != ids.size()) {
            // something is wrong, possibly a concurrency problem, a SegmentId
            // hashcode or equals bug, or a problem with this hash table
            // algorithm
            LOG.warn("Unexpected entry count mismatch, expected {} got {}", entryCount, ids.size());
            // we fix the count, because having a wrong entry count would be
            // very problematic; even worse than having a concurrency problem
            entryCount = ids.size();
        }

        while (2 * ids.size() > size) {
            size *= 2;
        }

        // we need to re-build the table if the new size is different,
        // but also if we removed some of the entries (because an entry was
        // garbage collected) and there is at least one entry at the "wrong"
        // location (due to open addressing)
        if ((hashCollisions && emptyReferences) || size != references.size()) {
            rebuildCount++;
            references.clear();
            references.addAll(nCopies(size, (WeakReference<SegmentId>) null));

            for (Map.Entry<SegmentId, WeakReference<SegmentId>> entry
                    : ids.entrySet()) {
                int index = getIndex(entry.getKey());
                while (references.get(index) != null) {
                    index = (index + 1) % size;
                }
                references.set(index, entry.getValue());
            }
        }

        return ids.keySet();
    }

    private int getIndex(SegmentId id) {
        return getIndex(id.getLeastSignificantBits());
    }

    private int getIndex(long lsb) {
        return ((int) lsb) & (references.size() - 1);
    }

    synchronized void clearSegmentIdTables(@Nonnull Set<UUID> reclaimed, @Nonnull String gcInfo) {
        for (WeakReference<SegmentId> reference : references) {
            if (reference != null) {
                SegmentId id = reference.get();
                if (id != null && reclaimed.contains(id.asUUID())) {
                    id.reclaimed(gcInfo);
                }
            }
        }
    }
    
    /**
     * Get the number of map rebuild operations (used for testing and diagnostics).
     * 
     * @return the rebuild count
     */
    int getMapRebuildCount() {
        return rebuildCount;
    }
    
    /**
     * Get the entry count (used for testing and diagnostics).
     * 
     * @return the entry count
     */
    int getEntryCount() {
        return entryCount;
    }
    
    /**
     * Get the size of the internal map (used for testing and diagnostics).
     * 
     * @return the map size
     */
    int getMapSize() {
        return references.size();
    }
    
    /**
     * Get the raw list of segment ids (used for testing).
     * 
     * @return the raw list
     */
    List<SegmentId> getRawSegmentIdList() {
        ArrayList<SegmentId> list = new ArrayList<SegmentId>();
        for (WeakReference<SegmentId> ref : references) {
            if (ref != null) {
                SegmentId id = ref.get();
                if (id != null) {
                    list.add(id);
                }
            }
        }
        return list;
    }

}
