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
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMapWithExpectedSize;
import static java.util.Collections.nCopies;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Hash table of weak references to segment identifiers.
 */
public class SegmentIdTable {

    /**
     * Hash table of weak references to segment identifiers that are currently
     * being accessed. The size of the table is always a power of two, which
     * optimizes the {@link #expand()} operation. The table is indexed by the
     * random identifier bits, which guarantees uniform distribution of entries.
     * Each table entry is either {@code null} (when there are no matching
     * identifiers) or a list of weak references to the matching identifiers.
     * <p>
     * Actually, this is a array. It's not a hash map, to conserve memory (maps
     * need much more memory).
     * <p>
     * The list is not sorted (we could; lookup would be faster, but adding and
     * removing entries would be slower).
     */
    private final ArrayList<WeakReference<SegmentId>> references =
            newArrayList(nCopies(1024, (WeakReference<SegmentId>) null));

    private final SegmentTracker tracker;

    SegmentIdTable(SegmentTracker tracker) {
        this.tracker = tracker;
    }

    /**
     * Get the segment id, and reference it in the weak references map.
     * 
     * @param msb
     * @param lsb
     * @return the segment id
     */
    synchronized SegmentId getSegmentId(long msb, long lsb) {
        int first = getIndex(lsb);
        int index = first;

        WeakReference<SegmentId> reference = references.get(index);
        while (reference != null) {
            SegmentId id = reference.get();
            if (id != null
                    && id.getMostSignificantBits() == msb
                    && id.getLeastSignificantBits() == lsb) {
                return id;
            }
            index = (index + 1) % references.size();
            reference = references.get(index);
        }

        SegmentId id = new SegmentId(tracker, msb, lsb);
        references.set(index, new WeakReference<SegmentId>(id));
        if (index != first) {
            refresh();
        }
        return id;
    }

    /**
     * Returns all segment identifiers that are currently referenced in memory.
     *
     * @return referenced segment identifiers
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
                    emptyReferences = true;
                }
            }
        }

        while (2 * ids.size() > size) {
            size *= 2;
        }

        if ((hashCollisions && emptyReferences) || size != references.size()) {
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

}
