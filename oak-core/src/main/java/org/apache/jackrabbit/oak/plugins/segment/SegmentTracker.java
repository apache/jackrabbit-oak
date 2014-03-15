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
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;

import java.lang.ref.WeakReference;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Tracker of references to segment identifiers and segment instances
 * that are currently kept in memory.
 */
public class SegmentTracker {

    private static final long MSB_MASK = ~(0xfL << 12);

    private static final long VERSION = (0x4L << 12);

    private static final long LSB_MASK = ~(0xfL << 60);

    private static final long DATA = 0xAL << 60;

    private static final long BULK = 0xBL << 60;

    private static final int MB = 1024 * 1024;

    private static final int DEFAULT_MEMORY_CACHE_SIZE = 256;

    /**
     * The random number source for generating new segment identifiers.
     */
    private final SecureRandom random = new SecureRandom();

    /**
     * Hash table of weak references to segment identifiers that are
     * currently being accessed. The size of the table is always a power
     * of two, which optimizes the {@link #expand()} operation. The table is
     * indexed by the random identifier bits, which guarantees uniform
     * distribution of entries. Each table entry is either {@code null}
     * (when there are no matching identifiers) or a list of weak references
     * to the matching identifiers.
     */
    private final ArrayList<List<WeakReference<SegmentId>>> uuids = newArrayList(
            Collections.<List<WeakReference<SegmentId>>>nCopies(1024, null));

    private final LinkedList<Segment> segments = newLinkedList();

    private final SegmentStore store;

    private final SegmentWriter writer;

    private final long cacheSize;

    private long currentSize = 0;

    public SegmentTracker(SegmentStore store, int cacheSizeMB) {
        this.store = store;
        this.writer = new SegmentWriter(store, this);
        this.cacheSize = cacheSizeMB * MB;
    }

    public SegmentTracker(SegmentStore store) {
        this(store, DEFAULT_MEMORY_CACHE_SIZE);
    }

    public SegmentWriter getWriter() {
        return writer;
    }

    public SegmentStore getStore() {
        return store;
    }

    Segment getSegment(SegmentId id) {
        Segment segment = store.readSegment(id);
        id.setSegment(segment);

        synchronized (this) {
            segments.addFirst(segment);
            currentSize += segment.getCacheSize();
            while (currentSize > cacheSize && segments.size() > 1) {
                Segment remove = segments.removeLast();
                remove.getSegmentId().setSegment(null);
                currentSize -= remove.getCacheSize();
            }
        }

        return segment;
    }


    /**
     * Returns all segment identifiers that are currently referenced in memory.
     *
     * @return referenced segment identifiers
     */
    public synchronized Set<SegmentId> getReferencedSegmentIds() {
        Set<SegmentId> set = newHashSetWithExpectedSize(uuids.size());

        for (int i = 0; i < uuids.size(); i++) {
            List<WeakReference<SegmentId>> list = uuids.get(i);
            if (list != null) {
                Iterator<WeakReference<SegmentId>> iterator = list.iterator();
                while (iterator.hasNext()) {
                    SegmentId id = iterator.next().get();
                    if (id == null) {
                        iterator.remove();
                    } else {
                        set.add(id);
                    }
                }
                if (list.isEmpty()) {
                    uuids.set(i, null);
                }
            }
        }

        return set;
    }

    /**
     * 
     * @param msb
     * @param lsb
     * @return
     */
    public synchronized SegmentId getSegmentId(long msb, long lsb) {
        int index = ((int) lsb) & (uuids.size() - 1);

        List<WeakReference<SegmentId>> list = uuids.get(index);
        if (list == null) {
            list = newLinkedList();
            uuids.set(index, list);
        }

        Iterator<WeakReference<SegmentId>> iterator = list.iterator();
        while (iterator.hasNext()) {
            SegmentId id = iterator.next().get();
            if (id == null) {
                iterator.remove();
            } else if (id.equals(msb, lsb)) {
                return id;
            }
        }

        SegmentId id = new SegmentId(this, msb, lsb);
        list.add(new WeakReference<SegmentId>(id));

        if (list.size() > 5) {
            expand();
        }

        return id;
    }

    SegmentId newDataSegmentId() {
        return newSegmentId(DATA);
    }

    SegmentId newBulkSegmentId() {
        return newSegmentId(BULK);
    }

    private SegmentId newSegmentId(long type) {
        long msb = (random.nextLong() & MSB_MASK) | VERSION;
        long lsb = (random.nextLong() & LSB_MASK) | type;
        return getSegmentId(msb, lsb);
    }

    private synchronized void expand() {
        int n = uuids.size();
        uuids.ensureCapacity(n * 2);
        for (int i = 0; i < n; i++) {
            List<WeakReference<SegmentId>> list = uuids.get(i);
            if (list == null) {
                uuids.add(null);
            } else {
                List<WeakReference<SegmentId>> newList = newLinkedList();

                Iterator<WeakReference<SegmentId>>iterator = list.iterator();
                while (iterator.hasNext()) {
                    WeakReference<SegmentId> reference = iterator.next();
                    SegmentId uuid = reference.get();
                    if (uuid == null) {
                        iterator.remove();
                    } else if ((uuid.getLeastSignificantBits() & n) != 0) {
                        iterator.remove();
                        newList.add(reference);
                    }
                }

                if (list.isEmpty()) {
                    uuids.set(i, null);
                }

                if (newList.isEmpty()) {
                    uuids.add(null);
                } else {
                    uuids.add(newList);
                }
            }
        }
    }

}
