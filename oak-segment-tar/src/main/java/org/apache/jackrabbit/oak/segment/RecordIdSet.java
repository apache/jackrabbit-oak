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

import static com.google.common.collect.Maps.newHashMap;
import static java.lang.System.arraycopy;
import static java.util.Arrays.binarySearch;

import java.util.Map;

/**
 * A memory optimised set of {@link RecordId}s.
 *
 * The set doesn't keep references to the actual record ids
 * it contains.
 */
public class RecordIdSet {
    private final Map<String, IntSet> seenIds = newHashMap();

    /**
     * Add {@code id} to this set if not already present
     * @param id  the record id to add
     * @return  {@code true} if added, {@code false} if already present
     */
    public boolean addIfNotPresent(RecordId id) {
        String segmentId = id.getSegmentId().toString();
        IntSet offsets = seenIds.get(segmentId);
        if (offsets == null) {
            offsets = new IntSet();
            seenIds.put(segmentId, offsets);
        }
        return offsets.add(id.getRecordNumber());
    }

    /**
     * Check whether {@code id} is present is this set.
     * @param id  the record id to check for
     * @return  {@code true} iff {@code id} is present.
     */
    public boolean contains(RecordId id) {
        String segmentId = id.getSegmentId().toString();
        IntSet offsets = seenIds.get(segmentId);
        return offsets != null && offsets.contains(id.getRecordNumber());
    }

    static class IntSet {
        int[] elements;

        boolean add(int n) {
            if (elements == null) {
                elements = new int[1];
                elements[0] = n;
                return true;
            } else {
                int k = binarySearch(elements, n);
                if (k < 0) {
                    int l = -k - 1;
                    int[] e = new int[elements.length + 1];
                    arraycopy(elements, 0, e, 0, l);
                    e[l] = n;
                    int c = elements.length - l;
                    if (c > 0) {
                        arraycopy(elements, l, e, l + 1, c);
                    }
                    elements = e;
                    return true;
                } else {
                    return false;
                }
            }
        }

        boolean contains(int n) {
            return elements != null && binarySearch(elements, n) >= 0;
        }
    }

}
