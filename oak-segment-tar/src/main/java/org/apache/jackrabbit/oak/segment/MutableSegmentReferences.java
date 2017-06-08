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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * A mutable, thread-safe implementation of {@link SegmentReferences}.
 */
class MutableSegmentReferences implements SegmentReferences {

    private final Object lock = new Object();

    private final List<SegmentId> ids = newArrayList();

    private final Map<SegmentId, Integer> numbers = newHashMap();

    @Override
    public SegmentId getSegmentId(int reference) {
        SegmentId id = ids.get(reference - 1);

        if (id != null) {
            return id;
        }

        synchronized (lock) {
            return ids.get(reference - 1);
        }
    }

    /**
     * Generate a reference or return the existing reference for the provided
     * segment ID.
     *
     * @param id an instance of {@link SegmentId}. It should not be {@code
     *           null}.
     * @return a reference to the provided segment ID. The reference number is
     * strictly greater than zero.
     */
    int addOrReference(SegmentId id) {
        Integer number = numbers.get(id);

        if (number != null) {
            return number;
        }

        synchronized (lock) {
            number = numbers.get(id);

            if (number != null) {
                return number;
            }

            ids.add(id);
            number = ids.size();
            numbers.put(id, number);
            return number;
        }
    }

    /**
     * Return the number of references (segment IDs) accessible from this
     * instance.
     *
     * @return the number of references (segment IDs) accessible from this
     * instance.
     */
    int size() {
        synchronized (lock) {
            return numbers.size();
        }
    }

    /**
     * Check if a reference exists for a provided segment ID.
     *
     * @param id The segment ID to check for a reference.
     * @return {@code true} if a reference exists for the provided segment ID,
     * {@code false} otherwise.
     */
    boolean contains(SegmentId id) {
        boolean contains = numbers.containsKey(id);

        if (contains) {
            return true;
        }

        synchronized (lock) {
            return numbers.containsKey(id);
        }
    }

    @Nonnull
    @Override
    public Iterator<SegmentId> iterator() {
        synchronized (lock) {
            return ids.iterator();
        }
    }

}
