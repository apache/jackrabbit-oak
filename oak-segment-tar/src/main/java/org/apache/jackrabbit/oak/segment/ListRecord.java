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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.util.List;

/**
 * A record of type "LIST".
 */
class ListRecord extends Record {

    static final int LEVEL_SIZE = 255;

    /** The maximum number of elements a list can contain */
    static final int MAX_ELEMENTS = LEVEL_SIZE * LEVEL_SIZE * LEVEL_SIZE;

    private final int size;

    private final int bucketSize;

    ListRecord(RecordId id, int size) {
        super(id);
        checkArgument(size >= 0,
                "Negative list size: " + size);
        checkArgument(size <= MAX_ELEMENTS,
                "Too many elements in list: " + size);
        this.size = size;

        int bs = 1;
        while (bs * LEVEL_SIZE < size) {
            bs *= LEVEL_SIZE;
        }
        this.bucketSize = bs;
    }

    public int size() {
        return size;
    }

    public RecordId getEntry(int index) {
        checkElementIndex(index, size);
        if (size == 1) {
            return getRecordId();
        } else {
            int bucketIndex = index / bucketSize;
            int bucketOffset = index % bucketSize;
            Segment segment = getSegment();
            RecordId id = segment.readRecordId(getRecordNumber(), 0, bucketIndex);
            ListRecord bucket = new ListRecord(
                    id, Math.min(bucketSize, size - bucketIndex * bucketSize));
            return bucket.getEntry(bucketOffset);
        }
    }

    public List<RecordId> getEntries() {
        return getEntries(0, size);
    }

    public List<RecordId> getEntries(int index, int count) {
        if (index + count > size) {
            count = size - index;
        }
        if (count == 0) {
            return emptyList();
        } else if (count == 1) {
            return singletonList(getEntry(index));
        } else {
            List<RecordId> ids = newArrayListWithCapacity(count);
            getEntries(index, count, ids);
            return ids;
        }
    }

    private void getEntries(int index, int count, List<RecordId> ids) {
        checkPositionIndexes(index, index + count, size);
        Segment segment = getSegment();
        if (size == 1) {
            ids.add(getRecordId());
        } else if (bucketSize == 1) {
            for (int i = 0; i < count; i++) {
                ids.add(segment.readRecordId(getRecordNumber(), 0, index + i));
            }
        } else {
            while (count > 0) {
                int bucketIndex = index / bucketSize;
                int bucketOffset = index % bucketSize;
                RecordId id = segment.readRecordId(getRecordNumber(), 0, bucketIndex);
                ListRecord bucket = new ListRecord(
                        id, Math.min(bucketSize, size - bucketIndex * bucketSize));
                int n = Math.min(bucket.size() - bucketOffset, count);
                bucket.getEntries(bucketOffset, n, ids);
                index += n;
                count -= n;
            }
        }
    }

}
