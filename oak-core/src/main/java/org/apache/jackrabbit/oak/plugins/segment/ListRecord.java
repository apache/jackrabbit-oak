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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.util.List;

class ListRecord extends Record {

    static final int LEVEL_SIZE = Segment.SEGMENT_REFERENCE_LIMIT;

    private final int size;

    private final int bucketSize;

    ListRecord(Segment segment, RecordId id, int size) {
        super(segment, id);
        checkArgument(size >= 0);
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
            RecordId bucketId = segment.readRecordId(getOffset(0, bucketIndex));
            ListRecord bucket = new ListRecord(segment, bucketId, bucketSize);
            return bucket.getEntry(bucketOffset);
        }
    }

    public List<RecordId> getEntries() {
        if (size == 0) {
            return emptyList();
        } else if (size == 1) {
            return singletonList(getRecordId());
        } else {
            List<RecordId> list = newArrayListWithCapacity(size);
            Segment segment = getSegment();
            int offset = getOffset();
            for (int i = 0; i < size; i += bucketSize) {
                RecordId id = segment.readRecordId(offset);
                if (bucketSize == 1) {
                    list.add(id);
                } else {
                    ListRecord bucket = new ListRecord(
                            segment, id, Math.min(bucketSize, size - offset));
                    list.addAll(bucket.getEntries());
                }
                offset += Segment.RECORD_ID_BYTES;
            }
            return list;
        }
    }

}
