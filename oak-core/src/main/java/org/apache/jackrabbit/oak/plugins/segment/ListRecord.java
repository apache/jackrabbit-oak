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

class ListRecord extends Record {

    static final int LEVEL_SIZE = 1 << 8; // 256

    private final int size;

    private final int bucketSize;

    ListRecord(RecordId id, int size) {
        super(id);
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

    public RecordId getEntry(SegmentReader reader, int index) {
        checkElementIndex(index, size);

        if (size == 1) {
            return getRecordId();
        } else {
            int bucketIndex = index / bucketSize;
            int bucketOffset = index % bucketSize;
            RecordId bucketId = reader.readRecordId(getRecordId(), bucketIndex * 4);
            ListRecord bucket =
                new ListRecord(bucketId, bucketSize);
            return bucket.getEntry(reader, bucketOffset);
        }
    }

}
