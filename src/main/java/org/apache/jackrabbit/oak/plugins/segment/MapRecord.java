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

import static com.google.common.base.Preconditions.checkNotNull;

class MapRecord extends Record {

    static final int LEVEL_BITS = 6;

    MapRecord(SegmentReader reader, RecordId id) {
        super(reader, id);
    }

    public int size() {
        return readInt(0);
    }

    public RecordId getEntry(String key) {
        checkNotNull(key);
        return getEntry(key, 0);
    }

    private RecordId getEntry(String key, int level) {
        int size = 1 << LEVEL_BITS;
        int mask = size - 1;
        int shift = level * LEVEL_BITS;

        int code = key.hashCode();
        int bucketSize = readInt(0);
        if (bucketSize == 0) {
            return null;
        } else if (bucketSize <= size) {
            int offset = 0;
            while (offset < bucketSize && readInt(4 + offset * 4) < code) {
                offset++;
            }
            while (offset < bucketSize && readInt(4 + offset * 4) == code) {
                RecordId keyId = readRecordId(4 + (bucketSize + offset) * 4);
                if (key.equals(getReader().readString(keyId))) {
                    return readRecordId(4 + (2 * bucketSize + offset) * 4);
                }
                offset++;
            }
            return null;
        } else {
            long bucketMap = readLong(4);
            int bucketIndex = (code >> shift) & mask;
            long bucketBit = 1L << bucketIndex;
            if ((bucketMap & bucketBit) != 0) {
                bucketIndex = Long.bitCount(bucketMap & (bucketBit - 1));
                RecordId bucketId = readRecordId(12 + bucketIndex * 4);
                return new MapRecord(getReader(), bucketId).getEntry(key, level + 1);
            } else {
                return null;
            }
        }
    }

}
