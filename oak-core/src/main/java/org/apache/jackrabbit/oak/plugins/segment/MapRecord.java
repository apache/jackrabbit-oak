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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

class MapRecord extends Record {

    public interface Entry extends Map.Entry<String, RecordId> {    
    }

    static final int LEVEL_BITS = 5;

    MapRecord(RecordId id) {
        super(id);
    }

    public int size(SegmentReader reader) {
        return reader.readInt(getRecordId(), 0);
    }

    public RecordId getEntry(SegmentReader reader, String key) {
        checkNotNull(key);
        return getEntry(reader, key, 0);
    }

    private int getHash(SegmentReader reader, int index) {
        return reader.readInt(getRecordId(), 4 + index * 4);
    }

    private String getKey(SegmentReader reader, int size, int index) {
        int offset = 4 + size * 4 + index * Segment.RECORD_ID_BYTES;
        return reader.readString(reader.readRecordId(getRecordId(), offset));
    }

    private RecordId getValue(SegmentReader reader, int size, int index) {
        int offset = 4 + size * 4 + (size + index) * Segment.RECORD_ID_BYTES;
        return reader.readRecordId(getRecordId(), offset);
    }

    private RecordId getEntry(SegmentReader reader, String key, int level) {
        int size = 1 << LEVEL_BITS;
        int mask = size - 1;
        int shift = level * LEVEL_BITS;

        int code = key.hashCode();
        int bucketSize = reader.readInt(getRecordId(), 0);
        if (bucketSize == 0) {
            return null;
        } else if (bucketSize <= size || shift >= 32) {
            int index = 0;
            while (index < bucketSize && getHash(reader, index) < code) {
                index++;
            }
            while (index < bucketSize && getHash(reader, index) == code) {
                if (key.equals(getKey(reader, bucketSize, index))) {
                    return getValue(reader, bucketSize, index);
                }
                index++;
            }
            return null;
        } else {
            int bucketMap = reader.readInt(getRecordId(), 4);
            int bucketIndex = (code >> shift) & mask;
            int bucketBit = 1 << bucketIndex;
            if ((bucketMap & bucketBit) != 0) {
                bucketIndex = Integer.bitCount(bucketMap & (bucketBit - 1));
                RecordId bucketId = reader.readRecordId(getRecordId(), 8 + bucketIndex * Segment.RECORD_ID_BYTES);
                return new MapRecord(bucketId).getEntry(reader, key, level + 1);
            } else {
                return null;
            }
        }
    }

    public Iterable<Entry> getEntries(SegmentReader reader) {
        return getEntries(reader, 0);
    }

    private Iterable<Entry> getEntries(
            final SegmentReader reader, int level) {
        int size = 1 << LEVEL_BITS;
        int shift = level * LEVEL_BITS;

        final int bucketSize = reader.readInt(getRecordId(), 0);
        if (bucketSize == 0) {
            return Collections.emptyList();
        } else if (bucketSize <= size || shift >= 32) {
            return new Iterable<Entry>() {
                @Override
                public Iterator<Entry> iterator() {
                    return new Iterator<Entry>() {
                        private int index = 0;
                        @Override
                        public boolean hasNext() {
                            return index < bucketSize;
                        }
                        @Override
                        public Entry next() {
                            final int i = index++;
                            return new Entry() {
                                @Override
                                public String getKey() {
                                    return MapRecord.this.getKey(
                                            reader, bucketSize, i);
                                }
                                @Override
                                public RecordId getValue() {
                                    return MapRecord.this.getValue(
                                            reader, bucketSize, i);
                                }
                                @Override
                                public RecordId setValue(RecordId value) {
                                    throw new UnsupportedOperationException();
                                }
                            };
                        }
                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        } else {
            int bucketMap = reader.readInt(getRecordId(), 4);
            int bucketCount = Integer.bitCount(bucketMap);
            List<Iterable<Entry>> iterables =
                    Lists.newArrayListWithCapacity(bucketCount);
            for (int i = 0; i < bucketCount; i++) {
                RecordId bucketId = reader.readRecordId(
                        getRecordId(), 8 + i * Segment.RECORD_ID_BYTES);
                iterables.add(new MapRecord(bucketId).getEntries(reader, level + 1));
            }
            return Iterables.concat(iterables);
        }
    }

}
