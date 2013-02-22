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
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ID_BYTES;

import java.util.Iterator;
import java.util.NoSuchElementException;

class MapLeaf extends MapRecord {

    MapLeaf(SegmentStore store, RecordId id, int size, int level) {
        super(store, id, size, level);
        System.out.println(size + " " + level);
        checkArgument(size != 0 || level == 0);
        checkArgument(size <= BUCKETS_PER_LEVEL || level == MAX_NUMBER_OF_LEVELS);
    }

    @Override
    RecordId getEntry(String key) {
        checkNotNull(key);

        if (size > 0) {
            int hash = key.hashCode();
            Segment segment = getSegment();

            int index = 0;
            while (index < size && getHash(segment, index) < hash) {
                index++;
            }
            while (index < size && getHash(segment, index) == hash) {
                if (key.equals(getKey(segment, index))) {
                    return getValue(segment, index);
                }
                index++;
            }
        }

        return null;
    }

    @Override
    Iterable<String> getKeys() {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return getKeyIterator();
            }

        };
    }

    @Override
    Iterable<Entry> getEntries() {
        return new Iterable<Entry>() {
            @Override
            public Iterator<Entry> iterator() {
                return getEntryIterator();
            }
        };
    }

    //-----------------------------------------------------------< private >--

    private Iterator<String> getKeyIterator() {
        return new Iterator<String>() {
            private final Segment segment = getSegment();
            private int index = 0;
            @Override
            public boolean hasNext() {
                return index < size;
            }
            @Override
            public String next() {
                int i = index++;
                if (i < size) {
                    return getKey(segment, i);
                } else {
                    throw new NoSuchElementException();
                }
            }
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private Iterator<Entry> getEntryIterator() {
        return new Iterator<Entry>() {
            private final Segment segment = getSegment();
            private int index = 0;
            @Override
            public boolean hasNext() {
                return index < size;
            }
            @Override
            public Entry next() {
                final int i = index++;
                if (i < size) {
                    return new Entry() {
                        @Override
                        public String getKey() {
                            return MapLeaf.this.getKey(segment, i);
                        }
                        @Override
                        public RecordId getValue() {
                            return MapLeaf.this.getValue(segment, i);
                        }
                        @Override
                        public RecordId setValue(RecordId value) {
                            throw new UnsupportedOperationException();
                        }
                    };
                } else {
                    throw new NoSuchElementException();
                }
            }
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private int getHash(Segment segment, int index) {
        return checkNotNull(segment).readInt(getOffset() + 4 + index * 4);
    }

    private String getKey(Segment segment, int index) {
        int offset = getOffset() + 4 + size * 4 + index * RECORD_ID_BYTES;
        RecordId id = checkNotNull(segment).readRecordId(offset);
        if (!segment.getSegmentId().equals(id.getSegmentId())) {
            // the string is stored in another segment
            segment = store.readSegment(id.getSegmentId());
        }
        return segment.readString(id.getOffset());
    }

    private RecordId getValue(Segment segment, int index) {
        int offset = getOffset()
                + 4 + size * 4 + size * RECORD_ID_BYTES
                + index * RECORD_ID_BYTES;
        return checkNotNull(segment).readRecordId(offset);
    }

}
