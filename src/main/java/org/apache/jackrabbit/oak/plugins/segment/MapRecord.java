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

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Integer.highestOneBit;
import static java.lang.Integer.numberOfTrailingZeros;

import java.util.Map;
import java.util.UUID;

abstract class MapRecord extends Record {

    /**
     * Number of bits of the hash code to look at on each level of the trie.
     */
    protected static final int BITS_PER_LEVEL = 5;

    /**
     * Number of buckets at each level of the trie.
     */
    protected static final int BUCKETS_PER_LEVEL = 1 << BITS_PER_LEVEL; // 32

    /**
     * Maximum number of trie levels.
     */
    protected static final int MAX_NUMBER_OF_LEVELS =
            (32 + BITS_PER_LEVEL - 1) / BITS_PER_LEVEL; // 7

    /**
     * Number of bits needed to indicate the current trie level.
     */
    protected static final int LEVEL_BITS = // 4, using nextPowerOfTwo():
            numberOfTrailingZeros(highestOneBit(MAX_NUMBER_OF_LEVELS) << 1);

    /**
     * Number of bits used to indicate the size of a map.
     */
    protected static final int SIZE_BITS = 32 - LEVEL_BITS;

    /**
     * Maximum size of a map.
     */
    protected static final int MAX_SIZE = (1 << SIZE_BITS) - 1; // ~268e6

    static MapRecord readMap(SegmentStore store, RecordId id) {
        checkNotNull(store);
        checkNotNull(id);

        Segment segment = checkNotNull(store).readSegment(id.getSegmentId());
        int head = segment.readInt(id.getOffset());
        int level = head >>> SIZE_BITS;
        int size = head & ((1 << SIZE_BITS) - 1);
        System.out.println("R: " + size + " " + level);
        if (size > BUCKETS_PER_LEVEL && level < MAX_NUMBER_OF_LEVELS) {
            int bitmap = segment.readInt(id.getOffset() + 4);
            return new MapBranch(store, id, size, level, bitmap);
        } else {
            return new MapLeaf(store, id, size, level);
        }
    }

    public interface Entry extends Map.Entry<String, RecordId> {    
    }

    protected final SegmentStore store;

    protected final int size;

    protected final int level;

    protected MapRecord(SegmentStore store, RecordId id, int size, int level) {
        super(checkNotNull(id));
        this.store = checkNotNull(store);
        this.size = checkElementIndex(size, MAX_SIZE);
        this.level = checkElementIndex(level, MAX_NUMBER_OF_LEVELS);
    }

    protected Segment getSegment() {
        return getSegment(getRecordId().getSegmentId());
    }

    protected Segment getSegment(UUID uuid) {
        return store.readSegment(uuid);
    }

    protected int getOffset() {
        return getRecordId().getOffset();
    }

    int size() {
        return size;
    }

    abstract RecordId getEntry(String key);

    abstract Iterable<String> getKeys();

    abstract Iterable<Entry> getEntries();

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        StringBuilder builder = null;
        for (Entry entry : getEntries()) {
            if (builder == null) {
                builder = new StringBuilder("{ ");
            } else {
                builder.append(", ");
            }
            builder.append(entry.getKey());
            builder.append("=");
            builder.append(entry.getValue());
        }
        if (builder == null) {
            return "{}";
        } else {
            builder.append(" }");
            return builder.toString();
        }
    }

}
