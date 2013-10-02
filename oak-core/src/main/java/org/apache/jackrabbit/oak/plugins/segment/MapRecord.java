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
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.Integer.highestOneBit;
import static java.lang.Integer.numberOfTrailingZeros;

import java.util.Set;

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

    protected final int size;

    protected final int level;

    protected MapRecord(Segment segment, int offset, int size, int level) {
        super(segment, offset);
        this.size = checkElementIndex(size, MAX_SIZE);
        this.level = checkPositionIndex(level, MAX_NUMBER_OF_LEVELS);
    }

    protected MapRecord(Segment segment, RecordId id, int size, int level) {
        super(segment, id);
        this.size = checkElementIndex(size, MAX_SIZE);
        this.level = checkPositionIndex(level, MAX_NUMBER_OF_LEVELS);
    }

    int size() {
        return size;
    }

    abstract MapEntry getEntry(String key);

    abstract Iterable<String> getKeys();

    abstract Iterable<MapEntry> getEntries();

    boolean compareAgainstEmptyMap(MapDiff diff) {
        for (MapEntry entry : getEntries()) {
            if (!diff.entryAdded(entry.getName(), entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    interface MapDiff {
        boolean entryAdded(String key, RecordId after);
        boolean entryChanged(String key, RecordId before, RecordId after);
        boolean entryDeleted(String key, RecordId before);
    }

    boolean compare(MapRecord that, MapDiff diff) {
        Set<String> keys = newHashSet();
        for (MapEntry entry : getEntries()) {
            String name = entry.getName();
            MapEntry thatEntry = that.getEntry(name);
            if (thatEntry == null) {
                if (!diff.entryAdded(name, entry.getValue())) {
                    return false;
                }
            } else if (!entry.getValue().equals(thatEntry.getValue())) {
                if (!diff.entryChanged(name, thatEntry.getValue(), entry.getValue())) {
                    return false;
                }
            }
            keys.add(name);
        }
        for (MapEntry entry : that.getEntries()) {
            String name = entry.getName();
            if (!keys.contains(name)) {
                if (!diff.entryDeleted(name, entry.getValue())) {
                    return false;
                }
            }
        }
        return true;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        StringBuilder builder = null;
        for (MapEntry entry : getEntries()) {
            if (builder == null) {
                builder = new StringBuilder("{ ");
            } else {
                builder.append(", ");
            }
            builder.append(entry);
        }
        if (builder == null) {
            return "{}";
        } else {
            builder.append(" }");
            return builder.toString();
        }
    }

}
