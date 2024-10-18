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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.segment.MapRecord.HASH_MASK;

import java.util.Map;

import org.apache.jackrabbit.guava.common.collect.ComparisonChain;
import org.apache.jackrabbit.guava.common.collect.Ordering;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Representation of a single key-value entry in a map.
 */
class MapEntry extends AbstractChildNodeEntry
        implements Map.Entry<RecordId, RecordId>, Comparable<MapEntry> {

    @NotNull
    private final SegmentReader reader;

    @NotNull
    private final String name;

    @NotNull
    private final RecordId key;

    @Nullable
    private final RecordId value;

    private MapEntry(
            @NotNull SegmentReader reader,
            @NotNull String name,
            @NotNull RecordId key,
            @Nullable RecordId value) {
        this.reader = requireNonNull(reader);
        this.name = requireNonNull(name);
        this.key = requireNonNull(key);
        this.value = value;
    }

    /**
     * Create a new instance of a {@code MapEntry} as it has been read from a segment.

     * @param reader segment reader for reading the node state this map entry's value points to.
     * @param name   name of the key
     * @param key    record id of the key
     * @param value  record id of the value
     */
    static MapEntry newMapEntry(
            @NotNull SegmentReader reader,
            @NotNull String name,
            @NotNull RecordId key,
            @NotNull RecordId value) {
        return new MapEntry(reader, name, key, requireNonNull(value));
    }

    /**
     * Create a new instance of a {@code MapEntry} to be written to a segment. Here the passed
     * {@code value} might be {@code null} to indicate an existing mapping for the this {@code key}
     * should be deleted. In this case calls to {@link #getValue()} should be guarded with a prior
     * call to {@link #isDeleted()} to prevent the former throwing an {@code IllegalStateException}.
     *
     * @param reader segment reader for reading the node state this map entry's value points to.
     * @param name   name of the key
     * @param key    record id of the key
     * @param value  record id of the value or {@code null}.
     *
     * @see #isDeleted()
     * @see #getValue()
     */
    static MapEntry newModifiedMapEntry(
            @NotNull SegmentReader reader,
            @NotNull String name,
            @NotNull RecordId key,
            @Nullable RecordId value) {
        return new MapEntry(reader, name, key, value);
    }

    public int getHash() {
        return MapRecord.getHash(name);
    }

    /**
     * @return  {@code true} to indicate that this value is to be deleted.
     */
    boolean isDeleted() {
        return value == null;
    }

    //----------------------------------------------------< ChildNodeEntry >--

    @Override @NotNull
    public String getName() {
        return name;
    }

    @Override @NotNull
    public SegmentNodeState getNodeState() {
        Validate.checkState(value != null);
        return reader.readNode(value);
    }

    //---------------------------------------------------------< Map.Entry >--

    @NotNull
    @Override
    public RecordId getKey() {
        return key;
    }

    /**
     * @return  the value of this mapping.
     * @throws IllegalStateException if {@link #isDeleted()} is {@code true}.
     */
    @NotNull
    @Override
    public RecordId getValue() {
        Validate.checkState(value != null);
        return value;
    }

    @Override
    public RecordId setValue(RecordId value) {
        throw new UnsupportedOperationException();
    }

    //--------------------------------------------------------< Comparable >--

    @Override
    public int compareTo(@NotNull MapEntry that) {
        return ComparisonChain.start()
                .compare(getHash() & HASH_MASK, that.getHash() & HASH_MASK)
                .compare(name, that.name)
                .compare(value, that.value, Ordering.natural().nullsLast())
                .result();
    }

}
