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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.segment.MapRecord.HASH_MASK;

import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;

/**
 * Representation of a single key-value entry in a map.
 */
class MapEntry extends AbstractChildNodeEntry
        implements Map.Entry<RecordId, RecordId>, Comparable<MapEntry> {

    @Nonnull
    private final SegmentReader reader;

    @Nonnull
    private final String name;

    @Nonnull
    private final RecordId key;

    @CheckForNull
    private final RecordId value;

    private MapEntry(
            @Nonnull SegmentReader reader,
            @Nonnull String name,
            @Nonnull RecordId key,
            @Nullable RecordId value) {
        this.reader = checkNotNull(reader);
        this.name = checkNotNull(name);
        this.key = checkNotNull(key);
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
            @Nonnull SegmentReader reader,
            @Nonnull String name,
            @Nonnull RecordId key,
            @Nonnull RecordId value) {
        return new MapEntry(reader, name, key, checkNotNull(value));
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
            @Nonnull SegmentReader reader,
            @Nonnull String name,
            @Nonnull RecordId key,
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

    @Override @Nonnull
    public String getName() {
        return name;
    }

    @Override @Nonnull
    public SegmentNodeState getNodeState() {
        checkState(value != null);
        return reader.readNode(value);
    }

    //---------------------------------------------------------< Map.Entry >--

    @Nonnull
    @Override
    public RecordId getKey() {
        return key;
    }

    /**
     * @return  the value of this mapping.
     * @throws IllegalStateException if {@link #isDeleted()} is {@code true}.
     */
    @Nonnull
    @Override
    public RecordId getValue() {
        checkState(value != null);
        return value;
    }

    @Override
    public RecordId setValue(RecordId value) {
        throw new UnsupportedOperationException();
    }

    //--------------------------------------------------------< Comparable >--

    @Override
    public int compareTo(@Nonnull MapEntry that) {
        return ComparisonChain.start()
                .compare(getHash() & HASH_MASK, that.getHash() & HASH_MASK)
                .compare(name, that.name)
                .compare(value, that.value, Ordering.natural().nullsLast())
                .result();
    }

}
