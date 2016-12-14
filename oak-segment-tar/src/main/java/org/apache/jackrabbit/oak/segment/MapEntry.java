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

    MapEntry(@Nonnull SegmentReader reader, @Nonnull String name,
             @Nonnull RecordId key, @Nullable RecordId value) {
        this.reader = checkNotNull(reader);
        this.name = checkNotNull(name);
        this.key = checkNotNull(key);
        this.value = value;
    }

    public int getHash() {
        return MapRecord.getHash(name);
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

    @CheckForNull
    @Override
    public RecordId getValue() {
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
                .compare(value, that.value)
                .result();
    }

}
