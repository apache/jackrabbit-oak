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
import static com.google.common.base.Preconditions.checkState;

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;

import com.google.common.collect.ComparisonChain;

/**
 * Representation of a single key-value entry in a map.
 */
class MapEntry extends AbstractChildNodeEntry
        implements Map.Entry<RecordId, RecordId>, Comparable<MapEntry> {

    private final Segment segment;

    private final String name;

    private final RecordId key;

    private final RecordId value;

    MapEntry(Segment segment, String name, RecordId key, RecordId value) {
        this.segment = checkNotNull(segment);
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
        return new SegmentNodeState(segment, value);
    }

    //---------------------------------------------------------< Map.Entry >--

    @Override
    public RecordId getKey() {
        return key;
    }

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
    public int compareTo(MapEntry that) {
        return ComparisonChain.start()
                .compare(getHash(), that.getHash())
                .compare(name, that.name)
                .compare(value, that.value)
                .result();
    }

}
