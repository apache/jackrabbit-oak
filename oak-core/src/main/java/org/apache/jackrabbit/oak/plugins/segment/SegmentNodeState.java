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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.segment.MapRecord.Entry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

class SegmentNodeState extends AbstractNodeState {

    public static RecordId getRecordIdIfAvailable(NodeState state) {
        if (state instanceof SegmentNodeState) {
            SegmentNodeState sstate = (SegmentNodeState) state;
            return sstate.recordId;
        }
        return null;
    }

    private final SegmentReader reader;

    private final RecordId recordId;

    private final MapRecord properties;

    private final MapRecord childNodes;

    SegmentNodeState(SegmentReader reader, RecordId id) {
        this.reader = checkNotNull(reader);
        this.recordId = checkNotNull(id);
        this.properties = new MapRecord(reader.readRecordId(id, 0));
        this.childNodes = new MapRecord(reader.readRecordId(id, 4));
    }

    @Override
    public long getPropertyCount() {
        return properties.size(reader);
    }

    @Override @CheckForNull
    public PropertyState getProperty(String name) {
        checkNotNull(name);
        RecordId propertyId = properties.getEntry(reader, name);
        if (propertyId != null) {
            return new SegmentPropertyState(reader, name, propertyId);
        } else {
            return null;
        }
    }

    @Override @Nonnull
    public Iterable<? extends PropertyState> getProperties() {
        return Iterables.transform(
                properties.getEntries(reader),
                new Function<MapRecord.Entry, PropertyState>() {
                    @Override @Nullable
                    public PropertyState apply(@Nullable Entry input) {
                        return new SegmentPropertyState(
                                reader, input.getKey(), input.getValue());
                    }
                });
    }

    @Override
    public long getChildNodeCount() {
        return childNodes.size(reader);
    }

    @Override
    public boolean hasChildNode(String name) {
        checkNotNull(name);
        return childNodes.getEntry(reader, name) != null;
    }

    @Override @CheckForNull
    public NodeState getChildNode(String name) {
        checkNotNull(name);
        RecordId childNodeId = childNodes.getEntry(reader, name);
        if (childNodeId != null) {
            return new SegmentNodeState(reader, childNodeId);
        } else {
            return null;
        }
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return Iterables.transform(
                childNodes.getEntries(reader),
                new Function<MapRecord.Entry, String>() {
                    @Override @Nullable
                    public String apply(@Nullable Entry input) {
                        return input.getKey();
                    }
                });
    }

    @Override @Nonnull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return Iterables.transform(
                childNodes.getEntries(reader),
                new Function<MapRecord.Entry, ChildNodeEntry>() {
                    @Override @Nullable
                    public ChildNodeEntry apply(@Nullable Entry input) {
                        return new MemoryChildNodeEntry(
                                input.getKey(),
                                new SegmentNodeState(reader, input.getValue()));
                    }
                });
    }

    @Override @Nonnull
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

}
