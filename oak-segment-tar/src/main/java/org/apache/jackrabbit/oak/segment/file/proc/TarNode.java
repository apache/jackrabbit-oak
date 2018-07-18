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
 *
 */

package org.apache.jackrabbit.oak.segment.file.proc;

import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

class TarNode extends AbstractNode {

    private final Backend backend;

    private final String name;

    TarNode(Backend backend, String name) {
        this.backend = backend;
        this.name = name;
    }

    @NotNull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return ImmutableList.of(
            createProperty("name", name),
            createProperty("size", backend.getTarSize(name).orElse(-1L))
        );
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        return backend.segmentExists(this.name, name);
    }

    @NotNull
    @Override
    public NodeState getChildNode(@NotNull String name) throws IllegalArgumentException {
        if (backend.segmentExists(this.name, name)) {
            return SegmentNode.newSegmentNode(backend, name);
        }
        return EmptyNodeState.MISSING_NODE;
    }

    @NotNull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return StreamSupport.stream(backend.getSegmentIds(name).spliterator(), false)
            .map(this::newSegmentEntry)
            .collect(Collectors.toList());
    }

    ChildNodeEntry newSegmentEntry(String segmentId) {
        return new SegmentEntry(backend, segmentId);
    }

}
