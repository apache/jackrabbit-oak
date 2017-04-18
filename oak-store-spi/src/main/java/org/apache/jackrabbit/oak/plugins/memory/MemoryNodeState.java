/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.memory;

import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * Basic in-memory node state implementation.
 */
class MemoryNodeState extends AbstractNodeState {

    private final Map<String, PropertyState> properties;

    private final Map<String, NodeState> nodes;

    /**
     * Creates a new node state with the given properties and child nodes.
     * The given maps are stored as references, so their contents and
     * iteration order must remain unmodified at least for as long as this
     * node state instance is in use.
     *
     * @param properties properties
     * @param nodes child nodes
     */
    public MemoryNodeState(
            Map<String, PropertyState> properties,
            Map<String, NodeState> nodes) {
        this.properties = properties;
        this.nodes = nodes;
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public boolean hasProperty(@Nonnull String name) {
        return properties.containsKey(name);
    }

    @Override
    public PropertyState getProperty(@Nonnull String name) {
        return properties.get(name);
    }

    @Override
    public long getPropertyCount() {
        return properties.size();
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return properties.values();
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        return nodes.containsKey(name);
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) {
        NodeState state = nodes.get(name);
        if (state == null) {
            checkValidName(name);
            state = MISSING_NODE;
        }
        return state;
    }

    @Override
    public long getChildNodeCount(long max) {
        return nodes.size();
    }

    @Nonnull
    @Override
    public Iterable<ChildNodeEntry> getChildNodeEntries() {
        return MemoryChildNodeEntry.iterable(nodes.entrySet());
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    /**
     * We don't keep track of a separate base node state for
     * {@link MemoryNodeState} instances, so this method will just do
     * a generic diff against the given state.
     */
    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (base == EMPTY_NODE || !base.exists()) {
            return EmptyNodeState.compareAgainstEmptyState(this, diff);
        }

        Map<String, PropertyState> newProperties =
                new HashMap<String, PropertyState>(properties);
        for (PropertyState before : base.getProperties()) {
            PropertyState after = newProperties.remove(before.getName());
            if (after == null) {
                if (!diff.propertyDeleted(before)) {
                    return false;
                }
            } else if (!after.equals(before)) {
                if (!diff.propertyChanged(before, after)) {
                    return false;
                }
            }
        }

        for (PropertyState after : newProperties.values()) {
            if (!diff.propertyAdded(after)) {
                return false;
            }
        }

        Map<String, NodeState> newNodes =
                new HashMap<String, NodeState>(nodes);
        for (ChildNodeEntry entry : base.getChildNodeEntries()) {
            String name = entry.getName();
            NodeState before = entry.getNodeState();
            NodeState after = newNodes.remove(name);
            if (after == null) {
                if (!diff.childNodeDeleted(name, before)) {
                    return false;
                }
            } else if (after != before) {
                if (!diff.childNodeChanged(name, before, after)) {
                    return false;
                }
            }
        }

        for (Map.Entry<String, NodeState> entry : newNodes.entrySet()) {
            if (!diff.childNodeAdded(entry.getKey(), entry.getValue())) {
                return false;
            }
        }

        return true;
    }

    static MemoryNodeState wrap(NodeState state) {
        if (state instanceof MemoryNodeState) {
            return (MemoryNodeState) state;
        }

        Map<String, PropertyState> properties = newHashMap();
        for (PropertyState property : state.getProperties()) {
            properties.put(property.getName(), property);
        }

        Map<String, NodeState> nodes = newHashMap();
        for (ChildNodeEntry child : state.getChildNodeEntries()) {
            nodes.put(child.getName(), child.getNodeState());
        }

        return new MemoryNodeState(properties, nodes);
    }
}
