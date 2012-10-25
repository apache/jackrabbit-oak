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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Maps.filterValues;
import static org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry.iterable;

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Immutable snapshot of a mutable node state.
 */
public class ModifiedNodeState extends AbstractNodeState {

    public static NodeState withProperties(
            NodeState base, Map<String, ? extends PropertyState> properties) {
        if (properties.isEmpty()) {
            return base;
        } else {
            return new ModifiedNodeState(
                    base, properties, ImmutableMap.<String, NodeState>of());
        }
    }

    public static NodeState withNodes(
            NodeState base, Map<String, ? extends NodeState> nodes) {
        if (nodes.isEmpty()) {
            return base;
        } else {
            return new ModifiedNodeState(
                    base, ImmutableMap.<String, PropertyState>of(), nodes);
        }
    }

    public static NodeState with(
            NodeState base,
            Map<String, ? extends PropertyState> properties,
            Map<String, ? extends NodeState> nodes) {
        if (properties.isEmpty() && nodes.isEmpty()) {
            return base;
        } else {
            return new ModifiedNodeState(base, properties, nodes);
        }
    }

    public static ModifiedNodeState collapse(ModifiedNodeState state) {
        NodeState base = state.getBaseState();
        if (base instanceof ModifiedNodeState) {
            ModifiedNodeState mbase = collapse((ModifiedNodeState) base);

            Map<String, PropertyState> properties =
                    Maps.newHashMap(mbase.properties);
            properties.putAll(state.properties);

            Map<String, NodeState> nodes =
                    Maps.newHashMap(mbase.nodes);
            nodes.putAll(state.nodes);

            return new ModifiedNodeState(
                    mbase.getBaseState(), properties, nodes);
        } else {
            return state;
        }
    }

    /**
     * The base state.
     */
    private final NodeState base;

    /**
     * Set of added, modified or removed ({@code null} value)
     * property states.
     */
    private final Map<String, ? extends PropertyState> properties;

    /**
     * Set of added, modified or removed ({@code null} value)
     * child nodes.
     */
    private final Map<String, ? extends NodeState> nodes;

    public ModifiedNodeState(
            @Nonnull NodeState base,
            @Nonnull Map<String, ? extends PropertyState> properties,
            @Nonnull Map<String, ? extends NodeState> nodes) {
        this.base = checkNotNull(base);
        this.properties = checkNotNull(properties);
        this.nodes = checkNotNull(nodes);
    }

    public ModifiedNodeState(
            @Nonnull NodeState base,
            @Nonnull Map<String, ? extends PropertyState> properties) {
        this(base, properties, ImmutableMap.<String, NodeState>of());
    }

    @Nonnull
    public NodeState getBaseState() {
        return base;
    }

    //---------------------------------------------------------< NodeState >--

    @Override
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    @Override
    public long getPropertyCount() {
        long count = base.getPropertyCount();

        for (Map.Entry<String, ? extends PropertyState> entry : properties.entrySet()) {
            if (base.getProperty(entry.getKey()) != null) {
                count--;
            }
            if (entry.getValue() != null) {
                count++;
            }
        }

        return count;
    }

    @Override
    public PropertyState getProperty(String name) {
        PropertyState property = properties.get(name);
        if (property != null) {
            return property;
        } else if (properties.containsKey(name)) {
            return null; // removed
        } else {
            return base.getProperty(name);
        }
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        if (properties.isEmpty()) {
            return base.getProperties(); // shortcut
        } else {
            Predicate<PropertyState> filter = new Predicate<PropertyState>() {
                @Override
                public boolean apply(PropertyState input) {
                    return !properties.containsKey(input.getName());
                }
            };
            return concat(
                    filter(base.getProperties(), filter),
                    filter(properties.values(), notNull()));
        }
    }

    @Override
    public long getChildNodeCount() {
        long count = base.getChildNodeCount();

        for (Map.Entry<String, ? extends NodeState> entry : nodes.entrySet()) {
            if (base.getChildNode(entry.getKey()) != null) {
                count--;
            }
            if (entry.getValue() != null) {
                count++;
            }
        }

        return count;
    }

    @Override
    public boolean hasChildNode(String name) {
        NodeState child = nodes.get(name);
        if (child != null) {
            return true;
        } else if (nodes.containsKey(name)) {
            return false; // removed
        } else {
            return base.hasChildNode(name);
        }
    }

    @Override
    public NodeState getChildNode(String name) {
        NodeState child = nodes.get(name);
        if (child != null) {
            return child;
        } else if (nodes.containsKey(name)) {
            return null; // removed
        } else {
            return base.getChildNode(name);
        }
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        if (nodes.isEmpty()) {
            return base.getChildNodeNames(); // shortcut
        } else {
            return concat(
                    filter(base.getChildNodeNames(), not(in(nodes.keySet()))),
                    filterValues(nodes, notNull()).keySet());
        }
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        if (nodes.isEmpty()) {
            return base.getChildNodeEntries(); // shortcut
        } else {
            Predicate<ChildNodeEntry> filter = new Predicate<ChildNodeEntry>() {
                @Override
                public boolean apply(ChildNodeEntry input) {
                    return !nodes.containsKey(input.getName());
                }
            };
            return concat(
                    filter(base.getChildNodeEntries(), filter),
                    iterable(filterValues(nodes, notNull()).entrySet()));
        }
    }

    /**
     * Since we keep track of an explicit base node state for a
     * {@link ModifiedNodeState} instance, we can do this in two steps:
     * first compare the base states to each other (often a fast operation),
     * ignoring all changed properties and child nodes for which we have
     * further modifications, and then compare all the modified properties
     * and child nodes to those in the given base state.
     */
    @Override
    public void compareAgainstBaseState(
            NodeState base, final NodeStateDiff diff) {
        if (this.base != base) {
            this.base.compareAgainstBaseState(base, new NodeStateDiff() {
                @Override
                public void propertyAdded(PropertyState after) {
                    if (!properties.containsKey(after.getName())) {
                        diff.propertyAdded(after);
                    }
                }
                @Override
                public void propertyChanged(
                        PropertyState before, PropertyState after) {
                    if (!properties.containsKey(before.getName())) {
                        diff.propertyChanged(before, after);
                    }
                }
                @Override
                public void propertyDeleted(PropertyState before) {
                    if (!properties.containsKey(before.getName())) {
                        diff.propertyDeleted(before);
                    }
                }
                @Override
                public void childNodeAdded(String name, NodeState after) {
                    if (!nodes.containsKey(name)) {
                        diff.childNodeAdded(name, after);
                    }
                }
                @Override
                public void childNodeChanged(String name, NodeState before, NodeState after) {
                    if (!nodes.containsKey(name)) {
                        diff.childNodeChanged(name, before, after);
                    }
                }
                @Override
                public void childNodeDeleted(String name, NodeState before) {
                    if (!nodes.containsKey(name)) {
                        diff.childNodeDeleted(name, before);
                    }
                }
            });
        }

        for (Map.Entry<String, ? extends PropertyState> entry : properties.entrySet()) {
            PropertyState before = base.getProperty(entry.getKey());
            PropertyState after = entry.getValue();
            if (before == null && after == null) {
                // do nothing
            } else if (after == null) {
                diff.propertyDeleted(before);
            } else if (before == null) {
                diff.propertyAdded(after);
            } else if (!before.equals(after)) {
                diff.propertyChanged(before, after);
            }
        }

        for (Map.Entry<String, ? extends NodeState> entry : nodes.entrySet()) {
            String name = entry.getKey();
            NodeState before = base.getChildNode(name);
            NodeState after = entry.getValue();
            if (before == null && after == null) {
                // do nothing
            } else if (after == null) {
                diff.childNodeDeleted(name, before);
            } else if (before == null) {
                diff.childNodeAdded(name, after);
            } else if (!before.equals(after)) {
                diff.childNodeChanged(name, before, after);
            }
        }
    }

}