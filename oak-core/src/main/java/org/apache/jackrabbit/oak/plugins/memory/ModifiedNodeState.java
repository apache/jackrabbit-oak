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
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Maps.filterValues;
import static com.google.common.collect.Maps.newHashMap;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry.iterable;

import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

/**
 * Immutable snapshot of a mutable node state.
 */
public class ModifiedNodeState extends AbstractNodeState {

    /**
     * Mapping from a PropertyState instance to its name.
     */
    private static final Function<PropertyState, String> GET_NAME =
            new Function<PropertyState, String>() {
                @Override @Nullable
                public String apply(@Nullable PropertyState input) {
                    if (input != null) {
                        return input.getName();
                    } else {
                        return null;
                    }
                }
            };

    /**
     * Unwraps the given {@code NodeState} instance into the given internals
     * of a {@link MutableNodeState} instance that is being created or reset.
     * <p>
     * If the given base state is a {@code ModifiedNodeState} instance,
     * then the contained modifications are applied to the given properties
     * property and child node maps and the contained base state is returned
     * for use as the base state of the {@link MutableNodeState} instance.
     * <p>
     * If the given base state is not a {@code ModifiedNodeState}, then
     * the given property and child node maps are simply reset and the given
     * base state is returned as-is for use as the base state of the
     * {@link MutableNodeState} instance.
     *
     * @param base new base state
     * @param properties {@link MutableNodeState} property map
     * @param nodes {@link MutableNodeState} child node map
     * @return new {@link MutableNodeState} base state
     */
    static NodeState unwrap(
            @Nonnull NodeState base,
            @Nonnull Map<String, PropertyState> properties,
            @Nonnull Map<String, MutableNodeState> nodes) {
        properties.clear();
        for (Entry<String, MutableNodeState> entry : nodes.entrySet()) {
            entry.getValue().reset(base.getChildNode(entry.getKey()));
        }

        if (base instanceof ModifiedNodeState) {
            ModifiedNodeState modified = (ModifiedNodeState) base;

            properties.putAll(modified.properties);
            for (Entry<String, NodeState> entry : modified.nodes.entrySet()) {
                String name = entry.getKey();
                if (!nodes.containsKey(name)) {
                    nodes.put(name, new MutableNodeState(entry.getValue()));
                }
            }

            return modified.base;
        } else {
            return base;
        }
    }

    /**
     * "Squeezes" {@link ModifiedNodeState} instances into equivalent
     * {@link MemoryNodeState}s. Other kinds of states are returned as-is.
     */
    public static NodeState squeeze(NodeState state) {
        if (state instanceof ModifiedNodeState) {
            Map<String, PropertyState> properties = newHashMap();
            for (PropertyState property : state.getProperties()) {
                properties.put(property.getName(), property);
            }

            Map<String, NodeState> nodes = newHashMap();
            for (ChildNodeEntry child : state.getChildNodeEntries()) {
                nodes.put(child.getName(), squeeze(child.getNodeState()));
            }

            state = new MemoryNodeState(properties, nodes);
        }
        return state;
    }


    static long getPropertyCount(
            NodeState base, Map<String, PropertyState> properties) {
        long count = 0;
        if (base.exists()) {
            count = base.getPropertyCount();
            for (Entry<String, PropertyState> entry : properties.entrySet()) {
                if (base.hasProperty(entry.getKey())) {
                    count--;
                }
                if (entry.getValue() != null) {
                    count++;
                }
            }
        }
        return count;
    }

    static boolean hasProperty(
            NodeState base, Map<String, PropertyState> properties,
            String name) {
        if (properties.containsKey(name)) {
            return properties.get(name) != null;
        } else {
            return base.hasProperty(name);
        }
    }

    static PropertyState getProperty(
            NodeState base, Map<String, PropertyState> properties,
            String name) {
        PropertyState property = properties.get(name);
        if (property == null && !properties.containsKey(name)) {
            property = base.getProperty(name);
        }
        return property;
    }

    static Iterable<? extends PropertyState> getProperties(
            NodeState base, Map<String, PropertyState> properties,
            boolean copy) {
        if (!base.exists()) {
            return emptyList();
        } else if (properties.isEmpty()) {
            return base.getProperties(); // shortcut
        } else {
            if (copy) {
                properties = newHashMap(properties);
            }
            Predicate<PropertyState> predicate = Predicates.compose(
                    not(in(properties.keySet())), GET_NAME);
            return concat(
                    filter(base.getProperties(), predicate),
                    filter(properties.values(), notNull()));
        }
    }

    static long getChildNodeCount(
            NodeState base, Map<String, ? extends NodeState> nodes, long max) {
        if (!base.exists()) {
            return 0;
        }
        long deleted = 0, added = 0;
        for (Entry<String, ? extends NodeState> entry
                : nodes.entrySet()) {
            if (!base.hasChildNode(entry.getKey())) {
                added++;
            }
            if (!entry.getValue().exists()) {
                deleted++;
            }
        }
        // if we deleted 100 entries, then we need to 
        // be sure there are 100 more entries than max
        if (max + deleted < 0) {
            // avoid overflow
            max = Long.MAX_VALUE;
        } else {
            max += deleted;
        }
        long count = base.getChildNodeCount(max);
        if (count + added - deleted < 0) {
            count = Long.MAX_VALUE;
        } else {
            count = count + added - deleted;
        }
        return count;
    }

    static Iterable<String> getChildNodeNames(
            NodeState base, Map<String, ? extends NodeState> nodes,
            boolean copy) {
        if (!base.exists()) {
            return emptyList();
        } else if (nodes.isEmpty()) {
            return base.getChildNodeNames(); // shortcut
        } else {
            if (copy) {
                nodes = newHashMap(nodes);
            }
            return concat(
                    filter(base.getChildNodeNames(), not(in(nodes.keySet()))),
                    filterValues(nodes, NodeState.EXISTS).keySet());
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
    private final Map<String, PropertyState> properties;

    /**
     * Set of added, modified or removed (non-existent value)
     * child nodes.
     */
    private final Map<String, NodeState> nodes;

    /**
     * Creates an immutable snapshot of the given internal state of a
     * {@link MutableNodeState} instance.
     *
     * @param base base state
     * @param properties current property modifications
     * @param nodes current child node modifications
     */
    ModifiedNodeState(
            @Nonnull NodeState base,
            @Nonnull Map<String, PropertyState> properties,
            @Nonnull Map<String, MutableNodeState> nodes) {
        this.base = checkNotNull(base);

        if (checkNotNull(properties).isEmpty()) {
            this.properties = emptyMap();
        } else {
            this.properties = newHashMap(properties);
        }

        if (checkNotNull(nodes).isEmpty()) {
            this.nodes = emptyMap();
        } else {
            this.nodes = newHashMap();
            for (Entry<String, MutableNodeState> entry : nodes.entrySet()) {
                this.nodes.put(entry.getKey(), entry.getValue().snapshot());
            }
        }
    }

    @Nonnull
    public NodeState getBaseState() {
        return base;
    }

    //---------------------------------------------------------< NodeState >--

    @Nonnull
    @Override
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    @Override
    public boolean exists() {
        return base.exists();
    }

    @Override
    public long getPropertyCount() {
        return getPropertyCount(base, properties);
    }

    @Override
    public boolean hasProperty(@Nonnull String name) {
        return hasProperty(base, properties, name);
    }

    @Override
    public PropertyState getProperty(@Nonnull String name) {
        return getProperty(base, properties, name);
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return getProperties(base, properties, false);
    }

    @Override
    public long getChildNodeCount(long max) {
        return getChildNodeCount(base, nodes, max);
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        NodeState child = nodes.get(name);
        if (child != null) {
            return child.exists();
        } else {
            return base.hasChildNode(name);
        }
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) {
        NodeState child = nodes.get(name);
        if (child == null) {
            child = base.getChildNode(name);
        }
        return child;
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return getChildNodeNames(base, nodes, false);
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        if (!base.exists()) {
            return emptyList();
        } else if (nodes.isEmpty()) {
            return base.getChildNodeEntries(); // shortcut
        } else {
            Predicate<ChildNodeEntry> predicate = Predicates.compose(
                    not(in(nodes.keySet())), ChildNodeEntry.GET_NAME);
            return concat(
                    filter(base.getChildNodeEntries(), predicate),
                    iterable(filterValues(nodes, NodeState.EXISTS).entrySet()));
        }
    }

    /**
     * Since we keep track of an explicit base node state for a
     * {@link ModifiedNodeState} instance, we can do this in two steps:
     * first compare all the modified properties and child nodes to those
     * of the given base state, and then compare the base states to each
     * other, ignoring all changed properties and child nodes that were
     * already covered earlier.
     */
    @Override
    public boolean compareAgainstBaseState(
            NodeState base, final NodeStateDiff diff) {
        if (this == base) {
            return true; // no differences
        }

        for (Map.Entry<String, PropertyState> entry : properties.entrySet()) {
            PropertyState before = base.getProperty(entry.getKey());
            PropertyState after = entry.getValue();
            if (after == null) {
                if (before != null && !diff.propertyDeleted(before)) {
                    return false;
                }
            } else if (before == null) {
                if (!diff.propertyAdded(after)) {
                    return false;
                }
            } else if (!before.equals(after)
                    && !diff.propertyChanged(before, after)) {
                return false;
            }
        }

        for (Map.Entry<String, NodeState> entry : nodes.entrySet()) {
            String name = entry.getKey();
            NodeState before = base.getChildNode(name);
            NodeState after = entry.getValue();
            if (!after.exists()) {
                if (before.exists() && !diff.childNodeDeleted(name, before)) {
                    return false;
                }
            } else if (!before.exists()) {
                if (!diff.childNodeAdded(name, after)) {
                    return false;
                }
            } else if (before != after // TODO: fastEquals?
                    && !diff.childNodeChanged(name, before, after)) {
                return false;
            }
        }

        return this.base.compareAgainstBaseState(base, new NodeStateDiff() {
            @Override
            public boolean propertyAdded(PropertyState after) {
                return properties.containsKey(after.getName())
                        || diff.propertyAdded(after);
            }
            @Override
            public boolean propertyChanged(
                    PropertyState before, PropertyState after) {
                return properties.containsKey(before.getName())
                        || diff.propertyChanged(before, after);
            }
            @Override
            public boolean propertyDeleted(PropertyState before) {
                return properties.containsKey(before.getName())
                        || diff.propertyDeleted(before);
            }
            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                return nodes.containsKey(name)
                        || diff.childNodeAdded(name, after);
            }
            @Override
            public boolean childNodeChanged(String name, NodeState before, NodeState after) {
                return nodes.containsKey(name)
                        || diff.childNodeChanged(name, before, after);
            }
            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                return nodes.containsKey(name)
                        || diff.childNodeDeleted(name, before);
            }
        });
    }

}
