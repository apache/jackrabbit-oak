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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * In-memory node state builder. 
 */
public class MemoryNodeStateBuilder implements NodeStateBuilder {

    private static final NodeState NULL_STATE = new MemoryNodeState(
            ImmutableMap.<String, PropertyState>of(),
            ImmutableMap.<String, NodeState>of());

    /**
     * Parent state builder reference, or {@code null} for a connected
     * builder.
     */
    private MemoryNodeStateBuilder parent;

    /**
     * Name of this child node within the parent builder, or {@code null}
     * for a connected builder.
     */
    private String name;

    // TODO: (Atomic)Reference for use with connect?
    private NodeState base;

    /**
     * Set of added, modified or removed ({@code null} value) property states.
     */
    private Map<String, PropertyState> properties;

    /**
     * Set of builders for added, modified or removed ({@code null} value)
     * child nodes.
     */
    private Map<String, MemoryNodeStateBuilder> builders;

    /**
     * Creates a new in-memory node state builder.
     *
     * @param parent parent node state builder
     * @param name name of this node
     * @param base base state of this node
     */
    protected MemoryNodeStateBuilder(
            MemoryNodeStateBuilder parent, String name, NodeState base) {
        this.parent = parent;
        this.name = name;
        this.properties = ImmutableMap.of();
        this.builders = ImmutableMap.of();
        this.base = base;
    }

    /**
     * Creates a new in-memory node state builder.
     *
     * @param base base state of the new builder
     */
    public MemoryNodeStateBuilder(NodeState base) {
        this.parent = null;
        this.name = null;
        this.properties = Maps.newHashMap();
        this.builders = Maps.newHashMap();
        this.base = base;
    }

    private void connect(boolean modify) {
        if (parent != null) {
            parent.connect(modify);

            MemoryNodeStateBuilder existing = parent.builders.get(name);
            if (existing != null) {
                properties = existing.properties;
                builders = existing.builders;
                parent = null;
                name = null;
            } else if (modify) {
                parent.builders.put(name, this);
                properties = Maps.newHashMap();
                builders = Maps.newHashMap();
                parent = null;
                name = null;
            }
        }
    }

    private void reset(NodeState newBase) {
        base = newBase;

        properties.clear();

        Iterator<Map.Entry<String, MemoryNodeStateBuilder>> iterator =
                builders.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, MemoryNodeStateBuilder> entry = iterator.next();
            MemoryNodeStateBuilder childBuilder = entry.getValue();
            NodeState childBase = base.getChildNode(entry.getKey());
            if (childBase == null || childBuilder == null) {
                iterator.remove();
            } else {
                childBuilder.reset(childBase);
            }
        }
    }

    /**
     * Factory method for creating new child state builders. Subclasses may
     * override this method to control the behavior of child state builders.
     *
     * @param child base state of the new builder, or {@code null}
     * @return new builder
     */
    protected MemoryNodeStateBuilder createChildBuilder(
            String name, NodeState child) {
        return new MemoryNodeStateBuilder(this, name, child);
    }

    /**
     * Called whenever <em>this</em> node is modified, i.e. a property is
     * added, changed or removed, or a child node is added or removed. Changes
     * inside child nodes or the subtrees below are not reported. The default
     * implementation does nothing, but subclasses may override this method
     * to better track changes.
     */
    protected void updated() {
        // do nothing
    }

    @Override
    public NodeState getNodeState() {
        connect(false);

        if (parent != null) {
            return base; // shortcut
        }

        Map<String, PropertyState> props = Maps.newHashMap(properties);
        Map<String, NodeState> nodes = Maps.newHashMap();
        for (Map.Entry<String, MemoryNodeStateBuilder> entry
                : builders.entrySet()) {
            NodeStateBuilder builder = entry.getValue();
            if (builder != null) {
                nodes.put(entry.getKey(), builder.getNodeState());
            } else {
                nodes.put(entry.getKey(), null);
            }
        }

        return new ModifiedNodeState(base, props, nodes);
    }

    @Override
    public long getChildNodeCount() {
        connect(false);

        long count = base.getChildNodeCount();

        for (Map.Entry<String, MemoryNodeStateBuilder> entry
                : builders.entrySet()) {
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
        connect(false);

        NodeStateBuilder builder = builders.get(name);
        if (builder != null) {
            return true;
        } else if (builders.containsKey(name)) {
            return false;
        }

        return base.getChildNode(name) != null;
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        connect(false);

        Iterable<String> unmodified = Iterables.transform(
                base.getChildNodeEntries(),
                new Function<ChildNodeEntry, String>() {
                    @Override
                    public String apply(ChildNodeEntry input) {
                        return input.getName();
                    }
                });
        if (parent != null) {
            return unmodified; // shortcut
        }

        Predicate<String> unmodifiedFilter = Predicates.not(Predicates.in(
                ImmutableSet.copyOf(builders.keySet())));
        Set<String> modified = ImmutableSet.copyOf(
                Maps.filterValues(builders, Predicates.notNull()).keySet());
        return Iterables.concat(
                Iterables.filter(unmodified, unmodifiedFilter),
                modified);
    }

    @Override
    public void setNode(String name, NodeState state) {
        connect(true);

        MemoryNodeStateBuilder builder = builders.get(name);
        if (builder != null) {
            builder.reset(state);
        } else {
            createChildBuilder(name, state).connect(true);
        }

        updated();
    }

    @Override
    public void removeNode(String name) {
        connect(true);

        if (base.getChildNode(name) != null) {
            builders.put(name, null);
        } else {
            builders.remove(name);
        }

        updated();
    }

    @Override
    public long getPropertyCount() {
        connect(false);

        long count = base.getPropertyCount();

        for (Map.Entry<String, PropertyState> entry : properties.entrySet()) {
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
    public Iterable<? extends PropertyState> getProperties() {
        connect(false);

        Iterable<? extends PropertyState> unmodified = base.getProperties();
        if (parent != null) {
            return unmodified; // shortcut
        }

        final Set<String> names = ImmutableSet.copyOf(properties.keySet());
        Predicate<PropertyState> filter = new Predicate<PropertyState>() {
            @Override
            public boolean apply(PropertyState input) {
                return !names.contains(input.getName());
            }
        };
        Collection<PropertyState> modified = ImmutableList.copyOf(
                Collections2.filter(properties.values(), Predicates.notNull()));
        return Iterables.concat(
                Iterables.filter(unmodified, filter),
                modified);
    }


    @Override
    public PropertyState getProperty(String name) {
        connect(false);

        PropertyState property = properties.get(name);
        if (property != null || properties.containsKey(name)) {
            return property;
        }

        return base.getProperty(name);
    }

    @Override
    public void setProperty(String name, CoreValue value) {
        connect(true);

        properties.put(name, new SinglePropertyState(name, value));

        updated();
    }

    @Override
    public void setProperty(String name, List<CoreValue> values) {
        connect(true);

        if (values.isEmpty()) {
            properties.put(name, new EmptyPropertyState(name));
        } else {
            properties.put(name, new MultiPropertyState(name, values));
        }

        updated();
    }

    @Override
    public void removeProperty(String name) {
        connect(true);

        if (base.getProperty(name) != null) {
            properties.put(name, null);
        } else {
            properties.remove(name);
        }

        updated();
    }

    @Override
    public NodeStateBuilder getChildBuilder(String name) {
        connect(true);

        MemoryNodeStateBuilder builder = builders.get(name);
        if (builder == null) {
            NodeState baseState = base.getChildNode(name);
            if (baseState == null) {
                baseState = NULL_STATE;
            }
            builder = createChildBuilder(name, baseState);
            builder.connect(true);
            builders.put(name, builder);
        }

        return builder;
    }

}
