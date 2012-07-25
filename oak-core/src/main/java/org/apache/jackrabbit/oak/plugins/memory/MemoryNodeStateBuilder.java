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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;

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

    /**
     * The current state of this builder. Initially set to the immutable
     * base state until this builder gets <em>connected</em>, after which
     * this reference will point to the {@link MutableNodeState} instance
     * that records all the changes to this node.
     */
    private NodeState state;

    /**
     * Creates a new in-memory node state builder.
     *
     * @param parent parent node state builder
     * @param name name of this node
     * @param base base state of this node
     */
    protected MemoryNodeStateBuilder(
            MemoryNodeStateBuilder parent, String name, NodeState base) {
        this.parent = checkNotNull(parent);
        this.name = checkNotNull(name);
        this.state = checkNotNull(base);
    }

    /**
     * Creates a new in-memory node state builder.
     *
     * @param base base state of the new builder
     */
    public MemoryNodeStateBuilder(NodeState base) {
        this.parent = null;
        this.name = null;
        this.state = new MutableNodeState(checkNotNull(base));
    }

    private NodeState read() {
        if (parent != null) {
            NodeState pstate = parent.read();
            if (pstate instanceof MutableNodeState) {
                MutableNodeState mstate = (MutableNodeState) pstate;
                MemoryNodeStateBuilder existing = mstate.builders.get(name);
                if (existing != null) {
                    state = existing.state;
                    parent = null;
                    name = null;
                }
            }
        }
        return state;
    }

    private MutableNodeState write() {
        if (parent != null) {
            MutableNodeState mstate = parent.write();
            MemoryNodeStateBuilder existing = mstate.builders.get(name);
            if (existing != null) {
                state = existing.state;
            } else {
                state = new MutableNodeState(state);
                mstate.builders.put(name, this);
            }
            parent = null;
            name = null;
        }
        return (MutableNodeState) state;
    }

    private void reset(NodeState newBase) {
        MutableNodeState mstate = write();

        mstate.base = newBase;

        mstate.properties.clear();

        Iterator<Map.Entry<String, MemoryNodeStateBuilder>> iterator =
                mstate.builders.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, MemoryNodeStateBuilder> entry = iterator.next();
            MemoryNodeStateBuilder childBuilder = entry.getValue();
            NodeState childBase = newBase.getChildNode(entry.getKey());
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

    protected NodeState getBaseState() {
        NodeState state = read();
        if (state instanceof MutableNodeState) { 
            return ((MutableNodeState) state).base;
        } else {
            return state;
        }
    }

    @Override
    public NodeState getNodeState() {
        NodeState state = read();
        if (state instanceof MutableNodeState) {
            return ((MutableNodeState) state).snapshot();
        } else {
            return state;
        }
    }

    @Override
    public long getChildNodeCount() {
        return read().getChildNodeCount();
    }

    @Override
    public boolean hasChildNode(String name) {
        return read().hasChildNode(name);
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return read().getChildNodeNames();
    }

    @Override
    public void setNode(String name, NodeState state) {
        MutableNodeState mstate = write();

        MemoryNodeStateBuilder builder = mstate.builders.get(name);
        if (builder != null) {
            builder.reset(state);
        } else {
            createChildBuilder(name, state).write();
        }

        updated();
    }

    @Override
    public void removeNode(String name) {
        MutableNodeState mstate = write();

        if (mstate.base.getChildNode(name) != null) {
            mstate.builders.put(name, null);
        } else {
            mstate.builders.remove(name);
        }

        updated();
    }

    @Override
    public long getPropertyCount() {
        return read().getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return read().getProperties();
    }


    @Override
    public PropertyState getProperty(String name) {
        return read().getProperty(name);
    }

    @Override
    public void setProperty(String name, CoreValue value) {
        MutableNodeState mstate = write();

        mstate.properties.put(name, new SinglePropertyState(name, value));

        updated();
    }

    @Override
    public void setProperty(String name, List<CoreValue> values) {
        MutableNodeState mstate = write();

        if (values.isEmpty()) {
            mstate.properties.put(name, new EmptyPropertyState(name));
        } else {
            mstate.properties.put(name, new MultiPropertyState(name, values));
        }

        updated();
    }

    @Override
    public void removeProperty(String name) {
        MutableNodeState mstate = write();

        if (mstate.base.getProperty(name) != null) {
            mstate.properties.put(name, null);
        } else {
            mstate.properties.remove(name);
        }

        updated();
    }

    @Override
    public NodeStateBuilder getChildBuilder(String name) {
        NodeState state = read();

        if (!(state instanceof MutableNodeState)) {
            NodeState base = state.getChildNode(name);
            if (base != null) {
                return createChildBuilder(name, base); // shortcut
            }
        }

        MutableNodeState mstate = write();
        MemoryNodeStateBuilder builder = mstate.builders.get(name);
        if (builder == null) {
            if (mstate.builders.containsKey(name)) {
                builder = createChildBuilder(name, NULL_STATE);
                builder.write();
            } else {
                NodeState base = mstate.base.getChildNode(name);
                if (base == null) {
                    base = NULL_STATE;
                }
                builder = createChildBuilder(name, base);
            }
        }
        return builder;
    }

    /**
     * The <em>mutable</em> state being built. Instances of this class
     * are never passed beyond the containing MemoryNodeStateBuilder,
     * so it's not a problem that we intentionally break the immutability
     * assumption of the {@link NodeState} interface.
     */
    private static class MutableNodeState extends AbstractNodeState {

        /**
         * The immutable base state.
         */
        private NodeState base;

        /**
         * Set of added, modified or removed ({@code null} value)
         * property states.
         */
        private final Map<String, PropertyState> properties =
                Maps.newHashMap();

        /**
         * Set of builders for added, modified or removed
         * ({@code null} value) child nodes.
         */
        private final Map<String, MemoryNodeStateBuilder> builders =
                Maps.newHashMap();

        public MutableNodeState(NodeState base) {
            this.base = base;
        }

        public NodeState snapshot() {
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

        //-----------------------------------------------------< NodeState >--

        @Override
        public long getPropertyCount() {
            long count = base.getPropertyCount();

            for (Map.Entry<String, PropertyState> entry
                    : properties.entrySet()) {
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
            if (property != null || properties.containsKey(name)) {
                return property;
            }

            return base.getProperty(name);
        }

        @Override
        public Iterable<? extends PropertyState> getProperties() {
            final Set<String> names = ImmutableSet.copyOf(properties.keySet());
            Predicate<PropertyState> filter = new Predicate<PropertyState>() {
                @Override
                public boolean apply(PropertyState input) {
                    return !names.contains(input.getName());
                }
            };
            Collection<PropertyState> modified = Collections2.filter(
                    properties.values(), Predicates.notNull());
            return Iterables.concat(
                    Iterables.filter(base.getProperties(), filter),
                    ImmutableList.copyOf(modified));
        }

        @Override
        public long getChildNodeCount() {
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
            Iterable<String> unmodified = base.getChildNodeNames();
            Predicate<String> unmodifiedFilter = Predicates.not(Predicates.in(
                    ImmutableSet.copyOf(builders.keySet())));
            Set<String> modified = ImmutableSet.copyOf(
                    Maps.filterValues(builders, Predicates.notNull()).keySet());
            return Iterables.concat(
                    Iterables.filter(unmodified, unmodifiedFilter),
                    modified);
        }

        @Override
        public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
            // TODO Auto-generated method stub
            return null;
        }


    }

}
