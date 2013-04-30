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

package org.apache.jackrabbit.oak.plugins.memory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.with;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.withNodes;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.withProperties;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * A <em>mutable</em> state being built.
 *
 * Instances of this class are never passed beyond the containing
 * {@link MemoryNodeBuilder}, so it's not a problem that we intentionally
 * break the immutability assumption of the
 * {@link org.apache.jackrabbit.oak.spi.state.NodeState} interface.
 */
class MutableNodeState extends AbstractNodeState {

    /**
     * The immutable base state.
     */
    private NodeState base;

    /**
     * Set of added, modified or removed ({@code null} value)
     * property states.
     */
    private final Map<String, PropertyState> properties = newHashMap();

    /**
     * Set of added, modified or removed ({@code null} value)
     * child nodes.
     */
    private final Map<String, MutableNodeState> nodes = newHashMap();

    private MutableNodeState(boolean exists) {
        this.base = exists ? EMPTY_NODE : MISSING_NODE;
    }

    MutableNodeState(@Nonnull NodeState base) {
        if (checkNotNull(base) instanceof ModifiedNodeState) {
            ModifiedNodeState modified = (ModifiedNodeState) base;
            this.base = modified.getBaseState();
            modified.compareAgainstBaseState(new NodeStateDiff() {
                @Override
                public boolean propertyAdded(PropertyState after) {
                    properties.put(after.getName(), after);
                    return true;
                }
                @Override
                public boolean propertyChanged(
                        PropertyState before, PropertyState after) {
                    properties.put(after.getName(), after);
                    return true;
                }
                @Override
                public boolean propertyDeleted(PropertyState before) {
                    properties.put(before.getName(), null);
                    return true;
                }
                @Override
                public boolean childNodeAdded(String name, NodeState after) {
                    nodes.put(name, new MutableNodeState(after));
                    return true;
                }
                @Override
                public boolean childNodeChanged(
                        String name, NodeState before, NodeState after) {
                    nodes.put(name, new MutableNodeState(after));
                    return true;
                }
                @Override
                public boolean childNodeDeleted(String name, NodeState before) {
                    nodes.put(name, null);
                    return true;
                }
            });
        } else {
            this.base = base;
        }
    }

    NodeState snapshot() {
        assert base != null;

        Map<String, NodeState> nodes = newHashMap();
        for (Map.Entry<String, MutableNodeState> entry : this.nodes.entrySet()) {
            String name = entry.getKey();
            MutableNodeState node = entry.getValue();
            NodeState before = base.getChildNode(name);
            if (node == null) {
                if (before.exists()) {
                    nodes.put(name, null);
                }
            } else {
                NodeState after = node.snapshot();
                if (after != before) {
                    nodes.put(name, after);
                }
            }
        }
        return with(base, newHashMap(this.properties), nodes);
    }

    private void reset(NodeState newBase) {
        assert base != null;

        if (newBase instanceof ModifiedNodeState) {
            ModifiedNodeState modified = (ModifiedNodeState) newBase;
            base = modified.getBaseState();
            properties.clear();

            Iterator<Entry<String, MutableNodeState>> iterator =
                    nodes.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, MutableNodeState> entry = iterator.next();
                MutableNodeState cstate = entry.getValue();
                NodeState cbase = newBase.getChildNode(entry.getKey());
                if (!cbase.exists() || cstate == null) {
                    iterator.remove();
                } else {
                    cstate.reset(cbase);
                }
            }

            modified.compareAgainstBaseState(new NodeStateDiff() {
                @Override
                public boolean propertyAdded(PropertyState after) {
                    properties.put(after.getName(), after);
                    return true;
                }
                @Override
                public boolean propertyChanged(
                        PropertyState before, PropertyState after) {
                    properties.put(after.getName(), after);
                    return true;
                }
                @Override
                public boolean propertyDeleted(PropertyState before) {
                    properties.put(before.getName(), null);
                    return true;
                }
                @Override
                public boolean childNodeAdded(String name, NodeState after) {
                    MutableNodeState cstate = nodes.get(name);
                    if (cstate != null) {
                        cstate.reset(after);
                    } else {
                        nodes.put(name, new MutableNodeState(after));
                    }
                    return true;
                }
                @Override
                public boolean childNodeChanged(
                        String name, NodeState before, NodeState after) {
                    MutableNodeState cstate = nodes.get(name);
                    if (cstate != null) {
                        cstate.reset(after);
                    } else {
                        nodes.put(name, new MutableNodeState(after));
                    }
                    return true;
                }
                @Override
                public boolean childNodeDeleted(String name, NodeState before) {
                    nodes.put(name, null);
                    return true;
                }
            });
        } else {
            base = newBase;
            properties.clear();

            Iterator<Entry<String, MutableNodeState>> iterator =
                    nodes.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, MutableNodeState> entry = iterator.next();
                MutableNodeState cstate = entry.getValue();
                NodeState cbase = newBase.getChildNode(entry.getKey());
                if (!cbase.exists() || cstate == null) {
                    iterator.remove();
                } else {
                    cstate.reset(cbase);
                }
            }
        }
    }

    /**
     * Get and optionally connect a potentially non existing child
     * node of a given {@code name}. Connected child nodes are kept
     * in the list of modified child nodes of this node.
     */
    MutableNodeState getChildNode(String name, boolean connect) {
        assert base != null;

        MutableNodeState child = nodes.get(name);
        if (child != null) {
            return child;
        }

        if (nodes.containsKey(name)) {
            // deleted: create new existing node if connect, otherwise non existing
            child = new MutableNodeState(connect);
        } else {
            child = new MutableNodeState(base.getChildNode(name));
        }

        if (connect) {
            nodes.put(name, child);
        }
        return child;
    }

    /**
     * Equivalent to
     * <pre>
     *   MutableNodeState child = getChildNode(name, true);
     *   child.reset(state);
     *   return child;
     * </pre>
     */
    @Nonnull
    MutableNodeState setChildNode(String name, NodeState state) {
        assert base != null;

        MutableNodeState child = nodes.get(name);
        if (child == null) {
            child = new MutableNodeState(state);
            nodes.put(name, child);
        } else {
            child.reset(state);
        }
        return child;
    }

    /**
     * Determine whether this node state is modified wrt. the passed
     * {@code before} state.
     * <p>
     * A node state is modified if it either has not the same properties
     * or has not the same child nodes as a {@code before} state. A node
     * state has the same properties as a {@code before} state iff its
     * set of properties is equal to the set of properties of
     * {@code before}. A node state has the same child nodes as a
     * {@code before} state iff its set of child node names is equal to
     * the set of child node names of {@code before}.
     */
    boolean isModified(NodeState before) {
        if (nodes.isEmpty() && properties.isEmpty()) {
            return false;
        }

        for (Entry<String, MutableNodeState> n : nodes.entrySet()) {
            if (n.getValue() == null) {
                return true;
            }
            if (!(before.hasChildNode(n.getKey()))) {
                return true;
            }
        }
        for (Entry<String, PropertyState> p : properties.entrySet()) {
            PropertyState pState = p.getValue();
            if (pState == null) {
                return true;
            }
            if (!before.exists() || !pState.equals(before.getProperty(p.getKey()))) {
                return true;
            }
        }
        return false;

    }

    /**
     * Remove the child node with the given {@code name}.
     * @param name  name of the child node to remove
     * @return  {@code true} if a child node {@code name} existed, {@code false} otherwise.
     */
    boolean removeChildNode(String name) {
        assert base != null;

        if (base.getChildNode(name).exists()) {
            nodes.put(name, null);
            return true;
        } else {
            return nodes.remove(name) != null;
        }
    }

    /**
     * Remove the property of the given {@code name}.
     * @param name  name of the property to remove
     * @return  {@code true} if a property {@code name} existed, {@code false} otherwise.
     */
    boolean removeProperty(String name) {
        assert base != null;

        if (base.hasProperty(name)) {
            properties.put(name, null);
            return true;
        } else {
            return properties.remove(name) != null;
        }
    }

    /**
     * Set the value of a property
     */
    void setProperty(PropertyState property) {
        properties.put(property.getName(), property);
    }

    @Override
    public String toString() {
        assert base != null;

        StringBuilder builder = new StringBuilder();
        builder.append(base).append(" + {");
        String separator = " ";
        for (PropertyState property : properties.values()) {
            builder.append(separator);
            separator = ", ";
            builder.append(property);
        }
        for (Entry<String, MutableNodeState> entry : nodes.entrySet()) {
            builder.append(separator);
            separator = ", ";
            builder.append(entry.getKey()).append(" : ").append(entry.getValue());
        }
        builder.append(" }");
        return builder.toString();
    }

    //-----------------------------------------------------< NodeState >--

    @Override
    public boolean exists() {
        assert base != null;
        return base.exists();
    }

    @Override
    public long getPropertyCount() {
        assert base != null;
        return withProperties(base, properties).getPropertyCount();
    }

    @Override
    public boolean hasProperty(String name) {
        assert base != null;
        return withProperties(base, properties).hasProperty(name);
    }

    @Override
    public PropertyState getProperty(String name) {
        assert base != null;
        return withProperties(base, properties).getProperty(name);
    }

    @Override @Nonnull
    public Iterable<? extends PropertyState> getProperties() {
        assert base != null;
        Map<String, PropertyState> copy = newHashMap(properties);
        return withProperties(base, copy).getProperties();
    }

    @Override
    public long getChildNodeCount() {
        assert base != null;
        return withNodes(base, nodes).getChildNodeCount();
    }

    @Override
    public boolean hasChildNode(String name) {
        assert base != null;
        checkNotNull(name);
        // checkArgument(!name.isEmpty()); TODO: should be caught earlier
        return withNodes(base, nodes).hasChildNode(name);
    }

    @Override
    public MutableNodeState getChildNode(String name) {
        throw new UnsupportedOperationException();
    }

    @Override @Nonnull
    public Iterable<String> getChildNodeNames() {
        assert base != null;
        Map<String, MutableNodeState> copy = newHashMap(nodes);
        return withNodes(base, copy).getChildNodeNames();
    }

    @Override @Nonnull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        throw new UnsupportedOperationException();
    }

    @Override @Nonnull
    public NodeBuilder builder() {
        throw new UnsupportedOperationException();
    }
}
