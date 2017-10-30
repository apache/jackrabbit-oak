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
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
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
     * Set of added, modified or removed (non-existent value)
     * child nodes.
     */
    private final Map<String, MutableNodeState> nodes = newHashMap();

    /**
     * Flag to indicate that this child has been replace in its parent.
     * @see org.apache.jackrabbit.oak.spi.state.NodeBuilder#isReplaced()
     */
    private boolean replaced;

    MutableNodeState(@Nonnull NodeState base) {
        checkNotNull(base);
        this.base = ModifiedNodeState.unwrap(base, properties, nodes);
    }

    NodeState snapshot() {
        assert base != null;
        if (properties.isEmpty() && nodes.isEmpty()) {
            return base;
        } else {
            return new ModifiedNodeState(base, properties, nodes);
        }
    }

    void reset(NodeState newBase) {
        assert base != null;
        checkNotNull(newBase);
        base = ModifiedNodeState.unwrap(newBase, properties, nodes);
    }

    /**
     * Equivalent to
     * <pre>
     *   MutableNodeState child = getChildNode(name, true);
     *   child.reset(state);
     *   return child;
     * </pre>
     *
     * @throws IllegalArgumentException if the given name string is empty
     *                                  or contains the forward slash character
     */
    @Nonnull
    MutableNodeState setChildNode(String name, NodeState state)
            throws IllegalArgumentException {
        assert base != null;

        MutableNodeState child = nodes.get(name);
        if (child == null) {
            checkValidName(name);
            child = new MutableNodeState(state);
            if (base.hasChildNode(name)) {
                child.replaced = true;
            }
            nodes.put(name, child);
        } else {
            child.replaced = true;
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
        if (!exists()) {
            return false;
        } else if (nodes.isEmpty() && properties.isEmpty()) {
            return EqualsDiff.modified(before, base);
        }

        // was a child node added or removed?
        for (Entry<String, MutableNodeState> n : nodes.entrySet()) {
            if (n.getValue().exists() != before.hasChildNode(n.getKey())) {
                return true;
            }
        }

        // was a property added, removed or modified
        for (Entry<String, PropertyState> p : properties.entrySet()) {
            PropertyState pState = p.getValue();
            if (pState == null
                    || !pState.equals(before.getProperty(p.getKey()))) {
                return true;
            }
        }

        return false;
    }

    boolean isReplaced(NodeState before) {
        return replaced;
    }

    boolean isReplaced(NodeState before, String name) {
        return before.hasProperty(name)
                && (!base.equals(before) || properties.containsKey(name));
    }

    /**
     * Remove the child node with the given {@code name}.
     * @param name  name of the child node to remove
     * @return  {@code true} if a child node {@code name} existed, {@code false} otherwise.
     */
    boolean removeChildNode(String name) {
        assert base != null;
        MutableNodeState child = nodes.get(name);
        if (child != null) {
            boolean existed = child.exists();
            child.reset(MISSING_NODE);
            return existed;
        } else {
            nodes.put(name, new MutableNodeState(MISSING_NODE));
            return base.hasChildNode(name);
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
        String name = property.getName();
        checkValidName(name);
        properties.put(name, property);
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
        return ModifiedNodeState.getPropertyCount(base, properties);
    }

    @Override
    public boolean hasProperty(@Nonnull String name) {
        return ModifiedNodeState.hasProperty(base, properties, name);
    }

    @Override
    public PropertyState getProperty(@Nonnull String name) {
        return ModifiedNodeState.getProperty(base, properties, name);
    }

    @Override @Nonnull
    public Iterable<? extends PropertyState> getProperties() {
        return ModifiedNodeState.getProperties(base, properties, true);
    }

    @Override
    public long getChildNodeCount(long max) {
        assert base != null;
        return ModifiedNodeState.getChildNodeCount(base, nodes, max);
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        assert base != null;
        NodeState child = nodes.get(name);
        if (child != null) {
            return child.exists();
        } else {
            return base.hasChildNode(name);
        }
    }

    /**
     * Returns a mutable child node with the given name. If the named
     * child node has already been modified, i.e. there's an entry for
     * it in the {@link #nodes} map, then that child instance is returned
     * directly. Otherwise a new mutable child instance is created based
     * on the (possibly non-existent) respective child node of the base
     * state, added to the {@link #nodes} map and returned.
     */
    MutableNodeState getMutableChildNode(String name) {
        assert base != null;
        MutableNodeState child = nodes.get(name);
        if (child == null) {
            child = new MutableNodeState(base.getChildNode(name));
            nodes.put(name, child);
        }
        return child;
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

    @Override @Nonnull
    public Iterable<String> getChildNodeNames() {
        assert base != null;
        return ModifiedNodeState.getChildNodeNames(base, nodes, true);
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
