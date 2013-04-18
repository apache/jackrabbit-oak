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
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.with;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.withNodes;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.withProperties;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * In-memory node state builder.
 * <p>
 * TODO: The following description is somewhat out of date
 * <p>
 * The following two builder states are used
 * to properly track uncommitted chances without relying on weak references
 * or requiring hard references on the entire accessed subtree:
 * <dl>
 *   <dt>unmodified</dt>
 *   <dd>
 *     A child builder with no content changes starts in this state.
 *     It keeps a reference to the parent builder and knows it's name for
 *     use when connecting. Before each access the unconnected builder
 *     checks the parent for relevant changes to connect to. As long as
 *     there are no such changes, the builder remains unconnected and
 *     uses the immutable base state to respond to any content accesses.
 *   </dd>
 *   <dt>connected</dt>
 *   <dd>
 *     Once a child node is first modified, it switches it's internal
 *     state from the immutable base state to a mutable one and records
 *     a hard reference to that state in the mutable parent state. After
 *     that the parent reference is cleared and no more state checks are
 *     made. Any other builder instances that refer to the same child node
 *     will update their internal states to point to that same mutable
 *     state instance and thus become connected at next access.
 *     A root state builder is always connected.
 *   </dd>
 * </dl>
 */
public class MemoryNodeBuilder implements NodeBuilder {

    /**
     * Parent builder, or {@code null} for a root builder.
     */
    private final MemoryNodeBuilder parent;

    /**
     * Name of this child node within the parent builder,
     * or {@code null} for a root builder.
     */
    private final String name;

    /**
     * Root builder, or {@code this} for the root itself.
     */
    private final MemoryNodeBuilder root;

    /**
     * Internal revision counter that is incremented in the root builder
     * whenever anything changes in the tree below. Each builder instance
     * has its own copy of the revision counter, for quickly checking whether
     * any state changes are needed.
     */
    private long revision;

    /**
     * The base state of this builder, possibly non-existent if this builder
     * represents a new node that didn't yet exist in the base content tree.
     */
    private NodeState baseState;

    /**
     * The shared mutable state of connected builder instances, or
     * {@code null} until this builder has been connected.
     */
    private MutableNodeState writeState;

    /**
     * Creates a new in-memory node state builder.
     *
     * @param parent parent node state builder
     * @param name name of this node
     */
    protected MemoryNodeBuilder(MemoryNodeBuilder parent, String name) {
        this.parent = checkNotNull(parent);
        this.name = checkNotNull(name);

        this.root = parent.root;
        this.revision = -1;

        this.baseState = parent.baseState.getChildNode(name);
        this.writeState = null;
    }

    /**
     * Creates a new in-memory node state builder.
     *
     * @param base base state of the new builder
     */
    public MemoryNodeBuilder(@Nonnull NodeState base) {
        this.parent = null;
        this.name = null;

        this.root = this;
        this.revision = 0;

        this.baseState = checkNotNull(base);
        this.writeState = new MutableNodeState(baseState);
    }

    private boolean classInvariants() {
        boolean rootHasNoParent = isRoot() == (parent == null);
        boolean rootHasWriteState = root.writeState != null;
        return rootHasNoParent && rootHasWriteState;
    }

    private boolean isRoot() {
        return this == root;
    }

    /**
     * Return the base state of the named child. Assumes {@code read()} needs not be called.
     * @param name  name of the child
     * @return  base state of the child
     */
    private NodeState getBaseState(String name) {
        return baseState.getChildNode(name);
    }

    /**
     * Determine whether the base state has a child of the given name.
     * Assumes {@code read()} needs not be called.
     * @param name  name of the child
     * @return  {@code true} iff the base state has a child of the given name.
     */
    private boolean hasBaseState(String name) {
        return baseState.hasChildNode(name);
    }

    /**
     * Return the write state of the named child. Assumes {@code read()}, {@code write()} needs not be called.
     * @param name  name of the child
     * @return  base state of the child
     */
    private MutableNodeState getWriteState(String name) {
        if (writeState != null) {
            return writeState.nodes.get(name);
        }
        else {
            return null;
        }
    }

    /**
     * Determine whether this child exists at its direct parent.
     * @return {@code true} iff this child exists at its direct parent.
     */
    private boolean exists() {
        if (isRoot()) {
            return true;
        } else if (parent.writeState == null) {
            return parent.baseState.hasChildNode(name);
        } else {
            return parent.writeState.hasChildNode(name);
        }
    }

    /**
     * Update the state of this builder for reading.
     * @return  {@code true} is this reader is connected, {@code false} otherwise.
     */
    private boolean updateReadState() {
        if (revision != root.revision) {
            assert(!isRoot()); // root never gets here since revision == root.revision

            if (parent.updateReadState() && exists()) {
                // The builder could have been reset, need to re-get base state
                baseState = parent.getBaseState(name);

                // ... same for the write state
                writeState = parent.getWriteState(name);

                revision = root.revision;
                return true;
            }

            return false;
        }
        return writeState != null || baseState.exists();
    }

    @Nonnull
    private NodeState read() {
        checkState(updateReadState(), "This node has been removed or is disconnected");
        assert classInvariants();
        return writeState != null ? writeState : baseState;
    }

    @Nonnull
    private MutableNodeState write() {
        return write(root.revision + 1, false);
    }

    @Nonnull
    private MutableNodeState write(long newRevision, boolean reconnect) {
        // make sure that all revision numbers up to the root gets updated
        if (!isRoot()) {
            parent.write(newRevision, reconnect);
            checkState(reconnect || exists(), "This node has been removed");
        }

        if (writeState == null || revision != root.revision) {
            assert(!isRoot()); // root never gets here since revision == root.revision

            // The builder could have been reset, need to re-get base state
            baseState = parent.getBaseState(name);

            writeState = parent.getWriteState(name);
            if (writeState == null) {
                if (exists()) {
                    writeState = new MutableNodeState(baseState);
                }
                else {
                    writeState = new MutableNodeState(null);
                }
                assert parent.writeState != null; // guaranteed by called parent.write()
                parent.writeState.nodes.put(name, writeState);
            }
        }

        revision = newRevision;
        assert classInvariants();
        assert writeState != null;
        return writeState;
    }

    /**
     * Factory method for creating new child state builders. Subclasses may
     * override this method to control the behavior of child state builders.
     *
     * @return new builder
     */
    protected MemoryNodeBuilder createChildBuilder(String name) {
        return new MemoryNodeBuilder(this, name);
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

    //--------------------------------------------------------< NodeBuilder >---

    @Override
    public boolean isNew() {
        return !isRoot() && parent.isNew(name);
    }

    private boolean isNew(String name) {
        read();
        return !hasBaseState(name) && hasChildNode(name);
    }

    @Override
    public boolean isConnected() {
        return updateReadState();
    }

    @Override
    public boolean isModified() {
        read();
        if (writeState == null) {
            return false;
        }
        else {
            for (Entry<String, MutableNodeState> n : writeState.nodes.entrySet()) {
                if (n.getValue() == null) {
                    return true;
                }
                if (!(hasBaseState(n.getKey()))) {
                    return true;
                }
            }
            for (Entry<String, PropertyState> p : writeState.properties.entrySet()) {
                PropertyState pState = p.getValue();
                if (pState == null) {
                    return true;
                }
                if (!baseState.exists()
                        || !pState.equals(baseState.getProperty(p.getKey()))) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public NodeState getNodeState() {
        read();
        if (writeState != null) {
            return writeState.snapshot();
        } else {
            assert baseState.exists();
            return baseState;
        }
    }

    @Override
    public NodeState getBaseState() {
        read();
        return baseState;
    }

    @Override
    public void reset(NodeState newBase) {
        checkState(isRoot(), "Cannot reset a non-root builder");
        baseState = checkNotNull(newBase);
        writeState = new MutableNodeState(baseState);
        revision++;
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

    @Override @Nonnull
    public NodeBuilder setNode(String name, NodeState state) {
        write();

        MutableNodeState childState = getWriteState(name);
        if (childState == null) {
            writeState.nodes.remove(name);
            childState = createChildBuilder(name).write(root.revision + 1, true);
        }
        childState.reset(state);

        updated();
        return this;
    }

    @Override @Nonnull
    public NodeBuilder removeNode(String name) {
        write();

        if (writeState.base.getChildNode(name).exists()) {
            writeState.nodes.put(name, null);
        } else {
            writeState.nodes.remove(name);
        }

        updated();
        return this;
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
    public boolean hasProperty(String name) {
        return read().hasProperty(name);
    }

    @Override
    public PropertyState getProperty(String name) {
        return read().getProperty(name);
    }

    @Override
    public boolean getBoolean(String name) {
        PropertyState property = getProperty(name);
        return property != null
                && property.getType() == BOOLEAN
                && property.getValue(BOOLEAN);
    }

    @Override @CheckForNull
    public String getName(@Nonnull String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == NAME) {
            return property.getValue(NAME);
        } else {
            return null;
        }
    }

    @Override @CheckForNull
    public Iterable<String> getNames(@Nonnull String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == NAMES) {
            return property.getValue(NAMES);
        } else {
            return null;
        }
    }

    @Override @Nonnull
    public NodeBuilder removeProperty(String name) {
        write();

        if (writeState.base.hasProperty(name)) {
            writeState.properties.put(name, null);
        } else {
            writeState.properties.remove(name);
        }

        updated();
        return this;
    }

    @Override
    public NodeBuilder setProperty(PropertyState property) {
        write();
        writeState.properties.put(property.getName(), property);
        updated();
        return this;
    }

    @Override
    public <T> NodeBuilder setProperty(String name, T value) {
        setProperty(PropertyStates.createProperty(name, value));
        return this;
    }

    @Override
    public <T> NodeBuilder setProperty(String name, T value, Type<T> type) {
        setProperty(PropertyStates.createProperty(name, value, type));
        return this;
    }

    @Override
    public NodeBuilder child(String name) {
        read();
        MemoryNodeBuilder builder = createChildBuilder(name);

        boolean modified = writeState != null && (writeState.base != baseState || writeState.nodes.containsKey(name));
        if (!hasBaseState(name) || modified) {
            builder.write(root.revision + 1, true);
        }
        return builder;
    }

    /**
     * The <em>mutable</em> state being built. Instances of this class
     * are never passed beyond the containing {@code MemoryNodeBuilder},
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
        private final Map<String, PropertyState> properties = newHashMap();

        /**
         * Set of added, modified or removed ({@code null} value)
         * child nodes.
         */
        private final Map<String, MutableNodeState> nodes = newHashMap();

        public MutableNodeState(NodeState base) {
            if (base instanceof ModifiedNodeState) {
                ModifiedNodeState modified = (ModifiedNodeState) base;
                this.base = modified.getBaseState();
                modified.compareAgainstBaseState(new NodeStateDiff() {
                    @Override
                    public void propertyAdded(PropertyState after) {
                        properties.put(after.getName(), after);
                    }
                    @Override
                    public void propertyChanged(
                            PropertyState before, PropertyState after) {
                        properties.put(after.getName(), after);
                    }
                    @Override
                    public void propertyDeleted(PropertyState before) {
                        properties.put(before.getName(), null);
                    }
                    @Override
                    public void childNodeAdded(String name, NodeState after) {
                        nodes.put(name, new MutableNodeState(after));
                    }
                    @Override
                    public void childNodeChanged(
                            String name, NodeState before, NodeState after) {
                        nodes.put(name, new MutableNodeState(after));
                    }
                    @Override
                    public void childNodeDeleted(String name, NodeState before) {
                        nodes.put(name, null);
                    }
                });
            } else if (base != null) {
                this.base = base;
            } else {
                this.base = EMPTY_NODE;
            }
        }

        public NodeState snapshot() {
            Map<String, NodeState> nodes = newHashMap();
            for (Map.Entry<String, MutableNodeState> entry : this.nodes.entrySet()) {
                String name = entry.getKey();
                MutableNodeState node = entry.getValue();
                NodeState before = base.getChildNode(name);
                if (node == null) {
                    if (before != null) {
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

        void reset(NodeState newBase) {
            if (newBase instanceof ModifiedNodeState) {
                ModifiedNodeState modified = (ModifiedNodeState) newBase;
                base = modified.getBaseState();
                properties.clear();

                Iterator<Map.Entry<String, MutableNodeState>> iterator =
                        nodes.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, MutableNodeState> entry = iterator.next();
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
                    public void propertyAdded(PropertyState after) {
                        properties.put(after.getName(), after);
                    }
                    @Override
                    public void propertyChanged(
                            PropertyState before, PropertyState after) {
                        properties.put(after.getName(), after);
                    }
                    @Override
                    public void propertyDeleted(PropertyState before) {
                        properties.put(before.getName(), null);
                    }
                    @Override
                    public void childNodeAdded(String name, NodeState after) {
                        MutableNodeState cstate = nodes.get(name);
                        if (cstate != null) {
                            cstate.reset(after);
                        } else {
                            nodes.put(name, new MutableNodeState(after));
                        }
                    }
                    @Override
                    public void childNodeChanged(
                            String name, NodeState before, NodeState after) {
                        MutableNodeState cstate = nodes.get(name);
                        if (cstate != null) {
                            cstate.reset(after);
                        } else {
                            nodes.put(name, new MutableNodeState(after));
                        }
                    }
                    @Override
                    public void childNodeDeleted(String name, NodeState before) {
                        nodes.put(name, null);
                    }
                });
            } else {
                base = newBase;
                properties.clear();

                Iterator<Map.Entry<String, MutableNodeState>> iterator =
                        nodes.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, MutableNodeState> entry = iterator.next();
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

        @Override
        public String toString() {
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
            return true;
        }

        @Override
        public long getPropertyCount() {
            return withProperties(base, properties).getPropertyCount();
        }

        @Override
        public boolean hasProperty(String name) {
            return withProperties(base, properties).hasProperty(name);
        }

        @Override
        public PropertyState getProperty(String name) {
            return withProperties(base, properties).getProperty(name);
        }

        @Override @Nonnull
        public Iterable<? extends PropertyState> getProperties() {
            Map<String, PropertyState> copy = newHashMap(properties);
            return withProperties(base, copy).getProperties();
        }

        @Override
        public long getChildNodeCount() {
            return withNodes(base, nodes).getChildNodeCount();
        }

        @Override
        public boolean hasChildNode(String name) {
            checkNotNull(name);
            // checkArgument(!name.isEmpty()); TODO: should be caught earlier
            return withNodes(base, nodes).hasChildNode(name);
        }

        @Override
        public NodeState getChildNode(String name) {
            throw new UnsupportedOperationException();
        }

        @Override @Nonnull
        public Iterable<String> getChildNodeNames() {
            Map<String, MutableNodeState> copy = newHashMap(nodes);
            return withNodes(base, copy).getChildNodeNames();
        }

        @Override @Nonnull
        public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
            throw new UnsupportedOperationException();
        }

        @Override @Nonnull
        public NodeBuilder builder() {
            throw new UnsupportedOperationException();
        }

    }

}
