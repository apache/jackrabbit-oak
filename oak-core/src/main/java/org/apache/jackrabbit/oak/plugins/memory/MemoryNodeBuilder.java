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
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * In-memory node state builder.
 * <p>
 * A {@code MemoryNodeBuilder} instance tracks uncommitted changes without
 * relying on weak references or requiring hard references on the entire
 * accessed subtree. It does this by relying on {@code MutableNodeState}
 * instances for tracking <em>uncommitted changes</em>. A child builders
 * keeps a reference to its parent builder and knows its own name. Before
 * each access the builder checks the mutable state of its parent for
 * relevant changes and updates its own mutable state.
 * <p>
 * A {@code MutableNodeState} instance does not keep a reference to its
 * parent state. It only keeps references to children that have been
 * modified. Instances representing an unmodified child are created on
 * the fly without keeping a reference. This effectively ensures that
 * such an instance can be GC'ed once no node builder references it
 * anymore.
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
     * Internal revision counter for the base state of this builder. The counter
     * is incremented in the root builder whenever its base state is reset.
     * Each builder instance has its own copy of this revision counter for
     * quickly checking whether its base state needs updating.
     * @see #reset(org.apache.jackrabbit.oak.spi.state.NodeState)
     */
    private long baseRevision;

    /**
     * The base state of this builder, possibly non-existent if this builder
     * represents a new node that didn't yet exist in the base content tree.
     * @see #base()
     */
    private NodeState base;

    /**
     * Internal revision counter for the head state of this builder. The counter
     * is incremented in the root builder whenever anything changes in the tree
     * below. Each builder instance has its own copy of this revision counter for
     * quickly checking whether its head state needs updating.
     */
    private long headRevision;

    /**
     * The shared mutable state this builder.
     * @see #write()
     * @see #read()
     */
    private MutableNodeState head;

    /**
     * Creates a new in-memory child builder.
     * @param parent parent builder
     * @param name name of this node
     */
    private MemoryNodeBuilder(MemoryNodeBuilder parent, String name) {
        this.parent = parent;
        this.name = name;
        this.root = parent.root;
    }

    /**
     * Creates a new in-memory node state builder rooted at
     * and based on the passed {@code base} state.
     * @param base base state of the new builder
     */
    public MemoryNodeBuilder(@Nonnull NodeState base) {
        this.parent = null;
        this.name = null;
        this.root = this;

        // ensure base is updated on next call to base()
        this.baseRevision = 1;
        this.base = checkNotNull(base);

        // ensure head is updated on next call to read() or write()
        this.headRevision = 1;
        this.head = new MutableNodeState(this.base);
    }

    private boolean isRoot() {
        return this == root;
    }

    /**
     * Update the base state of this builder by recursively retrieving it
     * from the parent builder.
     * @return  base state of this builder
     */
    @Nonnull
    private NodeState base() {
        if (root.baseRevision != baseRevision) {
            base = parent.base().getChildNode(name);
            baseRevision = root.baseRevision;
        }
        return base;
    }

    /**
     * Update the head state of this builder by recursively retrieving it
     * from the parent builder.
     * @return  head state of this builder
     */
    @Nonnull
    private MutableNodeState read() {
        if (headRevision != root.headRevision) {
            assert !isRoot() : "root should have headRevision == root.headRevision";
            head = parent.read().getChildNode(name, false);
            headRevision = root.headRevision;
        }
        return head;
    }

    /**
     * Update the head state of this builder by recursively retrieving it
     * from the parent builder and increment the head revision of the root
     * builder ensuring subsequent calls to {@link #read()} result in updating
     * of the respective head states.
     * @return  head state of this builder
     */
    @Nonnull
    private MutableNodeState write() {
        // TODO avoid traversing the parent hierarchy twice: once for exist and once for write
        if (!exists()) {
            throw new IllegalStateException("This builder does not exist: " + name);
        }
        return write(root.headRevision + 1);
    }

    /**
     * Recursive helper method to {@link #write()}. Don't call directly.
     */
    @Nonnull
    private MutableNodeState write(long newRevision) {
        if (headRevision != newRevision && !isRoot()) {
            head = parent.write(newRevision).getChildNode(name, true);
            headRevision = newRevision;
        }
        root.headRevision = newRevision;
        return head;
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
    public NodeState getNodeState() {
        return read().snapshot();
    }

    @Override
    public NodeState getBaseState() {
        return base();
    }

    @Override
    public boolean exists() {
        return read().exists();
    }

    @Override
    public boolean isNew() {
        return !isRoot() && !parent.base().hasChildNode(name) && parent.hasChildNode(name);
    }

    @Override
    public boolean isModified() {
        return read().isModified(base());
    }

    @Override
    public void reset(NodeState newBase) {
        checkState(isRoot(), "Cannot reset a non-root builder");
        base = checkNotNull(newBase);
        root.baseRevision++;
        root.headRevision++;
        head = new MutableNodeState(base);
    }

    @Override
    public long getChildNodeCount() {
        return read().getChildNodeCount();
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return read().getChildNodeNames();
    }

    @Override
    public boolean hasChildNode(String name) {
        return read().hasChildNode(checkNotNull(name));
    }

    @Override
    public NodeBuilder child(String name) {
        if (hasChildNode(name)) {
            return getChildNode(name);
        } else {
            return setChildNode(name);
        }
    }

    @Override
    public NodeBuilder getChildNode(String name) {
        return createChildBuilder(checkNotNull(name));
    }

    @Override
    public NodeBuilder setChildNode(String name) {
        return setChildNode(checkNotNull(name), EMPTY_NODE);
    }

    @Override
    public NodeBuilder setChildNode(String name, NodeState state) {
        write().setChildNode(checkNotNull(name), checkNotNull(state));
        MemoryNodeBuilder builder = createChildBuilder(name);
        updated();
        return builder;
    }

    @Override
    public boolean remove() {
        if (!exists()) {
            return false;
        } else {
            write();
            parent.head.removeChildNode(name);
            updated();
            return true;
        }
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
        return read().hasProperty(checkNotNull(name));
    }

    @Override
    public PropertyState getProperty(String name) {
        return read().getProperty(checkNotNull(name));
    }

    @Override
    public boolean getBoolean(String name) {
        PropertyState property = getProperty(name);
        return property != null
                && property.getType() == BOOLEAN
                && property.getValue(BOOLEAN);
    }

    @Override
    public String getName(@Nonnull String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == NAME) {
            return property.getValue(NAME);
        } else {
            return null;
        }
    }

    @Override
    public Iterable<String> getNames(@Nonnull String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == NAMES) {
            return property.getValue(NAMES);
        } else {
            return emptyList();
        }
    }

    @Override
    public NodeBuilder setProperty(PropertyState property) {
        write().setProperty(checkNotNull(property));
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
    public NodeBuilder removeProperty(String name) {
        if (write().removeProperty(checkNotNull(name))) {
            updated();
        }
        return this;
    }

}
