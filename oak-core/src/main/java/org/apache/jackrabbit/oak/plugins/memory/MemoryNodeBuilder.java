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

    private long baseRevision;

    /**
     * The base state of this builder, possibly non-existent if this builder
     * represents a new node that didn't yet exist in the base content tree.
     */
    private NodeState base;

    private long headRevision;

    /**
     * The shared mutable state of connected builder instances, or
     * {@code null} until this builder has been connected.
     */
    private MutableNodeState head;

    /**
     * Creates a new in-memory node state builder.
     *
     * @param parent parent node state builder
     * @param name name of this node
     */
    private MemoryNodeBuilder(MemoryNodeBuilder parent, String name) {
        this.parent = parent;
        this.name = name;
        this.root = parent.root;
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

        this.baseRevision = 1;
        this.base = checkNotNull(base);

        this.headRevision = 1;
        this.head = new MutableNodeState(this.base);
    }

    private boolean isRoot() {
        return this == root;
    }

    private void checkConnected() {
        if (!isConnected()) {
            throw new IllegalStateException("This builder is not connected");
        }
    }

    @Nonnull
    private NodeState base() {
        if (root.baseRevision != baseRevision) {
            base = parent.base().getChildNode(name);
            baseRevision = root.baseRevision;
        }
        return base;
    }

    @Nonnull
    private MutableNodeState read() {
        if (headRevision != root.headRevision) {
            assert !isRoot() : "root should have headRevision == root.headRevision";
            head = parent.read().getChildNode(name, false);
            headRevision = root.headRevision;
        }
        return head;
    }

    @Nonnull
    private MutableNodeState write() {
        return write(root.headRevision + 1);
    }

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
        checkConnected();
        return read().snapshot();
    }

    @Override
    public NodeState getBaseState() {
        return base();
    }

    @Override
    public boolean exists() {
        checkConnected();
        return read().exists();
    }

    @Override
    public boolean isNew() {
        checkConnected();
        return !isRoot() && !parent.base().hasChildNode(name) && parent.hasChildNode(name);
    }

    @Override
    public boolean isConnected() {
        return isRoot() || parent.read().isConnected(name);
    }

    @Override
    public boolean isModified() {
        checkConnected();
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
        checkConnected();
        return read().getChildNodeCount();
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        checkConnected();
        return read().getChildNodeNames();
    }

    @Override
    public boolean hasChildNode(String name) {
        checkConnected();
        return read().hasChildNode(checkNotNull(name));
    }

    @Override
    public NodeBuilder child(String name) {
        if (hasChildNode(name)) {
            return getChildNode(name);
        }
        else {
            return setChildNode(name);
        }
    }

    @Override
    public NodeBuilder getChildNode(String name) {
        checkConnected();
        return createChildBuilder(checkNotNull(name));
    }

    @Override
    public NodeBuilder setChildNode(String name) {
        return setChildNode(checkNotNull(name), EMPTY_NODE);
    }

    @Override
    public NodeBuilder setChildNode(String name, NodeState state) {
        checkConnected();
        write().setChildNode(checkNotNull(name), checkNotNull(state));
        MemoryNodeBuilder builder = createChildBuilder(name);
        updated();
        return builder;
    }

    @Override
    public NodeBuilder removeChildNode(String name) {
        checkConnected();
        if (write().removeChildNode(checkNotNull(name))) {
            updated();
        }
        return this;
    }

    @Override
    public long getPropertyCount() {
        checkConnected();
        return read().getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        checkConnected();
        return read().getProperties();
    }

    @Override
    public boolean hasProperty(String name) {
        checkConnected();
        return read().hasProperty(checkNotNull(name));
    }

    @Override
    public PropertyState getProperty(String name) {
        checkConnected();
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
        checkConnected();
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
        checkConnected();
        if (write().removeProperty(checkNotNull(name))) {
            updated();
        }
        return this;
    }

}
