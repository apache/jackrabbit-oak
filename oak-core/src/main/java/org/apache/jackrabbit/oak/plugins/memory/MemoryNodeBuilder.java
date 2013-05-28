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

import static com.google.common.base.Objects.toStringHelper;
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
 * instances for tracking <em>uncommitted changes</em> and on {@code Head}
 * instances for tracking the connectedness of the builder. A builder keeps
 * a reference to the parent builder and knows its own name, which is used
 * to check for relevant changes in its parent builder and update its state
 * accordingly.
 * <p>
 * A builder is in one of three possible states, which is tracked within
 * its {@code Head} instance:
 * <dl>
 *   <dt><em>unconnected</em></dt>
 *   <dd>
 *     A child builder with no content changes starts in this state.
 *     Before each access the unconnected builder checks its parent for
 *     relevant changes.
 *   </dd>
 *   <dt><em>connected</em></dt>
 *   <dd>
 *     Once a builder is first modified, it switches to the connected state
 *     and records all modification in a shared {@code MutableNodeState}
 *     instance. Before each access the connected builder checks whether its
 *     parents base state has been reset and if so, resets its own base state
 *     accordingly.
 *   </dd>
 *   <dt><em>root</em></dt>
 *   <dd>
 *     Same as the connected state but only the root of the builder hierarchy
 *     can have this state.
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
     * Root builder, or {@code this} for the root builder itself.
     */
    private final MemoryNodeBuilder rootBuilder;

    /**
     * Internal revision counter for the base state of this builder. The counter
     * is incremented in the root builder whenever its base state is reset.
     * Each builder instance has its own copy of this revision counter for
     * quickly checking whether its base state needs updating.
     * @see #reset(org.apache.jackrabbit.oak.spi.state.NodeState)
     * @see #base()
     */
    private long baseRevision;

    /**
     * The base state of this builder, possibly non-existent if this builder
     * represents a new node that didn't yet exist in the base content tree.
     */
    private NodeState base;

    /**
     * Head of this builder
     */
    private Head head;

    /**
     * Creates a new in-memory child builder.
     * @param parent parent builder
     * @param name name of this node
     */
    private MemoryNodeBuilder(MemoryNodeBuilder parent, String name) {
        this.parent = parent;
        this.name = name;
        this.rootBuilder = parent.rootBuilder;
        this.head = new UnconnectedHead();
    }

    /**
     * Creates a new in-memory node state builder rooted at
     * and based on the passed {@code base} state.
     * @param base base state of the new builder
     */
    public MemoryNodeBuilder(@Nonnull NodeState base) {
        this.parent = null;
        this.name = null;
        this.rootBuilder = this;

        // ensure child builder's base is updated on first access
        this.baseRevision = 1;
        this.base = checkNotNull(base);

        this.head = new RootHead();
    }

    /**
     * @return  {@code true} iff this is the root builder
     */
    private boolean isRoot() {
        return this == rootBuilder;
    }

    /**
     * Update the base state of this builder by recursively retrieving it
     * from its parent builder.
     * @return  base state of this builder
     */
    @Nonnull
    private NodeState base() {
        if (rootBuilder.baseRevision != baseRevision) {
            base = parent.base().getChildNode(name);
            baseRevision = rootBuilder.baseRevision;
        }
        return base;
    }

    /**
     * Factory method for creating new child state builders. Subclasses may
     * override this method to control the behavior of child state builders.
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
        return head.getNodeState();
    }

    @Override
    public NodeState getBaseState() {
        return base();
    }

    @Override
    public boolean exists() {
        return head.exists();
    }

    @Override
    public boolean isNew() {
        return !isRoot() && !parent.base().hasChildNode(name) && parent.hasChildNode(name);
    }

    @Override
    public boolean isModified() {
        return head.isModified(base());
    }

    @Override
    public void reset(NodeState newBase) {
        base = checkNotNull(newBase);
        baseRevision++;
        head.reset();
    }

    @Override
    public long getChildNodeCount() {
        return head.getChildNodeCount();
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return head.getChildNodeNames();
    }

    @Override
    public boolean hasChildNode(String name) {
        return head.hasChildNode(checkNotNull(name));
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
        checkState(exists(), "This builder does not exist: " + name);
        head.setChildNode(checkNotNull(name), checkNotNull(state));
        MemoryNodeBuilder builder = createChildBuilder(name);
        updated();
        return builder;
    }

    @Override
    public boolean remove() {
        if (exists()) {
            head.remove();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public long getPropertyCount() {
        return head.getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return head.getProperties();
    }

    @Override
    public boolean hasProperty(String name) {
        return head.hasProperty(checkNotNull(name));
    }

    @Override
    public PropertyState getProperty(String name) {
        return head.getProperty(checkNotNull(name));
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
        checkState(exists(), "This builder does not exist: " + name);
        head.setProperty(checkNotNull(property));
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
        checkState(exists(), "This builder does not exist: " + name);
        if (head.removeProperty(checkNotNull(name))) {
            updated();
        }
        return this;
    }

    /**
     * @return path of this builder. For debugging purposes only
     */
    private String getPath() {
        return parent == null ? "/" : getPath(new StringBuilder()).toString();
    }

    private StringBuilder getPath(StringBuilder parentPath) {
        return parent == null ? parentPath : parent.getPath(parentPath).append('/').append(name);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("path", getPath()).toString();
    }

    //------------------------------------------------------------< Head >---

    /**
     * The subclasses of this abstract base class represent the different
     * states builders can have: <em>unconnected</em>, <em>connected</em>,
     * and <em>root</em>. {@code MemoryNodeBuilder} implements most of its
     * functionality by forwarding to the methods of {@code Head} instances
     * where the actual type of {@code Head} determines the behaviour associated
     * with the current state.
     */
    private abstract class Head {
        protected long revision;
        protected NodeState state;

        protected abstract NodeState read();
        protected abstract MutableNodeState write();

        public NodeState getNodeState() {
            NodeState state = read();
            return state instanceof MutableNodeState
                    ? ((MutableNodeState) state).snapshot()
                    : state;
        }

        public boolean exists() {
            return read().exists();
        }

        public boolean isModified(NodeState base) {
            NodeState state = read();
            return state instanceof MutableNodeState && ((MutableNodeState) state).isModified(base);
        }

        public void reset() {
            throw new IllegalStateException("Cannot reset a non-root builder");
        }

        public long getChildNodeCount() {
            return read().getChildNodeCount();
        }

        public Iterable<String> getChildNodeNames() {
            return read().getChildNodeNames();
        }

        public boolean hasChildNode(String name) {
            return read().hasChildNode(name);
        }

        public void setChildNode(String name, NodeState nodeState) {
            write().setChildNode(name, nodeState);
        }

        public void remove() {
            head.write();
            parent.head.write().removeChildNode(name);
        }

        public long getPropertyCount() {
            return read().getPropertyCount();
        }

        public Iterable<? extends PropertyState> getProperties() {
            return read().getProperties();
        }

        public boolean hasProperty(String name) {
            return read().hasProperty(name);
        }

        public PropertyState getProperty(String name) {
            return read().getProperty(name);
        }

        public void setProperty(PropertyState propertyState) {
            write().setProperty(propertyState);
        }

        public boolean removeProperty(String name) {
            return write().removeProperty(name);
        }
    }

    private class ConnectedHead extends Head {
        @Override
        protected MutableNodeState read() {
            if (revision != rootBuilder.baseRevision) {
                // the root builder's base state has been reset: re-get
                // state from parent.
                MutableNodeState parentState = (MutableNodeState) parent.head.read();
                state = parentState.getMutableChildNode(name);
                revision = rootBuilder.baseRevision;
            }
            return (MutableNodeState) state;
        }

        @Override
        protected MutableNodeState write() {
            // incrementing the root revision triggers unconnected
            // child state to re-get their state on next access
            rootBuilder.head.revision++;
            return read();
        }

        @Override
        public String toString() {
            return toStringHelper(this).add("path", getPath()).toString();
        }
    }

    private class UnconnectedHead extends Head {
        @Override
        protected NodeState read() {
            if (revision != rootBuilder.head.revision) {
                // root revision changed: recursively re-get state from parent
                NodeState parentState = parent.head.read();
                state = parentState.getChildNode(name);
                revision = rootBuilder.head.revision;
            }
            return state;
        }

        @Override
        protected MutableNodeState write() {
            // switch to connected state recursively up to the parent
            parent.head.write();
            return (head = new ConnectedHead()).write();
        }

        @Override
        public String toString() {
            return toStringHelper(this).add("path", getPath()).toString();
        }
    }

    private class RootHead extends Head {
        public RootHead() {
            // ensure updating of child builders on first access
            reset();
        }

        @Override
        protected MutableNodeState read() {
            return (MutableNodeState) state;
        }

        @Override
        protected MutableNodeState write() {
            return (MutableNodeState) state;
        }

        @Override
        public final void reset() {
            state = new MutableNodeState(base);
            revision++;
        }

        @Override
        public String toString() {
            return toStringHelper(this).add("path", getPath()).toString();
        }
    }

}
