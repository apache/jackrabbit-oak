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

import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.with;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.withNodes;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.withProperties;

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

    private static final NodeState NULL_STATE = new MemoryNodeState(
            ImmutableMap.<String, PropertyState>of(),
            ImmutableMap.<String, NodeState>of());

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
     * The base state of this builder, or {@code null} if this builder
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

        this.baseState = null;
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

    private NodeState read() {
        if (revision != root.revision) {
            assert(parent != null); // root never gets here
            parent.read();

            // The builder could have been reset, need to re-get base state
            if (parent.baseState != null) {
                baseState = parent.baseState.getChildNode(name);
            } else {
                baseState = null;
            }

            // ... same for the write state
            if (parent.writeState != null) {
                writeState = parent.writeState.nodes.get(name);
                if (writeState == null
                        && parent.writeState.nodes.containsKey(name)) {
                    throw new IllegalStateException(
                            "This node has been removed");
                }
            } else {
                writeState = null;
            }

            revision = root.revision;
        }
        if (writeState != null) {
            return writeState;
        } else if (baseState != null) {
            return baseState;
        } else {
            return NULL_STATE;
        }
    }

    private MutableNodeState write() {
        return write(root.revision + 1);
    }

    private MutableNodeState write(long newRevision) {
        if (writeState == null || revision != root.revision) {
            assert(parent != null); // root never gets here
            parent.write(newRevision);

            // The builder could have been reset, need to re-get base state
            if (parent.baseState != null) {
                baseState = parent.baseState.getChildNode(name);
            } else {
                baseState = null;
            }

            assert parent.writeState != null; // we just called parent.write()
            writeState = parent.writeState.nodes.get(name);
            if (writeState == null) {
                if (parent.writeState.nodes.containsKey(name)) {
                    throw new IllegalStateException(
                            "This node has been removed");
                } else {
                    // need to make this node writable
                    NodeState base = baseState;
                    if (base == null) {
                        base = NULL_STATE;
                    }
                    writeState = new MutableNodeState(base);
                    parent.writeState.nodes.put(name, writeState);
                }
            }
        } else if (parent != null) {
            // make sure that all revision numbers up to the root gets updated
            parent.write(newRevision);
        }
        revision = newRevision;
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

    protected void compareAgainstBaseState(NodeStateDiff diff) {
        NodeState state = read();
        if (writeState != null) {
            writeState.compareAgainstBaseState(state, diff);
        }
    }

    //--------------------------------------------------------< NodeBuilder >---

    @Override
    public NodeState getNodeState() {
        read();
        if (writeState != null) {
            return writeState.snapshot();
        } else {
            // FIXME this assertion might fail when getNodeState() is called on a removed node.
            assert baseState != null; // guaranteed by read()
            return baseState;
        }
    }

    @Override
    public NodeState getBaseState() {
        return baseState;
    }

    @Override
    public void reset(NodeState newBase) {
        if (this == root) {
            baseState = checkNotNull(newBase);
            writeState = new MutableNodeState(baseState);
            revision++;
        } else {
            throw new IllegalStateException("Cannot reset a non-root builder");
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

    @Override @Nonnull
    public NodeBuilder setNode(String name, NodeState state) {
        write();

        MutableNodeState childState = writeState.nodes.get(name);
        if (childState == null) {
            writeState.nodes.remove(name);
            childState = createChildBuilder(name).write();
        }
        childState.reset(state);

        updated();
        return this;
    }

    @Override @Nonnull
    public NodeBuilder removeNode(String name) {
        MutableNodeState mstate = write();

        if (mstate.base.getChildNode(name) != null) {
            mstate.nodes.put(name, null);
        } else {
            mstate.nodes.remove(name);
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
    public PropertyState getProperty(String name) {
        return read().getProperty(name);
    }

    @Override @Nonnull
    public NodeBuilder removeProperty(String name) {
        MutableNodeState mstate = write();

        if (mstate.base.getProperty(name) != null) {
            mstate.properties.put(name, null);
        } else {
            mstate.properties.remove(name);
        }

        updated();
        return this;
    }

    @Override
    public NodeBuilder setProperty(PropertyState property) {
        MutableNodeState mstate = write();
        mstate.properties.put(property.getName(), property);
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
        read(); // shortcut when dealing with a read-only child node
        if (baseState != null
                && baseState.hasChildNode(name)
                && (writeState == null || !writeState.nodes.containsKey(name))) {
            return createChildBuilder(name);
        }

        // no read-only child node found, switch to write mode
        write();
        assert writeState != null; // guaranteed by write()

        NodeState childBase = null;
        if (baseState != null) {
            childBase = baseState.getChildNode(name);
        }

        if (writeState.nodes.get(name) == null) {
            if (writeState.nodes.containsKey(name)) {
                // The child node was removed earlier and we're creating
                // a new child with the same name. Use the null state to
                // prevent the previous child state from re-surfacing.
                childBase = null;
            }
            writeState.nodes.put(name, new MutableNodeState(childBase));
        }

        MemoryNodeBuilder builder = createChildBuilder(name);
        builder.write();
        return builder;
    }

    /**
     * The <em>mutable</em> state being built. Instances of this class
     * are never passed beyond the containing {@code MemoryNodeBuilder},
     * so it's not a problem that we intentionally break the immutability
     * assumption of the {@link NodeState} interface.
     */
    private class MutableNodeState extends AbstractNodeState {

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
         * Set of added, modified or removed ({@code null} value)
         * child nodes.
         */
        private final Map<String, MutableNodeState> nodes =
                Maps.newHashMap();

        public MutableNodeState(NodeState base) {
            if (base != null) {
                this.base = base;
            } else {
                this.base = MemoryNodeBuilder.NULL_STATE;
            }
        }

        public NodeState snapshot() {
            Map<String, NodeState> nodes = Maps.newHashMap();
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
            return with(base, Maps.newHashMap(this.properties), nodes);
        }

        void reset(NodeState newBase) {
            base = newBase;
            properties.clear();

            Iterator<Map.Entry<String, MutableNodeState>> iterator =
                    nodes.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, MutableNodeState> entry = iterator.next();
                MutableNodeState cstate = entry.getValue();
                NodeState cbase = newBase.getChildNode(entry.getKey());
                if (cbase == null || cstate == null) {
                    iterator.remove();
                } else {
                    cstate.reset(cbase);
                }
            }
        }

        //-----------------------------------------------------< NodeState >--

        @Override
        public long getPropertyCount() {
            return withProperties(base, properties).getPropertyCount();
        }

        @Override
        public PropertyState getProperty(String name) {
            return withProperties(base, properties).getProperty(name);
        }

        @Override @Nonnull
        public Iterable<? extends PropertyState> getProperties() {
            Map<String, PropertyState> copy = Maps.newHashMap(properties);
            return withProperties(base, copy).getProperties();
        }

        @Override
        public long getChildNodeCount() {
            return withNodes(base, nodes).getChildNodeCount();
        }

        @Override
        public boolean hasChildNode(String name) {
            return withNodes(base, nodes).hasChildNode(name);
        }

        @Override
        public NodeState getChildNode(String name) {
            return withNodes(base, nodes).getChildNode(name); // mutable
        }

        @Override @Nonnull
        public Iterable<String> getChildNodeNames() {
            Map<String, MutableNodeState> copy = Maps.newHashMap(nodes);
            return withNodes(base, copy).getChildNodeNames();
        }

        @Override
        public void compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
            with(this.base, properties, nodes).compareAgainstBaseState(base, diff);
        }

        @Override @Nonnull
        public NodeBuilder builder() {
            throw new UnsupportedOperationException();
        }

    }

}
