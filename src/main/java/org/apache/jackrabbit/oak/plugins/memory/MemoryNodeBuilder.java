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
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * In-memory node state builder. The following two builder states are used
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
     * Parent state builder reference, or {@code null} once this builder
     * has been connected.
     */
    private MemoryNodeBuilder parent;

    /**
     * Name of this child node within the parent builder, or {@code null}
     * once this builder has been connected.
     */
    private String name;

    /**
     * The read state of this builder. Originally the immutable base state
     * in an unconnected builder, or the shared mutable state
     * (see {@link #writeState}) once the this builder has been connected.
     */
    private NodeState readState;

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
     * @param base base state of this node
     */
    protected MemoryNodeBuilder(
            MemoryNodeBuilder parent, String name, NodeState base) {
        this.parent = checkNotNull(parent);
        this.name = checkNotNull(name);
        this.readState = checkNotNull(base);
        this.writeState = null;
    }

    /**
     * Creates a new in-memory node state builder.
     *
     * @param base base state of the new builder
     */
    public MemoryNodeBuilder(NodeState base) {
        this.parent = null;
        this.name = null;
        MutableNodeState mstate = new MutableNodeState(checkNotNull(base));
        this.readState = mstate;
        this.writeState = mstate;
    }

    private NodeState read() {
        if (writeState == null) {
            parent.read();
            MutableNodeState pstate = parent.writeState;
            if (pstate != null) {
                MutableNodeState mstate = pstate.nodes.get(name);
                if (mstate == null && pstate.nodes.containsKey(name)) {
                    throw new IllegalStateException(
                            "This builder refers to a node that has"
                            + " been removed from its parent.");
                }
                if (mstate != null) {
                    parent = null;
                    name = null;
                    readState = mstate;
                    writeState = mstate;
                }
            }
        }
        return readState;
    }

    private MutableNodeState write() {
        if (writeState == null) {
            MutableNodeState pstate = parent.write();
            MutableNodeState mstate = pstate.nodes.get(name);
            if (mstate == null) {
                if (pstate.nodes.containsKey(name)) {
                    throw new IllegalStateException(
                            "This builder refers to a node that has"
                            + " been removed from its parent.");
                }
                mstate = new MutableNodeState(readState);
                pstate.nodes.put(name, mstate);
            }
            parent = null;
            name = null;
            readState = mstate;
            writeState = mstate;
        }
        return writeState;
    }

    protected void reset(NodeState newBase) {
        write().reset(newBase);
    }

    /**
     * Factory method for creating new child state builders. Subclasses may
     * override this method to control the behavior of child state builders.
     *
     * @param child base state of the new builder, or {@code null}
     * @return new builder
     */
    protected MemoryNodeBuilder createChildBuilder(
            String name, NodeState child) {
        return new MemoryNodeBuilder(this, name, child);
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
        NodeState state = read();
        if (writeState != null) {
            return new ModifiedNodeState(writeState);
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

    @Override @Nonnull
    public NodeBuilder setNode(String name, NodeState state) {
        MutableNodeState mstate = write();

        MutableNodeState cstate = mstate.nodes.get(name);
        if (cstate != null) {
            cstate.reset(state);
        } else {
            mstate.nodes.remove(name);
            createChildBuilder(name, state).write();
        }

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
            mstate.props.put(name, null);
        } else {
            mstate.props.remove(name);
        }

        updated();
        return this;
    }

    @Override
    public NodeBuilder setProperty(PropertyState property) {
        MutableNodeState mstate = write();
        mstate.props.put(property.getName(), property);
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
        NodeState state = read();

        if (writeState == null) {
            NodeState base = state.getChildNode(name);
            if (base != null) {
                return createChildBuilder(name, base); // shortcut
            }
        }

        NodeState cbase = NULL_STATE;
        MutableNodeState mstate = write();
        MutableNodeState cstate = mstate.nodes.get(name);
        if (cstate != null) {
            cbase = cstate.base;
        } else if (mstate.nodes.containsKey(name)) {
            // The child node was removed earlier and we're creating
            // a new child with the same name. Remove the removal marker
            // so that the new child builder won't try reconnecting with
            // the previously removed node.
            mstate.nodes.remove(name);
        } else if (mstate.base.hasChildNode(name)) {
            return createChildBuilder(name, mstate.base.getChildNode(name));
        }

        MemoryNodeBuilder builder = createChildBuilder(name, cbase);
        builder.write();
        return builder;
    }

    /**
     * Filter for skipping property states with given names.
     */
    private static class SkipNamedProps implements Predicate<PropertyState> {

        private final Set<String> names;

        private SkipNamedProps(Set<String> names) {
            this.names = names;
        }

        @Override
        public boolean apply(PropertyState input) {
            return !names.contains(input.getName());
        }

    }

    /**
     * Filter for skipping child node states with given names.
     */
    private static class SkipNamedNodes implements Predicate<ChildNodeEntry> {

        private final Set<String> names;

        private SkipNamedNodes(Set<String> names) {
            this.names = names;
        }

        @Override
        public boolean apply(ChildNodeEntry input) {
            return !names.contains(input.getName());
        }

    }

    /**
     * The <em>mutable</em> state being built. Instances of this class
     * are never passed beyond the containing {@code MemoryNodeBuilder},
     * so it's not a problem that we intentionally break the immutability
     * assumption of the {@link NodeState} interface.
     */
    protected static class MutableNodeState extends AbstractNodeState {

        /**
         * The immutable base state.
         */
        protected NodeState base;

        /**
         * Set of added, modified or removed ({@code null} value)
         * property states.
         */
        protected final Map<String, PropertyState> props =
                Maps.newHashMap();

        /**
         * Set of added, modified or removed ({@code null} value)
         * child nodes.
         */
        protected final Map<String, MutableNodeState> nodes =
                Maps.newHashMap();

        public MutableNodeState(NodeState base) {
            this.base = checkNotNull(base);
        }

        private void reset(NodeState newBase) {
            base = newBase;
            props.clear();

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

        public NodeState getBaseState() {
            return base;
        }

        //-----------------------------------------------------< NodeState >--

        @Override
        public long getPropertyCount() {
            long count = base.getPropertyCount();

            for (Map.Entry<String, PropertyState> entry : props.entrySet()) {
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
            PropertyState property = props.get(name);
            if (property != null || props.containsKey(name)) {
                return property;
            }

            return base.getProperty(name);
        }

        @Override
        public Iterable<? extends PropertyState> getProperties() {
            if (props.isEmpty()) {
                return base.getProperties(); // shortcut
            } else {
                return internalGetProperties();
            }
        }

        protected Iterable<? extends PropertyState> internalGetProperties() {
            Predicate<PropertyState> unmodifiedFilter =
                    new SkipNamedProps(ImmutableSet.copyOf(props.keySet()));
            Predicate<PropertyState> modifiedFilter = Predicates.notNull();
            return Iterables.concat(
                    Iterables.filter(base.getProperties(), unmodifiedFilter),
                    ImmutableList.copyOf(Collections2.filter(
                            props.values(), modifiedFilter)));
        }

        @Override
        public long getChildNodeCount() {
            long count = base.getChildNodeCount();

            for (Map.Entry<String, MutableNodeState> entry : nodes.entrySet()) {
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
            MutableNodeState node = nodes.get(name);
            if (node != null) {
                return true;
            } else if (nodes.containsKey(name)) {
                return false;
            }

            return base.hasChildNode(name);
        }

        @Override
        public Iterable<String> getChildNodeNames() {
            if (nodes.isEmpty()) {
                return base.getChildNodeNames(); // shortcut
            } else {
                return internalGetChildNodeNames();
            }
        }

        protected Iterable<String> internalGetChildNodeNames() {
            Iterable<String> unmodified = base.getChildNodeNames();
            Predicate<String> unmodifiedFilter = Predicates.not(Predicates.in(
                    ImmutableSet.copyOf(nodes.keySet())));
            Set<String> modified = ImmutableSet.copyOf(
                    Maps.filterValues(nodes, Predicates.notNull()).keySet());
            return Iterables.concat(
                    Iterables.filter(unmodified, unmodifiedFilter),
                    modified);
        }

        @Override
        public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
            if (nodes.isEmpty()) {
                return base.getChildNodeEntries(); // shortcut
            } else {
                return internalGetChildNodeEntries();
            }
        }

        protected Iterable<? extends ChildNodeEntry> internalGetChildNodeEntries() {
            Iterable<? extends ChildNodeEntry> unmodified =
                    base.getChildNodeEntries();
            Predicate<ChildNodeEntry> unmodifiedFilter =
                    new SkipNamedNodes(ImmutableSet.copyOf(nodes.keySet()));

            List<ChildNodeEntry> modified = Lists.newArrayList();
            for (Map.Entry<String, MutableNodeState> entry : nodes.entrySet()) {
                MutableNodeState cstate = entry.getValue();
                if (cstate != null) {
                    modified.add(new MemoryChildNodeEntry(
                            entry.getKey(),
                            new ModifiedNodeState(cstate)));
                }
            }

            return Iterables.concat(
                    Iterables.filter(unmodified, unmodifiedFilter),
                    modified);
        }

        @Override
        public NodeBuilder builder() {
            return new ModifiedNodeState(this).builder();
        }

        /**
         * Since we keep track of an explicit base node state for a
         * {@link ModifiedNodeState} instance, we can do this in two steps:
         * first compare the base states to each other (often a fast operation),
         * ignoring all changed properties and child nodes for which we have
         * further modifications, and then compare all the modified properties
         * and child nodes to those in the given base state.
         */
        @Override
        public void compareAgainstBaseState(
                NodeState base, final NodeStateDiff diff) {
            this.base.compareAgainstBaseState(base, new NodeStateDiff() {
                @Override
                public void propertyAdded(PropertyState after) {
                    if (!props.containsKey(after.getName())) {
                        diff.propertyAdded(after);
                    }
                }
                @Override
                public void propertyChanged(
                        PropertyState before, PropertyState after) {
                    if (!props.containsKey(before.getName())) {
                        diff.propertyChanged(before, after);
                    }
                }
                @Override
                public void propertyDeleted(PropertyState before) {
                    if (!props.containsKey(before.getName())) {
                        diff.propertyDeleted(before);
                    }
                }
                @Override
                public void childNodeAdded(String name, NodeState after) {
                    if (!nodes.containsKey(name)) {
                        diff.childNodeAdded(name, after);
                    }
                }
                @Override
                public void childNodeChanged(String name, NodeState before, NodeState after) {
                    if (!nodes.containsKey(name)) {
                        diff.childNodeChanged(name, before, after);
                    }
                }
                @Override
                public void childNodeDeleted(String name, NodeState before) {
                    if (!nodes.containsKey(name)) {
                        diff.childNodeDeleted(name, before);
                    }
                }
            });

            for (Map.Entry<String, PropertyState> entry : props.entrySet()) {
                PropertyState before = base.getProperty(entry.getKey());
                PropertyState after = entry.getValue();
                if (before == null && after == null) {
                    // do nothing
                } else if (after == null) {
                    diff.propertyDeleted(before);
                } else if (before == null) {
                    diff.propertyAdded(after);
                } else if (!before.equals(after)) {
                    diff.propertyChanged(before, after);
                }
            }

            for (Map.Entry<String, MutableNodeState> entry : nodes.entrySet()) {
                String name = entry.getKey();
                NodeState before = base.getChildNode(name);
                NodeState after = entry.getValue();
                if (before == null && after == null) {
                    // do nothing
                } else if (after == null) {
                    diff.childNodeDeleted(name, before);
                } else if (before == null) {
                    diff.childNodeAdded(name, after);
                } else if (!before.equals(after)) {
                    diff.childNodeChanged(name, before, after);
                }
            }
        }

    }

    /**
     * Immutable snapshot of a mutable node state.
     */
    protected static class ModifiedNodeState extends MutableNodeState {

        public ModifiedNodeState(MutableNodeState mstate) {
            super(mstate.base);
            props.putAll(mstate.props);
            for (Map.Entry<String, MutableNodeState> entry
                    : mstate.nodes.entrySet()) {
                String name = entry.getKey();
                MutableNodeState node = entry.getValue();
                if (node != null) {
                    nodes.put(name, new ModifiedNodeState(node));
                } else {
                    nodes.put(name, null);
                }
            }
        }

        @Override
        public NodeBuilder builder() {
            return new MemoryNodeBuilder(this);
        }

        //----------------------------------------------< MutableNodeState >--

        @Override
        protected Iterable<? extends PropertyState> internalGetProperties() {
            Predicate<PropertyState> unmodifiedFilter =
                    new SkipNamedProps(props.keySet());
            Predicate<PropertyState> modifiedFilter = Predicates.notNull();
            return Iterables.concat(
                    Iterables.filter(base.getProperties(), unmodifiedFilter),
                    Collections2.filter(props.values(), modifiedFilter));
        }

        @Override
        protected Iterable<String> internalGetChildNodeNames() {
            Iterable<String> unmodified = base.getChildNodeNames();
            Predicate<String> unmodifiedFilter =
                    Predicates.not(Predicates.in(nodes.keySet()));
            return Iterables.concat(
                    Iterables.filter(unmodified, unmodifiedFilter),
                    Maps.filterValues(nodes, Predicates.notNull()).keySet());
        }

        @Override
        protected Iterable<? extends ChildNodeEntry> internalGetChildNodeEntries() {
            Iterable<? extends ChildNodeEntry> unmodified =
                    base.getChildNodeEntries();
            Predicate<ChildNodeEntry> unmodifiedFilter =
                    new SkipNamedNodes(nodes.keySet());
            Map<String, MutableNodeState> modified =
                    Maps.filterValues(nodes, Predicates.notNull());
            return Iterables.concat(
                    Iterables.filter(unmodified, unmodifiedFilter),
                    MemoryChildNodeEntry.iterable(modified.entrySet()));
        }

    }

}
