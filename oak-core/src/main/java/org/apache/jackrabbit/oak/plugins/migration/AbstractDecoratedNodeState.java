/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.migration;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import static com.google.common.base.Predicates.notNull;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;

public abstract class AbstractDecoratedNodeState extends AbstractNodeState {

    protected final NodeState delegate;

    private final boolean useNativeEquals;

    protected AbstractDecoratedNodeState(@NotNull final NodeState delegate, boolean useNativeEquals) {
        this.delegate = delegate;
        this.useNativeEquals = useNativeEquals;
    }

    public NodeState getDelegate() {
        return delegate;
    }

    protected boolean hideChild(@NotNull final String name, @NotNull final NodeState delegateChild) {
        return false;
    }

    @NotNull
    protected abstract NodeState decorateChild(@NotNull final String name, @NotNull final NodeState delegateChild);

    @NotNull
    private NodeState decorate(@NotNull final String name, @NotNull final NodeState child) {
        return hideChild(name, child) ? EmptyNodeState.MISSING_NODE : decorateChild(name, child);
    }

    protected boolean hideProperty(@NotNull final String name) {
        return false;
    }

    @NotNull
    protected Iterable<PropertyState> getNewPropertyStates() {
        return Collections.emptyList();
    }

    @Nullable
    protected abstract PropertyState decorateProperty(@NotNull final PropertyState delegatePropertyState);

    @Nullable
    private PropertyState decorate(@Nullable final PropertyState property) {
        return property == null || hideProperty(property.getName()) ? null : decorateProperty(property);
    }

    /**
     * Convenience method to help implementations that hide nodes set the
     * :childOrder (OAK_CHILD_ORDER) property to its correct value.
     * <br>
     * Intended to be used to implement {@link #decorateProperty(PropertyState)}.
     *
     * @param nodeState The current node state.
     * @param propertyState The property that chould be checked.
     * @return The original propertyState, unless the property is called {@code :childOrder}.
     */
    protected static PropertyState fixChildOrderPropertyState(NodeState nodeState, PropertyState propertyState) {
        if (propertyState != null && OAK_CHILD_ORDER.equals(propertyState.getName())) {
            final Collection<String> childNodeNames = new ArrayList<String>();
            Iterables.addAll(childNodeNames, nodeState.getChildNodeNames());
            final Iterable<String> values = Iterables.filter(
                    propertyState.getValue(Type.NAMES), Predicates.in(childNodeNames));
            return PropertyStates.createProperty(OAK_CHILD_ORDER, values, Type.NAMES);
        }
        return propertyState;
    }

    /**
     * The AbstractDecoratedNodeState implementation returns a ReadOnlyBuilder, which
     * will fail for any mutable operation.
     *
     * This method can be overridden to return a different NodeBuilder implementation.
     *
     * @return a NodeBuilder instance corresponding to this NodeState.
     */
    @Override
    @NotNull
    public NodeBuilder builder() {
        return new ReadOnlyBuilder(this);
    }

    @Override
    public boolean exists() {
        return delegate.exists();
    }

    @Override
    public boolean hasChildNode(@NotNull final String name) {
        return getChildNode(name).exists();
    }

    @Override
    @NotNull
    public NodeState getChildNode(@NotNull final String name) throws IllegalArgumentException {
        return decorate(name, delegate.getChildNode(name));
    }

    @Override
    @NotNull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        final Iterable<ChildNodeEntry> transformed = Iterables.transform(
                delegate.getChildNodeEntries(),
                new Function<ChildNodeEntry, ChildNodeEntry>() {
                    @Nullable
                    @Override
                    public ChildNodeEntry apply(@Nullable final ChildNodeEntry childNodeEntry) {
                        if (childNodeEntry != null) {
                            final String name = childNodeEntry.getName();
                            final NodeState nodeState = decorate(name, childNodeEntry.getNodeState());
                            if (nodeState.exists()) {
                                return new MemoryChildNodeEntry(name, nodeState);
                            }
                        }
                        return null;
                    }
                }
        );
        return Iterables.filter(transformed, notNull());
    }

    @Override
    @Nullable
    public PropertyState getProperty(@NotNull String name) {
        PropertyState ps = decorate(delegate.getProperty(name));
        if (ps == null) {
            for (PropertyState p : getNewPropertyStates()) {
                if (name.equals(p.getName())) {
                    ps = p;
                    break;
                }
            }
        }
        return ps;
    }

    @Override
    @NotNull
    public Iterable<? extends PropertyState> getProperties() {
        final Iterable<PropertyState> propertyStates = Iterables.transform(
                delegate.getProperties(),
                new Function<PropertyState, PropertyState>() {
                    @Override
                    @Nullable
                    public PropertyState apply(@Nullable final PropertyState propertyState) {
                        return decorate(propertyState);
                    }
                }
        );
        return Iterables.filter(Iterables.concat(propertyStates, getNewPropertyStates()), notNull());
    }

    /**
     * Note that any implementation-specific optimizations of wrapped NodeStates
     * will not work if a AbstractDecoratedNodeState is passed into their {@code #equals()}
     * method. This implementation will compare the wrapped NodeState, however. So
     * optimizations work when calling {@code #equals()} on a ReportingNodeState.
     *
     * @param other Object to compare with this NodeState.
     * @return true if the given object is equal to this NodeState, false otherwise.
     */
    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof NodeState)) {
            return false;
        }

        if (useNativeEquals) {
            if (this.getClass() == other.getClass()) {
                final AbstractDecoratedNodeState o = (AbstractDecoratedNodeState) other;
                return delegate.equals(o.delegate);
            }
        }

        return AbstractDecoratedNodeState.equals(this, (NodeState) other);
    }

    @Override
    public boolean compareAgainstBaseState(final NodeState base, final NodeStateDiff diff) {
        NodeStateDiff decoratingDiff = new DecoratingDiff(diff, this);

        if (!comparePropertiesAgainstBaseState(this, base, decoratingDiff)) {
            return false;
        }

        Set<String> baseChildNodes = new HashSet<String>();
        for (ChildNodeEntry beforeCNE : base.getChildNodeEntries()) {
            String name = beforeCNE.getName();
            NodeState beforeChild = beforeCNE.getNodeState();
            NodeState afterChild = this.getChildNode(name);
            if (!afterChild.exists()) {
                if (!decoratingDiff.childNodeDeleted(name, beforeChild)) {
                    return false;
                }
            } else {
                baseChildNodes.add(name);
                if (!afterChild.equals(beforeChild)) { // TODO: fastEquals?
                    if (!decoratingDiff.childNodeChanged(name, beforeChild, afterChild)) {
                        return false;
                    }
                }
            }
        }

        for (ChildNodeEntry afterChild : this.getChildNodeEntries()) {
            String name = afterChild.getName();
            if (!baseChildNodes.contains(name)) {
                if (!decoratingDiff.childNodeAdded(name, afterChild.getNodeState())) {
                    return false;
                }
            }
        }

        return true;
    }

    private static class DecoratingDiff implements NodeStateDiff {

        private final NodeStateDiff diff;

        private AbstractDecoratedNodeState nodeState;

        private DecoratingDiff(final NodeStateDiff diff, final AbstractDecoratedNodeState nodeState) {
            this.diff = diff;
            this.nodeState = nodeState;
        }

        @Override
        public boolean childNodeAdded(final String name, final NodeState after) {
            return diff.childNodeAdded(name, nodeState.decorate(name, after));
        }

        @Override
        public boolean childNodeChanged(final String name, final NodeState before, final NodeState after) {
            return diff.childNodeChanged(name, before, nodeState.decorate(name, after));
        }

        @Override
        public boolean childNodeDeleted(final String name, final NodeState before) {
            return diff.childNodeDeleted(name, before);
        }

        @Override
        public boolean propertyAdded(final PropertyState after) {
            return diff.propertyAdded(nodeState.decorate(after));
        }

        @Override
        public boolean propertyChanged(final PropertyState before, final PropertyState after) {
            return diff.propertyChanged(nodeState.decorate(before), nodeState.decorate(after));
        }

        @Override
        public boolean propertyDeleted(final PropertyState before) {
            return diff.propertyDeleted(nodeState.decorate(before));
        }
    }
}
