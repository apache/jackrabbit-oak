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
package org.apache.jackrabbit.oak.upgrade;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.tree.impl.TreeConstants.OAK_CHILD_ORDER;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.io.Charsets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * This is a node state wrapper that filters out all children nodes with names
 * longer than supported by the DocumentNodeStore. Returned children are wrapped
 * as well.
 */
class NameFilteringNodeState extends AbstractNodeState {

    /**
     * Max character size in bytes in UTF8 = 4. Therefore if the number of characters is smaller
     * than NODE_NAME_LIMIT / 4 we don't need to count bytes.
     */
    private static final int SAFE_NODE_NAME_LENGTH = Utils.NODE_NAME_LIMIT / 4;

    private final NodeState delegate;

    private NameFilteringNodeState(@Nonnull NodeState delegate) {
        this.delegate = checkNotNull(delegate);
    }

    @Nonnull
    public static NodeState wrap(@Nonnull NodeState nodeState) {
        return new NameFilteringNodeState(checkNotNull(nodeState));
    }

    @Override
    public boolean exists() {
        return delegate.exists();
    }

    @Override
    public boolean hasProperty(String name) {
        return delegate.hasProperty(name);
    }

    @Override
    public boolean getBoolean(String name) {
        return delegate.getBoolean(name);
    }

    @Override
    public long getLong(String name) {
        return delegate.getLong(name);
    }

    @Override
    public String getString(String name) {
        return delegate.getString(name);
    }

    @Override
    public Iterable<String> getStrings(String name) {
        return delegate.getStrings(name);
    }

    @Override
    public String getName(String name) {
        return delegate.getName(name);
    }

    @Override
    public Iterable<String> getNames(String name) {
        return delegate.getNames(name);
    }

    @Override
    public long getPropertyCount() {
        return delegate.getPropertyCount();
    }

    @Override
    @Nonnull
    public Iterable<? extends PropertyState> getProperties() {
        return Iterables.transform(delegate.getProperties(), new Function<PropertyState, PropertyState>() {
            @Nullable
            @Override
            public PropertyState apply(@Nullable final PropertyState propertyState) {
                return fixChildOrderPropertyState(propertyState);
            }
        });
    }

    @Override
    public PropertyState getProperty(String name) {
        return fixChildOrderPropertyState(delegate.getProperty(name));
    }

    @Override
    public boolean hasChildNode(String name) {
        if (isNameTooLong(name)) {
            return false;
        } else {
            return delegate.hasChildNode(name);
        }
    }

    @Override
    public NodeState getChildNode(String name) throws IllegalArgumentException {
        if (isNameTooLong(name)) {
            return EmptyNodeState.MISSING_NODE;
        } else {
            return wrap(delegate.getChildNode(name));
        }
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        final Iterable<ChildNodeEntry> transformed = Iterables.transform(delegate.getChildNodeEntries(),
                new Function<ChildNodeEntry, ChildNodeEntry>() {
                    @Nullable
                    @Override
                    public ChildNodeEntry apply(@Nullable final ChildNodeEntry childNodeEntry) {
                        if (childNodeEntry != null) {
                            final String name = childNodeEntry.getName();
                            final NodeState nodeState = childNodeEntry.getNodeState();
                            if (isNameTooLong(name)) {
                                return null;
                            } else {
                                return new MemoryChildNodeEntry(name, wrap(nodeState));
                            }
                        }
                        return null;
                    }
                });
        return Iterables.filter(transformed, new Predicate<ChildNodeEntry>() {
            @Override
            public boolean apply(@Nullable final ChildNodeEntry childNodeEntry) {
                return childNodeEntry != null;
            }
        });

    }

    @Override
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof NameFilteringNodeState) {
            return delegate.equals(((NameFilteringNodeState) obj).delegate);
        } else if (obj instanceof NodeState){
            for (String name : ((NodeState) obj).getChildNodeNames()) {
                if (isNameTooLong(name)) {
                    return false;
                }
            }
            return delegate.equals(obj);
        }
        return false;
    }

    /**
     * This method checks whether the name is no longer than the maximum node
     * name length supported by the DocumentNodeStore.
     * 
     * @param name
     *            to check
     * @return true if the name is longer than {@link Utils#NODE_NAME_LIMIT}
     */
    private static boolean isNameTooLong(@Nonnull String name) {
        return name.length() > SAFE_NODE_NAME_LENGTH && name.getBytes(Charsets.UTF_8).length > Utils.NODE_NAME_LIMIT;
    }

    /**
     * Utility method to fix the PropertyState of properties called
     * {@code :childOrder}.
     *
     * @param propertyState
     *            A property-state.
     * @return The original property-state or if the property name is
     *         {@code :childOrder}, a property-state with hidden child names
     *         removed from the value.
     */
    @CheckForNull
    private PropertyState fixChildOrderPropertyState(@Nullable final PropertyState propertyState) {
        if (propertyState != null && OAK_CHILD_ORDER.equals(propertyState.getName())) {
            final Iterable<String> values = Iterables.filter(propertyState.getValue(Type.NAMES),
                    new Predicate<String>() {
                        @Override
                        public boolean apply(@Nullable final String name) {
                            return !(name == null || isNameTooLong(name));
                        }
                    });
            return PropertyStates.createProperty(OAK_CHILD_ORDER, values, Type.NAMES);
        }
        return propertyState;
    }
}
