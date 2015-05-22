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
package org.apache.jackrabbit.oak.core;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.emptyList;

class SecureNodeState extends AbstractNodeState {

    /**
     * Underlying node state.
     */
    private final NodeState state;

    /**
     * Tree permissions of this subtree.
     */
    private final TreePermission treePermission;

    private long childNodeCount = -1;

    private long propertyCount = -1;

    SecureNodeState(@Nonnull NodeState state, @Nonnull TreePermission treePermission) {
        this.state = checkNotNull(state);
        this.treePermission = checkNotNull(treePermission);
    }

    @Override
    public boolean exists() {
        return treePermission.canRead();
    }

    @Override @CheckForNull
    public PropertyState getProperty(@Nonnull String name) {
        PropertyState property = state.getProperty(name);
        if (property != null && treePermission.canRead(property)) {
            return property;
        } else {
            return null;
        }
    }

    @Override
    public synchronized long getPropertyCount() {
        if (propertyCount == -1) {
            if (treePermission.canReadProperties()) {
                propertyCount = state.getPropertyCount();
            } else {
                propertyCount = count(filter(
                        state.getProperties(),
                        new ReadablePropertyPredicate()));
            }
        }
        return propertyCount;
    }

    @Override @Nonnull
    public Iterable<? extends PropertyState> getProperties() {
        if (treePermission.canReadProperties()) {
            return state.getProperties();
        } else {
            return filter(
                    state.getProperties(),
                    new ReadablePropertyPredicate());
        }
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        if (!state.hasChildNode(name)) {
            return false;
        } else if (treePermission.canReadAll()) {
            return true;
        } else {
            NodeState child = state.getChildNode(name);
            return treePermission.getChildPermission(name, child).canRead();
        }
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) {
        NodeState child = state.getChildNode(name);
        if (child.exists() && !treePermission.canReadAll()) {
            ChildNodeEntry entry = new MemoryChildNodeEntry(name, child);
            return new WrapChildEntryFunction().apply(entry).getNodeState();
        } else {
            return child;
        }
    }

    @Override
    public synchronized long getChildNodeCount(long max) {
        if (childNodeCount == -1) {
            long count;
            if (treePermission.canReadAll()) {
                count = state.getChildNodeCount(max);
            } else {
                count = super.getChildNodeCount(max);
            }
            if (count == Long.MAX_VALUE) {
                return count;
            }
            childNodeCount = count;
        }
        return childNodeCount;
    }

    @Override @Nonnull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        if (treePermission.canReadAll()) {
            // everything is readable including ac-content -> no secure wrapper needed
            return state.getChildNodeEntries();
        } else if (treePermission.canRead()) {
            Iterable<ChildNodeEntry> readable = transform(
                    state.getChildNodeEntries(),
                    new WrapChildEntryFunction());
            return filter(readable, new IterableNodePredicate());
        } else {
            return emptyList();
       }
    }

    @Override @Nonnull
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    //------------------------------------------------------< inner classes >---

    /**
     * Predicate for testing whether a given property is readable.
     */
    private class ReadablePropertyPredicate implements Predicate<PropertyState> {
        @Override
        public boolean apply(@Nullable PropertyState property) {
            return property != null && treePermission.canRead(property);
        }
    }

    /**
     * Predicate for testing whether the node state in a child node entry is iterable.
     */
    private static class IterableNodePredicate implements Predicate<ChildNodeEntry> {
        @Override
        public boolean apply(@Nullable ChildNodeEntry input) {
            return input != null && input.getNodeState().exists();
        }
    }

    /**
     * Function that that adds a security wrapper to node states from
     * in child node entries. The {@link IterableNodePredicate} predicate should be
     * used on the result to filter out non-existing/iterable child nodes.
     * <p>
     * Note that the SecureNodeState wrapper is needed only when the child
     * or any of its descendants has read access restrictions. Otherwise
     * we can optimize access by skipping the security wrapper entirely.
     */
    private class WrapChildEntryFunction implements Function<ChildNodeEntry, ChildNodeEntry> {
        @Nonnull
        @Override
        public ChildNodeEntry apply(@Nonnull ChildNodeEntry input) {
            String name = input.getName();
            NodeState child = input.getNodeState();
            TreePermission childContext = treePermission.getChildPermission(name, child);
            SecureNodeState secureChild = new SecureNodeState(child, childContext);
            if (child.getChildNodeCount(1) == 0
                    && secureChild.treePermission.canRead()
                    && secureChild.treePermission.canReadProperties()) {
                // Since this is an accessible leaf node whose all properties
                // are readable, we don't need the SecureNodeState wrapper
                // TODO: A further optimization would be to return the raw
                // underlying node state even for non-leaf nodes if we can
                // tell in advance that the full subtree is readable. Then
                // we also wouldn't need the above getChildNodeCount() call
                // that's somewhat expensive on the DocumentMK.
                return input;
            } else {
                return new MemoryChildNodeEntry(name, secureChild);
            }
        }
    }
}
