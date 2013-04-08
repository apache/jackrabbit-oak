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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * SecureNodeState...
 *
 * TODO: clarify if HIDDEN items should be filtered by this NodeState implementation
 * TODO: clarify usage of ReadStatus in getChildNodeEntries
 * TODO: add proper equals/hashcode implementation
 * TODO: should be package-private
 */
public class SecureNodeState extends AbstractNodeState {

    /**
     * Underlying node state.
     */
    private final NodeState state;

    /**
     * Immutable tree based on the underlying node state.
     */
    private final ImmutableTree base;

    private final PermissionProvider permissionProvider;

    private final ReadStatus readStatus;

    /**
     * Predicate for testing whether a given property is readable.
     */
    private final Predicate<PropertyState> isPropertyReadable = new Predicate<PropertyState>() {
        @Override
        public boolean apply(@Nonnull PropertyState property) {
            ReadStatus status =
                    permissionProvider.getReadStatus(base, property);
            return status.isAllow();
        }
    };

    /**
     * Predicate for testing whether the node state in a child node entry
     * is iterable.
     */
    private final Predicate<ChildNodeEntry> isIterableNode = new Predicate<ChildNodeEntry>() {
        @Override
        public boolean apply(@Nonnull ChildNodeEntry input) {
            return input.getNodeState().exists();
        }
    };

   /**
    * Function that that adds a security wrapper to node states from
    * in child node entries. The {@link #isIterableNode} predicate should be
    * used on the result to filter out non-existing/iterable child nodes.
    * <p>
    * Note that the SecureNodeState wrapper is needed only when the child
    * or any of its descendants has read access restrictions. Otherwise
    * we can optimize access by skipping the security wrapper entirely.
    */
    private final Function<ChildNodeEntry, ChildNodeEntry> wrapChildNodeEntry = new Function<ChildNodeEntry, ChildNodeEntry>() {
        @Override
        public ChildNodeEntry apply(@Nonnull ChildNodeEntry input) {
            String name = input.getName();
            NodeState child = input.getNodeState();
            SecureNodeState secure = new SecureNodeState(
                    SecureNodeState.this, name, child);
            if (!secure.readStatus.includes(ReadStatus.ALLOW_ALL)
                    || child.getChildNodeCount() > 0) {
                // secure wrapper is needed
                return new MemoryChildNodeEntry(name, secure);
            } else {
                return input;
            }
        }
    };


    private long childNodeCount = -1;

    private long propertyCount = -1;

    public SecureNodeState(@Nonnull NodeState rootState,
                           @Nonnull PermissionProvider permissionProvider,
                           @Nonnull ImmutableTree.TypeProvider typeProvider) {
        this.state = checkNotNull(rootState);
        this.base = new ImmutableTree(rootState, typeProvider);
        this.permissionProvider = permissionProvider;
        this.readStatus = permissionProvider.getReadStatus(base, null);
    }

    private SecureNodeState(
            @Nonnull SecureNodeState parent,
            @Nonnull String name, @Nonnull NodeState nodeState) {
        this.state = checkNotNull(nodeState);
        this.base = new ImmutableTree(parent.base, name, nodeState);
        this.permissionProvider = parent.permissionProvider;

        ReadStatus status = null;
        if (base.getType() == parent.base.getType()) {
            status = ReadStatus.getChildStatus(parent.readStatus);
        }
        if (status == null) {
            status = permissionProvider.getReadStatus(base, null);
        }
        this.readStatus = status;
    }

    @Override
    public boolean exists() {
        return readStatus.includes(ReadStatus.ALLOW_THIS);
    }

    @Override @CheckForNull
    public PropertyState getProperty(String name) {
        PropertyState property = state.getProperty(name);
        if (property != null
                && !readStatus.includes(ReadStatus.ALLOW_PROPERTIES)) {
            if (!readStatus.appliesToThis()
                    || isPropertyReadable.apply(property)) {
                // this property is not readable
                property = null;
            }
        }
        return property;
    }

    @Override
    public synchronized long getPropertyCount() {
        if (propertyCount == -1) {
            if (readStatus.includes(ReadStatus.ALLOW_PROPERTIES)) {
                // all properties are readable
                propertyCount = state.getPropertyCount();
            } else if (readStatus.includes(ReadStatus.DENY_PROPERTIES)
                    || !readStatus.appliesToThis()) {
                // no properties are readable
                propertyCount = 0;
            } else {
                // some properties are readable
                propertyCount = count(Iterables.filter(
                        state.getProperties(), isPropertyReadable));
            }
        }
        return propertyCount;
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        if (readStatus.includes(ReadStatus.ALLOW_PROPERTIES)) {
            // all properties are readable
            return state.getProperties();
        } else if (readStatus.includes(ReadStatus.DENY_PROPERTIES)
                || !readStatus.appliesToThis()) {
            // no properties are readable
            return Collections.emptySet();
        } else {
            // some properties are readable
            return Iterables.filter(
                    state.getProperties(), isPropertyReadable);
        }
    }

    @Override
    public NodeState getChildNode(@Nonnull String name) {
        NodeState child = state.getChildNode(checkNotNull(name));
        if (child.exists()) {
            ChildNodeEntry entry = new MemoryChildNodeEntry(name, child);
            return  wrapChildNodeEntry.apply(entry).getNodeState();
        } else {
            return child;
        }
    }

    @Override
    public synchronized long getChildNodeCount() {
        if (childNodeCount == -1) {
            childNodeCount = super.getChildNodeCount();
        }
        return childNodeCount;
    }

    @Override
    @Nonnull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        if (readStatus.includes(ReadStatus.DENY_CHILDREN)) {
            return Collections.emptySet();
        } else {
            // TODO: review if ALLOW_CHILDREN could be used as well although we
            // don't know the type of all child-nodes where ac node would need special treatment
            Iterable<ChildNodeEntry> readable = Iterables.transform(
                    state.getChildNodeEntries(), wrapChildNodeEntry);
            return Iterables.filter(readable, isIterableNode);
        }
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    @Override
    public void compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        // FIXME: should not bypass access controls
        state.compareAgainstBaseState(base, diff);
    }

    //-------------------------------------------------------------< Object >---
    // FIXME: add proper equals/hashcode implementation (see OAK-709)
    @Override
    public boolean equals(Object obj) {
        return state.equals(obj);
    }
}
