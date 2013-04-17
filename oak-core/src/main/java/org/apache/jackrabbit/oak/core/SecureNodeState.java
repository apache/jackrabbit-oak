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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

/**
 * SecureNodeState...
 *
 * TODO: clarify if HIDDEN items should be filtered by this NodeState implementation
 * TODO: clarify usage of ReadStatus in getChildNodeEntries
 * TODO: add proper equals/hashcode implementation
 * TODO: should be package-private
 */
class SecureNodeState extends AbstractNodeState {

    /**
     * Underlying node state.
     */
    private final NodeState state;

    /**
     * Security context of this subtree.
     */
    private final SecurityContext context;

    /**
     * Predicate for testing whether a given property is readable.
     */
    private final Predicate<PropertyState> isPropertyReadable = new ReadablePropertyPredicate();

    /**
     * Predicate for testing whether the node state in a child node entry
     * is iterable.
     */
    private final Predicate<ChildNodeEntry> isIterableNode = new IterableNodePredicate();

   /**
    * Function that that adds a security wrapper to node states from
    * in child node entries. The {@link #isIterableNode} predicate should be
    * used on the result to filter out non-existing/iterable child nodes.
    * <p>
    * Note that the SecureNodeState wrapper is needed only when the child
    * or any of its descendants has read access restrictions. Otherwise
    * we can optimize access by skipping the security wrapper entirely.
    */
    private final WrapChildEntryFunction wrapChildNodeEntry = new WrapChildEntryFunction();

    private long childNodeCount = -1;

    private long propertyCount = -1;

    SecureNodeState(
            @Nonnull NodeState state, @Nonnull SecurityContext context) {
        this.state = checkNotNull(state);
        this.context = checkNotNull(context);
    }

    @Override
    public boolean exists() {
        return context.canReadThisNode();
    }

    @Override @CheckForNull
    public PropertyState getProperty(String name) {
        PropertyState property = state.getProperty(name);
        if (property != null && context.canReadProperty(property)) {
            return property;
        } else {
            return null;
        }
    }

    @Override
    public synchronized long getPropertyCount() {
        if (propertyCount == -1) {
            if (context.canReadAll()) {
                propertyCount = state.getPropertyCount();
            } else {
                propertyCount = count(filter(state.getProperties(), isPropertyReadable));
            }
        }
        return propertyCount;
    }

    @Override @Nonnull
    public Iterable<? extends PropertyState> getProperties() {
        if (context.canReadAll()) {
            return state.getProperties();
        } else {
            return filter(state.getProperties(), isPropertyReadable);
        }
    }

    @Override
    public NodeState getChildNode(@Nonnull String name) {
        NodeState child = state.getChildNode(checkNotNull(name));
        if (child.exists() && !context.canReadAll()) {
            ChildNodeEntry entry = new MemoryChildNodeEntry(name, child);
            return wrapChildNodeEntry.apply(entry).getNodeState();
        } else {
            return child;
        }
    }

    @Override
    public synchronized long getChildNodeCount() {
        if (childNodeCount == -1) {
            if (context.canReadAll()) {
                childNodeCount = state.getChildNodeCount();
            } else {
                childNodeCount = super.getChildNodeCount();
            }
        }
        return childNodeCount;
    }

    @Override @Nonnull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        if (context.canReadAll()) {
            // everything is readable including ac-content -> no secure wrapper needed
            return state.getChildNodeEntries();
        } else {
            Iterable<ChildNodeEntry> readable = transform(state.getChildNodeEntries(), wrapChildNodeEntry);
            return filter(readable, isIterableNode);
        }
    }

    @Override @Nonnull
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    @Override
    public void compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        // FIXME: decide if comparison during commit should compare the secure
        // states or the original node states without ac restrictions
        super.compareAgainstBaseState(base, diff);
    }

    //-------------------------------------------------------------< Object >---

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        } else if (object instanceof SecureNodeState) {
            SecureNodeState that = (SecureNodeState) object;
            // TODO: We should be able to do this optimization also across
            // different revisions (root states) as long as the path,
            // the subtree, and any security-related areas like the
            // permission store are equal for both states.
            if (state.equals(that.state) && context.equals(that.context)) {
                return true;
            }
        }
        return super.equals(object);
    }


    //------------------------------------------------------< inner classes >---
    /**
     * Predicate for testing whether a given property is readable.
     */
    private class ReadablePropertyPredicate implements Predicate<PropertyState> {
        @Override
        public boolean apply(@Nonnull PropertyState property) {
            return context.canReadProperty(property);
        }
    }

    /**
     * Predicate for testing whether the node state in a child node entry is iterable.
     */
    private class IterableNodePredicate implements Predicate<ChildNodeEntry> {
        @Override
        public boolean apply(@Nonnull ChildNodeEntry input) {
            return input.getNodeState().exists();
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
            SecurityContext childContext = context.getChildContext(name, child);
            SecureNodeState secureChild = new SecureNodeState(child, childContext);
            if (child.getChildNodeCount() == 0
                    && secureChild.context.canReadThisNode()
                    && secureChild.context.canReadAllProperties()) {
                // Since this is an accessible leaf node whose all properties
                // are readable, we don't need the SecureNodeState wrapper
                // TODO: A further optimization would be to return the raw
                // underlying node state even for non-leaf nodes if we can
                // tell in advance that the full subtree is readable. Then
                // we also wouldn't need the above getChildNodeCount() call
                // that's somewhat expensive on the MongoMK.
                return input;
            } else {
                return new MemoryChildNodeEntry(name, secureChild);
            }
        }
    }
}
