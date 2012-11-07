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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHandlerWrapper;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.spi.commit.ConflictHandler.Resolution.MERGED;
import static org.apache.jackrabbit.oak.spi.commit.ConflictHandler.Resolution.OURS;

/**
 * MergingNodeStateDiff... TODO
 */
class MergingNodeStateDiff implements NodeStateDiff {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(MergingNodeStateDiff.class);

    private final NodeBuilder target;
    private final ConflictHandler conflictHandler;

    private MergingNodeStateDiff(NodeBuilder target, ConflictHandler conflictHandler) {
        this.target = target;
        this.conflictHandler = conflictHandler;
    }

    static void merge(NodeState fromState, NodeState toState, final NodeBuilder target,
            final ConflictHandler conflictHandler) {
        toState.compareAgainstBaseState(fromState, new MergingNodeStateDiff(
                checkNotNull(target), new ChildOrderConflictHandler(conflictHandler)));
    }

    //------------------------------------------------------< NodeStateDiff >---
    @Override
    public void propertyAdded(PropertyState after) {
        ConflictHandler.Resolution resolution;
        PropertyState p = target.getProperty(after.getName());

        if (p == null) {
            resolution = OURS;
        }
        else {
            resolution = conflictHandler.addExistingProperty(target, after, p);
        }

        switch (resolution) {
            case OURS:
                target.setProperty(after);
                break;
            case THEIRS:
            case MERGED:
                break;
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        checkArgument(before.getName().equals(after.getName()),
                "before and after must have the same name");

        ConflictHandler.Resolution resolution;
        PropertyState p = target.getProperty(after.getName());

        if (p == null) {
            resolution = conflictHandler.changeDeletedProperty(target, after);
        }
        else if (before.equals(p)) {
            resolution = OURS;
        }
        else {
            resolution = conflictHandler.changeChangedProperty(target, after, p);
        }

        switch (resolution) {
            case OURS:
                target.setProperty(after);
                break;
            case THEIRS:
            case MERGED:
                break;
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        ConflictHandler.Resolution resolution;
        PropertyState p = target.getProperty(before.getName());

        if (before.equals(p)) {
            resolution = OURS;
        }
        else if (p == null) {
            resolution = conflictHandler.deleteDeletedProperty(target, before);
        }
        else {
            resolution = conflictHandler.deleteChangedProperty(target, p);
        }

        switch (resolution) {
            case OURS:
                target.removeProperty(before.getName());
                break;
            case THEIRS:
            case MERGED:
                break;
        }
    }

    @Override
    public void childNodeAdded(String name, NodeState after) {
        ConflictHandler.Resolution resolution;
        if (!target.hasChildNode(name)) {
            resolution = OURS;
        } else {
            NodeBuilder n = target.child(name);
            resolution = conflictHandler.addExistingNode(target, name, after, n.getNodeState());
        }

        switch (resolution) {
            case OURS:
                addChild(target, name, after);
                break;
            case THEIRS:
            case MERGED:
                break;
        }
    }

    @Override
    public void childNodeChanged(String name, NodeState before, NodeState after) {
        ConflictHandler.Resolution resolution;
        if (!target.hasChildNode(name)) {
            resolution = conflictHandler.changeDeletedNode(target, name, after);
        } else {
            merge(before, after, target.child(name), conflictHandler);
            resolution = MERGED;
        }

        switch (resolution) {
            case OURS:
                addChild(target, name, after);
                break;
            case THEIRS:
            case MERGED:
                break;
        }
    }

    @Override
    public void childNodeDeleted(String name, NodeState before) {
        ConflictHandler.Resolution resolution;
        NodeBuilder n = target.hasChildNode(name) ? target.child(name) : null;

        if (n == null) {
            resolution = conflictHandler.deleteDeletedNode(target, name);
        }
        else if (before.equals(n.getNodeState())) {
            resolution = OURS;
        }
        else {
            resolution = conflictHandler.deleteChangedNode(target, name, n.getNodeState());
        }

        switch (resolution) {
            case OURS:
                if (n != null) {
                    removeChild(target, name);
                }
                break;
            case THEIRS:
            case MERGED:
                break;
        }
    }

    //-------------------------------------------------------------<private >---

    private static void addChild(NodeBuilder target, String name, NodeState state) {
        NodeBuilder child = target.child(name);
        for (PropertyState property : state.getProperties()) {
            child.setProperty(property);
        }
        PropertyState childOrder = target.getProperty(TreeImpl.OAK_CHILD_ORDER);
        if (childOrder != null) {
            PropertyBuilder<String> builder = MemoryPropertyBuilder.copy(Type.STRING, childOrder);
            builder.addValue(name);
            target.setProperty(builder.getPropertyState());
        }
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            addChild(child, entry.getName(), entry.getNodeState());
        }
    }

    private static void removeChild(NodeBuilder target, String name) {
        target.removeNode(name);
        PropertyState childOrder = target.getProperty(TreeImpl.OAK_CHILD_ORDER);
        if (childOrder != null) {
            PropertyBuilder<String> builder = MemoryPropertyBuilder.copy(Type.STRING, childOrder);
            builder.removeValue(name);
            target.setProperty(builder.getPropertyState());
        }
    }

    /**
     * {@code ChildOrderConflictHandler} ignores conflicts on the
     * {@link TreeImpl#OAK_CHILD_ORDER} property. All other conflicts are forwarded
     * to the wrapped handler.
     */
    private static class ChildOrderConflictHandler extends ConflictHandlerWrapper {

        ChildOrderConflictHandler(ConflictHandler delegate) {
            super(delegate);
        }

        @Override
        public Resolution addExistingProperty(NodeBuilder parent,
                                              PropertyState ours,
                                              PropertyState theirs) {
            if (isChildOrderProperty(ours)) {
                // two sessions concurrently called orderBefore() on a Tree
                // that was previously unordered.
                return Resolution.THEIRS;
            } else {
                return handler.addExistingProperty(parent, ours, theirs);
            }
        }

        @Override
        public Resolution changeDeletedProperty(NodeBuilder parent,
                                                PropertyState ours) {
            if (isChildOrderProperty(ours)) {
                // orderBefore() on trees that were deleted
                return Resolution.THEIRS;
            } else {
                return handler.changeDeletedProperty(parent, ours);
            }
        }

        @Override
        public Resolution changeChangedProperty(NodeBuilder parent,
                                                PropertyState ours,
                                                PropertyState theirs) {
            if (isChildOrderProperty(ours)) {
                // concurrent orderBefore(), other changes win
                return Resolution.THEIRS;
            } else {
                return handler.changeChangedProperty(parent, ours, theirs);
            }
        }

        @Override
        public Resolution deleteDeletedProperty(NodeBuilder parent,
                                                PropertyState ours) {
            if (isChildOrderProperty(ours)) {
                // concurrent remove of ordered trees
                return Resolution.THEIRS;
            } else {
                return handler.deleteDeletedProperty(parent, ours);
            }
        }

        @Override
        public Resolution deleteChangedProperty(NodeBuilder parent,
                                                PropertyState theirs) {
            if (isChildOrderProperty(theirs)) {
                // remove trees that were reordered by another session
                return Resolution.THEIRS;
            } else {
                return handler.deleteChangedProperty(parent, theirs);
            }
        }

        //----------------------------< internal >----------------------------------

        private static boolean isChildOrderProperty(PropertyState p) {
            return TreeImpl.OAK_CHILD_ORDER.equals(p.getName());
        }
    }
}