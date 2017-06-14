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

package org.apache.jackrabbit.oak.spi.state;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import org.apache.jackrabbit.oak.api.PropertyState;

/**
 * {@code AbstractRebaseDiff} serves as base for rebase implementations.
 * It implements a {@link NodeStateDiff}, which performs the conflict
 * handling as defined in {@link org.apache.jackrabbit.oak.spi.state.NodeStore#rebase(NodeBuilder)}
 * on the Oak SPI state level.
 * <p>
 * Intended use of this class is to re-base a branched version of the node state
 * tree. Given below situation:
 * <pre>
 *     + head (master)
 *     |
 *     | + branch
 *     |/
 *     + base
 *     |
 * </pre>
 * The current state on the master branch is {@code head} and a branch
 * was created at {@code base}. The current state on the branch is
 * {@code branch}. Re-basing {@code branch} to the current
 * {@code head} works as follows:
 * <pre>
 *     NodeState head = ...
 *     NodeState branch = ...
 *     NodeState base = ...
 *     NodeBuilder builder = new MemoryNodeBuilder(head);
 *     branch.compareAgainstBaseState(base, new MyRebaseDiff(builder));
 *     branch = builder.getNodeState();
 * </pre>
 * The result is:
 * <pre>
 *       + branch
 *      /
 *     + head (master)
 *     |
 * </pre>
 * <p>
 * Conflicts during rebase cause calls to the various abstracts conflict resolution
 * methods of this class. Concrete subclasses of this class need to implement these
 * methods for handling such conflicts.
 */
public abstract class AbstractRebaseDiff implements NodeStateDiff {
    private final NodeBuilder builder;

    protected AbstractRebaseDiff(NodeBuilder builder) {
        this.builder = builder;
    }

    /**
     * Factory method for creating a rebase handler for the named child of the passed
     * parent builder.
     *
     * @param builder  parent builder
     * @param name  name of the child for which to return a rebase handler
     * @return  rebase handler for child {@code name} in {@code builder}
     */
    protected abstract AbstractRebaseDiff createDiff(NodeBuilder builder, String name);

    /**
     * Called when the property {@code after} was added on the branch but the property
     * exists already in the trunk.
     *
     * @param builder  parent builder
     * @param before existing property
     * @param after  added property
     */
    protected abstract void addExistingProperty(NodeBuilder builder, PropertyState before, PropertyState after);

    /**
     * Called when the property {@code after} was changed on the branch but was
     * deleted already in the trunk.
     *
     * @param builder  parent builder
     * @param after  changed property
     * @param base  base property
     */
    protected abstract void changeDeletedProperty(NodeBuilder builder, PropertyState after, PropertyState base);

    /**
     * Called when the property {@code after} was changed on the branch but was
     * already changed to {@code before} in the trunk.
     *
     * @param builder  parent property
     * @param before  changed property in branch
     * @param after  changed property in trunk
     */
    protected abstract void changeChangedProperty(NodeBuilder builder, PropertyState before, PropertyState after);

    /**
     * Called when the property {@code before} was deleted in the branch but was
     * already deleted in the trunk.
     *
     * @param builder  parent builder
     * @param before  deleted property
     */
    protected abstract void deleteDeletedProperty(NodeBuilder builder, PropertyState before);

    /**
     * Called when the property {@code before} was deleted in the branch but was
     * already changed in the trunk.
     *
     * @param builder  parent builder
     * @param before  deleted property
     */
    protected abstract void deleteChangedProperty(NodeBuilder builder, PropertyState before);

    /**
     * Called when the node {@code after} was added on the branch but the node
     * exists already in the trunk.
     *
     * @param builder  parent builder
     * @param name  name of the added node
     * @param before existing node
     * @param after  added added
     */
    protected abstract void addExistingNode(NodeBuilder builder, String name, NodeState before, NodeState after);

    /**
     * Called when the node {@code after} was changed on the branch but was
     * deleted already in the trunk.
     *
     * @param builder  parent builder
     * @param name  name of the changed node
     * @param after  changed node
     * @param base  base node
     */
    protected abstract void changeDeletedNode(NodeBuilder builder, String name, NodeState after, NodeState base);

    /**
     * Called when the node {@code before} was deleted in the branch but was
     * already deleted in the trunk.
     *
     * @param builder  parent builder
     * @param before  deleted node
     */
    protected abstract void deleteDeletedNode(NodeBuilder builder, String name, NodeState before);

    /**
     * Called when the node {@code before} was deleted in the branch but was
     * already changed in the trunk.
     *
     * @param builder  parent builder
     * @param before  deleted node
     */
    protected abstract void deleteChangedNode(NodeBuilder builder, String name, NodeState before);

    @Override
    public boolean propertyAdded(PropertyState after) {
        PropertyState other = builder.getProperty(after.getName());
        if (other == null) {
            builder.setProperty(after);
        } else if (!other.equals(after)) {
            addExistingProperty(builder, other, after);
        }
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        PropertyState other = builder.getProperty(before.getName());
        if (other == null) {
            changeDeletedProperty(builder, after, before);
        } else if (other.equals(before)) {
            builder.setProperty(after);
        } else if (!other.equals(after)) {
            changeChangedProperty(builder, before, after);
        }
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        PropertyState other = builder.getProperty(before.getName());
        if (other == null) {
            deleteDeletedProperty(builder, before);
        } else if (other.equals(before)) {
            builder.removeProperty(before.getName());
        } else {
            deleteChangedProperty(builder, before);
        }
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        if (builder.hasChildNode(name)) {
            after.compareAgainstBaseState(EMPTY_NODE, createDiff(builder, name));
        } else {
            builder.setChildNode(name, after);
        }
        return true;
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        if (builder.hasChildNode(name)) {
            after.compareAgainstBaseState(before, createDiff(builder, name));
        } else if (after.equals(before)) {
            return false;
        } else {
            changeDeletedNode(builder, name, after, before);
        }
        return true;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        if (!builder.hasChildNode(name)) {
            deleteDeletedNode(builder, name, before);
        } else if (before.equals(builder.child(name).getNodeState())) {
            builder.getChildNode(name).remove();
        } else {
            deleteChangedNode(builder, name, before);
        }
        return true;
    }

}
