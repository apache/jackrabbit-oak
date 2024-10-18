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
package org.apache.jackrabbit.oak.plugins.tree.impl;

import static org.apache.jackrabbit.guava.common.collect.Iterables.filter;
import static org.apache.jackrabbit.guava.common.collect.Iterables.size;
import static org.apache.jackrabbit.guava.common.collect.Iterables.transform;
import static org.apache.jackrabbit.oak.api.Tree.Status.MODIFIED;
import static org.apache.jackrabbit.oak.api.Tree.Status.NEW;
import static org.apache.jackrabbit.oak.api.Tree.Status.UNCHANGED;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.reference.NodeReferenceConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@code AbstractTree} provides default implementations for most
 * read methods of {@code Tree}. Furthermore it handles hides hidden
 * items.
 */
public abstract class AbstractTree implements Tree {

    // TODO: make this configurable
    private static final String[] INTERNAL_NODE_NAMES = {
            IndexConstants.INDEX_CONTENT_NODE_NAME,
            NodeReferenceConstants.REF_NAME,
            NodeReferenceConstants.WEAK_REF_NAME,
            ConflictAnnotatingRebaseDiff.CONFLICT};

    /**
     * Factory method for creating child trees
     * @param name  name of the child tree
     * @return child tree of this tree with the given {@code name}
     * @throws IllegalArgumentException if the given name string is empty
     *                                  or contains the forward slash character
     */
    @NotNull
    protected abstract AbstractTree createChild(@NotNull String name) throws IllegalArgumentException;

    /**
     * @return  the parent of this tree or {@code null} for the root
     */
    @Nullable
    protected abstract AbstractTree getParentOrNull();

    /**
     * @return  The {@code NodeBuilder} for the underlying node state
     */
    @NotNull
    protected abstract NodeBuilder getNodeBuilder();

    /**
     * Determine whether an item should be hidden. I.e. not exposed through this
     * tree.
     *
     * @param name  name of an item
     * @return  {@code true} if the item is hidden, {@code false} otherwise.
     */
    protected boolean isHidden(@NotNull String name) {
        return NodeStateUtils.isHidden(name);
    }

    @NotNull
    protected String[] getInternalNodeNames() {
        return INTERNAL_NODE_NAMES;
    }

    /**
     * @return  the underlying {@code NodeState} of this tree
     */
    @NotNull
    public NodeState getNodeState() {
        return getNodeBuilder().getNodeState();
    }

    /**
     * @return {@code true} if this tree has orderable children;
     *         {@code false} otherwise.
     */
    protected boolean hasOrderableChildren() {
        return getNodeBuilder().hasProperty(OAK_CHILD_ORDER);
    }

    /**
     * Returns the list of child names considering its ordering
     * when the {@link TreeConstants#OAK_CHILD_ORDER} property is set.
     *
     * @return the list of child names.
     */
    @NotNull
    protected Iterable<String> getChildNames() {
        NodeBuilder nodeBuilder = getNodeBuilder();
        PropertyState order = nodeBuilder.getProperty(OAK_CHILD_ORDER);
        if (order != null && order.getType() == NAMES) {
            Set<String> names = CollectionUtils.toLinkedSet(nodeBuilder.getChildNodeNames());
            List<String> ordered = new ArrayList<>(names.size());
            for (String name : order.getValue(NAMES)) {
                // only include names of child nodes that actually exist
                if (names.remove(name)) {
                    ordered.add(name);
                }
            }
            // add names of child nodes that are not explicitly ordered
            ordered.addAll(names);
            return ordered;
        } else {
            return nodeBuilder.getChildNodeNames();
        }
    }

    //------------------------------------------------------------< Object >---

    @Override
    public String toString() {
        return toString(5);
    }

    private String toString(int childNameCountLimit) {
        StringBuilder sb = new StringBuilder();
        sb.append(getPath()).append(": ");

        sb.append('{');
        for (PropertyState p : getProperties()) {
            sb.append(' ').append(p).append(',');
        }

        Iterator<String> names = this.getChildNames().iterator();
        int count = 0;
        while (names.hasNext() && ++count <= childNameCountLimit) {
            sb.append(' ').append(names.next()).append(" = { ... },");
        }

        if (names.hasNext()) {
            sb.append(" ...");
        }

        if (sb.charAt(sb.length() - 1) == ',') {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append('}');

        return sb.toString();
    }

    //---------------------------------------------------------------< Tree >---

    @Override
    public boolean isRoot() {
        return getParentOrNull() == null;
    }

    @Override
    @NotNull
    public String getPath() {
        if (isRoot()) {
            return PathUtils.ROOT_PATH;
        } else {
            StringBuilder sb = new StringBuilder(128);
            buildPath(sb);
            return sb.toString();
        }
    }

    protected void buildPath(@NotNull StringBuilder sb) {
        AbstractTree parent = getParentOrNull();
        if (parent != null) {
            parent.buildPath(sb);
            sb.append('/').append(getName());
        }
    }

    @Override
    @NotNull
    public Status getStatus() {
        NodeBuilder nodeBuilder = getNodeBuilder();
        if (nodeBuilder.isNew() || nodeBuilder.isReplaced()) {
            return NEW;
        } else if (nodeBuilder.isModified()) {
            return MODIFIED;
        } else {
            return UNCHANGED;
        }
    }

    @Override
    public boolean exists() {
        return getNodeBuilder().exists() && !isHidden(getName());
    }

    @Override
    @NotNull
    public AbstractTree getParent() {
        AbstractTree parent = getParentOrNull();
        Validate.checkState(parent != null, "root tree does not have a parent");
        return parent;
    }

    @Override
    @NotNull
    public Tree getChild(@NotNull String name) throws IllegalArgumentException {
        if (!isHidden(name)) {
            return createChild(name);
        } else {
            return new HiddenTree(this, name);
        }
    }

    @Override
    @Nullable
    public PropertyState getProperty(@NotNull String name) {
        return !isHidden(name)
            ? getNodeBuilder().getProperty(name)
            : null;
    }

    @Override
    public boolean hasProperty(@NotNull String name) {
        return (!isHidden(name)) && getNodeBuilder().hasProperty(name);
    }

    @Override
    public long getPropertyCount() {
        return size(getProperties());
    }

    @Override
    @Nullable
    public Status getPropertyStatus(@NotNull String name) {
        NodeBuilder nodeBuilder = getNodeBuilder();
        if (!hasProperty(name)) {
            return null;
        } else if (nodeBuilder.isNew(name)) {
            return NEW;
        } else if (nodeBuilder.isReplaced(name)) {
            return MODIFIED;
        } else {
            return UNCHANGED;
        }
    }

    @Override
    @NotNull
    public Iterable<? extends PropertyState> getProperties() {
        return filter(getNodeBuilder().getProperties(), propertyState -> !isHidden(propertyState.getName()));
    }

    @Override
    public boolean hasChild(@NotNull String name) {
        return getNodeBuilder().hasChildNode(name) && !isHidden(name);
    }

    @Override
    public long getChildrenCount(long max) {
        String[] internalNodeNames = getInternalNodeNames();
        int len = internalNodeNames.length;
        if (max + len < 0) {
            // avoid overflow (if max is near Long.MAX_VALUE)
            max = Long.MAX_VALUE;
        } else {
            // fetch a few more
            max += len;
        }
        NodeBuilder nodeBuilder = getNodeBuilder();
        long count = nodeBuilder.getChildNodeCount(max);
        if (count > 0) {
            for (String name : internalNodeNames) {
                if (nodeBuilder.hasChildNode(name)) {
                    count--;
                }
            }
        }
        return count;
    }

    @Override
    @NotNull
    public Iterable<Tree> getChildren() {
        Iterable<Tree> children = transform(getChildNames(),
                name ->  {
                    AbstractTree child = createChild(name);
                    return child.exists() ? child : null;
                });
        return filter(children, x -> x != null);
    }
}
