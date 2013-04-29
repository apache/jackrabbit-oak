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
package org.apache.jackrabbit.oak.core;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.indexOf;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Iterables.transform;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.api.Tree.Status.EXISTING;
import static org.apache.jackrabbit.oak.api.Tree.Status.MODIFIED;
import static org.apache.jackrabbit.oak.api.Tree.Status.NEW;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAbsolute;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.RootImpl.Move;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;

public class TreeImpl implements Tree {

    /**
     * Internal and hidden property that contains the child order
     */
    public static final String OAK_CHILD_ORDER = ":childOrder";

    /**
     * Underlying {@code Root} of this {@code Tree} instance
     */
    private final RootImpl root;

    /**
     * Parent of this tree. Null for the root.
     */
    private TreeImpl parent;

    /**
     * Name of this tree
     */
    private String name;

    /**
     * The {@code NodeBuilder} for the underlying node state
     */
    private NodeBuilder nodeBuilder;

    /** Pointer into the list of pending moves */
    private Move pendingMoves;

    TreeImpl(RootImpl root, NodeBuilder builder, Move pendingMoves) {
        this.root = checkNotNull(root);
        this.name = "";
        this.nodeBuilder = checkNotNull(builder);
        this.pendingMoves = checkNotNull(pendingMoves);
    }

    private TreeImpl(RootImpl root, TreeImpl parent, String name, Move pendingMoves) {
        this.root = checkNotNull(root);
        this.parent = checkNotNull(parent);
        this.name = checkNotNull(name);
        this.nodeBuilder = parent.nodeBuilder.getChildNode(name);
        this.pendingMoves = checkNotNull(pendingMoves);
    }

    @Override
    public String getName() {
        enterNoStateCheck();
        return name;
    }

    @Override
    public boolean isRoot() {
        enterNoStateCheck();
        return parent == null;
    }

    @Override
    public String getPath() {
        enterNoStateCheck();
        return getPathInternal();
    }

    @Override
    public Status getStatus() {
        enter();

        if (nodeBuilder.isNew()) {
            return NEW;
        } else if (nodeBuilder.isModified()) {
            return MODIFIED;
        } else {
            return EXISTING;
        }
    }

    @Override
    public TreeLocation getLocation() {
        enterNoStateCheck();
        return new NodeLocation(this);
    }

    @Override
    public boolean exists() {
        return enterNoStateCheck();
    }

    @Override
    public Tree getParentNonNull() {
        checkState(parent != null, "root tree does not have a parent");
        root.checkLive();
        return parent;
    }

    @Override
    public Tree getParent() {
        enterNoStateCheck();
        if (parent != null && parent.nodeBuilder.exists()) {
            return parent;
        } else {
            return null;
        }
    }

    @Override
    public PropertyState getProperty(String name) {
        enter();
        return getVisibleProperty(name);
    }

    @Override
    public Status getPropertyStatus(String name) {
        // TODO: see OAK-212
        Status nodeStatus = getStatus();
        if (nodeStatus == NEW) {
            return (hasProperty(name)) ? NEW : null;
        }
        PropertyState head = getVisibleProperty(name);
        if (head == null) {
            return null;
        }

        PropertyState base = getSecureBase().getProperty(name);

        if (base == null) {
            return NEW;
        } else if (head.equals(base)) {
            return EXISTING;
        } else {
            return MODIFIED;
        }
    }

    @Override
    public boolean hasProperty(String name) {
        return getProperty(name) != null;
    }

    @Override
    public long getPropertyCount() {
        return size(getProperties());
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        enter();
        return filter(nodeBuilder.getProperties(),
                new Predicate<PropertyState>() {
                    @Override
                    public boolean apply(PropertyState propertyState) {
                        return !isHidden(propertyState.getName());
                    }
                });
    }

    @Override
    public TreeImpl getChildNonNull(@Nonnull String name) {
        checkNotNull(name);
        enterNoStateCheck();
        return new TreeImpl(root, this, name, pendingMoves);
    }

    @Override
    public TreeImpl getChild(@Nonnull String name) {
        TreeImpl child = getChildNonNull(name);
        return child.nodeBuilder.exists() ? child : null;
    }

    @Override
    public boolean hasChild(@Nonnull String name) {
        checkNotNull(name);
        enter();
        TreeImpl child = new TreeImpl(root, this, name, pendingMoves);
        return child.nodeBuilder.exists();
    }

    @Override
    public long getChildrenCount() {
        enter();
        return nodeBuilder.getChildNodeCount();
    }

    @Override
    public Iterable<Tree> getChildren() {
        enter();
        Iterable<String> childNames;
        if (hasOrderableChildren()) {
            childNames = getOrderedChildNames();
        } else {
            childNames = nodeBuilder.getChildNodeNames();
        }
        return transform(
                childNames,
                new Function<String, Tree>() {
                    @Override
                    public Tree apply(String input) {
                        return new TreeImpl(root, TreeImpl.this, input, pendingMoves);
                    }
                });
    }

    @Override
    public boolean remove() {
        enter();
        if (parent != null && parent.hasChild(name)) {
            NodeBuilder parentBuilder = parent.nodeBuilder;
            parentBuilder.removeChildNode(name);
            if (parent.hasOrderableChildren()) {
                parentBuilder.setProperty(
                        MemoryPropertyBuilder.copy(STRING, parent.nodeBuilder.getProperty(OAK_CHILD_ORDER))
                                .removeValue(name)
                                .getPropertyState()
                );
            }
            root.updated();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Tree addChild(String name) {
        enter();
        if (!hasChild(name)) {
            nodeBuilder.setChildNode(name);
            if (hasOrderableChildren()) {
                nodeBuilder.setProperty(
                        MemoryPropertyBuilder.copy(STRING, nodeBuilder.getProperty(OAK_CHILD_ORDER))
                                .addValue(name)
                                .getPropertyState());
            }
            root.updated();
        }
        return new TreeImpl(root, this, name, pendingMoves);
    }

    @Override
    public void setOrderableChildren(boolean enable) {
        enter();
        if (enable) {
            ensureChildOrderProperty();
        } else {
            nodeBuilder.removeProperty(OAK_CHILD_ORDER);
        }
    }

    @Override
    public boolean orderBefore(final String name) {
        enter();
        if (parent == null) {
            // root does not have siblings
            return false;
        }
        if (name != null && !parent.hasChild(name)) {
            // so such sibling or not accessible
            return false;
        }
        // perform the reorder
        parent.ensureChildOrderProperty();
        // all siblings but not this one
        Iterable<String> siblings = filter(
                parent.getOrderedChildNames(),
                new Predicate<String>() {
                    @Override
                    public boolean apply(@Nullable String name) {
                        return !TreeImpl.this.name.equals(name);
                    }
                });
        // create head and tail
        Iterable<String> head;
        Iterable<String> tail;
        if (name == null) {
            head = siblings;
            tail = Collections.emptyList();
        } else {
            int idx = indexOf(siblings, new Predicate<String>() {
                @Override
                public boolean apply(@Nullable String sibling) {
                    return name.equals(sibling);
                }
            });
            head = Iterables.limit(siblings, idx);
            tail = Iterables.skip(siblings, idx);
        }
        // concatenate head, this name and tail
        parent.nodeBuilder.setProperty(
                MultiStringPropertyState.stringProperty(
                        OAK_CHILD_ORDER, Iterables.concat(head, Collections.singleton(getName()), tail))
        );
        root.updated();
        return true;
    }

    @Override
    public void setProperty(PropertyState property) {
        enter();
        nodeBuilder.setProperty(property);
        root.updated();
    }

    @Override
    public <T> void setProperty(String name, T value) {
        enter();
        nodeBuilder.setProperty(name, value);
        root.updated();
    }

    @Override
    public <T> void setProperty(String name, T value, Type<T> type) {
        enter();
        nodeBuilder.setProperty(name, value, type);
        root.updated();
    }

    @Override
    public void removeProperty(String name) {
        enter();
        nodeBuilder.removeProperty(name);
        root.updated();
    }

    @Override
    public String toString() {
        return getPathInternal() + ": " + getNodeState();
    }

    //-----------------------------------------------------------< internal >---

    @Nonnull
    NodeState getNodeState() {
        return nodeBuilder.getNodeState();
    }

    /**
     * Move this tree to the parent at {@code destParent} with the new name
     * {@code destName}.
     * @param destParent new parent for this tree
     * @param destName   new name for this tree
     */
    void moveTo(TreeImpl destParent, String destName) {
        name = destName;
        parent = destParent;
    }

    /**
     * Reset this (root) tree instance's underlying node state to the passed {@code state}.
     * @param state
     * @throws IllegalStateException  if {@code isRoot()} is {@code false}.
     */
    void reset(NodeState state) {
        checkState(parent == null);
        nodeBuilder.reset(state);
    }

    /**
     * Get a possibly non existing tree.
     * @param path the path to the tree
     * @return a {@link Tree} instance for the child at {@code path}.
     */
    @CheckForNull
    TreeImpl getTree(@Nonnull String path) {
        checkArgument(isAbsolute(checkNotNull(path)));
        TreeImpl child = this;
        for (String name : elements(path)) {
            child = new TreeImpl(child.root, child, name, child.pendingMoves);
        }
        return child;
    }

    /**
     * Get a tree for the tree identified by {@code path}.
     *
     * @param path the path to the child
     * @return a {@link Tree} instance for the child at {@code path} or
     *         {@code null} if no such tree exits or if the tree is not accessible.
     */
    @CheckForNull
    @Deprecated
    TreeImpl getTreeOrNull(String path) {
        TreeImpl child = getTree(path);
        return child.nodeBuilder.exists() ? child : null;
    }

    /**
     * Update the child order with children that have been removed or added.
     * Added children are appended to the end of the {@link #OAK_CHILD_ORDER}
     * property.
     */
    void updateChildOrder() {
        if (!hasOrderableChildren()) {
            return;
        }
        Set<String> names = Sets.newLinkedHashSet();
        for (String name : getOrderedChildNames()) {
            if (nodeBuilder.hasChildNode(name)) {
                names.add(name);
            }
        }
        for (String name : nodeBuilder.getChildNodeNames()) {
            names.add(name);
        }
        PropertyBuilder<String> builder = MemoryPropertyBuilder.array(
                STRING, OAK_CHILD_ORDER);
        builder.setValues(names);
        nodeBuilder.setProperty(builder.getPropertyState());
    }

    @Nonnull
    String getIdentifier() {
        PropertyState property = nodeBuilder.getProperty(JCR_UUID);
        if (property != null) {
            return property.getValue(STRING);
        } else if (parent == null) {
            return "/";
        } else {
            return PathUtils.concat(parent.getIdentifier(), name);
        }
    }

    String getPathInternal() {
        if (parent == null) {
            return "/";
        }

        StringBuilder sb = new StringBuilder();
        buildPath(sb);
        return sb.toString();
    }

    //------------------------------------------------------------< private >---

    private boolean reconnect() {
        if (parent != null && parent.reconnect()) {
            nodeBuilder = parent.nodeBuilder.getChildNode(name);
        }
        return nodeBuilder.exists();
    }

    private void enter() {
        checkState(enterNoStateCheck(), "This tree is not connected");
    }

    private boolean enterNoStateCheck() {
        root.checkLive();
        applyPendingMoves();
        return reconnect();
    }

    private static boolean isHidden(String name) {
        // FIXME clarify handling of hidden items (OAK-753).
        return NodeStateUtils.isHidden(name);
    }

    /**
     * The (possibly non-existent) node state this tree is based on.
     * @return the base node state of this tree
     */
    @Nonnull
    private NodeState getSecureBase() {
        if (parent == null) {
            return root.getSecureBase();
        } else {
            return parent.getSecureBase().getChildNode(name);
        }
    }

    private void applyPendingMoves() {
        if (parent != null) {
            parent.applyPendingMoves();
        }

        pendingMoves = pendingMoves.apply(this);
    }

    private PropertyState getVisibleProperty(String name) {
        return !isHidden(name)
            ? nodeBuilder.getProperty(name)
            : null;

    }

    private void buildPath(StringBuilder sb) {
        if (parent != null) {
            parent.buildPath(sb);
            sb.append('/').append(name);
        }
    }

    /**
     * @return {@code true} if this tree has orderable children;
     *         {@code false} otherwise.
     */
    private boolean hasOrderableChildren() {
        return nodeBuilder.hasProperty(OAK_CHILD_ORDER);
    }

    /**
     * Returns the ordered child names. This method must only be called when
     * this tree {@link #hasOrderableChildren()}.
     *
     * @return the ordered child names.
     */
    private Iterable<String> getOrderedChildNames() {
        assert hasOrderableChildren();
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {
                    final PropertyState childOrder = nodeBuilder.getProperty(OAK_CHILD_ORDER);
                    int index = 0;

                    @Override
                    public boolean hasNext() {
                        return index < childOrder.count();
                    }

                    @Override
                    public String next() {
                        return childOrder.getValue(STRING, index++);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * Ensures that the {@link #OAK_CHILD_ORDER} exists. This method will create
     * the property if it doesn't exist and initialize the value with the names
     * of the children as returned by {@link NodeBuilder#getChildNodeNames()}.
     */
    private void ensureChildOrderProperty() {
        if (!nodeBuilder.hasProperty(OAK_CHILD_ORDER)) {
            nodeBuilder.setProperty(
                    MultiStringPropertyState.stringProperty(OAK_CHILD_ORDER, nodeBuilder.getChildNodeNames()));
        }
    }

    //-------------------------------------------------------< TreeLocation >---

    private final class NodeLocation extends AbstractNodeLocation<TreeImpl> {

        private NodeLocation(TreeImpl tree) {
            super(tree);
        }

        @Override
        protected NodeLocation createNodeLocation(TreeImpl tree) {
            return new NodeLocation(tree);
        }

        @Override
        protected TreeLocation createPropertyLocation(AbstractNodeLocation<TreeImpl> parentLocation, String name) {
            return new PropertyLocation(parentLocation, name);
        }

        @Override
        protected TreeImpl getParentTree() {
            return tree.parent;
        }

        @Override
        protected TreeImpl getChildTree(String name) {
            return new TreeImpl(tree.root, tree, name, tree.pendingMoves);
        }

        @Override
        protected PropertyState getPropertyState(String name) {
            return tree.getVisibleProperty(name);
        }

        @Override
        protected boolean canRead(TreeImpl tree) {
            return tree.nodeBuilder.exists();
        }
    }

    private static final class PropertyLocation extends AbstractPropertyLocation<TreeImpl> {

        private PropertyLocation(AbstractNodeLocation<TreeImpl> parentLocation, String name) {
            super(parentLocation, name);
        }

        @Override
        protected boolean canRead(PropertyState property) {
            return !isHidden(property.getName());
        }

    }

}


