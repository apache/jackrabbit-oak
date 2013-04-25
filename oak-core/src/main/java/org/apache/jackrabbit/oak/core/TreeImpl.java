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
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

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
import org.apache.jackrabbit.JcrConstants;
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
     * The {@code NodeBuilder} for the underlying node state
     */
    private NodeBuilder nodeBuilder;

    /**
     * Parent of this tree. Null for the root.
     */
    private TreeImpl parent;

    /**
     * Name of this tree
     */
    private String name;

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
    public Tree getParent() {
        enterNoStateCheck();
        if (parent != null && canRead(parent)) {
            return parent;
        } else {
            return null;
        }
    }

    @Override
    public PropertyState getProperty(String name) {
        enter();
        PropertyState property = internalGetProperty(name);
        if (canRead(property)) {
            return property;
        } else {
            return null;
        }
    }

    @Override
    public Status getPropertyStatus(String name) {
        // TODO: see OAK-212
        Status nodeStatus = getStatus();
        if (nodeStatus == Status.NEW) {
            return (hasProperty(name)) ? Status.NEW : null;
        }
        PropertyState head = internalGetProperty(name);
        if (head == null || !canRead(head)) {
            // no permission to read status information for existing property
            return null;
        }

        PropertyState base = getSecureBase().getProperty(name);

        if (base == null) {
            return Status.NEW;
        } else if (head.equals(base)) {
            return Status.EXISTING;
        } else {
            return Status.MODIFIED;
        }
    }

    @Override
    public boolean hasProperty(String name) {
        enter();
        return getProperty(name) != null;
    }

    @Override
    public long getPropertyCount() {
        enter();
        return Iterables.size(getProperties());
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        enter();
        return Iterables.filter(nodeBuilder.getProperties(),
                new Predicate<PropertyState>() {
                    @Override
                    public boolean apply(PropertyState propertyState) {
                        return canRead(propertyState);
                    }
                });
    }

    @Override
    public TreeImpl getChild(@Nonnull String name) {
        checkNotNull(name);
        enter();
        TreeImpl child = internalGetChild(name);
        return canRead(child) ? child : null;
    }

    @Override
    public boolean isConnected() {
        enterNoStateCheck();
        if (parent == null || nodeBuilder.exists()) {
            return true;
        }

        return reconnect();
    }

    private boolean reconnect() {
        if (parent != null && parent.reconnect()) {
            nodeBuilder = parent.nodeBuilder.getChildNode(name);
        }
        return nodeBuilder.exists();
    }

    @Override
    public Status getStatus() {
        enter();

        if (nodeBuilder.isNew()) {
            return Status.NEW;
        } else if (nodeBuilder.isModified()) {
            return Status.MODIFIED;
        } else {
            return Status.EXISTING;
        }
    }

    @Override
    public boolean hasChild(@Nonnull String name) {
        return getChild(name) != null;
    }

    @Override
    public long getChildrenCount() {
        // TODO: make sure cnt respects access control
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
        return Iterables.filter(Iterables.transform(
                childNames,
                new Function<String, Tree>() {
                    @Override
                    public Tree apply(String input) {
                        return new TreeImpl(root, TreeImpl.this, input, pendingMoves);
                    }
                }),
                new Predicate<Tree>() {
                    @Override
                    public boolean apply(Tree tree) {
                        return tree != null && canRead((TreeImpl) tree);
                    }
                });
    }

    @Override
    public Tree addChild(String name) {
        enter();
        if (!hasChild(name)) {
            nodeBuilder.child(name);
            if (hasOrderableChildren()) {
                nodeBuilder.setProperty(
                        MemoryPropertyBuilder.copy(Type.STRING, internalGetProperty(OAK_CHILD_ORDER))
                                .addValue(name)
                                .getPropertyState());
            }
            root.updated();
        }

        TreeImpl child = new TreeImpl(root, this, name, pendingMoves);

        // Make sure to allocate the node builder for new nodes in order to correctly
        // track removes and moves. See OAK-621
        return child;
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
    public boolean remove() {
        enter();

        if (!isRoot() && parent.hasChild(name)) {
            NodeBuilder parentBuilder = parent.nodeBuilder;
            parentBuilder.removeChildNode(name);
            if (parent.hasOrderableChildren()) {
                parentBuilder.setProperty(
                        MemoryPropertyBuilder.copy(Type.STRING, parent.internalGetProperty(OAK_CHILD_ORDER))
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
    public boolean orderBefore(final String name) {
        enter();
        if (isRoot()) {
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
        Iterable<String> filtered = Iterables.filter(
                parent.getOrderedChildNames(),
                new Predicate<String>() {
                    @Override
                    public boolean apply(@Nullable String input) {
                        return !TreeImpl.this.getName().equals(input);
                    }
                });
        // create head and tail
        Iterable<String> head;
        Iterable<String> tail;
        if (name == null) {
            head = filtered;
            tail = Collections.emptyList();
        } else {
            int idx = Iterables.indexOf(filtered, new Predicate<String>() {
                @Override
                public boolean apply(@Nullable String input) {
                    return name.equals(input);
                }
            });
            head = Iterables.limit(filtered, idx);
            tail = Iterables.skip(filtered, idx);
        }
        // concatenate head, this name and tail
        parent.nodeBuilder.setProperty(MultiStringPropertyState.stringProperty(OAK_CHILD_ORDER, Iterables.concat(head, Collections.singleton(getName()), tail))
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
    public TreeLocation getLocation() {
        enter();
        return new NodeLocation(this);
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
     * Get a tree for the tree identified by {@code path}.
     *
     * @param path the path to the child
     * @return a {@link Tree} instance for the child at {@code path} or
     *         {@code null} if no such tree exits or if the tree is not accessible.
     */
    @CheckForNull
    TreeImpl getTree(String path) {
        checkArgument(PathUtils.isAbsolute(path));
        TreeImpl child = this;
        for (String name : elements(path)) {
            child = child.internalGetChild(name);
        }
        return canRead(child) ? child : null;
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
                Type.STRING, OAK_CHILD_ORDER);
        builder.setValues(names);
        nodeBuilder.setProperty(builder.getPropertyState());
    }

    @Nonnull
    String getIdentifier() {
        PropertyState property = internalGetProperty(JcrConstants.JCR_UUID);
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

    private void enter() {
        root.checkLive();
        checkState(isConnected());
        applyPendingMoves();
    }

    private void enterNoStateCheck() {
        root.checkLive();
        applyPendingMoves();
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

    private TreeImpl internalGetChild(String childName) {
        return new TreeImpl(root, this, childName, pendingMoves);
    }

    private PropertyState internalGetProperty(String propertyName) {
        return nodeBuilder.getProperty(propertyName);
    }

    private void buildPath(StringBuilder sb) {
        if (parent != null) {
            parent.buildPath(sb);
            sb.append('/').append(name);
        }
    }

    private boolean canRead(TreeImpl tree) {
        // TODO: OAK-753 TreeImpl exposes hidden child trees
        // return tree.getNodeState().exists() && !NodeStateUtils.isHidden(tree.getName());
        return tree.getNodeState().exists();
    }

    private boolean canRead(PropertyState property) {
        return property != null && !NodeStateUtils.isHidden(property.getName());
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
                    final PropertyState childOrder = internalGetProperty(OAK_CHILD_ORDER);
                    int index = 0;

                    @Override
                    public boolean hasNext() {
                        return index < childOrder.count();
                    }

                    @Override
                    public String next() {
                        return childOrder.getValue(Type.STRING, index++);
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
            return tree.internalGetChild(name);
        }

        @Override
        protected PropertyState getPropertyState(String name) {
            return tree.internalGetProperty(name);
        }

        @Override
        protected boolean canRead(TreeImpl tree) {
            return TreeImpl.this.canRead(tree);
        }
    }

    private final class PropertyLocation extends AbstractPropertyLocation<TreeImpl> {

        private PropertyLocation(AbstractNodeLocation<TreeImpl> parentLocation, String name) {
            super(parentLocation, name);
        }

        @Override
        protected boolean canRead(PropertyState property) {
            return TreeImpl.this.canRead(property);
        }

    }

}


