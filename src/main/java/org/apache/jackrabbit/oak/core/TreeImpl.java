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
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

public class TreeImpl implements Tree {

    /**
     * Internal and hidden property that contains the child order
     */
    static final String OAK_CHILD_ORDER = ":childOrder";

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
     * Lazily initialised {@code NodeBuilder} for the underlying node state
     */
    NodeBuilder nodeBuilder;

    private TreeImpl(RootImpl root, TreeImpl parent, String name) {
        this.root = checkNotNull(root);
        this.parent = parent;
        this.name = checkNotNull(name);
    }

    @Nonnull
    static TreeImpl createRoot(final RootImpl root) {
        return new TreeImpl(root, null, "") {
            @Override
            protected NodeState getBaseState() {
                return root.getBaseState();
            }

            @Override
            protected synchronized NodeBuilder getNodeBuilder() {
                if (nodeBuilder == null) {
                    nodeBuilder = root.createRootBuilder();
                }
                return nodeBuilder;
            }
        };
    }

    @Override
    public String getName() {
        root.checkLive();
        return name;
    }

    @Override
    public boolean isRoot() {
        root.checkLive();
        return parent == null;
    }

    @Override
    public String getPath() {
        root.checkLive();
        if (isRoot()) {
            // shortcut
            return "/";
        }

        StringBuilder sb = new StringBuilder();
        buildPath(sb);
        return sb.toString();
    }

    @Override
    public Tree getParent() {
        root.checkLive();
        if (parent != null && canRead(parent)) {
            return parent;
        } else {
            return null;
        }
    }

    @Override
    public PropertyState getProperty(String name) {
        root.checkLive();
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
        root.checkLive();
        Status nodeStatus = getStatus();
        if (nodeStatus == Status.NEW) {
            return (hasProperty(name)) ? Status.NEW : null;
        } else if (nodeStatus == Status.REMOVED) {
            return Status.REMOVED; // FIXME not correct if no property existed with that name
        } else {
            PropertyState head = internalGetProperty(name);
            if (head != null && !canRead(head)) {
                // no permission to read status information for existing property
                return null;
            }

            NodeState parentBase = getBaseState();
            PropertyState base = parentBase == null ? null : parentBase.getProperty(name);
            if (head == null) {
                return (base == null) ? null : Status.REMOVED;
            } else {
                if (base == null) {
                    return Status.NEW;
                } else if (head.equals(base)) {
                    return Status.EXISTING;
                } else {
                    return Status.MODIFIED;
                }
            }
        }
    }

    @Override
    public boolean hasProperty(String name) {
        root.checkLive();
        return getProperty(name) != null;
    }

    @Override
    public long getPropertyCount() {
        root.checkLive();
        return Iterables.size(getProperties());
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        root.checkLive();
        return Iterables.filter(getNodeBuilder().getProperties(),
                new Predicate<PropertyState>() {
                    @Override
                    public boolean apply(PropertyState propertyState) {
                        return canRead(propertyState);
                    }
                });
    }

    @Override
    public TreeImpl getChild(String name) {
        root.checkLive();
        TreeImpl child = internalGetChild(name);
        if (child != null && canRead(child)) {
            return child;
        } else {
            return null;
        }
    }

    private boolean isRemoved() {
        if (isRoot()) {
            return false;
        }
        if (parent.nodeBuilder == null) {
            return false;
        }
        if (parent.nodeBuilder.isRemoved()) {
            return true;
        }
        return getNodeBuilder().isRemoved();
    }

    @Override
    public Status getStatus() {
        root.checkLive();

        if (isRemoved()) {
            return Status.REMOVED;
        }

        NodeBuilder builder = getNodeBuilder();
        if (builder.isNew()) {
            return Status.NEW;
        } else if (builder.isModified()) {
            return Status.MODIFIED;
        } else {
            return Status.EXISTING;
        }
    }

    @Override
    public boolean hasChild(String name) {
        root.checkLive();
        return getChild(name) != null;
    }

    @Override
    public long getChildrenCount() {
        // TODO: make sure cnt respects access control
        root.checkLive();
        return getNodeBuilder().getChildNodeCount();
    }

    @Override
    public Iterable<Tree> getChildren() {
        root.checkLive();
        Iterable<String> childNames;
        if (hasOrderableChildren()) {
            childNames = getOrderedChildNames();
        } else {
            childNames = getNodeBuilder().getChildNodeNames();
        }
        return Iterables.filter(Iterables.transform(
                childNames,
                new Function<String, Tree>() {
                    @Override
                    public Tree apply(String input) {
                        return new TreeImpl(root, TreeImpl.this, input);
                    }
                }),
                new Predicate<Tree>() {
                    @Override
                    public boolean apply(Tree tree) {
                        return tree != null && canRead(tree);
                    }
                });
    }

    @Override
    public Tree addChild(String name) {
        root.checkLive();
        if (!hasChild(name)) {
            getNodeBuilder().child(name);
            if (hasOrderableChildren()) {
                getNodeBuilder().setProperty(
                        MemoryPropertyBuilder.copy(Type.STRING, internalGetProperty(OAK_CHILD_ORDER))
                                .addValue(name)
                                .getPropertyState());
            }
            root.updated();
        }

        TreeImpl child = getChild(name);
        assert child != null;
        return child;
    }

    @Override
    public boolean remove() {
        root.checkLive();
        if (isRemoved()) {
            throw new IllegalStateException("Cannot remove removed tree");
        }

        if (!isRoot() && parent.hasChild(name)) {
            NodeBuilder builder = parent.getNodeBuilder();
            builder.removeNode(name);
            if (parent.hasOrderableChildren()) {
                builder.setProperty(
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
        root.checkLive();
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
        parent.getNodeBuilder().setProperty(MultiStringPropertyState.stringProperty(OAK_CHILD_ORDER, Iterables.concat(head, Collections.singleton(getName()), tail))
        );
        root.updated();
        return true;
    }

    @Override
    public void setProperty(PropertyState property) {
        root.checkLive();
        NodeBuilder builder = getNodeBuilder();
        builder.setProperty(property);
        root.updated();
    }

    @Override
    public <T> void setProperty(String name, T value) {
        root.checkLive();
        NodeBuilder builder = getNodeBuilder();
        builder.setProperty(name, value);
        root.updated();
    }

    @Override
    public <T> void setProperty(String name, T value, Type<T> type) {
        root.checkLive();
        NodeBuilder builder = getNodeBuilder();
        builder.setProperty(name, value, type);
        root.updated();
    }

    @Override
    public void removeProperty(String name) {
        root.checkLive();
        NodeBuilder builder = getNodeBuilder();
        builder.removeProperty(name);
        root.updated();
    }

    @Override
    public TreeLocation getLocation() {
        root.checkLive();
        return new NodeLocation(this);
    }

    //----------------------------------------------------------< protected >---

    @CheckForNull
    protected NodeState getBaseState() {
        if (isRemoved()) {
            throw new IllegalStateException("Cannot get the base state of a removed tree");
        }

        NodeState parentBaseState = parent.getBaseState();
        return parentBaseState == null
                ? null
                : parentBaseState.getChildNode(name);
    }

    @Nonnull
    protected synchronized NodeBuilder getNodeBuilder() {
        if (nodeBuilder == null) {
            nodeBuilder = parent.getNodeBuilder().child(name);
        }
        return nodeBuilder;
    }

    //-----------------------------------------------------------< internal >---

    /**
     * Move this tree to the parent at {@code destParent} with the new name
     * {@code destName}.
     *
     * @param destParent new parent for this tree
     * @param destName   new name for this tree
     */
    void moveTo(TreeImpl destParent, String destName) {
        if (isRemoved()) {
            throw new IllegalStateException("Cannot move removed tree");
        }

        name = destName;
        parent = destParent;
    }

    @Nonnull
    NodeState getNodeState() {
        return getNodeBuilder().getNodeState();
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
        checkArgument(path.startsWith("/"));
        TreeImpl child = this;
        for (String name : elements(path)) {
            child = child.internalGetChild(name);
            if (child == null) {
                return null;
            }
        }
        return (canRead(child)) ? child : null;
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
            if (getNodeBuilder().hasChildNode(name)) {
                names.add(name);
            }
        }
        for (String name : getNodeBuilder().getChildNodeNames()) {
            names.add(name);
        }
        PropertyBuilder<String> builder = MemoryPropertyBuilder.array(
                Type.STRING, OAK_CHILD_ORDER);
        builder.setValues(names);
        getNodeBuilder().setProperty(builder.getPropertyState());
    }

    //------------------------------------------------------------< private >---

    private TreeImpl internalGetChild(String childName) {
        return getNodeBuilder().hasChildNode(childName)
                ? new TreeImpl(root, this, childName)
                : null;
    }

    private PropertyState internalGetProperty(String propertyName) {
        return getNodeBuilder().getProperty(propertyName);
    }

    private void buildPath(StringBuilder sb) {
        if (!isRoot()) {
            parent.buildPath(sb);
            sb.append('/').append(name);
        }
    }

    private boolean canRead(Tree tree) {
        // FIXME: access control eval must have full access to the tree
        // FIXME: special handling for access control item and version content
        return root.getPermissionProvider().canRead(tree);
    }

    private boolean canRead(PropertyState property) {
        // FIXME: access control eval must have full access to the tree/property
        // FIXME: special handling for access control item and version content
        return (property != null)
                && root.getPermissionProvider().canRead(this, property)
                && !NodeStateUtils.isHidden(property.getName());
    }

    /**
     * @return {@code true} if this tree has orderable children;
     *         {@code false} otherwise.
     */
    private boolean hasOrderableChildren() {
        return internalGetProperty(OAK_CHILD_ORDER) != null;
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
        PropertyState childOrder = getNodeBuilder().getProperty(OAK_CHILD_ORDER);
        if (childOrder == null) {
            getNodeBuilder().setProperty(
                    MultiStringPropertyState.stringProperty(OAK_CHILD_ORDER, getNodeBuilder().getChildNodeNames()));
        }
    }

    //-------------------------------------------------------< TreeLocation >---

    private final class NodeLocation extends AbstractNodeLocation<TreeImpl> {

        private NodeLocation(TreeImpl tree) {
            super(tree);
        }

        @Override
        public TreeLocation getParent() {
            return tree.parent == null
                    ? TreeLocation.NULL
                    : new NodeLocation(tree.parent);
        }

        @Override
        public TreeLocation getChild(String relPath) {
            checkArgument(!relPath.startsWith("/"));
            if (relPath.isEmpty()) {
                return this;
            }

            TreeImpl child = tree;
            String parentPath = PathUtils.getParentPath(relPath);
            for (String name : PathUtils.elements(parentPath)) {
                child = child.internalGetChild(name);
                if (child == null) {
                    return TreeLocation.NULL;
                }
            }

            String name = PathUtils.getName(relPath);
            PropertyState property = child.internalGetProperty(name);
            if (property != null) {
                return new PropertyLocation(new NodeLocation(child), name);
            } else {
                child = child.internalGetChild(name);
                return child == null
                        ? TreeLocation.NULL
                        : new NodeLocation(child);
            }
        }

        @Override
        public boolean remove() {
            return tree.remove();
        }

        @Override
        public Tree getTree() {
            return canRead(tree) ? tree : null;
        }
    }

    // TODO: OAK-599 (accessed by PropertyDelegate)
    public final class PropertyLocation extends AbstractPropertyLocation<NodeLocation> {

        private PropertyLocation(NodeLocation parentLocation, String name) {
            super(parentLocation, name);
        }

        @Override
        public PropertyState getProperty() {
            PropertyState property = parentLocation.tree.internalGetProperty(name);
            return canRead(property)
                    ? property
                    : null;
        }

        @Override
        public Status getStatus() {
            return parentLocation.tree.getPropertyStatus(name);
        }

        /**
         * Set the underlying property
         *
         * @param property The property to set
         */
        public void set(PropertyState property) {
            parentLocation.tree.setProperty(property);
        }

        /**
         * Remove the underlying property
         *
         * @return {@code true} on success false otherwise
         */
        @Override
        public boolean remove() {
            parentLocation.tree.removeProperty(name);
            return true;
        }
    }

}


