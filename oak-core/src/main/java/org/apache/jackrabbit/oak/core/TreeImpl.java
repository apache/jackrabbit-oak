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

import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.RootImpl.PurgeListener;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState.EMPTY_NODE;

public class TreeImpl implements Tree, PurgeListener {

    /** Underlying {@code Root} of this {@code Tree} instance */
    private final RootImpl root;

    /** Parent of this tree. Null for the root. */
    private TreeImpl parent;

    /** Marker for removed trees */
    private boolean removed;

    /** Name of this tree */
    private String name;

    /** Lazily initialised {@code NodeBuilder} for the underlying node state */
    NodeBuilder nodeBuilder;

    /**
     * Cache for child trees that have been accessed before.
     */
    private final Map<String, TreeImpl> children =
            CacheBuilder.newBuilder().weakValues().<String, TreeImpl>build().asMap();

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
                    root.addListener(this);
                }
                return nodeBuilder;
            }
        };
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isRoot() {
        return parent == null;
    }

    @Override
    public String getPath() {
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
        if (parent != null && canRead(parent)) {
            return parent;
        } else {
            return null;
        }
    }

    @Override
    public PropertyState getProperty(String name) {
        if (canReadProperty(name)) {
            return internalGetProperty(name);
        } else {
            return null;
        }
    }

    @Override
    public Status getPropertyStatus(String name) {
        // TODO: see OAK-212
        if (canReadProperty(name)) {
            return internalGetPropertyStatus(name);
        }
        else {
            return null;
        }
    }

    @Override
    public boolean hasProperty(String name) {
        return getProperty(name) != null;
    }

    @Override
    public long getPropertyCount() {
        // TODO: make sure cnt respects access control
        return getNodeBuilder().getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return Iterables.filter(getNodeBuilder().getProperties(),
                new Predicate<PropertyState>() {
                    @Override
                    public boolean apply(PropertyState propertyState) {
                        return propertyState != null && canReadProperty(propertyState.getName());
                    }
                });
    }

    @Override
    public TreeImpl getChild(String name) {
        TreeImpl child = internalGetChild(name);
        if (child != null && canRead(child)) {
            return child;
        } else {
            return null;
        }
    }

    @Override
    public Status getStatus() {
        if (isRemoved()) {
            return Status.REMOVED;
        }

        NodeState baseState = getBaseState();
        if (baseState == null) {
            // Did not exist before, so its NEW
            return Status.NEW;
        } else {
            // Did exit it before. So...
            if (isSame(baseState, getNodeState())) {
                // ...it's EXISTING if it hasn't changed
                return Status.EXISTING;
            } else {
                // ...and MODIFIED otherwise.
                return Status.MODIFIED;
            }
        }
    }

    @Override
    public boolean hasChild(String name) {
        return getChild(name) != null;
    }

    @Override
    public long getChildrenCount() {
        // TODO: make sure cnt respects access control
        return getNodeBuilder().getChildNodeCount();
    }

    @Override
    public Iterable<Tree> getChildren() {
        return Iterables.filter(Iterables.transform(
                getNodeBuilder().getChildNodeNames(),
                new Function<String, Tree>() {
                    @Override
                    public Tree apply(String input) {
                        TreeImpl child = children.get(input);
                        if (child == null) {
                            child = new TreeImpl(root, TreeImpl.this, input);
                            children.put(input, child);
                        }
                        return  child;
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
        if (!hasChild(name)) {
            NodeBuilder builder = getNodeBuilder();
            builder.setNode(name, EMPTY_NODE);
            root.purge();
        }

        TreeImpl child = getChild(name);
        assert child != null;
        return child;
    }

    @Override
    public boolean remove() {
        if (isRemoved()) {
            throw new IllegalStateException("Cannot remove removed tree");
        }

        if (!isRoot() && parent.hasChild(name)) {
            NodeBuilder builder = parent.getNodeBuilder();
            builder.removeNode(name);
            parent.children.remove(name);
            removed = true;
            root.purge();
            return true;
        } else {
            return false;
        }
    }

    @Override
    @Deprecated
    public PropertyState setProperty(String name, CoreValue value) {
        NodeBuilder builder = getNodeBuilder();
        builder.setProperty(name, value);
        root.purge();
        PropertyState property = getProperty(name);
        assert property != null;
        return property;
    }

    @Override
    @Deprecated
    public PropertyState setProperty(String name, List<CoreValue> values) {
        NodeBuilder builder = getNodeBuilder();
        builder.setProperty(name, values);
        root.purge();
        PropertyState property = getProperty(name);
        assert property != null;
        return property;
    }

    @Override
    public void setProperty(PropertyState property) {
        NodeBuilder builder = getNodeBuilder();
        builder.setProperty(property);
        root.purge();
    }

    @Override
    public <T> void setProperty(String name, T value) {
        NodeBuilder builder = getNodeBuilder();
        builder.setProperty(name, value);
        root.purge();
    }

    @Override
    public <T> void setProperty(String name, T value, Type<T> type) {
        NodeBuilder builder = getNodeBuilder();
        builder.setProperty(name, value, type);
        root.purge();
    }

    @Override
    public void removeProperty(String name) {
        NodeBuilder builder = getNodeBuilder();
        builder.removeProperty(name);
        root.purge();
    }

    @Override
    public TreeLocation getLocation() {
        return new NodeLocation(this);
    }

    //--------------------------------------------------< RootImpl.Listener >---

    @Override
    public void purged() {
        nodeBuilder = null;
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
        if (isRemoved()) {
            throw new IllegalStateException("Cannot get a builder for a removed tree");
        }

        if (nodeBuilder == null) {
            nodeBuilder = parent.getNodeBuilder().getChildBuilder(name);
            root.addListener(this);
        }
        return nodeBuilder;
    }

    //-----------------------------------------------------------< internal >---

    /**
     * Move this tree to the parent at {@code destParent} with the new name
     * {@code destName}.
     *
     * @param destParent  new parent for this tree
     * @param destName  new name for this tree
     */
    void moveTo(TreeImpl destParent, String destName) {
        if (isRemoved()) {
            throw new IllegalStateException("Cannot move removed tree");
        }

        parent.children.remove(name);
        destParent.children.put(destName, this);

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
     * {@code null} if no such tree exits or if the tree is not accessible.
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

    //------------------------------------------------------------< private >---

    private TreeImpl internalGetChild(String childName) {
        TreeImpl child = children.get(childName);
        if (child == null && getNodeBuilder().hasChildNode(childName)) {
            child = new TreeImpl(root, this, childName);
            children.put(childName, child);
        }
        return child;
    }

    private PropertyState internalGetProperty(String propertyName) {
        return getNodeBuilder().getProperty(propertyName);
    }

    private Status internalGetPropertyStatus(String name) {
        if (isRemoved()) {
            return Status.REMOVED;
        }

        NodeState baseState = getBaseState();
        boolean exists = internalGetProperty(name) != null;
        if (baseState == null) {
            // This instance is NEW...
            if (exists) {
                // ...so all children are new
                return Status.NEW;
            } else {
                // ...unless they don't exist.
                return null;
            }
        } else {
            if (exists) {
                // We have the property...
                if (baseState.getProperty(name) == null) {
                    // ...but didn't have it before. So its NEW.
                    return Status.NEW;
                } else {
                    // ... and did have it before. So...
                    PropertyState base = baseState.getProperty(name);
                    PropertyState head = getProperty(name);
                    if (base == null ? head == null : base.equals(head)) {
                        // ...it's EXISTING if it hasn't changed
                        return Status.EXISTING;
                    } else {
                        // ...and MODIFIED otherwise.
                        return Status.MODIFIED;
                    }
                }
            } else {
                // We don't have the property
                if (baseState.getProperty(name) == null) {
                    // ...and didn't have it before. So it doesn't exist.
                    return null;
                } else {
                    // ...but did have it before. So it's REMOVED
                    return Status.REMOVED;
                }
            }
        }
    }

    private boolean isRemoved() {
        return removed || (parent != null && parent.isRemoved());
    }

    private void buildPath(StringBuilder sb) {
        if (!isRoot()) {
            parent.buildPath(sb);
            sb.append('/').append(name);
        }
    }

    private boolean canRead(Tree tree) {
        // FIXME: special handling for access control item and version content
        return root.getPermissions().canRead(tree.getPath(), false);
    }

    private boolean canReadProperty(String name) {
        String path = PathUtils.concat(getPath(), name);

        // FIXME: special handling for access control item and version content
        return root.getPermissions().canRead(path, true);
    }

    private static boolean isSame(NodeState state1, NodeState state2) {
        final boolean[] isDirty = {false};
        state2.compareAgainstBaseState(state1, new NodeStateDiff() {
            @Override
            public void propertyAdded(PropertyState after) {
                isDirty[0] = true;
            }

            @Override
            public void propertyChanged(PropertyState before, PropertyState after) {
                isDirty[0] = true;
            }

            @Override
            public void propertyDeleted(PropertyState before) {
                isDirty[0] = true;
            }

            @Override
            public void childNodeAdded(String name, NodeState after) {
                isDirty[0] = true;
            }

            @Override
            public void childNodeChanged(String name, NodeState before, NodeState after) {
                // cut transitivity here
            }

            @Override
            public void childNodeDeleted(String name, NodeState before) {
                isDirty[0] = true;
            }
        });

        return !isDirty[0];
    }

    //------------------------------------------------------------< TreeLocation >---

    public class NodeLocation implements TreeLocation {
        private final TreeImpl tree;

        private NodeLocation(TreeImpl tree) {
            this.tree = checkNotNull(tree);
        }

        @Override
        public TreeLocation getParent() {
            return tree.parent == null
                ? NullLocation.INSTANCE
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
                    return NullLocation.INSTANCE;
                }
            }

            String name = PathUtils.getName(relPath);
            PropertyState property = child.internalGetProperty(name);
            if (property != null) {
                return new PropertyLocation(new NodeLocation(child), property);
            }
            else {
                child = child.internalGetChild(name);
                return child == null
                    ? NullLocation.INSTANCE
                    : new NodeLocation(child);
            }
        }

        @Override
        public String getPath() {
            return tree.getPath();
        }

        @Override
        public Tree getTree() {
            return canRead(tree) ? tree : null;
        }

        @Override
        public PropertyState getProperty() {
            return null;
        }

        @Override
        public Status getStatus() {
            return tree.getStatus();
        }
    }

    public class PropertyLocation implements TreeLocation {
        private final NodeLocation parent;
        private final PropertyState property;

        private PropertyLocation(NodeLocation parent, PropertyState property) {
            this.parent = checkNotNull(parent);
            this.property = checkNotNull(property);
        }

        @Override
        public TreeLocation getParent() {
            return parent;
        }

        @Override
        public TreeLocation getChild(String relPath) {
            return NullLocation.INSTANCE;
        }

        @Override
        public String getPath() {
            return PathUtils.concat(parent.getPath(), property.getName());
        }

        @Override
        public Tree getTree() {
            return null;
        }

        @Override
        public PropertyState getProperty() {
            return root.getPermissions().canRead(getPath(), true)
                ? property
                : null;
        }

        @Override
        public Status getStatus() {
            return parent.tree.internalGetPropertyStatus(property.getName());
        }

        /**
         * Set the underlying property
         * @param property The property to set
         */
        public <T> void set(PropertyState property) {
            parent.tree.setProperty(property);
        }

        /**
         * Remove the underlying property
         * @return  {@code true} on success false otherwise
         */
        public boolean remove() {
            parent.tree.removeProperty(property.getName());
            return true;
        }

        private boolean canRead() {
            return root.getPermissions().canRead(getPath(), true);
        }
    }

    public static class NullLocation implements TreeLocation {
        public static final NullLocation INSTANCE = new NullLocation();

        private NullLocation() {
        }

        @Override
        public TreeLocation getParent() {
            return this;
        }

        @Override
        public TreeLocation getChild(String relPath) {
            return this;
        }

        @Override
        public String getPath() {
            return null;
        }

        @Override
        public Tree getTree() {
            return null;
        }

        @Override
        public PropertyState getProperty() {
            return null;
        }

        @Override
        public Status getStatus() {
            return null;
        }
    }

}


