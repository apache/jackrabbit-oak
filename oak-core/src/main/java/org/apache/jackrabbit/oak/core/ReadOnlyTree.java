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

import java.util.Iterator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ReadOnlyTree implements Tree {

    /**
     * Parent of this tree, {@code null} for the root
     */
    private final ReadOnlyTree parent;

    /**
     * Name of this tree
     */
    private final String name;

    /**
     * Path of this tree
     */
    private final String path;

    /**
     * Underlying node state
     */
    private final NodeState state;

    public ReadOnlyTree(NodeState rootState) {
        this(null, "", rootState);
    }

    public ReadOnlyTree(@Nullable ReadOnlyTree parent, @Nonnull String name, @Nonnull NodeState state) {
        this.parent = parent;
        this.name = checkNotNull(name);
        this.path = buildPath(parent, name);
        this.state = checkNotNull(state);
        checkArgument(!name.isEmpty() || parent == null);
    }

    private static String buildPath(ReadOnlyTree parent, String name) {
        if (parent == null) {
            return "/";
        } else if (parent.isRoot()) {
            return parent.path + name;
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(parent.path).append('/').append(name);
            return sb.toString();
        }
    }

    public static ReadOnlyTree createFromRoot(Root root) {
        if (root instanceof RootImpl) {
            return new ReadOnlyTree(((RootImpl) root).getBaseState());
        } else {
            throw new IllegalArgumentException("Unsupported Root implementation.");
        }
    }

    public static ReadOnlyTree createFromRootTree(Tree rootTree) {
        if (rootTree instanceof ReadOnlyTree) {
            return (ReadOnlyTree) rootTree;
        } else if (rootTree instanceof TreeImpl) {
            TreeImpl impl = (TreeImpl) rootTree;
            return new ReadOnlyTree(null, "", impl.getNodeState());
        } else {
            throw new IllegalArgumentException("Unsupported Tree implementation");
        }
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
        return path;
    }

    @Override
    public Tree getParent() {
        return parent;
    }

    @Override
    public PropertyState getProperty(String name) {
        return state.getProperty(name);
    }

    @Override
    public Status getPropertyStatus(String name) {
        if (hasProperty(name)) {
            return Status.EXISTING;
        } else {
            return null;
        }
    }

    @Override
    public boolean hasProperty(String name) {
        return state.getProperty(name) != null;
    }

    @Override
    public long getPropertyCount() {
        return state.getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return state.getProperties();
    }

    @Override
    public ReadOnlyTree getChild(String name) {
        NodeState child = state.getChildNode(name);
        if (child != null) {
            return new ReadOnlyTree(this, name, child);
        } else {
            return null;
        }
    }

    @Override
    public Status getStatus() {
        return Status.EXISTING;
    }

    @Override
    public TreeLocation getLocation() {
        return new NodeLocation(this);
    }

    @Override
    public boolean hasChild(String name) {
        return state.getChildNode(name) != null;
    }

    @Override
    public long getChildrenCount() {
        return state.getChildNodeCount();
    }

    /**
     * This implementation does not respect ordered child nodes, but always
     * returns them in some implementation specific order.
     * <p/>
     * TODO: respect orderable children (needed?)
     *
     * @return the children.
     */
    @Override
    public Iterable<Tree> getChildren() {
        return new Iterable<Tree>() {
            @Override
            public Iterator<Tree> iterator() {
                final Iterator<? extends ChildNodeEntry> iterator =
                        state.getChildNodeEntries().iterator();
                return new Iterator<Tree>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Tree next() {
                        ChildNodeEntry entry = iterator.next();
                        return new ReadOnlyTree(
                                ReadOnlyTree.this,
                                entry.getName(), entry.getNodeState());
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @Override
    public Tree addChild(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean orderBefore(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setProperty(PropertyState property) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void setProperty(String name, T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void setProperty(String name, T value, Type<T> type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeProperty(String name) {
        throw new UnsupportedOperationException();
    }

    //-------------------------------------------------------< TreeLocation >---

    private final class NodeLocation extends AbstractNodeLocation<ReadOnlyTree> {

        private NodeLocation(ReadOnlyTree tree) {
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

            ReadOnlyTree child = tree;
            String parentPath = PathUtils.getParentPath(relPath);
            for (String name : PathUtils.elements(parentPath)) {
                child = child.getChild(name);
                if (child == null) {
                    return TreeLocation.NULL;
                }
            }

            String name = PathUtils.getName(relPath);
            PropertyState property = child.getProperty(name);
            if (property != null) {
                return new PropertyLocation(new NodeLocation(child), name);
            } else {
                child = child.getChild(name);
                return child == null
                        ? TreeLocation.NULL
                        : new NodeLocation(child);
            }
        }

        @Override
        public boolean remove() {
            return false;
        }

        @Override
        public Tree getTree() {
            return tree;
        }

        @Override
        public Status getStatus() {
            return tree.getStatus();
        }
    }

    private final class PropertyLocation extends AbstractPropertyLocation<ReadOnlyTree, NodeLocation> {

        private PropertyLocation(NodeLocation parentLocation, String name) {
            super(parentLocation, name);
        }

        @Override
        public boolean remove() {
            return false;
        }

        @Override
        public PropertyState getProperty() {
            return parentLocation.tree.getProperty(name);
        }

        @Override
        public Status getStatus() {
            return Status.EXISTING;
        }
    }
}
