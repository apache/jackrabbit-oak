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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.TreeImpl.Listener;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.util.Iterators;
import org.apache.jackrabbit.oak.util.PagedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

/**
 * This {@code Root} implementation listens on the root of the underlying
 * {@link Tree} using a {@link Listener}. All changes are directly applied
 * to the {@link NodeStateBuilder} for the relevant sub-tree.
 */
public class RootImpl implements Root {
    static final Logger log = LoggerFactory.getLogger(RootImpl.class);

    /** The underlying store to which this root belongs */
    private final NodeStore store;

    /** The name of the workspace we are operating on */
    private final String workspaceName;

    /** Listener for changes on the tree */
    private TreeListener treeListener = new TreeListener();

    /** Base node state of this tree */
    private NodeState base;

    /** The builder for this root */
    private NodeStateBuilder nodeStateBuilder;

    /** Root state of this tree */
    private TreeImpl root;

    /**
     * New instance bases on a given {@link NodeStore} and a workspace
     * @param store  node store
     * @param workspaceName  name of the workspace
     */
    public RootImpl(NodeStore store, String workspaceName) {
        this.store = store;
        this.workspaceName = workspaceName;
        this.base = store.getRoot().getChildNode(workspaceName);
        nodeStateBuilder = store.getBuilder(base);
        this.root = new TreeImpl(store, nodeStateBuilder, treeListener);
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        TreeImpl source = getChild(sourcePath);
        if (source == null) {
            return false;
        }

        TreeImpl destParent = getChild(getParentPath(destPath));
        String destName = getName(destPath);
        return destParent != null && source.move(destParent, destName);

    }

    @Override
    public boolean copy(String sourcePath, String destPath) {
        TreeImpl sourceNode = getChild(sourcePath);
        if (sourceNode == null) {
            return false;
        }

        TreeImpl destParent = getChild(getParentPath(destPath));
        String destName = getName(destPath);
        return destParent != null && sourceNode.copy(destParent, destName);

    }

    @Override
    public Tree getTree(String path) {
        return getChild(path);
    }

    @Override
    public void rebase() {
        rebase(true);
    }

    @Override
    public void commit() throws CommitFailedException {
        store.apply(nodeStateBuilder);
        rebase(false);
    }

    @Override
    public boolean hasPendingChanges() {
        return treeListener.hasChanges();
    }

    //------------------------------------------------------------< private >---

    /**
     * Get a tree for the child identified by {@code path}
     * @param path  the path to the child
     * @return  a {@link Tree} instance for the child
     *          at {@code path} or {@code null} if no such item exits.
     */
    private TreeImpl getChild(String path) {
        TreeImpl state = root;
        for (String name : elements(path)) {
            state = state.getChild(name);
            if (state == null) {
                return null;
            }
        }
        return state;
    }

    private void rebase(boolean mergeChanges) {
        NodeState oldBase;
        NodeState oldHead;
        if (mergeChanges) {
            oldBase = base;
            oldHead = nodeStateBuilder.getNodeState();
        }
        else {
            oldBase = null;
            oldHead = null;
        }

        treeListener = new TreeListener();
        base = store.getRoot().getChildNode(workspaceName);
        nodeStateBuilder = store.getBuilder(base);
        root = new TreeImpl(store, nodeStateBuilder, treeListener);

        if (mergeChanges) {
            merge(oldBase, oldHead, root);
        }

    }

    private void merge(NodeState fromState, NodeState toState, final Tree target) {
        store.compare(fromState, toState, new NodeStateDiff() {
            @Override
            public void propertyAdded(PropertyState after) {
                setProperty(after, target);
            }

            @Override
            public void propertyChanged(PropertyState before, PropertyState after) {
                setProperty(after, target);
            }

            @Override
            public void propertyDeleted(PropertyState before) {
                target.removeProperty(before.getName());
            }

            @Override
            public void childNodeAdded(String name, NodeState after) {
                addChild(name, after, target);
            }

            @Override
            public void childNodeChanged(String name, NodeState before, NodeState after) {
                Tree child = target.getChild(name);
                if (child != null) {
                    merge(before, after, child);
                }
            }

            @Override
            public void childNodeDeleted(String name, NodeState before) {
                target.removeChild(name);
            }

            private void addChild(String name, NodeState state, Tree target) {
                Tree child = target.addChild(name);
                for (PropertyState property : state.getProperties()) {
                    setProperty(property, child);
                }
                for (ChildNodeEntry entry : childNodeEntries(state)) {
                    addChild(entry.getName(), entry.getNodeState(), child);
                }
            }

            private void setProperty(PropertyState property, Tree target) {
                if (property.isArray()) {
                    target.setProperty(property.getName(), toList(property.getValues()));
                }
                else {
                    target.setProperty(property.getName(), property.getValue());
                }
            }

            @SuppressWarnings("unchecked")
            private Iterable<? extends ChildNodeEntry> childNodeEntries(final NodeState nodeState) {
                return new Iterable() {  // Java's type system is too weak to express the exact type here
                    @Override
                    public Iterator<? extends ChildNodeEntry> iterator() {
                        return Iterators.flatten(
                            new PagedIterator<ChildNodeEntry>(1024) {
                                @Override
                                protected Iterator<? extends ChildNodeEntry> getPage(long pos, int size) {
                                    return nodeState.getChildNodeEntries(pos, size).iterator();
                                }
                            });
                    }
                };
            }

        });
    }

    private static <T> List<T> toList(Iterable<T> values) {
        List<T> l = new ArrayList<T>();
        for (T value : values) {
            l.add(value);
        }
        return l;
    }

    private static class TreeListener implements Listener {
        private boolean hasChanges;

        @Override
        public void addChild(TreeImpl parent, String name) {
            hasChanges = true;
        }

        @Override
        public void removeChild(TreeImpl parent, String name) {
            hasChanges = true;
        }

        @Override
        public void setProperty(TreeImpl parent, String name, CoreValue value) {
            hasChanges = true;
        }

        @Override
        public void setProperty(TreeImpl parent, String name, List<CoreValue> values) {
            hasChanges = true;
        }

        @Override
        public void removeProperty(TreeImpl parent, String name) {
            hasChanges = true;
        }

        @Override
        public void move(TreeImpl sourceParent, String sourceName, TreeImpl moved) {
            hasChanges = true;
        }

        @Override
        public void copy(TreeImpl sourceParent, String sourceName, TreeImpl copied) {
            hasChanges = true;
        }

        boolean hasChanges() {
            return hasChanges;
        }

    }

}
