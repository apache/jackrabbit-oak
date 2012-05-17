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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

public class RootImpl implements Root {
    static final Logger log = LoggerFactory.getLogger(RootImpl.class);

    /** The underlying store to which this root belongs */
    private final NodeStore store;

    /** The name of the workspace we are operating on */
    private final String workspaceName;

    /** Actual root element of the {@code Tree} */
    private final TreeImpl root;

    /** Current branch this root operates on */
    private NodeStoreBranch branch;

    /**
     * New instance bases on a given {@link NodeStore} and a workspace
     * @param store  node store
     * @param workspaceName  name of the workspace
     */
    public RootImpl(NodeStore store, String workspaceName) {
        this.store = store;
        this.workspaceName = workspaceName;
        branch = store.branch();
        root = TreeImpl.createRoot(this);
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        TreeImpl source = getChild(sourcePath);
        if (source == null) {
            return false;
        }
        TreeImpl destParent = getChild(getParentPath(destPath));
        if (destParent == null) {
            return false;
        }

        String destName = getName(destPath);
        if (source.moveTo(destParent, destName)) {
            branch.move(
                PathUtils.concat(workspaceName, sourcePath),
                PathUtils.concat(workspaceName, destPath));

            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public boolean copy(String sourcePath, String destPath) {
        return branch.copy(
            PathUtils.concat(workspaceName, sourcePath),
            PathUtils.concat(workspaceName, destPath));
    }

    @Override
    public Tree getTree(String path) {
        return getChild(path);
    }

    @Override
    public void rebase() {
        root.clear();
        NodeState base = getWorkspaceBaseState();
        NodeState head = getWorkspaceRootState();
        branch = store.branch();
        merge(base, head, getRoot());
    }

    @Override
    public void clear() {
        root.clear();
        branch = store.branch();
    }

    @Override
    public void commit() throws CommitFailedException {
        branch.merge();
        root.clear();
        branch = store.branch();
    }

    @Override
    public boolean hasPendingChanges() {
        return !branch.getBase().equals(branch.getRoot());
    }

    /**
     * Returns the current root node state of the workspace
     * @return root node state
     */
    public NodeState getWorkspaceRootState() {
        return branch.getRoot().getChildNode(workspaceName);
    }

    /**
     * Returns the node state of the workspace from which
     * the current branch was created.
     * @return base node state
     */
    public NodeState getWorkspaceBaseState() {
        return branch.getBase().getChildNode(workspaceName);
    }

    /**
     * Returns a builder for constructing a new or modified node state.
     * The builder is initialized with all the properties and child nodes
     * from the given base node state.
     *
     * @param nodeState  base node state, or {@code null} for building new nodes
     * @return  builder instance
     */
    public NodeStateBuilder getBuilder(NodeState nodeState) {
        return store.getBuilder(nodeState);
    }

    /**
     * Set the node state of the current workspace
     *
     * @param nodeState  node state representing the modified workspace
     */
    public void setWorkspaceRootState(NodeState nodeState) {
        NodeStateBuilder builder = getBuilder(branch.getRoot());
        builder.setNode(workspaceName, nodeState);
        branch.setRoot(builder.getNodeState());
    }

    /**
     * Compares the given two node states. Any found differences are
     * reported by calling the relevant added, changed or deleted methods
     * of the given handler.
     *
     * @param before node state before changes
     * @param after node state after changes
     * @param diffHandler handler of node state differences
     */
    public void compare(NodeState before, NodeState after, NodeStateDiff diffHandler) {
        store.compare(before, after, diffHandler);
    }

    //------------------------------------------------------------< private >---

    private TreeImpl getRoot() {
        return root;
    }

    /**
     * Get a tree for the child identified by {@code path}
     * @param path  the path to the child
     * @return  a {@link Tree} instance for the child
     *          at {@code path} or {@code null} if no such item exits.
     */
    private TreeImpl getChild(String path) {
        TreeImpl child = getRoot();
        for (String name : elements(path)) {
            child = child.getChild(name);
            if (child == null) {
                return null;
            }
        }
        return child;
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
                for (ChildNodeEntry entry : state.getChildNodeEntries()) {
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

        });
    }

    private static <T> List<T> toList(Iterable<T> values) {
        List<T> l = new ArrayList<T>();
        for (T value : values) {
            l.add(value);
        }
        return l;
    }

}
