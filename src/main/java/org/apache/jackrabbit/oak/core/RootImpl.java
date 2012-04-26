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
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.TreeImpl.Listener;
import org.apache.jackrabbit.oak.kernel.NodeState;
import org.apache.jackrabbit.oak.kernel.NodeStateBuilder;
import org.apache.jackrabbit.oak.kernel.NodeStore;

import java.util.List;

import static org.apache.jackrabbit.mk.util.PathUtils.elements;
import static org.apache.jackrabbit.mk.util.PathUtils.getName;
import static org.apache.jackrabbit.mk.util.PathUtils.getParentPath;

/**
 * This {@code Root} implementation listens on the root of the underlying
 * {@link Tree} using a {@link Listener}. All changes are directly applied
 * to the {@link NodeStateBuilder} for the relevant sub-tree.
 *
 * TODO: Refactor tree to be based on the individual NodeStateBuilders instead of NodeStates
 */
public class RootImpl implements Root {

    private final NodeStore store;
    private final String workspaceName;

    /** Base node state of this tree */
    private NodeState base;

    /** Root state of this tree */
    private TreeImpl root;

    /** Listener for changes on the content tree */
    private TreeListener treeListener = new TreeListener();

    private NodeStateBuilder nodeStateBuilder;

    public RootImpl(NodeStore store, String workspaceName) {
        this.store = store;
        this.workspaceName = workspaceName;
        this.base = store.getRoot().getChildNode(workspaceName);
        nodeStateBuilder = store.getBuilder(base);
        this.root = new TreeImpl(store, nodeStateBuilder, treeListener);
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        TreeImpl source = getTransientState(sourcePath);
        if (source == null) {
            return false;
        }

        TreeImpl destParent = getTransientState(getParentPath(destPath));
        String destName = getName(destPath);
        return destParent != null && source.move(destParent, destName);

    }

    @Override
    public boolean copy(String sourcePath, String destPath) {
        TreeImpl sourceNode = getTransientState(sourcePath);
        if (sourceNode == null) {
            return false;
        }

        TreeImpl destParent = getTransientState(getParentPath(destPath));
        String destName = getName(destPath);
        return destParent != null && sourceNode.copy(destParent, destName);

    }

    @Override
    public Tree getTree(String path) {
        return getTransientState(path);
    }

    @Override
    public void rebase() {
        // TODO implement rebase base = store.getRoot().getChildNode(workspaceName);
    }

    @Override
    public void commit() throws CommitFailedException {
        store.apply(nodeStateBuilder);
        base = store.getRoot().getChildNode(workspaceName);
        nodeStateBuilder = store.getBuilder(base);
        treeListener = new TreeListener();
        root = new TreeImpl(store, nodeStateBuilder, treeListener);
    }

    @Override
    public boolean hasPendingChanges() {
        return treeListener.hasChanges();
    }

    //------------------------------------------------------------< private >---

    /**
     * Get a transient node state for the node identified by
     * {@code path}
     * @param path  the path to the node state
     * @return  a {@link Tree} instance for the item
     *          at {@code path} or {@code null} if no such item exits.
     */
    private TreeImpl getTransientState(String path) {
        TreeImpl state = root;
        for (String name : elements(path)) {
            state = state.getChild(name);
            if (state == null) {
                return null;
            }
        }
        return state;
    }

    // TODO accumulate change log for refresh/rebase
    private class TreeListener implements Listener {
        private boolean isDirty;

        @Override
        public void addChild(TreeImpl tree, String name) {
            isDirty = true;
        }

        @Override
        public void removeChild(TreeImpl tree, String name) {
            isDirty = true;
        }

        @Override
        public void setProperty(TreeImpl tree, String name, CoreValue value) {
            isDirty = true;
        }

        @Override
        public void setProperty(TreeImpl tree, String name, List<CoreValue> values) {
            isDirty = true;
        }

        @Override
        public void removeProperty(TreeImpl tree, String name) {
            isDirty = true;
        }

        @Override
        public void move(TreeImpl tree, String name, TreeImpl moved) {
            isDirty = true;
        }

        @Override
        public void copy(TreeImpl tree, String name, TreeImpl copied) {
            isDirty = true;
        }

        public boolean hasChanges() {
            return isDirty;
        }
    }

}
