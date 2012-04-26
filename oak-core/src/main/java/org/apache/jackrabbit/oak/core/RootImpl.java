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
 */
public class RootImpl implements Root {

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
        // TODO implement rebase base = store.getRoot().getChildNode(workspaceName);
    }

    @Override
    public void commit() throws CommitFailedException {
        store.apply(nodeStateBuilder);
        treeListener = new TreeListener();
        base = store.getRoot().getChildNode(workspaceName);
        nodeStateBuilder = store.getBuilder(base);
        root = new TreeImpl(store, nodeStateBuilder, treeListener);
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
