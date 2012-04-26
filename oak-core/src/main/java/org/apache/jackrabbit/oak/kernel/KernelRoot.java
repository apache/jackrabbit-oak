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
package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.kernel.KernelTree2.Listener;

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
public class KernelRoot implements Root {

    private final KernelNodeStore store;
    private final String workspaceName;

    /** Base node state of this tree */
    private NodeState base;

    /** Root state of this tree */
    private KernelTree2 root;

    /** Listener for changes on the content tree */
    private TreeListener treeListener = new TreeListener();

    private NodeStateBuilder nodeStateBuilder;

    public KernelRoot(KernelNodeStore store, String workspaceName) {
        this.store = store;
        this.workspaceName = workspaceName;
        this.base = store.getRoot().getChildNode(workspaceName);
        nodeStateBuilder = store.getBuilder(base);
        this.root = new KernelTree2(store, nodeStateBuilder, treeListener);
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        KernelTree2 source = getTransientState(sourcePath);
        if (source == null) {
            return false;
        }

        KernelTree2 destParent = getTransientState(getParentPath(destPath));
        String destName = getName(destPath);
        return destParent != null && source.move(destParent, destName);

    }

    @Override
    public boolean copy(String sourcePath, String destPath) {
        KernelTree2 sourceNode = getTransientState(sourcePath);
        if (sourceNode == null) {
            return false;
        }

        KernelTree2 destParent = getTransientState(getParentPath(destPath));
        String destName = getName(destPath);
        return destParent != null && sourceNode.copy(destParent, destName);

    }

    @Override
    public Tree getTree(String path) {
        return getTransientState(path);
    }

    @Override
    public void refresh() {
        // TODO implement refresh base = store.getRoot().getChildNode(workspaceName);
    }

    @Override
    public void commit() throws CommitFailedException {
        store.apply(nodeStateBuilder);
        base = store.getRoot().getChildNode(workspaceName);
        nodeStateBuilder = store.getBuilder(base);
        treeListener = new TreeListener();
        root = new KernelTree2(store, nodeStateBuilder, treeListener);
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
     * @return  a {@link KernelTree} instance for the item
     *          at {@code path} or {@code null} if no such item exits.
     */
    private KernelTree2 getTransientState(String path) {
        KernelTree2 state = root;
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
        public void addChild(KernelTree2 tree, String name) {
            isDirty = true;
        }

        @Override
        public void removeChild(KernelTree2 tree, String name) {
            isDirty = true;
        }

        @Override
        public void setProperty(KernelTree2 tree, String name, CoreValue value) {
            isDirty = true;
        }

        @Override
        public void setProperty(KernelTree2 tree, String name, List<CoreValue> values) {
            isDirty = true;
        }

        @Override
        public void removeProperty(KernelTree2 tree, String name) {
            isDirty = true;
        }

        @Override
        public void move(KernelTree2 tree, String name, KernelTree2 moved) {
            isDirty = true;
        }

        @Override
        public void copy(KernelTree2 tree, String name, KernelTree2 copied) {
            isDirty = true;
        }

        public boolean hasChanges() {
            return isDirty;
        }
    }

}
