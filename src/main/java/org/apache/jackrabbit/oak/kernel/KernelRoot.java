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

import org.apache.jackrabbit.mk.util.PathUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.kernel.KernelTree.Listener;

import java.util.List;

import static org.apache.jackrabbit.mk.util.PathUtils.elements;
import static org.apache.jackrabbit.mk.util.PathUtils.getName;
import static org.apache.jackrabbit.mk.util.PathUtils.getParentPath;

/**
 * FIXME: update javadoc
 * This {@code Root} implementation accumulates all changes into a json diff
 * and applies them to the microkernel on {@link #commit()}
 *
 * TODO: review/rewrite when OAK-45 is resolved
 * When the MicroKernel has support for branching and merging private working copies,
 * this implementation could:
 * - directly write every operation through to the private working copy
 * - batch write operations through to the private working copy when the
 *   transient space gets too big.
 * - spool write operations through to the private working copy on a background thread
 */
public class KernelRoot implements Root {

    private final KernelNodeStore store;
    private final String workspaceName;

    /** Base node state of this tree */
    private NodeState base;

    /** Root state of this tree */
    private KernelTree root;

    /** Listener for changes on the content tree */
    private TreeListener treeListener = new TreeListener();

    private NodeStateBuilder nodeStateBuilder;

    public KernelRoot(KernelNodeStore store, String workspaceName) {
        this.store = store;
        this.workspaceName = workspaceName;
        this.base = store.getRoot().getChildNode(workspaceName);
        this.root = new KernelTree(base, treeListener);
        nodeStateBuilder = store.getBuilder(base);
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        KernelTree source = getTransientState(sourcePath);
        if (source == null) {
            return false;
        }

        KernelTree destParent = getTransientState(getParentPath(destPath));
        String destName = getName(destPath);
        return destParent != null && source.move(destParent, destName);

    }

    @Override
    public boolean copy(String sourcePath, String destPath) {
        KernelTree sourceNode = getTransientState(sourcePath);
        if (sourceNode == null) {
            return false;
        }

        KernelTree destParent = getTransientState(getParentPath(destPath));
        String destName = getName(destPath);
        return destParent != null && sourceNode.copy(destParent, destName);

    }

    @Override
    public Tree getTree(String path) {
        return getTransientState(path);
    }

    @Override
    public void refresh() {
        // TODO implement base = store.getRoot().getChildNode(workspaceName);
    }

    @Override
    public void commit() throws CommitFailedException {
        store.apply(nodeStateBuilder);
        base = store.getRoot().getChildNode(workspaceName);
        nodeStateBuilder = store.getBuilder(base);
        treeListener = new TreeListener();
        root = new KernelTree(base, treeListener);
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
    private KernelTree getTransientState(String path) {
        KernelTree state = root;
        for (String name : elements(path)) {
            state = state.getChild(name);
            if (state == null) {
                return null;
            }
        }
        return state;
    }

    /**
     * Path of the item {@code name} of the given {@code state}
     *
     * @param state
     * @param name The item name.
     * @return relative path of the item {@code name}
     */
    private static String path(Tree state, String name) {
        String path = state.getPath();
        return path.isEmpty() ? name : path + '/' + name;
    }

    private class TreeListener implements Listener {
        private boolean isDirty;
        
        @Override
        public void addChild(KernelTree tree, String name) {
            NodeStateBuilder target = getBuilder(tree);
            target.addNode(name);
            isDirty = true;
        }

        @Override
        public void removeChild(KernelTree tree, String name) {
            NodeStateBuilder target = getBuilder(tree);
            target.removeNode(name);
            isDirty = true;
        }

        @Override
        public void setProperty(KernelTree tree, String name, CoreValue value) {
            NodeStateBuilder target = getBuilder(tree);
            PropertyState propertyState = new KernelPropertyState(name, value);
            target.setProperty(propertyState);
            isDirty = true;
        }

        @Override
        public void setProperty(KernelTree tree, String name, List<CoreValue> values) {
            NodeStateBuilder target = getBuilder(tree);
            PropertyState propertyState = new KernelPropertyState(name, values);
            target.setProperty(propertyState);
            isDirty = true;
        }

        @Override
        public void removeProperty(KernelTree tree, String name) {
            NodeStateBuilder target = getBuilder(tree);
            target.removeProperty(name);
            isDirty = true;
        }

        @Override
        public void move(KernelTree tree, String name, KernelTree moved) {
            NodeStateBuilder source = getBuilder(tree).getChildBuilder(name);
            NodeStateBuilder destParent = getBuilder(moved.getParent());
            source.moveTo(destParent, moved.getName());
            isDirty = true;
        }

        @Override
        public void copy(KernelTree tree, String name, KernelTree copied) {
            NodeStateBuilder source = getBuilder(tree).getChildBuilder(name);
            NodeStateBuilder destParent = getBuilder(copied.getParent());
            source.copyTo(destParent, copied.getName());
            isDirty = true;
        }

        public boolean hasChanges() {
            return isDirty;
        }
    }

    private NodeStateBuilder getBuilder(Tree tree) {
        String path = tree.getPath();
        NodeStateBuilder builder = nodeStateBuilder;
        for (String name : PathUtils.elements(path)) {
            builder = builder.getChildBuilder(name);
        }
        
        return builder;
    }
}
