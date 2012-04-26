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

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.util.PathUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.TreeImpl.Listener;
import org.apache.jackrabbit.oak.kernel.NodeState;
import org.apache.jackrabbit.oak.kernel.NodeStateBuilder;
import org.apache.jackrabbit.oak.kernel.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.mk.util.PathUtils.elements;
import static org.apache.jackrabbit.mk.util.PathUtils.getName;
import static org.apache.jackrabbit.mk.util.PathUtils.getParentPath;

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
        TreeListener changes = treeListener;

        treeListener = new TreeListener();
        base = store.getRoot().getChildNode(workspaceName);
        nodeStateBuilder = store.getBuilder(base);
        root = new TreeImpl(store, nodeStateBuilder, treeListener);

        if (mergeChanges) {
            merge(changes);
        }
    }

    private void merge(TreeListener changes) {
        for (Operation operation : changes.getChanges()) {
            try {
                switch (operation.type) {
                    case ADD_NODE: {
                        String parentPath = PathUtils.getParentPath(operation.targetPath);
                        String name = PathUtils.getName(operation.targetPath);
                        getChild(parentPath).addChild(name);
                        break;
                    }
                    case REMOVE_NODE: {
                        String parentPath = PathUtils.getParentPath(operation.targetPath);
                        String name = PathUtils.getName(operation.targetPath);
                        getChild(parentPath).removeChild(name);
                        break;
                    }
                    case SET_PROPERTY: {
                        String parentPath = PathUtils.getParentPath(operation.targetPath);
                        String name = PathUtils.getName(operation.targetPath);
                        if (operation.isMultiple) {
                            getChild(parentPath).setProperty(name, operation.values.get(0));
                        }
                        else {
                            getChild(parentPath).setProperty(name, operation.values);
                        }
                        break;
                    }
                    case REMOVE_PROPERTY: {
                        String parentPath = PathUtils.getParentPath(operation.targetPath);
                        String name = PathUtils.getName(operation.targetPath);
                        getChild(parentPath).removeProperty(name);
                        break;
                    }
                    case MOVE: {
                        move(operation.sourcePath, operation.targetPath);
                        break;
                    }
                    case COPY: {
                        copy(operation.sourcePath, operation.targetPath);
                        break;
                    }
                }
            }
            catch (MicroKernelException e) {
                log.warn("Skipping failed operation on refresh:" + operation);
            }
        }
    }


    private static class TreeListener implements Listener {
        private final List<Operation> operations = new ArrayList<Operation>();

        @Override
        public void addChild(TreeImpl parent, String name) {
            String targetPath = PathUtils.concat(parent.getPath(), name);
            operations.add(Operation.addNode(targetPath));
        }

        @Override
        public void removeChild(TreeImpl parent, String name) {
            String targetPath = PathUtils.concat(parent.getPath(), name);
            operations.add(Operation.removeNode(targetPath));
        }

        @Override
        public void setProperty(TreeImpl parent, String name, CoreValue value) {
            String targetPath = PathUtils.concat(parent.getPath(), name);
            operations.add(Operation.setProperty(targetPath, value));
        }

        @Override
        public void setProperty(TreeImpl parent, String name, List<CoreValue> values) {
            String targetPath = PathUtils.concat(parent.getPath(), name);
            operations.add(Operation.setProperty(targetPath, values));
        }

        @Override
        public void removeProperty(TreeImpl parent, String name) {
            String targetPath = PathUtils.concat(parent.getPath(), name);
            operations.add(Operation.removeProperty(targetPath));
        }

        @Override
        public void move(TreeImpl sourceParent, String sourceName, TreeImpl moved) {
            String sourcePath = PathUtils.concat(sourceParent.getPath(), sourceName);
            operations.add(Operation.move(sourcePath, moved.getPath()));
        }

        @Override
        public void copy(TreeImpl sourceParent, String sourceName, TreeImpl copied) {
            String sourcePath = PathUtils.concat(sourceParent.getPath(), sourceName);
            operations.add(Operation.copy(sourcePath, copied.getPath()));
        }

        boolean hasChanges() {
            return !operations.isEmpty();
        }

        List<Operation> getChanges() {
            return operations;
        }
    }

    private static class Operation {
        final Type type;
        final String targetPath;
        final String sourcePath;
        final List<CoreValue> values;
        final boolean isMultiple;

        enum Type {ADD_NODE, REMOVE_NODE, SET_PROPERTY, REMOVE_PROPERTY, MOVE, COPY}

        private Operation(Type type, String targetPath, String sourcePath,
                List<CoreValue> values, boolean isMultiple) {

            this.type = type;
            this.targetPath = targetPath;
            this.sourcePath = sourcePath;
            this.values = values;
            this.isMultiple = isMultiple;
        }

        static Operation addNode(String targetPath) {
            return new Operation(Type.ADD_NODE, targetPath, null, null, false);
        }

        static Operation removeNode(String targetPath) {
            return new Operation(Type.REMOVE_NODE, targetPath, null, null, false);
        }

        static Operation setProperty(String targetPath, CoreValue value) {
            return new Operation(Type.SET_PROPERTY, targetPath, null, singletonList(value), false);
        }

        static Operation setProperty(String targetPath, List<CoreValue> values) {
            return new Operation(Type.SET_PROPERTY, targetPath, null, values, true);
        }

        static Operation removeProperty(String targetPath) {
            return new Operation(Type.REMOVE_PROPERTY, targetPath, null, null, false);
        }

        static Operation move(String sourcePath, String targetPath) {
            return new Operation(Type.MOVE, targetPath, sourcePath, null, false);
        }

        static Operation copy(String sourcePath, String targetPath) {
            return new Operation(Type.COPY, targetPath, sourcePath, null, false);
        }

        @Override
        public String toString() {
            switch (type) {
                case ADD_NODE:
                    return '+' + targetPath + ":{}";
                case REMOVE_NODE:
                    return '-' + targetPath;
                case SET_PROPERTY:
                    return '^' + targetPath + ':' + (isMultiple ? values : values.get(0));
                case REMOVE_PROPERTY:
                    return '^' + targetPath + ":null";
                case MOVE:
                    return '>' + sourcePath + ':' + targetPath;
                case COPY:
                    return '*' + sourcePath + ':' + targetPath;
            }
            throw new IllegalStateException("We should never get here");
        }
    }

}
