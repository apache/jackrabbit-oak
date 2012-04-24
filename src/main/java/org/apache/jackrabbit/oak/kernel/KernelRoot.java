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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsonBuilder;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Scalar;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.kernel.KernelTree.Listener;

import java.util.List;

import static org.apache.jackrabbit.mk.util.PathUtils.elements;
import static org.apache.jackrabbit.mk.util.PathUtils.getName;
import static org.apache.jackrabbit.mk.util.PathUtils.getParentPath;

/**
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

    private final NodeStore store;
    private final String workspaceName;

    /** Base node state of this tree */
    private KernelNodeState base;

    /** Root state of tree */
    private KernelTree root;

    /** Log of changes to tree */
    private ChangeLog changeLog = new ChangeLog();

    public KernelRoot(NodeStore store, String workspaceName) {
        this.store = store;
        this.workspaceName = workspaceName;
        this.base = (KernelNodeState) store.getRoot().getChildNode(workspaceName);  // FIXME don't cast to implementation
        this.root = new KernelTree(base, changeLog);
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
        this.base = (KernelNodeState) store.getRoot().getChildNode(workspaceName);  // FIXME don't cast to implementation
    }

    @Override
    public void commit() throws CommitFailedException {
        MicroKernel kernel = ((KernelNodeStore) store).kernel;  // FIXME don't cast to implementation
        try {
            String targetPath = base.getPath();
            String targetRevision = base.getRevision();
            kernel.commit(targetPath, changeLog.toJsop(), targetRevision, null);
            changeLog = new ChangeLog();
            refresh();
        } catch (MicroKernelException e) {
            throw new CommitFailedException(e);
        }
    }


    //------------------------------------------------------------< internal >---
    /**
     * Return the base node state of this tree
     * @return base node state
     */
    NodeState getBaseNodeState() {
        return base;
    }

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

    private static String encode(Scalar scalar) {
        switch (scalar.getType()) {
            case BOOLEAN: return JsonBuilder.encode(scalar.getBoolean());
            case LONG:    return JsonBuilder.encode(scalar.getLong());
            case DOUBLE:  return JsonBuilder.encode(scalar.getDouble());
            case BINARY:  return null; // TODO implement encoding of binaries
            case STRING:  return JsonBuilder.encode(scalar.getString());
            case NULL:    return "null";
        }
        throw new IllegalStateException("unreachable");  // Make javac happy
    }

    private static String encode(Iterable<Scalar> scalars) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (Scalar scalar : scalars) {
            sb.append(encode(scalar));
            sb.append(',');
        }
        if (sb.length() > 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(']');
        return sb.toString();
    }

    /**
     * This {@code Listener} implementation records all changes to
     * a associated tree as JSOP.
     */
    private static class ChangeLog implements Listener {
        private final StringBuilder jsop = new StringBuilder();

        @Override
        public void addChild(KernelTree tree, String name) {
            jsop.append("+\"").append(path(tree, name)).append("\":{}");
        }

        @Override
        public void removeChild(KernelTree tree, String name) {
            jsop.append("-\"").append(path(tree, name)).append('"');
        }

        @Override
        public void setProperty(KernelTree tree, String name, Scalar value) {
            jsop.append("^\"").append(path(tree, name)).append("\":").append(encode(value));
        }

        @Override
        public void setProperty(KernelTree tree, String name, List<Scalar> values) {
            jsop.append("^\"").append(path(tree, name)).append("\":").append(encode(values));
        }

        @Override
        public void removeProperty(KernelTree tree, String name) {
            jsop.append("^\"").append(path(tree, name)).append("\":null");
        }

        @Override
        public void move(KernelTree tree, String name, KernelTree moved) {
            jsop.append(">\"").append(path(tree, name)).append("\":\"")
                    .append(moved.getPath()).append('"');
        }

        @Override
        public void copy(KernelTree state, String name, KernelTree copied) {
            jsop.append("*\"").append(path(state, name)).append("\":\"")
                    .append(copied.getPath()).append('"');
        }

        public String toJsop() {
            return jsop.toString();
        }
    }
}
