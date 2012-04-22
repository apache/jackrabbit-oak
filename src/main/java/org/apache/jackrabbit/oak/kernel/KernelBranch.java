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
import org.apache.jackrabbit.mk.json.JsonBuilder;
import org.apache.jackrabbit.oak.api.Branch;
import org.apache.jackrabbit.oak.api.ContentTree;
import org.apache.jackrabbit.oak.api.Scalar;
import org.apache.jackrabbit.oak.kernel.KernelContentTree.Listener;

import java.util.List;

import static org.apache.jackrabbit.mk.util.PathUtils.elements;
import static org.apache.jackrabbit.mk.util.PathUtils.getName;
import static org.apache.jackrabbit.mk.util.PathUtils.getParentPath;

/**
 * This {@code Branch} implementation accumulates all changes into a json diff
 * and applies them to the microkernel on
 * {@link NodeStore#merge(org.apache.jackrabbit.oak.api.Branch)}
 *
 * TODO: review/rewrite when OAK-45 is resolved
 * When the MicroKernel has support for branching and merging private working copies,
 * this implementation could:
 * - directly write every operation through to the private working copy
 * - batch write operations through to the private working copy when the
 *   transient space gets too big.
 * - spool write operations through to the private working copy on a background thread
 */
public class KernelBranch implements Branch {

    /** Log of changes to this branch */
    private final ChangeLog changeLog = new ChangeLog();

    /** Base node state of this private branch */
    private final NodeState base;

    /** Root state of this branch */
    private final KernelContentTree root;

    /**
     * Create a new branch for the given base node state
     * @param base  base node state of the private branch
     */
    KernelBranch(NodeState base) {
        this.base = base;
        root = new KernelContentTree(base, changeLog);
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        KernelContentTree source = getTransientState(sourcePath);
        if (source == null) {
            return false;
        }

        KernelContentTree destParent = getTransientState(getParentPath(destPath));
        String destName = getName(destPath);
        if (destParent == null || destParent.hasChild(destName)) {
            return false;
        }

        source.move(destParent, destName);
        return true;
    }

    @Override
    public boolean copy(String sourcePath, String destPath) {
        KernelContentTree sourceNode = getTransientState(sourcePath);
        if (sourceNode == null) {
            return false;
        }

        KernelContentTree destParent = getTransientState(getParentPath(destPath));
        String destName = getName(destPath);
        if (destParent == null || destParent.hasChild(destName)) {
            return false;
        }

        sourceNode.copy(destParent, destName);
        return true;
    }

    @Override
    public ContentTree getContentTree(String path) {
        return getTransientState(path);
    }


    //------------------------------------------------------------< internal >---
    /**
     * Return the base node state of this private branch
     * @return base node state
     */
    NodeState getBaseNodeState() {
        return base;
    }

    /**
     * Atomically merges the changes from this branch back into the
     * {@code target}.
     *
     * @param microkernel Microkernel instance for applying the changes
     * @param target target of the merge operation
     * @return node state resulting from merging
     */
    KernelNodeState mergeInto(MicroKernel microkernel, KernelNodeState target) {
        String targetPath = target.getPath();
        String targetRevision = target.getRevision();
        String rev = microkernel.commit(targetPath, changeLog.toJsop(), targetRevision, null);
        return new KernelNodeState(microkernel, targetPath, rev);
    }

    /**
     * Get a transient node state for the node identified by
     * {@code path}
     * @param path  the path to the node state
     * @return  a {@link KernelContentTree} instance for the item
     *          at {@code path} or {@code null} if no such item exits.
     */
    private KernelContentTree getTransientState(String path) {
        KernelContentTree state = root;
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
    private static String path(ContentTree state, String name) {
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
     * a associated branch as JSOP.
     */
    private static class ChangeLog implements Listener {
        private final StringBuilder jsop = new StringBuilder();

        @Override
        public void addChild(KernelContentTree tree, String name) {
            jsop.append("+\"").append(path(tree, name)).append("\":{}");
        }

        @Override
        public void removeChild(KernelContentTree tree, String name) {
            jsop.append("-\"").append(path(tree, name)).append('"');
        }

        @Override
        public void setProperty(KernelContentTree tree, String name, Scalar value) {
            jsop.append("^\"").append(path(tree, name)).append("\":").append(encode(value));
        }

        @Override
        public void setProperty(KernelContentTree tree, String name, List<Scalar> values) {
            jsop.append("^\"").append(path(tree, name)).append("\":").append(encode(values));
        }

        @Override
        public void removeProperty(KernelContentTree tree, String name) {
            jsop.append("^\"").append(path(tree, name)).append("\":null");
        }

        @Override
        public void move(KernelContentTree tree, String name, KernelContentTree moved) {
            jsop.append(">\"").append(path(tree, name)).append("\":\"")
                    .append(moved.getPath()).append('"');
        }

        @Override
        public void copy(KernelContentTree state, String name, KernelContentTree copied) {
            jsop.append("*\"").append(path(state, name)).append("\":\"")
                    .append(copied.getPath()).append('"');
        }

        public String toJsop() {
            return jsop.toString();
        }
    }
}
