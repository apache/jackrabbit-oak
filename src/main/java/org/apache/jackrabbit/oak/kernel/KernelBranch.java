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
import org.apache.jackrabbit.oak.api.Scalar;
import org.apache.jackrabbit.oak.api.TransientNodeState;
import org.apache.jackrabbit.oak.kernel.TransientKernelNodeState.Listener;

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
    private final TransientKernelNodeState root;

    /**
     * Create a new branch for the given base node state
     * @param base  base node state of the private branch
     */
    KernelBranch(NodeState base) {
        this.base = base;
        root = new TransientKernelNodeState(base, changeLog);
    }

    @Override
    public void move(String sourcePath, String destPath) {
        TransientKernelNodeState sourceNode = getTransientState(sourcePath);
        if (sourceNode == null) {
            return;
        }

        TransientKernelNodeState destParent = getTransientState(getParentPath(destPath));
        String destName = getName(destPath);
        if (destParent == null || destParent.hasNode(destName)) {
            return;
        }

        sourceNode.move(destParent, destName);
    }

    @Override
    public void copy(String sourcePath, String destPath) {
        TransientKernelNodeState sourceNode = getTransientState(sourcePath);
        if (sourceNode == null) {
            return;
        }

        TransientKernelNodeState destParent = getTransientState(getParentPath(destPath));
        String destName = getName(destPath);
        if (destParent == null || destParent.hasNode(destName)) {
            return;
        }

        sourceNode.copy(destParent, destName);
    }

    @Override
    public TransientNodeState getNode(String path) {
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
     * @return  a {@link TransientKernelNodeState} instance for the item
     *          at {@code path} or {@code null} if no such item exits.
     */
    private TransientKernelNodeState getTransientState(String path) {
        TransientKernelNodeState state = root;
        for (String name : elements(path)) {
            state = state.getChildNode(name);
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
    private static String path(TransientNodeState state, String name) {
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
        public void addNode(TransientKernelNodeState state, String name) {
            jsop.append("+\"").append(path(state, name)).append("\":{}");
        }

        @Override
        public void removeNode(TransientKernelNodeState state, String name) {
            jsop.append("-\"").append(path(state, name)).append('"');
        }

        @Override
        public void setProperty(TransientKernelNodeState state, String name, Scalar value) {
            jsop.append("^\"").append(path(state, name)).append("\":").append(encode(value));
        }

        @Override
        public void setProperty(TransientKernelNodeState state, String name, List<Scalar> values) {
            jsop.append("^\"").append(path(state, name)).append("\":").append(encode(values));
        }

        @Override
        public void removeProperty(TransientKernelNodeState state, String name) {
            jsop.append("^\"").append(path(state, name)).append("\":null");
        }

        @Override
        public void move(TransientKernelNodeState state, String name, TransientKernelNodeState moved) {
            jsop.append(">\"").append(path(state, name)).append("\":\"")
                    .append(moved.getPath()).append('"');
        }

        @Override
        public void copy(TransientKernelNodeState state, String name, TransientKernelNodeState copied) {
            jsop.append("*\"").append(path(state, name)).append("\":\"")
                    .append(copied.getPath()).append('"');
        }

        public String toJsop() {
            return jsop.toString();
        }
    }
}
