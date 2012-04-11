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
import org.apache.jackrabbit.oak.api.NodeState;
import org.apache.jackrabbit.oak.api.NodeStateEditor;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Scalar;

import java.util.List;

import static org.apache.jackrabbit.mk.util.PathUtils.elements;
import static org.apache.jackrabbit.mk.util.PathUtils.getName;
import static org.apache.jackrabbit.mk.util.PathUtils.getParentPath;

/**
 * This {@code NodeStateEditor} implementation accumulates all changes into a json diff
 * and applies them to the microkernel on
 * {@link org.apache.jackrabbit.mk.model.NodeStore#merge(NodeStateEditor, NodeState)}.
 *
 * TODO: review/rewrite when OAK-45 is resolved
 * When the MicroKernel has support for branching and merging private working copies,
 * this implementation could:
 * - directly write every operation through to the private working copy
 * - batch write operations through to the private working copy when the
 *   transient space gets too big.
 * - spool write operations through to the private working copy on a background thread
 */
public class KernelNodeStateEditor implements NodeStateEditor {

    /** Base node state of this private branch */
    private final NodeState base;

    /** Transient state this editor is acting upon */
    private final TransientNodeState transientState;

    /** Json diff of this private branch */
    private final StringBuilder jsop;

    /**
     * Create a new node state editor representing the root of a fresh
     * private branch.
     * @param base  base node state of the private branch
     */
    KernelNodeStateEditor(NodeState base) {
        this.base = base;
        transientState = new TransientNodeState(base, this);
        jsop = new StringBuilder();
    }

    /**
     * Create a new node state editor for a given transient node state.
     * @param parentEditor  editor of the parent of {@code state}
     * @param state  transient node state for which to create the node state editor
     */
    KernelNodeStateEditor(KernelNodeStateEditor parentEditor, TransientNodeState state) {
        base = parentEditor.base;
        transientState = state;
        jsop = parentEditor.jsop;
    }

    @Override
    public void addNode(String name) {
        if (!transientState.hasNode(name)) {
            transientState.addNode(name);
            jsop.append("+\"").append(path(name)).append("\":{}");
        }
    }

    @Override
    public void removeNode(String name) {
        if (transientState.hasNode(name)) {
            transientState.removeNode(name);
            jsop.append("-\"").append(path(name)).append('"');
        }
    }

    @Override
    public void setProperty(String name, Scalar value) {
        PropertyState propertyState = new KernelPropertyState(name, value);
        transientState.setProperty(propertyState);
        jsop.append("^\"").append(path(propertyState.getName())).append("\":")
                .append(encode(propertyState));
    }

    @Override
    public void setProperty(String name, List<Scalar> values) {
        PropertyState propertyState = new KernelPropertyState(name, values);
        transientState.setProperty(propertyState);
        jsop.append("^\"").append(path(propertyState.getName())).append("\":")
                .append(encode(propertyState));
    }

    @Override
    public void removeProperty(String name) {
        transientState.removeProperty(name);
        jsop.append("^\"").append(path(name)).append("\":null");
    }

    @Override
    public void move(String sourcePath, String destPath) {
        TransientNodeState sourceParent = getTransientState(getParentPath(sourcePath));
        String sourceName = getName(sourcePath);
        if (sourceParent == null || !sourceParent.hasNode(sourceName)) {
            return;
        }
        
        TransientNodeState destParent = getTransientState(getParentPath(destPath));
        String destName = getName(destPath);
        if (destParent == null || destParent.hasNode(destName)) {
            return;
        }

        sourceParent.move(sourceName, destParent, destName);
        jsop.append(">\"").append(path(sourcePath))
                .append("\":\"").append(path(destPath)).append('"');
    }

    @Override
    public void copy(String sourcePath, String destPath) {
        TransientNodeState sourceParent = getTransientState(getParentPath(sourcePath));
        String sourceName = getName(sourcePath);
        if (sourceParent == null || !sourceParent.hasNode(sourceName)) {
            return;
        }

        TransientNodeState destParent = getTransientState(getParentPath(destPath));
        String destName = getName(destPath);
        if (destParent == null || destParent.hasNode(destName)) {
            return;
        }

        sourceParent.copy(sourceName, destParent, destName);
        jsop.append("*\"").append(path(sourcePath)).append("\":\"")
                .append(path(destPath)).append('"');
    }

    @Override
    public KernelNodeStateEditor edit(String name) {
        TransientNodeState childState = transientState.getChildNode(name);
        return childState == null
            ? null
            : childState.getEditor();
    }

    @Override
    public NodeState getBaseNodeState() {
        return base;
    }

    /**
     * @return the {@link TransientNodeState} instance this editor is
     *         acting upon.
     */
    public TransientNodeState getTransientState() {
        return transientState;
    }

    //------------------------------------------------------------< internal >---

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
        String rev = microkernel.commit(targetPath, jsop.toString(), targetRevision, null);
        return new KernelNodeState(microkernel, targetPath, rev);
    }

    /**
     * Get a transient node state for the node identified by
     * {@code path}
     * @param path  the path to the node state
     * @return  a {@link TransientNodeState} instance for the item
     *          at {@code path} or {@code null} if no such item exits.
     */
    private TransientNodeState getTransientState(String path) {
        TransientNodeState state = transientState;
        for (String name : elements(path)) {
            state = state.getChildNode(name);
            if (state == null) {
                return null;
            }
        }
        return state;
    }

    /**
     * Path of the item {@code name}
     * @param name
     * @return relative path of the item {@code name}
     */
    private String path(String name) {
        String path = transientState.getPath();
        return path.isEmpty() ? name : path + '/' + name;
    }

    private String encode(PropertyState state) {
        if (state.isArray()) {
            return encode(state.getArray());
        } else {
            return encode(state.getScalar());
        }
    }

    private String encode(Scalar scalar) {
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

    private String encode(Iterable<Scalar> scalars) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (Scalar scalar : scalars) {
            sb.append(encode(scalar));
            sb.append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(']');
        return sb.toString();
    }

}
