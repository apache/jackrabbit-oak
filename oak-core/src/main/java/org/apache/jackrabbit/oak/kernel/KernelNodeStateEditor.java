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
import org.apache.jackrabbit.mk.model.NodeState;
import org.apache.jackrabbit.mk.model.NodeStateEditor;
import org.apache.jackrabbit.mk.model.PropertyState;
import org.apache.jackrabbit.mk.util.PathUtils;

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
    private final NodeState base;
    private final TransientNodeState transientState;
    private final StringBuilder jsop;

    KernelNodeStateEditor(NodeState base) {
        this.base = base;
        transientState = new TransientNodeState(base, this, null, "");
        jsop = new StringBuilder();
    }

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
    public void setProperty(PropertyState state) {
        transientState.setProperty(state);
        jsop.append("^\"").append(path(state.getName())).append("\":")
                .append(state.getEncodedValue());
    }

    @Override
    public void removeProperty(String name) {
        transientState.removeProperty(name);
        jsop.append("^\"").append(path(name)).append("\":null");
    }

    @Override
    public void move(String sourcePath, String destPath) {
        TransientNodeState sourceParent = getTransientState(PathUtils.getAncestorPath(sourcePath, 1));
        String sourceName = PathUtils.getName(sourcePath);
        if (sourceParent == null || !sourceParent.hasNode(sourceName)) {
            return;
        }
        
        TransientNodeState destParent = getTransientState(PathUtils.getAncestorPath(destPath, 1));
        String destName = PathUtils.getName(destPath);
        if (destParent == null || destParent.hasNode(destName)) {
            return;
        }

        sourceParent.move(sourceName, destParent, destName);
        jsop.append(">\"").append(path(sourcePath))
                .append("\":\"").append(path(destPath)).append('"');
    }

    @Override
    public void copy(String sourcePath, String destPath) {
        TransientNodeState sourceParent = getTransientState(PathUtils.getAncestorPath(sourcePath, 1));
        String sourceName = PathUtils.getName(sourcePath);
        if (sourceParent == null || !sourceParent.hasNode(sourceName)) {
            return;
        }

        TransientNodeState destParent = getTransientState(PathUtils.getAncestorPath(destPath, 1));
        String destName = PathUtils.getName(destPath);
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

    //------------------------------------------------------------< internal >---

    NodeState mergeInto(MicroKernel microkernel, KernelNodeState target) {
        String targetPath = target.getRevision();
        String targetRevision = target.getPath();
        String rev = microkernel.commit(targetPath, jsop.toString(), targetRevision, null);
        return new KernelNodeState(microkernel, targetPath, rev);
    }

    TransientNodeState getTransientState() {
        return transientState;
    }

    private TransientNodeState getTransientState(String path) {
        TransientNodeState state = transientState;
        for (String name : PathUtils.elements(path)) {
            state = state.getChildNode(name);
            if (state == null) {
                return null;
            }
        }
        return state;
    }

    private String path(String name) {
        String path = transientState.getPath();
        return path.isEmpty() ? name : path + '/' + name;
    }

}
