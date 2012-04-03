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

    private KernelNodeStateEditor(KernelNodeStateEditor parentEditor,
            NodeState state, String name) {

        base = parentEditor.base;
        transientState = new TransientNodeState(state, this, parentEditor.getNodeState(), name);
        jsop = parentEditor.jsop;
    }

    KernelNodeStateEditor(KernelNodeStateEditor parentEditor, TransientNodeState state) {
        base = parentEditor.base;
        transientState = state;
        jsop = parentEditor.jsop;
    }

    @Override
    public void addNode(String name) {
        if (!hasNode(transientState, name)) {
            transientState.addNode(name);
            jsop.append("+\"").append(path(name)).append("\":{}");
        }
    }

    @Override
    public void removeNode(String name) {
        if (hasNode(transientState, name)) {
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
        if (sourceParent == null || !hasNode(sourceParent, sourceName)) {
            return;
        }
        
        TransientNodeState destParent = getTransientState(PathUtils.getAncestorPath(destPath, 1));
        String destName = PathUtils.getName(destPath);
        if (destParent == null || hasNode(destParent, destName)) {
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
        if (sourceParent == null || !hasNode(sourceParent, sourceName)) {
            return;
        }

        TransientNodeState destParent = getTransientState(PathUtils.getAncestorPath(destPath, 1));
        String destName = PathUtils.getName(destPath);
        if (destParent == null || hasNode(destParent, destName)) {
            return;
        }

        sourceParent.copy(sourceName, destParent, destName);
        jsop.append("*\"").append(path(sourcePath)).append("\":\"")
                .append(path(destPath)).append('"');
    }

    @Override
    public KernelNodeStateEditor edit(String name) {
        NodeState childState = transientState.getChildNode(name);
        if (childState == null) {
            return null;
        }
        else if (childState instanceof TransientNodeState) {
            return ((TransientNodeState) childState).getEditor();
        }
        else {
            return new KernelNodeStateEditor(this, childState, name);
        }
    }

    @Override
    public TransientNodeState getNodeState() {
        return transientState;
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

    private TransientNodeState getTransientState(String path) {
        KernelNodeStateEditor editor = this;
        for(String name : PathUtils.elements(path)) {
            editor = editor.edit(name);
            if (editor == null) {
                return null;
            }
        }
        
        return editor.transientState;
    }

    private String path(String name) {
        String path = transientState.getPath();
        return path.isEmpty() ? name : path + '/' + name;
    }

    private static boolean hasNode(NodeState nodeState, String name) {
        return nodeState.getChildNode(name) != null;
    }

}
