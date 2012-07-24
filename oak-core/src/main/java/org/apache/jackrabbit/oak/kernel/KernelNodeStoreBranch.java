/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.kernel;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitEditor;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.kernel.CoreValueMapper.TYPE2HINT;

/**
 * {@code NodeStoreBranch} based on {@link MicroKernel} branching and merging.
 * This implementation keeps changes in memory up to a certain limit and writes
 * them back when the to the Microkernel branch when the limit is exceeded.
 */
class KernelNodeStoreBranch implements NodeStoreBranch {

    /** The underlying store to which this branch belongs */
    private final KernelNodeStore store;

    /** Base state of this branch */
    private final NodeState base;

    /** Revision of this branch in the Microkernel */
    private String branchRevision;

    /** Current root state of this branch */
    private NodeState currentRoot;

    /** Last state which was committed to this branch */
    private NodeState committed;

    KernelNodeStoreBranch(KernelNodeStore store) {
        this.store = store;

        MicroKernel kernel = store.getKernel();
        this.branchRevision = kernel.branch(null);
        this.currentRoot = new KernelNodeState(kernel, "/", branchRevision);
        this.base = currentRoot;
        this.committed = currentRoot;
    }

    @Override
    public NodeState getRoot() {
        return currentRoot;
    }

    @Override
    public NodeState getBase() {
        return base;
    }

    @Override
    public void setRoot(NodeState newRoot) {
        if (!currentRoot.equals(newRoot)) {
            currentRoot = newRoot;
            commit(buildJsop());
        }
    }

    @Override
    public boolean move(String source, String target) {
        if (getNode(source) == null) {
            // source does not exist
            return false;
        }
        NodeState destParent = getNode(getParentPath(target));
        if (destParent == null) {
            // parent of destination does not exist
            return false;
        }
        if (destParent.getChildNode(getName(target)) != null) {
            // destination exists already
            return false;
        }

        commit(">\"" + source + "\":\"" + target + '"');
        return true;
    }

    @Override
    public boolean copy(String source, String target) {
        if (getNode(source) == null) {
            // source does not exist
            return false;
        }
        NodeState destParent = getNode(getParentPath(target));
        if (destParent == null) {
            // parent of destination does not exist
            return false;
        }
        if (destParent.getChildNode(getName(target)) != null) {
            // destination exists already
            return false;
        }

        commit("*\"" + source + "\":\"" + target + '"');
        return true;
    }

    @Override
    public KernelNodeState merge() throws CommitFailedException {
        MicroKernel kernel = store.getKernel();
        CommitEditor editor = store.getEditor();

        NodeState oldRoot = store.getRoot();
        NodeState toCommit = editor.editCommit(store, oldRoot, currentRoot);
        setRoot(toCommit);

        try {
            String mergedRevision = kernel.merge(branchRevision, null);
            branchRevision = null;
            currentRoot = null;
            committed = null;
            KernelNodeState committed = new KernelNodeState(kernel, "/", mergedRevision);
            return committed;
        }
        catch (MicroKernelException e) {
            throw new CommitFailedException(e);
        }
    }

    //------------------------------------------------------------< private >---

    private NodeState getNode(String path) {
        NodeState node = getRoot();
        for (String name : elements(path)) {
            node = node.getChildNode(name);
            if (node == null) {
                break;
            }
        }

        return node;
    }

    private void commit(String jsop) {
        MicroKernel kernel = store.getKernel();
        branchRevision = kernel.commit("/", jsop, branchRevision, null);
        currentRoot = new KernelNodeState(kernel, "/", branchRevision);
        committed = currentRoot;
    }

    private String buildJsop() {
        JsopBuilder jsop = new JsopBuilder();
        diffToJsop(committed, currentRoot, "", jsop);
        return jsop.toString();
    }

    private static void diffToJsop(NodeState before, NodeState after, final String path,
            final JsopBuilder jsop) {
        after.compareAgainstBaseState(before, new NodeStateDiff() {
            @Override
            public void propertyAdded(PropertyState after) {
                jsop.tag('^').key(buildPath(after.getName()));
                toJson(after, jsop);
            }

            @Override
            public void propertyChanged(PropertyState before, PropertyState after) {
                jsop.tag('^').key(buildPath(after.getName()));
                toJson(after, jsop);
            }

            @Override
            public void propertyDeleted(PropertyState before) {
                jsop.tag('^').key(buildPath(before.getName())).value(null);
            }

            @Override
            public void childNodeAdded(String name, NodeState after) {
                jsop.tag('+').key(buildPath(name));
                toJson(after, jsop);
            }

            @Override
            public void childNodeDeleted(String name, NodeState before) {
                jsop.tag('-').value(buildPath(name));
            }

            @Override
            public void childNodeChanged(String name, NodeState before, NodeState after) {
                diffToJsop(before, after, PathUtils.concat(path, name), jsop);
            }

            private String buildPath(String name) {
                return PathUtils.concat(path, name);
            }

            private void toJson(NodeState nodeState, JsopBuilder jsop) {
                jsop.object();
                for (PropertyState property : nodeState.getProperties()) {
                    jsop.key(property.getName());
                    toJson(property, jsop);
                }
                for (ChildNodeEntry child : nodeState.getChildNodeEntries()) {
                    jsop.key(child.getName());
                    toJson(child.getNodeState(), jsop);
                }
                jsop.endObject();
            }

            private void toJson(PropertyState propertyState, JsopBuilder jsop) {
                if (propertyState.isArray()) {
                    jsop.array();
                    for (CoreValue value : propertyState.getValues()) {
                        toJson(value, jsop);
                    }
                    jsop.endArray();
                } else {
                    toJson(propertyState.getValue(), jsop);
                }
            }

            private void toJson(CoreValue value, JsopBuilder jsop) {
                int type = value.getType();
                if (type == PropertyType.BOOLEAN) {
                    jsop.value(value.getBoolean());
                } else if (type == PropertyType.LONG) {
                    jsop.value(value.getLong());
                } else {
                    String string = value.getString();
                    if (type != PropertyType.STRING
                            || CoreValueMapper.startsWithHint(string)) {
                        string = TYPE2HINT.get(type) + ':' + string;
                    }
                    jsop.value(string);
                }
            }

        });
    }
}
