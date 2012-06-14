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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

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
        this.currentRoot = new KernelNodeState(kernel, getValueFactory(), "/", branchRevision);
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
        currentRoot = newRoot;
        commit(buildJsop());
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
        CommitHook commitHook = store.getCommitHook();

        NodeState oldRoot = store.getRoot();
        NodeState toCommit = commitHook.beforeCommit(store, oldRoot, currentRoot);
        while (!currentRoot.equals(toCommit)) {
            setRoot(toCommit);
            oldRoot = store.getRoot();
            toCommit = commitHook.beforeCommit(store, oldRoot, currentRoot);
        }

        try {
            String mergedRevision = kernel.merge(branchRevision, null);
            branchRevision = null;
            currentRoot = null;
            committed = null;
            KernelNodeState committed = new KernelNodeState(kernel, getValueFactory(), "/", mergedRevision);
            commitHook.afterCommit(store, oldRoot, committed);
            return committed;
        }
        catch (MicroKernelException e) {
            throw new CommitFailedException(e);
        }
    }

    //------------------------------------------------------------< private >---

    private CoreValueFactory getValueFactory() {
        return store.getValueFactory();
    }

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
        currentRoot = new KernelNodeState(kernel, getValueFactory(), "/", branchRevision);
        committed = currentRoot;
    }

    private String buildJsop() {
        StringBuilder jsop = new StringBuilder();
        diffToJsop(committed, currentRoot, "", jsop);
        return jsop.toString();
    }

    private void diffToJsop(NodeState before, NodeState after, final String path,
            final StringBuilder jsop) {

        store.compare(before, after, new NodeStateDiff() {
            @Override
            public void propertyAdded(PropertyState after) {
                jsop.append('^').append(buildPath(after.getName()))
                        .append(':').append(toJson(after));
            }

            @Override
            public void propertyChanged(PropertyState before, PropertyState after) {
                jsop.append('^').append(buildPath(after.getName()))
                        .append(':').append(toJson(after));
            }

            @Override
            public void propertyDeleted(PropertyState before) {
                jsop.append('^').append(buildPath(before.getName())).append(":null");
            }

            @Override
            public void childNodeAdded(String name, NodeState after) {
                jsop.append('+').append(buildPath(name)).append(':');
                toJson(after);
            }

            @Override
            public void childNodeDeleted(String name, NodeState before) {
                jsop.append('-').append(buildPath(name));
            }

            @Override
            public void childNodeChanged(String name, NodeState before, NodeState after) {
                diffToJsop(before, after, PathUtils.concat(path, name), jsop);
            }

            private String buildPath(String name) {
                return '"' + PathUtils.concat(path, name) + '"';
            }

            private String toJson(PropertyState propertyState) {
                return propertyState.isArray()
                    ? CoreValueMapper.toJsonArray(propertyState.getValues())
                    : CoreValueMapper.toJsonValue(propertyState.getValue());
            }

            private void toJson(NodeState nodeState) {
                jsop.append('{');
                String comma = "";
                for (PropertyState property : nodeState.getProperties()) {
                    String value = toJson(property);
                    jsop.append(comma);
                    comma = ",";
                    jsop.append('"').append(property.getName()).append("\":").append(value);
                }

                for (ChildNodeEntry child : nodeState.getChildNodeEntries()) {
                    jsop.append(comma);
                    comma = ",";
                    jsop.append('"').append(child.getName()).append("\":");
                    toJson(child.getNodeState());
                }
                jsop.append('}');
            }
        });
    }
}
