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
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.state.NodeState;
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

    /** Revision from which to branch */
    private final String headRevision;

    /** Revision of this branch in the Microkernel, null if not yet branched */
    private String branchRevision;

    /** Current root state of this branch */
    private NodeState currentRoot;

    /** Last state which was committed to this branch */
    private NodeState committed;

    KernelNodeStoreBranch(KernelNodeStore store) {
        this.store = store;
        MicroKernel kernel = store.getKernel();
        this.headRevision = kernel.getHeadRevision();
        this.currentRoot = new KernelNodeState(kernel, "/", headRevision);
        this.base = currentRoot;
        this.committed = currentRoot;
    }

    @Override
    public NodeState getBase() {
        return base;
    }

    @Override
    public NodeState getRoot() {
        return currentRoot;
    }

    @Override
    public void setRoot(NodeState newRoot) {
        if (!currentRoot.equals(newRoot)) {
            currentRoot = newRoot;
            JsopDiff diff = new JsopDiff();
            currentRoot.compareAgainstBaseState(committed, diff);
            commit(diff.toString());
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
    public NodeState merge(CommitHook hook) throws CommitFailedException {
        NodeState oldRoot = base;
        CommitHook commitHook = hook == null
                ? store.getHook()
                : new CompositeHook(store.getHook(), hook);
        NodeState toCommit = commitHook.processCommit(store, oldRoot, currentRoot);
        setRoot(toCommit);

        try {
            if (branchRevision == null) {
                // Nothing was written to this branch: return initial node state.
                branchRevision = null;
                currentRoot = null;
                return committed;
            }
            else {
                MicroKernel kernel = store.getKernel();
                String mergedRevision = kernel.merge(branchRevision, null);
                branchRevision = null;
                currentRoot = null;
                return new KernelNodeState(kernel, "/", mergedRevision);
            }
        }
        catch (MicroKernelException e) {
            throw new CommitFailedException(e);
        }
    }

    //------------------------------------------------------------< private >---

    private NodeState getNode(String path) {
        assert path.startsWith("/");
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
        if (branchRevision == null) {
            // create the branch if this is the first commit
            branchRevision = kernel.branch(headRevision);
        }

        branchRevision = kernel.commit("", jsop, branchRevision, null);
        currentRoot = new KernelNodeState(kernel, "/", branchRevision);
        committed = currentRoot;
    }
}
