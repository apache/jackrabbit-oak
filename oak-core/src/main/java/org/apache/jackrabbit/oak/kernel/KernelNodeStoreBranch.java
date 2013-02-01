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
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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

    /** Root state of the base revision of this branch */
    private NodeState base;

    /** Revision of the base state of this branch*/
    private String baseRevision;

    /** Root state of the head revision of this branch*/
    private NodeState head;

    /** Head revision of this branch, null if not yet branched*/
    private String headRevision;

    KernelNodeStoreBranch(KernelNodeStore store, KernelNodeState root) {
        this.store = store;
        this.base = root;
        this.baseRevision = root.getRevision();
        this.head = root;
    }

    @Override
    public NodeState getBase() {
        return base;
    }

    @Override
    public NodeState getRoot() {
        checkNotMerged();
        return head;
    }

    @Override
    public void setRoot(NodeState newRoot) {
        checkNotMerged();
        if (!head.equals(newRoot)) {
            JsopDiff diff = new JsopDiff(store.getKernel());
            newRoot.compareAgainstBaseState(head, diff);
            commit(diff.toString());
        }
    }

    @Override
    public boolean move(String source, String target) {
        checkNotMerged();
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
        checkNotMerged();
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
    public NodeState merge() throws CommitFailedException {
        checkNotMerged();
        CommitHook commitHook = store.getHook();
        NodeState toCommit = commitHook.processCommit(base, head);
        NodeState oldRoot = head;
        setRoot(toCommit);

        try {
            if (headRevision == null) {
                // Nothing was written to this branch: return base state
                head = null;  // Mark as merged
                return base;
            }
            else {
                MicroKernel kernel = store.getKernel();
                String mergedRevision = kernel.merge(headRevision, null);
                headRevision = null;
                head = null;  // Mark as merged
                return store.getRootState(mergedRevision);
            }
        }
        catch (MicroKernelException e) {
            setRoot(oldRoot);
            throw new CommitFailedException(e);
        }
    }

    @Override
    public void rebase() {
        KernelNodeState root = store.getRoot();
        if (headRevision == null) {
            // Nothing was written to this branch: set new base revision
            head = root;
            base = root;
            baseRevision = root.getRevision();
        }
        else {
            headRevision = store.getKernel().rebase(headRevision, root.getRevision());
            head = store.getRootState(headRevision);
            base = root;
            baseRevision = root.getRevision();
        }
    }

    //------------------------------------------------------------< private >---

    private void checkNotMerged() {
        checkState(head != null, "Branch has already been merged");
    }

    private NodeState getNode(String path) {
        checkArgument(path.startsWith("/"));
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
        if (headRevision == null) {
            // create the branch if this is the first commit
            headRevision = kernel.branch(baseRevision);
        }

        headRevision = kernel.commit("", jsop, headRevision, null);
        head = store.getRootState(headRevision);
    }
}
