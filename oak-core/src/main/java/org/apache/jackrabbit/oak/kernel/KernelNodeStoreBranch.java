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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStoreBranch;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code NodeStoreBranch} based on {@link MicroKernel} branching and merging.
 * This implementation keeps changes in memory up to a certain limit and writes
 * them back when the to the Microkernel branch when the limit is exceeded.
 */
class KernelNodeStoreBranch extends AbstractNodeStoreBranch {

    /** The underlying store to which this branch belongs */
    private final KernelNodeStore store;

    /** Root state of the base revision of this branch */
    private KernelNodeState base;

    /** Root state of the transient head revision on top of persisted branch, null if merged. */
    private NodeState head;

    /** Head revision of persisted branch, null if not yet branched*/
    private String headRevision;

    /** Number of updates to this branch via {@link #setRoot(NodeState)} */
    private int updates = 0;

    KernelNodeStoreBranch(KernelNodeStore store, KernelNodeState root) {
        this.store = store;
        this.base = root;
        this.head = root;
    }

    @Override
    public NodeState getBase() {
        return base;
    }

    @Override
    public NodeState getHead() {
        checkNotMerged();
        return head;
    }

    @Override
    public void setRoot(NodeState newRoot) {
        checkNotMerged();
        if (!head.equals(newRoot)) {
            NodeState oldHead = head;
            head = newRoot;
            if (++updates > 1) {
                // persist unless this is the first update
                boolean success = false;
                try {
                    persistTransientHead();
                    success = true;
                } finally {
                    if (!success) {
                        head = oldHead;
                    }
                }
            }
        }
    }

    @Override
    public boolean move(String source, String target) {
        checkNotMerged();
        if (!getNode(source).exists()) {
            // source does not exist
            return false;
        }
        NodeState destParent = getNode(getParentPath(target));
        if (!destParent.exists()) {
            // parent of destination does not exist
            return false;
        }
        if (destParent.getChildNode(getName(target)).exists()) {
            // destination exists already
            return false;
        }

        commit(">\"" + source + "\":\"" + target + '"');
        return true;
    }

    @Override
    public boolean copy(String source, String target) {
        checkNotMerged();
        if (!getNode(source).exists()) {
            // source does not exist
            return false;
        }
        NodeState destParent = getNode(getParentPath(target));
        if (!destParent.exists()) {
            // parent of destination does not exist
            return false;
        }
        if (destParent.getChildNode(getName(target)).exists()) {
            // destination exists already
            return false;
        }

        commit("*\"" + source + "\":\"" + target + '"');
        return true;
    }

    @Override
    public NodeState merge(CommitHook hook) throws CommitFailedException {
        checkNotMerged();
        NodeState toCommit = checkNotNull(hook).processCommit(base, head);
        NodeState oldRoot = head;
        head = toCommit;

        try {
            if (head.equals(base)) {
                // Nothing was written to this branch: return base state
                head = null;  // Mark as merged
                return base;
            } else {
                NodeState newRoot;
                JsopDiff diff = new JsopDiff(store);
                if (headRevision == null) {
                    // no branch created yet, commit directly
                    head.compareAgainstBaseState(base, diff);
                    newRoot = store.commit(diff.toString(), base.getRevision());
                } else {
                    // commit into branch and merge
                    head.compareAgainstBaseState(store.getRootState(headRevision), diff);
                    String jsop = diff.toString();
                    if (!jsop.isEmpty()) {
                        headRevision = store.getKernel().commit(
                                "", jsop, headRevision, null);
                    }
                    newRoot = store.merge(headRevision);
                    headRevision = null;
                }
                head = null;  // Mark as merged
                return newRoot;
            }
        } catch (MicroKernelException e) {
            head = oldRoot;
            throw new CommitFailedException(
                    "Kernel", 1,
                    "Failed to merge changes to the underlying MicroKernel", e);
        }
    }

    @Override
    public void rebase() {
        KernelNodeState root = store.getRoot();
        if (head.equals(root)) {
            // Nothing was written to this branch: set new base revision
            head = root;
            base = root;
        } else if (headRevision == null) {
            // Nothing written to persistent branch yet
            // perform rebase in memory
            NodeBuilder builder = root.builder();
            getHead().compareAgainstBaseState(getBase(), new ConflictAnnotatingRebaseDiff(builder));
            head = builder.getNodeState();
            base = root;
        } else {
            // perform rebase in kernel
            persistTransientHead();
            headRevision = store.getKernel().rebase(headRevision, root.getRevision());
            head = store.getRootState(headRevision);
            base = root;
        }
    }

    //------------------------------------------------------------< private >---

    private void checkNotMerged() {
        checkState(head != null, "Branch has already been merged");
    }

    private NodeState getNode(String path) {
        checkArgument(path.startsWith("/"));
        NodeState node = getHead();
        for (String name : elements(path)) {
            node = node.getChildNode(name);
        }
        return node;
    }

    private void commit(String jsop) {
        MicroKernel kernel = store.getKernel();
        if (headRevision == null) {
            // create the branch if this is the first commit
            headRevision = kernel.branch(base.getRevision());
        }

        // persist transient changes first
        persistTransientHead();

        headRevision = kernel.commit("", jsop, headRevision, null);
        head = store.getRootState(headRevision);
    }

    private void persistTransientHead() {
        KernelNodeState oldBase = base;
        NodeState oldHead = head;
        String oldHeadRevision = headRevision;
        boolean success = false;
        try {
            MicroKernel kernel = store.getKernel();
            JsopDiff diff = new JsopDiff(store);
            if (headRevision == null) {
                // no persistent branch yet
                if (head.equals(base)) {
                    // nothing to persist
                    success = true;
                    return;
                } else {
                    // create branch
                    headRevision = kernel.branch(base.getRevision());
                    head.compareAgainstBaseState(base, diff);
                }
            } else {
                // compare against head of branch
                NodeState branchHead = store.getRootState(headRevision);
                if (head.equals(branchHead)) {
                    // nothing to persist
                    success = true;
                    return;
                } else {
                    head.compareAgainstBaseState(branchHead, diff);
                }
            }
            // if we get here we have something to persist
            // and a branch exists
            headRevision = kernel.commit("", diff.toString(), headRevision, null);
            head = store.getRootState(headRevision);
            success = true;
        } finally {
            // revert to old state if unsuccessful
            if (!success) {
                base = oldBase;
                head = oldHead;
                headRevision = oldHeadRevision;
            }
        }
    }
}
