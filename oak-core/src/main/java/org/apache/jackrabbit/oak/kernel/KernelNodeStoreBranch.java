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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

import java.util.concurrent.locks.Lock;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.PostCommitHook;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStoreBranch;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code NodeStoreBranch} based on {@link MicroKernel} branching and merging.
 * This implementation keeps changes in memory up to a certain limit and writes
 * them back to the Microkernel branch when the limit is exceeded.
 */
class KernelNodeStoreBranch extends AbstractNodeStoreBranch {

    /** The underlying store to which this branch belongs */
    private final KernelNodeStore store;

    /** Lock for coordinating concurrent merge operations */
    private final Lock mergeLock;

    /**
     * State of the this branch. Either {@link Unmodified}, {@link InMemory}, {@link Persisted}
     * or {@link Merged}.
     * @see BranchState
     */
    private BranchState branchState;

    public KernelNodeStoreBranch(KernelNodeStore kernelNodeStore, Lock mergeLock,
            KernelNodeState base) {

        this.store = checkNotNull(kernelNodeStore);
        this.mergeLock = checkNotNull(mergeLock);
        branchState = new Unmodified(checkNotNull(base));
    }

    @Override
    public String toString() {
        return branchState.toString();
    }

    @Nonnull
    @Override
    public NodeState getBase() {
        return branchState.getBase();
    }

    @Nonnull
    @Override
    public NodeState getHead() {
        return branchState.getHead();
    }

    @Override
    public void setRoot(NodeState newRoot) {
        branchState.setRoot(checkNotNull(newRoot));
    }

    @Override
    public boolean move(String source, String target) {
        if (PathUtils.isAncestor(checkNotNull(source), checkNotNull(target))) {
            return false;
        } else if (source.equals(target)) {
            return true;
        }

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
        branchState.persist().commit(">\"" + source + "\":\"" + target + '"');
        return true;
    }

    @Override
    public boolean copy(String source, String target) {
        if (!getNode(checkNotNull(source)).exists()) {
            // source does not exist
            return false;
        }
        NodeState destParent = getNode(getParentPath(checkNotNull(target)));
        if (!destParent.exists()) {
            // parent of destination does not exist
            return false;
        }
        if (destParent.getChildNode(getName(target)).exists()) {
            // destination exists already
            return false;
        }
        branchState.persist().commit("*\"" + source + "\":\"" + target + '"');
        return true;
    }

    @Nonnull
    @Override
    public NodeState merge(@Nonnull CommitHook hook, PostCommitHook committed) throws CommitFailedException {
        return branchState.merge(checkNotNull(hook), checkNotNull(committed));
    }

    @Override
    public void rebase() {
        branchState.rebase();
    }

    private NodeState getNode(String path) {
        NodeState node = getHead();
        for (String name : elements(path)) {
            node = node.getChildNode(name);
        }
        return node;
    }

    /**
     * Sub classes of this class represent a state a branch can be in. See the individual
     * sub classes for permissible state transitions.
     */
    private abstract class BranchState {
        /** Root state of the base revision of this branch */
        protected KernelNodeState base;

        protected BranchState(KernelNodeState base) {
            this.base = base;
        }

        /**
         * Persist this branch to an underlying branch in the {@code MicroKernel}.
         */
        Persisted persist() {
            branchState = new Persisted(base, getHead());
            return (Persisted) branchState;
        }

        KernelNodeState getBase(){
            return base;
        }

        @Nonnull
        abstract NodeState getHead();

        abstract void setRoot(NodeState root);

        abstract void rebase();

        @Nonnull
        abstract NodeState merge(@Nonnull CommitHook hook, PostCommitHook committed) throws CommitFailedException;
    }

    /**
     * Instances of this class represent a branch whose base and head are the same.
     * <p>
     * Transitions to:
     * <ul>
     *     <li>{@link InMemory} on {@link #setRoot(NodeState)} if the new root differs
     *         from the current base</li>.
     *     <li>{@link Merged} on {@link #merge(CommitHook, PostCommitHook)}</li>
     * </ul>
     */
    private class Unmodified extends BranchState {
        Unmodified(KernelNodeState base) {
            super(base);
        }

        @Override
        public String toString() {
            return "Unmodified[" + base + ']';
        }

        @Override
        NodeState getHead() {
            return base;
        }

        @Override
        void setRoot(NodeState root) {
            if (!base.equals(root)) {
                branchState = new InMemory(base, root);
            }
        }

        @Override
        void rebase() {
            base = store.getRoot();
        }

        @Override
        NodeState merge(CommitHook hook, PostCommitHook committed) throws CommitFailedException {
            branchState = new Merged(base);
            return base;
        }
    }

    /**
     * Instances of this class represent a branch whose base and head differ.
     * All changes are kept in memory.
     * <p>
     * Transitions to:
     * <ul>
     *     <li>{@link Unmodified} on {@link #setRoot(NodeState)} if the new root is the same
     *         as the base of this branch or
     *     <li>{@link Persisted} otherwise.
     *     <li>{@link Merged} on {@link #merge(CommitHook, PostCommitHook)}</li>
     * </ul>
     */
    private class InMemory extends BranchState {
        /** Root state of the transient head. */
        private NodeState head;

        @Override
        public String toString() {
            return "InMemory[" + base + ", " + head + ']';
        }

        InMemory(KernelNodeState base, NodeState head) {
            super(base);
            this.head = head;
        }

        @Override
        NodeState getHead() {
            return head;
        }

        @Override
        void setRoot(NodeState root) {
            if (base.equals(root)) {
                branchState = new Unmodified(base);
            } else if (!head.equals(root)) {
                head = root;
                persist();
            }
        }

        @Override
        void rebase() {
            KernelNodeState root = store.getRoot();
            NodeBuilder builder = root.builder();
            head.compareAgainstBaseState(base, new ConflictAnnotatingRebaseDiff(builder));
            head = builder.getNodeState();
            base = root;
        }

        @Override
        NodeState merge(CommitHook hook, PostCommitHook committed) throws CommitFailedException {
            mergeLock.lock();
            try {
                rebase();
                NodeState toCommit = checkNotNull(hook).processCommit(base, head);
                JsopDiff diff = new JsopDiff(store);
                toCommit.compareAgainstBaseState(base, diff);
                NodeState newHead = store.commit(diff.toString(), base);
                committed.contentChanged(base, newHead);
                branchState = new Merged(base);
                return newHead;
            } catch (MicroKernelException e) {
                throw new CommitFailedException(
                        "Kernel", 1,
                        "Failed to merge changes to the underlying MicroKernel", e);
            } finally {
                mergeLock.unlock();
            }
        }
    }

    /**
     * Instances of this class represent a branch whose base and head differ.
     * All changes are persisted to an underlying branch in the {@code MicroKernel}.
     * <p>
     * Transitions to:
     * <ul>
     *     <li>{@link Unmodified} on {@link #setRoot(NodeState)} if the new root is the same
     *         as the base of this branch.
     *     <li>{@link Merged} on {@link #merge(CommitHook, PostCommitHook)}</li>
     * </ul>
     */
    private class Persisted extends BranchState {
        /** Root state of the transient head, top of persisted branch. */
        private KernelNodeState head;

        @Override
        public String toString() {
            return "Persisted[" + base + ", " + head + ']';
        }

        Persisted(KernelNodeState base, NodeState head) {
            super(base);
            this.head = store.branch(base);
            persistTransientHead(head);
        }

        void commit(String jsop) {
            if (!jsop.isEmpty()) {
                head = store.commit(jsop, head);
            }
        }

        @Override
        NodeState getHead() {
            return head;
        }

        @Override
        void setRoot(NodeState root) {
            if (base.equals(root)) {
                branchState = new Unmodified(base);
            } else if (!head.equals(root)) {
                persistTransientHead(root);
            }
        }

        @Override
        void rebase() {
            KernelNodeState root = store.getRoot();
            if (head.equals(root)) {
                // Nothing was written to this branch: set new base revision
                head = root;
                base = root;
            } else {
                // perform rebase in kernel
                head = store.rebase(head, root);
                base = root;
            }
        }

        @Override
        NodeState merge(CommitHook hook, PostCommitHook committed) throws CommitFailedException {
            mergeLock.lock();
            try {
                rebase();
                NodeState toCommit = checkNotNull(hook).processCommit(base, head);
                if (toCommit.equals(base)) {
                    committed.contentChanged(base, base);
                    branchState = new Merged(base);
                    return base;
                } else {
                    JsopDiff diff = new JsopDiff(store);
                    toCommit.compareAgainstBaseState(head, diff);
                    commit(diff.toString());
                    NodeState newRoot = store.merge(head);
                    committed.contentChanged(base, newRoot);
                    branchState = new Merged(base);
                    return newRoot;
                }
            } catch (MicroKernelException e) {
                throw new CommitFailedException(
                        "Kernel", 1,
                        "Failed to merge changes to the underlying MicroKernel", e);
            } finally {
                mergeLock.unlock();
            }
        }

        private void persistTransientHead(NodeState newHead) {
            if (!newHead.equals(head)) {
                JsopDiff diff = new JsopDiff(store);
                newHead.compareAgainstBaseState(head, diff);
                head = store.commit(diff.toString(), head);
            }
        }
    }

    /**
     * Instances of this class represent a branch that has already been merged.
     * All methods throw an {@code IllegalStateException}.
     * <p>
     * Transitions to: none.
     */
    private class Merged extends BranchState {
        protected Merged(KernelNodeState base) {
            super(base);
        }

        @Override
        public String toString() {
            return "Merged[" + base + ']';
        }

        @Override
        NodeState getHead() {
            throw new IllegalStateException("Branch has already been merged");
        }

        @Override
        void setRoot(NodeState root) {
            throw new IllegalStateException("Branch has already been merged");
        }

        @Override
        void rebase() {
            throw new IllegalStateException("Branch has already been merged");
        }

        @Override
        NodeState merge(CommitHook hook, PostCommitHook committed) throws CommitFailedException {
            throw new IllegalStateException("Branch has already been merged");
        }
    }
}
