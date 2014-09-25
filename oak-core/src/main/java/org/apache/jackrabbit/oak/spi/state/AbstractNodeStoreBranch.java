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
package org.apache.jackrabbit.oak.spi.state;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.api.CommitFailedException.MERGE;
import static org.apache.jackrabbit.oak.api.CommitFailedException.OAK;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;

import com.google.common.collect.Maps;

/**
 * A base implementation of a node store branch, which supports partially
 * persisted branches.
 * <p>
 * This implementation keeps changes in memory up to a certain limit and writes
 * them back to the underlying branch when the limit is exceeded.
 */
public abstract class AbstractNodeStoreBranch<S extends NodeStore, N extends NodeState>
        implements NodeStoreBranch {

    private static final Random RANDOM = new Random();

    private static final long MIN_BACKOFF = 50;

    protected static final ConcurrentMap<Thread, AbstractNodeStoreBranch> BRANCHES = Maps.newConcurrentMap();

    /** The underlying store to which this branch belongs */
    protected final S store;

    /** The dispatcher to report changes */
    protected final ChangeDispatcher dispatcher;

    protected final long maximumBackoff;

    /**
     * The maximum time in milliseconds to wait for the merge lock.
     */
    protected final long maxLockTryTimeMS;

    /** Lock for coordinating concurrent merge operations */
    private final Lock mergeLock;

    /**
     * State of the this branch. Either {@link Unmodified}, {@link InMemory}, {@link Persisted}
     * or {@link Merged}.
     * @see BranchState
     */
    private BranchState branchState;

    public AbstractNodeStoreBranch(S kernelNodeStore,
                                   ChangeDispatcher dispatcher,
                                   Lock mergeLock,
                                   N base) {
        this(kernelNodeStore, dispatcher, mergeLock, base, null,
                MILLISECONDS.convert(10, SECONDS),
                Integer.MAX_VALUE); // default: wait 'forever'
    }

    public AbstractNodeStoreBranch(S kernelNodeStore,
                                   ChangeDispatcher dispatcher,
                                   Lock mergeLock,
                                   N base,
                                   N head,
                                   long maximumBackoff,
                                   long maxLockTryTimeMS) {
        this.store = checkNotNull(kernelNodeStore);
        this.dispatcher = dispatcher;
        this.mergeLock = checkNotNull(mergeLock);
        if (head == null) {
            this.branchState = new Unmodified(checkNotNull(base));
        } else {
            this.branchState = new Persisted(checkNotNull(base), head);
        }
        this.maximumBackoff = Math.max(maximumBackoff, MIN_BACKOFF);
        this.maxLockTryTimeMS = maxLockTryTimeMS;
    }

    /**
     * @return the current root of the underlying store.
     */
    protected abstract N getRoot();

    /**
     * Create a new branch state from the given state.
     *
     * @param state the state from where to create a branch from.
     * @return the branch state.
     */
    protected abstract N createBranch(N state);

    /**
     * Rebases the branch head to the given base.
     *
     * @param branchHead the head state of a branch.
     * @param base the new base state for the branch.
     * @return the rebased branch head.
     */
    protected abstract N rebase(N branchHead, N base);

    /**
     * Merges the branch head and returns the result state of the merge.
     *
     * @param branchHead the head of the branch to merge.
     * @param info the commit info or <code>null</code> if none available.
     * @return the result state of the merge.
     * @throws CommitFailedException if the merge fails. The type of the
     *                    exception will be {@code CommitFailedException.MERGE}.
     */
    protected abstract N merge(N branchHead, CommitInfo info)
            throws CommitFailedException;

    /**
     * Resets the branch head to the given ancestor on the same branch.
     *
     * @param branchHead the head of the branch to reset.
     * @param ancestor the state of the branch to reset to.
     * @return the state of the reset branch. This is not necessarily the same
     *         instance as {@code ancestor} but is guaranteed to be equal to it.
     */
    @Nonnull
    protected abstract N reset(@Nonnull N branchHead, @Nonnull N ancestor);

    /**
     * Persists the changes between <code>toPersist</code> and <code>base</code>
     * to the underlying store.
     * <p>
     * While this method does not declare any exceptions to be thrown, an
     * implementation may still throw a runtime exception specific to the
     * concrete implementation of this node store branch.
     *
     * @param toPersist the state with the changes on top of <code>base</code>.
     * @param base the base state.
     * @param info the commit info or <code>null</code> if there is none.
     * @return the state with the persisted changes.
     */
    protected abstract N persist(NodeState toPersist, N base, CommitInfo info);

    /**
     * Perform a potentially optimized copy operation directly on the underlying
     * store.
     * <p>
     * This base class ensures that preconditions are met (e.g. the source
     * exists), which means an implementation of this method just needs to
     * perform the copy operation.
     * <p>
     * While this method does not declare any exceptions to be thrown, an
     * implementation may still throw a runtime exception specific to the
     * concrete implementation of this node store branch.
     *
     * @param source the source of the copy operation.
     * @param target the destination of the copy operation.
     * @param base the base state.
     * @return the result of the copy operation.
     */
    protected abstract N copy(String source, String target, N base);

    /**
     * Perform a potentially optimized move operation directly on the underlying
     * store.
     * <p>
     * This base class ensures that preconditions are met (e.g. the source
     * exists), which means an implementation of this method just needs to
     * perform the move operation.
     * <p>
     * While this method does not declare any exceptions to be thrown, an
     * implementation may still throw a runtime exception specific to the
     * concrete implementation of this node store branch.
     *
     * @param source the source of the move operation.
     * @param target the destination of the move operation.
     * @param base the base state.
     * @return the result of the move operation.
     */
    protected abstract N move(String source, String target, N base);

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

    /**
     * Moves a node in this private branch.
     *
     * @param source source path
     * @param target target path
     * @return  {@code true} iff the move succeeded
     * @throws IllegalStateException if the branch is already merged
     */
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
        branchState.persist().move(source, target);
        return true;
    }

    /**
     * Copies a node in this private branch.
     *
     * @param source source path
     * @param target target path
     * @return  {@code true} iff the copy succeeded
     * @throws IllegalStateException if the branch is already merged
     */
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
        branchState.persist().copy(source, target);
        return true;
    }

    @Nonnull
    @Override
    public NodeState merge(@Nonnull CommitHook hook, @Nonnull CommitInfo info)
            throws CommitFailedException {
        CommitFailedException ex = null;
        long time = System.currentTimeMillis();
        int numRetries = 0;
        for (long backoff = MIN_BACKOFF; backoff <= maximumBackoff; backoff *= 2) {
            if (ex != null) {
                try {
                    numRetries++;
                    Thread.sleep(backoff + RANDOM.nextInt((int) Math.min(backoff, Integer.MAX_VALUE)));
                } catch (InterruptedException e) {
                    throw new CommitFailedException(
                            MERGE, 3, "Merge interrupted", e);
                }
            }
            try {
                boolean acquired = mergeLock.tryLock(maxLockTryTimeMS, MILLISECONDS);
                try {
                    return branchState.merge(checkNotNull(hook), checkNotNull(info));
                } catch (CommitFailedException e) {
                    ex = e;
                    // only retry on merge failures. these may be caused by
                    // changes introduce by a commit hook and may be resolved
                    // by a rebase and running the hook again
                    if (!e.isOfType(MERGE)) {
                        throw e;
                    }
                } finally {
                    if (acquired) {
                        mergeLock.unlock();
                    }
                }
            } catch (InterruptedException e) {
                throw new CommitFailedException(OAK, 1,
                        "Unable to acquire merge lock", e);
            }
        }
        // if we get here retrying failed
        time = System.currentTimeMillis() - time;
        String msg = ex.getMessage() + " (retries " + numRetries + ", " + time + " ms)";
        throw new CommitFailedException(ex.getSource(), ex.getType(),
                ex.getCode(), msg, ex.getCause());
    }

    @Override
    public void rebase() {
        branchState.rebase();
    }

    //----------------------------< internal >----------------------------------

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
        protected N base;

        protected BranchState(N base) {
            this.base = base;
        }

        /**
         * Persist this branch to an underlying branch in the {@code MicroKernel}.
         */
        Persisted persist() {
            Persisted p = new Persisted(base);
            p.persistTransientHead(getHead());
            branchState = p;
            return p;
        }

        N getBase(){
            return base;
        }

        @Nonnull
        abstract NodeState getHead();

        abstract void setRoot(NodeState root);

        abstract void rebase();

        /**
         * Runs the commit hook on the changes tracked with this branch state
         * merges the result.
         * <p>
         * In addition to the {@link CommitFailedException}, an implementation
         * may also throw an unchecked exception when an error occurs while
         * persisting the changes. This exception is implementation specific
         * and it is the responsibility of the caller to convert it into a
         * {@link CommitFailedException}.
         *
         * @param hook the commit hook to run.
         * @param info the associated commit info.
         * @return the result of the merge.
         * @throws CommitFailedException if a commit hook rejected the changes
         *          or the actual merge operation failed. An implementation must
         *          use the appropriate type in {@code CommitFailedException} to
         *          indicate the cause of the exception.
         */
        @Nonnull
        abstract NodeState merge(
                @Nonnull CommitHook hook, @Nonnull CommitInfo info)
                throws CommitFailedException;
    }

    /**
     * Instances of this class represent a branch whose base and head are the same.
     * <p>
     * Transitions to:
     * <ul>
     *     <li>{@link InMemory} on {@link #setRoot(NodeState)} if the new root differs
     *         from the current base</li>.
     *     <li>{@link Merged} on {@link #merge(CommitHook, CommitInfo)}</li>
     * </ul>
     */
    private class Unmodified extends BranchState {
        Unmodified(N base) {
            super(base);
        }

        @Override
        public String toString() {
            return "Unmodified[" + base + ']';
        }

        @Override
        @Nonnull
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
            base = getRoot();
        }

        @Override
        @Nonnull
        NodeState merge(@Nonnull CommitHook hook, @Nonnull CommitInfo info) {
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
     *     <li>{@link Merged} on {@link #merge(CommitHook, CommitInfo)}</li>
     * </ul>
     */
    private class InMemory extends BranchState {
        /** Root state of the transient head. */
        private NodeState head;

        @Override
        public String toString() {
            return "InMemory[" + base + ", " + head + ']';
        }

        InMemory(N base, NodeState head) {
            super(base);
            this.head = head;
        }

        @Override
        @Nonnull
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
            N root = getRoot();
            NodeBuilder builder = root.builder();
            head.compareAgainstBaseState(base, new ConflictAnnotatingRebaseDiff(builder));
            head = builder.getNodeState();
            base = root;
        }

        @Override
        @Nonnull
        NodeState merge(@Nonnull CommitHook hook, @Nonnull CommitInfo info)
                throws CommitFailedException {
            checkNotNull(hook);
            checkNotNull(info);
            try {
                rebase();
                dispatcher.contentChanged(base, null);
                NodeState toCommit = hook.processCommit(base, head, info);
                try {
                    NodeState newHead = AbstractNodeStoreBranch.this.persist(toCommit, base, info);
                    dispatcher.contentChanged(newHead, info);
                    branchState = new Merged(base);
                    return newHead;
                } catch (MicroKernelException e) {
                    throw new CommitFailedException(MERGE, 1,
                            "Failed to merge changes to the underlying store", e);
                } catch (Exception e) {
                    throw new CommitFailedException(OAK, 1,
                            "Failed to merge changes to the underlying store", e);
                }
            } finally {
                dispatcher.contentChanged(getRoot(), null);
            }
        }
    }

    /**
     * Instances of this class represent a branch whose head is persisted to an
     * underlying branch in the {@code MicroKernel}.
     * <p>
     * Transitions to:
     * <ul>
     *     <li>{@link ResetFailed} on failed reset in {@link #merge(CommitHook, CommitInfo)}</li>
     *     <li>{@link Merged} on successful {@link #merge(CommitHook, CommitInfo)}</li>
     * </ul>
     */
    private class Persisted extends BranchState {
        /** Root state of the transient head, top of persisted branch. */
        private N head;

        @Override
        public String toString() {
            return "Persisted[" + base + ", " + head + ']';
        }

        Persisted(N base) {
            super(base);
            this.head = createBranch(base);
        }

        Persisted(N base, N head) {
            super(base);
            createBranch(base);
            this.head = head;
        }

        void move(String source, String target) {
            head = AbstractNodeStoreBranch.this.move(source, target, head);
        }

        void copy(String source, String target) {
            head = AbstractNodeStoreBranch.this.copy(source, target, head);
        }

        @Override
        @Nonnull
        NodeState getHead() {
            return head;
        }

        @Override
        void setRoot(NodeState root) {
            if (!head.equals(root)) {
                persistTransientHead(root);
            }
        }

        @Override
        void rebase() {
            N root = getRoot();
            // perform rebase in store
            head = AbstractNodeStoreBranch.this.rebase(head, root);
            base = root;
        }

        @Override
        @Nonnull
        NodeState merge(@Nonnull final CommitHook hook,
                        @Nonnull final CommitInfo info)
                throws CommitFailedException {
            boolean success = false;
            N previousHead = head;
            try {
                rebase();
                previousHead = head;
                dispatcher.contentChanged(base, null);
                N newRoot = withCurrentBranch(new Callable<N>() {
                    @Override
                    public N call() throws Exception {
                        NodeState toCommit = checkNotNull(hook).processCommit(base, head, info);
                        head = AbstractNodeStoreBranch.this.persist(toCommit, head, info);
                        return AbstractNodeStoreBranch.this.merge(head, info);
                    }
                });
                branchState = new Merged(base);
                success = true;
                dispatcher.contentChanged(newRoot, info);
                return newRoot;
            } catch (Exception e) {
                if (e instanceof CommitFailedException) {
                    throw (CommitFailedException) e;
                } else {
                    throw new CommitFailedException(MERGE, 1,
                            "Failed to merge changes to the underlying store", e);
                }
            } finally {
                if (!success) {
                    resetBranch(head, previousHead);
                }
                dispatcher.contentChanged(getRoot(), null);
            }
        }

        private void persistTransientHead(NodeState newHead) {
            head = AbstractNodeStoreBranch.this.persist(newHead, head, null);
        }

        private void resetBranch(N branchHead, N ancestor) {
            try {
                head = AbstractNodeStoreBranch.this.reset(branchHead, ancestor);
            } catch (Exception e) {
                CommitFailedException ex = new CommitFailedException(
                        OAK, 100, "Branch reset failed", e);
                branchState = new ResetFailed(base, ex);
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
        protected Merged(N base) {
            super(base);
        }

        @Override
        public String toString() {
            return "Merged[" + base + ']';
        }

        @Override
        @Nonnull
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
        @Nonnull
        NodeState merge(@Nonnull CommitHook hook, @Nonnull CommitInfo info) {
            throw new IllegalStateException("Branch has already been merged");
        }
    }

    /**
     * Instances of this class represent a branch with persisted changes and
     * a failed attempt to reset changes.
     * <p>
     * Transitions to: none.
     */
    private class ResetFailed extends BranchState {

        /**
         * The exception of the failed reset.
         */
        private final CommitFailedException ex;

        protected ResetFailed(N base, CommitFailedException e) {
            super(base);
            this.ex = e;
        }

        @Nonnull
        @Override
        NodeState getHead() {
            throw new IllegalStateException("Branch with failed reset");
        }

        @Override
        void setRoot(NodeState root) {
            throw new IllegalStateException("Branch with failed reset");
        }

        @Override
        void rebase() {
            throw new IllegalStateException("Branch with failed reset");
        }

        /**
         * Always throws the {@code CommitFailedException} passed to the
         * constructor of this branch state.
         *
         * @throws CommitFailedException the exception of the failed reset.
         */
        @Nonnull
        @Override
        NodeState merge(@Nonnull CommitHook hook, @Nonnull CommitInfo info)
                throws CommitFailedException {
            throw ex;
        }
    }

    private <T> T withCurrentBranch(Callable<T> callable) throws Exception {
        Thread t = Thread.currentThread();
        Object previous = BRANCHES.putIfAbsent(t, this);
        try {
            return callable.call();
        } finally {
            if (previous == null) {
                BRANCHES.remove(t, this);
            }
        }
    }
}
