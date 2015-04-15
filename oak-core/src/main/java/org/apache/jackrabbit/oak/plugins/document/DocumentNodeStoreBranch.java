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
package org.apache.jackrabbit.oak.plugins.document;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.jackrabbit.oak.api.CommitFailedException.MERGE;
import static org.apache.jackrabbit.oak.api.CommitFailedException.OAK;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a DocumentMK based node store branch.
 */
class DocumentNodeStoreBranch implements NodeStoreBranch {
    private static final Logger LOG = LoggerFactory.getLogger(DocumentNodeStoreBranch.class);
    private static final PerfLogger perfLogger = new PerfLogger(
            LoggerFactory.getLogger(DocumentNodeStoreBranch.class.getName()
                    + ".perf"));
    private static final int MAX_LOCK_TRY_TIME_MULTIPLIER = Integer.getInteger("oak.maxLockTryTimeMultiplier", 3);

    private static final ConcurrentMap<Thread, DocumentNodeStoreBranch> BRANCHES = Maps.newConcurrentMap();
    private static final Random RANDOM = new Random();
    private static final long MIN_BACKOFF = 50;

    /** The underlying store to which this branch belongs */
    protected final DocumentNodeStore store;

    protected final long maximumBackoff;

    /** The maximum time in milliseconds to wait for the merge lock. */
    protected final long maxLockTryTimeMS;

    /** Lock for coordinating concurrent merge operations */
    private final ReadWriteLock mergeLock;

    /**
     * State of the this branch. Either {@link Unmodified}, {@link InMemory}, {@link Persisted},
     * {@link ResetFailed} or {@link Merged}.
     * @see BranchState
     */
    private BranchState branchState;

    DocumentNodeStoreBranch(DocumentNodeStore store,
                            DocumentNodeState base,
                            ReadWriteLock mergeLock) {
        this.store = checkNotNull(store);
        this.branchState = new Unmodified(checkNotNull(base));
        this.maximumBackoff = Math.max((long) store.getMaxBackOffMillis(), MIN_BACKOFF);
        this.maxLockTryTimeMS = (long) (store.getMaxBackOffMillis() * MAX_LOCK_TRY_TIME_MULTIPLIER);
        this.mergeLock = mergeLock;
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

    @Nonnull
    @Override
    public NodeState merge(@Nonnull CommitHook hook, @Nonnull CommitInfo info)
            throws CommitFailedException {
        try {
            return merge0(hook, info);
        } catch (CommitFailedException e) {
            if (!e.isOfType(MERGE)) {
                throw e;
            }
        }
        // retry with exclusive lock, blocking other
        // concurrent writes
        // do not wait forever
        Lock lock = null;
        try {
            lock = acquireMergeLock(true);
        } catch (InterruptedException e) {
            // ignore and proceed with shared lock used in base class
        }
        try {
            return merge0(hook, info);
        } finally {
            if (lock != null) {
                lock.unlock();
            }
        }
    }

    @Override
    public void rebase() {
        branchState.rebase();
    }

    @Override
    public String toString() {
        return branchState.toString();
    }

    //------------------------------< internal >--------------------------------

    @Nonnull
    private NodeState merge0(@Nonnull CommitHook hook, @Nonnull CommitInfo info)
            throws CommitFailedException {
        CommitFailedException ex = null;
        long time = System.currentTimeMillis();
        int numRetries = 0;
        for (long backoff = MIN_BACKOFF; backoff <= maximumBackoff; backoff *= 2) {
            if (ex != null) {
                try {
                    numRetries++;
                    final long start = perfLogger.start();
                    Thread.sleep(backoff + RANDOM.nextInt((int) Math.min(backoff, Integer.MAX_VALUE)));
                    perfLogger.end(start, 1, "Merge - Retry attempt [{}]", numRetries);
                } catch (InterruptedException e) {
                    throw new CommitFailedException(
                            MERGE, 3, "Merge interrupted", e);
                }
            }
            try {
                final long start = perfLogger.start();
                Lock lock = acquireMergeLock(false);
                try {
                    perfLogger.end(start, 1, "Merge - Acquired lock");
                    return branchState.merge(checkNotNull(hook), checkNotNull(info));
                } catch (CommitFailedException e) {
                    LOG.trace("Merge Error", e);
                    ex = e;
                    // only retry on merge failures. these may be caused by
                    // changes introduce by a commit hook and may be resolved
                    // by a rebase and running the hook again
                    if (!e.isOfType(MERGE)) {
                        throw e;
                    }
                } finally {
                    if (lock != null) {
                        lock.unlock();
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

    /**
     * Acquires the merge lock either exclusive or shared.
     *
     * @param exclusive whether to acquire the merge lock exclusive.
     * @return the acquired merge lock or {@code null} if the operation timed
     * out.
     * @throws InterruptedException if the current thread is interrupted while
     *                              acquiring the lock
     */
    @CheckForNull
    private Lock acquireMergeLock(boolean exclusive)
            throws InterruptedException {
        Lock lock;
        if (exclusive) {
            lock = mergeLock.writeLock();
        } else {
            lock = mergeLock.readLock();
        }
        boolean acquired = lock.tryLock(maxLockTryTimeMS, MILLISECONDS);
        if (!acquired) {
            String mode = exclusive ? "exclusive" : "shared";
            LOG.info("Time out while acquiring merge lock ({})", mode);
            lock = null;
        }
        return lock;
    }

    private interface Changes {
        void with(Commit c);
    }

    /**
     * Persists the changes between {@code toPersist} and {@code base}
     * to the underlying store.
     * <p>
     * While this method does not declare any exceptions to be thrown, an
     * implementation may still throw a runtime exception specific to the
     * concrete implementation of this node store branch.
     *
     * @param toPersist the state with the changes on top of {@code base}.
     * @param base the base state.
     * @param info the commit info or {@code null} if there is none.
     * @return the state with the persisted changes.
     */
    private DocumentNodeState persist(final NodeState toPersist,
            final DocumentNodeState base,
            final CommitInfo info) {
        return persist(new Changes() {
            @Override
            public void with(Commit c) {
                toPersist.compareAgainstBaseState(base,
                        new CommitDiff(store, c, store.getBlobSerializer()));
            }
        }, base, info);
    }

    /**
     * Persist some changes on top of the given base state.
     *
     * @param op the changes to persist.
     * @param base the base state.
     * @param info the commit info.
     * @return the result state.
     */
    private DocumentNodeState persist(Changes op,
            DocumentNodeState base,
            CommitInfo info) {
        boolean success = false;
        Commit c = store.newCommit(base.getRevision(), this);
        Revision rev;
        try {
            op.with(c);
            if (c.isEmpty()) {
                // no changes to persist. return base state and let
                // finally clause cancel the commit
                return base;
            }
            rev = c.apply();
            success = true;
        } finally {
            if (success) {
                store.done(c, base.getRevision().isBranch(), info);
            } else {
                store.canceled(c);
            }
        }
        return store.getRoot(rev);
    }

    private NodeState getNode(String path) {
        NodeState node = getHead();
        for (String name : elements(path)) {
            node = node.getChildNode(name);
        }
        return node;
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

    /**
     * Returns the branch instance in use by the current thread or
     * {@code null} if there is none.
     * <p>
     * See also {@link #withCurrentBranch(Callable)}.
     *
     */
    @CheckForNull
    static DocumentNodeStoreBranch getCurrentBranch() {
        return BRANCHES.get(Thread.currentThread());
    }

    /**
     * Sub classes of this class represent a state a branch can be in. See the individual
     * sub classes for permissible state transitions.
     */
    private abstract class BranchState {
        /** Root state of the base revision of this branch */
        protected DocumentNodeState base;

        protected BranchState(DocumentNodeState base) {
            this.base = base;
        }

        /**
         * Persist this branch to an underlying branch in the {@code NodeStore}.
         */
        Persisted persist() {
            Persisted p = new Persisted(base);
            p.persistTransientHead(getHead());
            branchState = p;
            return p;
        }

        DocumentNodeState getBase(){
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
        Unmodified(DocumentNodeState base) {
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
            base = store.getRoot();
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

        InMemory(DocumentNodeState base, NodeState head) {
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
            DocumentNodeState root = store.getRoot();
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
            rebase();
            NodeState toCommit = hook.processCommit(base, head, info);
            try {
                NodeState newHead = DocumentNodeStoreBranch.this.persist(toCommit, base, info);
                branchState = new Merged(base);
                return newHead;
            } catch(DocumentStoreException e) {
                throw new CommitFailedException(MERGE, 1, "Failed to merge changes to the underlying store", e);
            } catch (Exception e) {
                throw new CommitFailedException(OAK, 1, "Failed to merge changes to the underlying store", e);
            }
        }
    }

    /**
     * Instances of this class represent a branch whose head is persisted to an
     * underlying branch in the {@code NodeStore}.
     * <p>
     * Transitions to:
     * <ul>
     *     <li>{@link ResetFailed} on failed reset in {@link #merge(CommitHook, CommitInfo)}</li>
     *     <li>{@link Merged} on successful {@link #merge(CommitHook, CommitInfo)}</li>
     * </ul>
     */
    private class Persisted extends BranchState {
        /** Root state of the transient head, top of persisted branch. */
        private DocumentNodeState head;

        @Override
        public String toString() {
            return "Persisted[" + base + ", " + head + ']';
        }

        Persisted(DocumentNodeState base) {
            super(base);
            this.head = createBranch(base);
        }

        /**
         * Create a new branch state from the given state.
         *
         * @param state the state from where to create a branch from.
         * @return the branch state.
         */
        final DocumentNodeState createBranch(DocumentNodeState state) {
            return store.getRoot(state.getRevision().asBranchRevision());
        }

        void move(String source, final String target) {
            final DocumentNodeState src = store.getNode(source, head.getRevision());
            checkNotNull(src, "Source node %s@%s does not exist", source, head.getRevision());
            head = DocumentNodeStoreBranch.this.persist(new Changes() {
                @Override
                public void with(Commit c) {
                    store.moveNode(src, target, c);
                }
            }, head, null);
        }

        void copy(String source, final String target) {
            final DocumentNodeState src = store.getNode(source, head.getRevision());
            checkNotNull(src, "Source node %s@%s does not exist", source, head.getRevision());
            head = DocumentNodeStoreBranch.this.persist(new Changes() {
                @Override
                public void with(Commit c) {
                    store.copyNode(src, target, c);
                }
            }, head, null);
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
            DocumentNodeState root = store.getRoot();
            // perform rebase in store
            head = store.getRoot(store.rebase(head.getRevision(), root.getRevision()));
            base = root;
        }

        @Override
        @Nonnull
        NodeState merge(@Nonnull final CommitHook hook,
                        @Nonnull final CommitInfo info)
                throws CommitFailedException {
            boolean success = false;
            DocumentNodeState previousHead = head;
            try {
                rebase();
                previousHead = head;
                DocumentNodeState newRoot = withCurrentBranch(new Callable<DocumentNodeState>() {
                    @Override
                    public DocumentNodeState call() throws Exception {
                        NodeState toCommit = checkNotNull(hook).processCommit(base, head, info);
                        head = DocumentNodeStoreBranch.this.persist(toCommit, head, info);
                        return store.getRoot(store.merge(head.getRevision(), info));
                    }
                });
                branchState = new Merged(base);
                success = true;
                return newRoot;
            } catch (CommitFailedException e) {
                throw e;
            } catch (Exception e) {
                throw new CommitFailedException(MERGE, 1,
                        "Failed to merge changes to the underlying store", e);
            } finally {
                if (!success) {
                    resetBranch(head, previousHead);
                }
            }
        }

        private void persistTransientHead(NodeState newHead) {
            head = DocumentNodeStoreBranch.this.persist(newHead, head, null);
        }

        private void resetBranch(DocumentNodeState branchHead, DocumentNodeState ancestor) {
            try {
                head = store.getRoot(
                        store.reset(branchHead.getRevision(), 
                                ancestor.getRevision(), 
                                DocumentNodeStoreBranch.this));
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
        protected Merged(DocumentNodeState base) {
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

        protected ResetFailed(DocumentNodeState base, CommitFailedException e) {
            super(base);
            this.ex = e;
        }

        @Nonnull
        @Override
        NodeState getHead() {
            throw new IllegalStateException("Branch with failed reset", ex);
        }

        @Override
        void setRoot(NodeState root) {
            throw new IllegalStateException("Branch with failed reset", ex);
        }

        @Override
        void rebase() {
            throw new IllegalStateException("Branch with failed reset", ex);
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
}
