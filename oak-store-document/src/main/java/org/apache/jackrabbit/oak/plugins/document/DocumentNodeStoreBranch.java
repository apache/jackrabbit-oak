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

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.jackrabbit.oak.api.CommitFailedException.MERGE;
import static org.apache.jackrabbit.oak.api.CommitFailedException.OAK;
import static org.apache.jackrabbit.oak.api.CommitFailedException.STATE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.COLLISIONS;
import static org.apache.jackrabbit.oak.plugins.document.util.CountingDiff.countChanges;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Sets;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
    private static final int MAX_LOCK_TRY_TIME_MULTIPLIER = Integer.getInteger("oak.maxLockTryTimeMultiplier", 30);

    private static final Random RANDOM = new Random();
    private static final long MIN_BACKOFF = 50;

    /** The underlying store to which this branch belongs */
    protected final DocumentNodeStore store;

    protected final long maximumBackoff;

    /** The maximum time in milliseconds to wait for the merge lock. */
    protected final long maxLockTryTimeMS;

    /** Lock for coordinating concurrent merge operations */
    private final ReadWriteLock mergeLock;

    /** The maximum number of updates to keep in memory */
    private final int updateLimit;

    /**
     * State of the this branch. Either {@link Unmodified}, {@link InMemory}, {@link Persisted},
     * {@link ResetFailed} or {@link Merged}.
     * @see BranchState
     */
    private BranchState branchState;

    DocumentNodeStoreBranch(DocumentNodeStore store,
                            DocumentNodeState base,
                            ReadWriteLock mergeLock) {
        this.store = requireNonNull(store);
        this.branchState = new Unmodified(requireNonNull(base));
        this.maximumBackoff = Math.max((long) store.getMaxBackOffMillis(), MIN_BACKOFF);
        this.maxLockTryTimeMS = (long) (store.getMaxBackOffMillis() * MAX_LOCK_TRY_TIME_MULTIPLIER);
        this.mergeLock = mergeLock;
        this.updateLimit = store.getUpdateLimit();
    }

    @NotNull
    @Override
    public NodeState getBase() {
        return branchState.getBase();
    }

    @NotNull
    @Override
    public NodeState getHead() {
        return branchState.getHead();
    }

    @Override
    public void setRoot(NodeState newRoot) {
        branchState.setRoot(requireNonNull(newRoot));
    }

    @NotNull
    @Override
    public NodeState merge(@NotNull CommitHook hook, @NotNull CommitInfo info)
            throws CommitFailedException {
        try {
            return merge0(hook, info, false);
        } catch (CommitFailedException e) {
            if (!e.isOfType(MERGE)) {
                throw e;
            }
        }
        // retry with exclusive lock, blocking other
        // concurrent writes
        return merge0(hook, info, true);
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

    /**
     * For test purposes only!
     */
    @NotNull
    ReadWriteLock getMergeLock() {
        return mergeLock;
    }

    /**
     * For test purposes only!
     * <p>
     * Forces the branch to persist the changes to the underlying store.
     */
    void persist() {
        branchState.persist();
    }

    @NotNull
    private NodeState merge0(@NotNull CommitHook hook,
                             @NotNull CommitInfo info,
                             boolean exclusive)
            throws CommitFailedException {
        CommitFailedException ex = null;
        Set<Revision> conflictRevisions = new HashSet<Revision>();
        long time = System.currentTimeMillis();
        int numRetries = 0;
        long suspendMillis = 0;
        for (long backoff = MIN_BACKOFF; backoff <= maximumBackoff; backoff *= 2) {
            if (ex != null) {
                try {
                    numRetries++;
                    final long start = perfLogger.start();
                    // suspend until conflict revision is visible
                    // or as a fallback sleep for a while
                    if (!conflictRevisions.isEmpty()) {
                        // suspend until conflicting revision is visible
                        LOG.debug("Suspending until {} is visible. Current head {}.",
                                conflictRevisions, store.getHeadRevision());
                        suspendMillis += store.suspendUntilAll(conflictRevisions);
                        conflictRevisions.clear();
                        LOG.debug("Resumed. Current head {}.", store.getHeadRevision());
                    } else {
                        long sleepMillis = backoff + RANDOM.nextInt((int) Math.min(backoff, Integer.MAX_VALUE));
                        suspendMillis += sleepMillis;
                        Thread.sleep(sleepMillis);
                    }
                    perfLogger.end(start, 1, "Merge - Retry attempt [{}]", numRetries);
                } catch (InterruptedException e) {
                    throw new CommitFailedException(
                            MERGE, 3, "Merge interrupted", e);
                }
            }
            try {
                NodeState result = branchState.merge(requireNonNull(hook),
                        requireNonNull(info), exclusive);
                store.getStatsCollector().doneMerge(branchState.getMergedChanges(),
                        numRetries, System.currentTimeMillis() - time, suspendMillis, exclusive);
                return result;
            } catch (FailedWithConflictException e) {
                ex = e;
                conflictRevisions.addAll(e.getConflictRevisions());
            } catch (CommitFailedException e) {
                ex = e;
            }
            LOG.trace("Merge Error", ex);
            // only retry on merge failures. these may be caused by
            // changes introduce by a commit hook and may be resolved
            // by a rebase and running the hook again
            if (!ex.isOfType(MERGE)) {
                throw ex;
            }

        }
        // if we get here retrying failed
        time = System.currentTimeMillis() - time;
        store.getStatsCollector().failedMerge(numRetries, time, suspendMillis, exclusive);
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
     * @throws CommitFailedException if the current thread is interrupted while
     *                               acquiring the lock
     */
    @Nullable
    private Lock acquireMergeLock(boolean exclusive)
            throws CommitFailedException {
        final long start = System.nanoTime();
        Lock lock;
        if (exclusive) {
            lock = mergeLock.writeLock();
        } else {
            lock = mergeLock.readLock();
        }
        boolean acquired;
        try {
            acquired = lock.tryLock(maxLockTryTimeMS, MILLISECONDS);
        } catch (InterruptedException e) {
            throw new CommitFailedException(OAK, 1,
                    "Unable to acquire merge lock", e);
        }
        String mode = exclusive ? "exclusive" : "shared";
        if (acquired) {
            perfLogger.end(start, 1, "Merge - Acquired lock ({})", mode);
        } else {
            LOG.info("Time out while acquiring merge lock ({})", mode);
            lock = null;
        }
        long micros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
        store.getStatsCollector().doneMergeLockAcquired(micros);
        return lock;
    }

    /**
     * Persists the changes between {@code toPersist} and {@code base}
     * to the underlying store.
     *
     * @param toPersist the state with the changes on top of {@code base}.
     * @param base the base state.
     * @param info the commit info.
     * @param stats the merge stats.
     * @return the state with the persisted changes.
     * @throws ConflictException if changes cannot be persisted because a
     *          conflict occurred. The exception may contain the revisions of
     *          the conflicting operations.
     * @throws DocumentStoreException if the persist operation failed because
     *          of an exception in the underlying {@link DocumentStore}.
     */
    private DocumentNodeState persist(final @NotNull NodeState toPersist,
                                      final @NotNull DocumentNodeState base,
                                      final @NotNull CommitInfo info,
                                      final @NotNull MergeStats stats)
            throws ConflictException, DocumentStoreException {
        return persist(new Changes() {
            @Override
            public void with(@NotNull CommitBuilder commitBuilder) {
                CommitDiff diff = new CommitDiff(
                        store.getBundlingConfigHandler().newBundlingHandler(),
                        commitBuilder, store.getBlobSerializer());
                toPersist.compareAgainstBaseState(base, diff);
                stats.numDocuments += diff.getNumChanges();
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
     * @throws ConflictException if changes cannot be persisted because a
     *          conflict occurred. The exception may contain the revisions of
     *          the conflicting operations.
     * @throws DocumentStoreException if the persist operation failed because
     *          of an exception in the underlying {@link DocumentStore}.
     */
    private DocumentNodeState persist(@NotNull Changes op,
                                      @NotNull DocumentNodeState base,
                                      @NotNull CommitInfo info)
            throws ConflictException, DocumentStoreException {
        boolean success = false;
        Commit c = store.newCommit(op, base.getRootRevision(), this);
        RevisionVector rev;
        try {
            if (c.isEmpty()) {
                // no changes to persist. return base state and let
                // finally clause cancel the commit
                return base;
            }
            c.apply();
            rev = store.done(c, base.getRootRevision().isBranch(), info);
            success = true;
        } finally {
            if (!success) {
                store.canceled(c);
            }
        }
        return store.getRoot(rev);
    }

    private static CommitFailedException mergeFailed(Throwable t) {
        String msg = t.getMessage();
        if (msg == null) {
            msg = "Failed to merge changes to the underlying store";
        }
        String type = OAK;
        if (t instanceof DocumentStoreException) {
            DocumentStoreException dse = (DocumentStoreException) t;
            if (dse.getType() == DocumentStoreException.Type.TRANSIENT) {
                // set type to MERGE, which indicates a retry may work
                type = MERGE;
            }
        }
        return new CommitFailedException(type, 1, msg, t);
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

        int getMergedChanges() {
            return 0;
        }

        @NotNull
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
         * @param exclusive whether the merge lock must be acquired exclusively
         *                  or shared while performing the merge.
         * @return the result of the merge.
         * @throws CommitFailedException if a commit hook rejected the changes
         *          or the actual merge operation failed. An implementation must
         *          use the appropriate type in {@code CommitFailedException} to
         *          indicate the cause of the exception.
         */
        @NotNull
        abstract NodeState merge(@NotNull CommitHook hook,
                                 @NotNull CommitInfo info,
                                 boolean exclusive)
                throws CommitFailedException;
    }

    /**
     * Instances of this class represent a branch whose base and head are the same.
     * <p>
     * Transitions to:
     * <ul>
     *     <li>{@link InMemory} on {@link #setRoot(NodeState)} if the new root differs
     *         from the current base</li>.
     *     <li>{@link Merged} on {@link BranchState#merge(CommitHook, CommitInfo, boolean)}</li>
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
        @NotNull
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
        @NotNull
        NodeState merge(@NotNull CommitHook hook,
                        @NotNull CommitInfo info,
                        boolean exclusive) {
            branchState = new Merged(base, new MergeStats());
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
     *         as the base of this branch</li>
     *     <li>{@link Persisted} on {@link #setRoot(NodeState)} if the number of
     *         changes counted from the base to the new root reaches
     *         {@link DocumentNodeStoreBuilder#getUpdateLimit()}.</li>
     *     <li>{@link Merged} on {@link BranchState#merge(CommitHook, CommitInfo, boolean)}</li>
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
            this.head = newModifiedDocumentNodeState(head);
        }

        @Override
        @NotNull
        NodeState getHead() {
            return head;
        }

        @Override
        void setRoot(NodeState root) {
            if (base.equals(root)) {
                branchState = new Unmodified(base);
            } else {
                int numChanges = countChanges(base, root);
                if (numChanges > updateLimit) {
                    head = newModifiedDocumentNodeState(root);
                    persist();
                } else {
                    NodeBuilder builder = new MemoryNodeBuilder(base);
                    root.compareAgainstBaseState(base, new ApplyDiff(builder));
                    head = newModifiedDocumentNodeState(builder.getNodeState());
                }
            }
        }

        @Override
        void rebase() {
            DocumentNodeState root = store.getRoot();
            // is a rebase necessary?
            if (root.getRootRevision().equals(base.getRootRevision())) {
                // fresh root is still at the same revision as
                // the base of the branch
                return;
            }
            NodeBuilder builder = new MemoryNodeBuilder(root);
            head.compareAgainstBaseState(base, new ConflictAnnotatingRebaseDiff(builder));
            base = root;
            head = newModifiedDocumentNodeState(builder.getNodeState());
        }

        @Override
        @NotNull
        NodeState merge(@NotNull CommitHook hook,
                        @NotNull CommitInfo info,
                        boolean exclusive)
                throws CommitFailedException {
            requireNonNull(hook);
            requireNonNull(info);
            Lock lock = acquireMergeLock(exclusive);
            try {
                rebase();
                boolean success = false;
                NodeState previousHead = head;
                try {
                    DocumentNodeStoreStatsCollector stats = store.getStatsCollector();
                    NodeState toCommit = TimingHook.wrap(hook, (time, unit) -> stats.doneCommitHookProcessed(unit.toMicros(time)))
                            .processCommit(base, head, info);
                    try {
                        MergeStats ms = new MergeStats();
                        NodeState newHead;
                        if (this != branchState) {
                            // branch state is not in-memory anymore
                            Persisted p = branchState.persist();
                            RevisionVector branchRev = p.getHead().getRootRevision();
                            newHead = store.getRoot(store.merge(branchRev, info));
                            stats.doneMergeBranch(p.numCommits, p.getMergedChanges());
                        } else {
                            newHead = DocumentNodeStoreBranch.this.persist(toCommit, base, info, ms);
                        }
                        branchState = new Merged(base, ms);
                        success = true;
                        return newHead;
                    } catch (ConflictException e) {
                        throw e.asCommitFailedException();
                    } catch (Throwable t) {
                        throw mergeFailed(t);
                    }
                } finally {
                    if (!success) {
                        this.head = previousHead;
                        if (this != branchState) {
                            // the branch state transitioned to persisted while
                            // processing the commit hook and then failed.
                            // remember the persisted branch state
                            BranchState currentState = branchState;
                            // reset branch state back to in-memory
                            branchState = this;
                            // reset the entire persisted branch state
                            reset(currentState.persist());
                        }
                    }
                }
            } finally {
                if (lock != null) {
                    lock.unlock();
                }
            }
        }

        /**
         * Reset the entire persisted branch.
         *
         * @param p the persisted branch.
         */
        private void reset(Persisted p) {
            RevisionVector branchHeadRev = p.getHead().getRootRevision();
            // get the branch that belongs to this persisted branch state
            Branch b = store.getBranches().getBranch(branchHeadRev);
            if (b != null) {
                try {
                    store.reset(branchHeadRev,
                            b.getBase().asBranchRevision(store.getClusterId()));
                } catch (Exception e) {
                    LOG.warn("Resetting persisted branch failed", e);
                }
            }
        }

        private ModifiedDocumentNodeState newModifiedDocumentNodeState(NodeState modified) {
            return new ModifiedDocumentNodeState(store, DocumentNodeStoreBranch.this, base, modified);
        }
    }

    /**
     * Instances of this class represent a branch whose head is persisted to an
     * underlying branch in the {@code NodeStore}.
     * <p>
     * Transitions to:
     * <ul>
     *     <li>{@link ResetFailed} on failed reset in {@link BranchState#merge(CommitHook, CommitInfo, boolean)}</li>
     *     <li>{@link Merged} on successful {@link BranchState#merge(CommitHook, CommitInfo, boolean)}</li>
     * </ul>
     */
    private class Persisted extends BranchState {
        /** Root state of the transient head, top of persisted branch. */
        private DocumentNodeState head;

        /**
         * Number of commits on this persisted branch.
         */
        private int numCommits;

        private final MergeStats ms = new MergeStats();

        @Override
        public String toString() {
            return "Persisted[" + base + ", " + head + ']';
        }

        Persisted(DocumentNodeState base) {
            super(base);
            this.head = createBranch(base).asBranchRootState(DocumentNodeStoreBranch.this);
        }

        @Override
        Persisted persist() {
            // nothing to do, this branch state is already persisted
            return this;
        }

        /**
         * Create a new branch state from the given state.
         *
         * @param state the state from where to create a branch from.
         * @return the branch state.
         */
        final DocumentNodeState createBranch(DocumentNodeState state) {
            return store.getRoot(state.getRootRevision().asBranchRevision(store.getClusterId()));
        }

        @Override
        @NotNull
        DocumentNodeState getHead() {
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
            head = store.getRoot(store.rebase(head.getRootRevision(), root.getRootRevision()))
                    .asBranchRootState(DocumentNodeStoreBranch.this);
            base = root;
        }

        @Override
        @NotNull
        NodeState merge(@NotNull final CommitHook hook,
                        @NotNull final CommitInfo info,
                        boolean exclusive)
                throws CommitFailedException {
            boolean success = false;
            DocumentNodeState previousHead = head;
            Lock lock = acquireMergeLock(exclusive);
            try {
                rebase();
                previousHead = head;
                checkForConflicts();
                DocumentNodeStoreStatsCollector stats = store.getStatsCollector();
                NodeState toCommit = TimingHook.wrap(requireNonNull(hook), (time, unit) -> stats.doneCommitHookProcessed(unit.toMicros(time)))
                        .processCommit(base, head, info);
                persistTransientHead(toCommit);
                DocumentNodeState newRoot = store.getRoot(store.merge(head.getRootRevision(), info));
                success = true;
                branchState = new Merged(base, ms);
                stats.doneMergeBranch(numCommits, branchState.getMergedChanges());
                return newRoot;
            } catch (CommitFailedException e) {
                throw e;
            } catch (ConflictException e) {
                throw e.asCommitFailedException();
            } catch (Throwable t) {
                throw mergeFailed(t);
            } finally {
                if (lock != null) {
                    lock.unlock();
                }
                if (!success) {
                    resetBranch(head, previousHead);
                }
            }
        }

        private void persistTransientHead(NodeState newHead)
                throws DocumentStoreException {
            try {
                head = DocumentNodeStoreBranch.this.persist(newHead, head, CommitInfo.EMPTY, ms)
                        .asBranchRootState(DocumentNodeStoreBranch.this);
            } catch (ConflictException e) {
                throw DocumentStoreException.convert(e);
            }
            numCommits++;
            store.getStatsCollector().doneBranchCommit();
        }

        private void resetBranch(DocumentNodeState branchHead, DocumentNodeState ancestor) {
            try {
                head = store.getRoot(store.reset(branchHead.getRootRevision(),
                            ancestor.getRootRevision()))
                        .asBranchRootState(DocumentNodeStoreBranch.this);
            } catch (Exception e) {
                CommitFailedException ex = new CommitFailedException(
                        OAK, 100, "Branch reset failed", e);
                branchState = new ResetFailed(base, ex);
            }
        }

        /**
         * Checks if any of the commits on this branch have a collision marker
         * set.
         *
         * @throws CommitFailedException if a collision marker is set for one
         *          of the commits on this branch.
         */
        private void checkForConflicts() throws CommitFailedException {
            Branch b = store.getBranches().getBranch(head.getRootRevision());
            if (b == null) {
                return;
            }
            NodeDocument doc = Utils.getRootDocument(store.getDocumentStore());
            Set<Revision> collisions = new HashSet<>(doc.getLocalMap(COLLISIONS).keySet());
            Set<Revision> commits = new HashSet<>();
            Iterables.transform(b.getCommits(), Revision::asTrunkRevision).forEach(commits::add);
            Set<Revision> conflicts = Sets.intersection(collisions, commits);
            if (!conflicts.isEmpty()) {
                throw new CommitFailedException(STATE, 2,
                        "Conflicting concurrent change on branch commits " + conflicts);
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

        private final MergeStats stats;

        protected Merged(@NotNull DocumentNodeState base,
                         @NotNull MergeStats stats) {
            super(base);
            this.stats = stats;
        }

        @Override
        public String toString() {
            return "Merged[" + base + ']';
        }

        @Override
        @NotNull
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
        @NotNull
        NodeState merge(@NotNull CommitHook hook,
                        @NotNull CommitInfo info,
                        boolean exclusive) {
            throw new IllegalStateException("Branch has already been merged");
        }

        @Override
        int getMergedChanges() {
            return stats.numDocuments;
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

        @NotNull
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
        @NotNull
        @Override
        NodeState merge(@NotNull CommitHook hook,
                        @NotNull CommitInfo info,
                        boolean exclusive)
                throws CommitFailedException {
            throw ex;
        }
    }

    /**
     * Configures the performance logger with the specified info log interval.
     *
     * @param infoLogMillis the interval in milliseconds for logging performance information.
     */
    static void configurePerfLogger(long infoLogMillis) {
        perfLogger.setInfoLogMillis(infoLogMillis);
    }
}
