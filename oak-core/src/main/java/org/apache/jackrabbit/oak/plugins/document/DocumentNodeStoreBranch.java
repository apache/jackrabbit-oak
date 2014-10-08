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

import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReadWriteLock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStoreBranch;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.jackrabbit.oak.api.CommitFailedException.MERGE;

/**
 * Implementation of a DocumentMK based node store branch.
 */
class DocumentNodeStoreBranch
        extends AbstractNodeStoreBranch<DocumentNodeStore, DocumentNodeState> {

    /** Lock for coordinating concurrent merge operations */
    private final ReadWriteLock mergeLock;

    DocumentNodeStoreBranch(DocumentNodeStore store,
                            DocumentNodeState base,
                            ReadWriteLock mergeLock) {
        super(store, new ChangeDispatcher(store.getRoot()), mergeLock.readLock(),
                base, null, store.getMaxBackOffMillis(),
                store.getMaxBackOffMillis() * 3);
        this.mergeLock = mergeLock;
    }

    @Override
    protected DocumentNodeState getRoot() {
        return store.getRoot();
    }

    @Override
    protected DocumentNodeState createBranch(DocumentNodeState state) {
        return store.getRoot(state.getRevision().asBranchRevision());
    }

    @Override
    protected DocumentNodeState rebase(DocumentNodeState branchHead,
                                    DocumentNodeState base) {
        return store.getRoot(store.rebase(branchHead.getRevision(), base.getRevision()));
    }

    @Override
    protected DocumentNodeState merge(DocumentNodeState branchHead, CommitInfo info)
            throws CommitFailedException {
        return store.getRoot(store.merge(branchHead.getRevision(), info));
    }

    @Nonnull
    @Override
    protected DocumentNodeState reset(@Nonnull DocumentNodeState branchHead,
                                   @Nonnull DocumentNodeState ancestor) {
        return store.getRoot(store.reset(branchHead.getRevision(), ancestor.getRevision()));
    }

    @Override
    protected DocumentNodeState persist(final NodeState toPersist,
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

    @Override
    protected DocumentNodeState copy(final String source,
                                  final String target,
                                  DocumentNodeState base) {
        final DocumentNodeState src = store.getNode(source, base.getRevision());
        checkState(src != null, "Source node %s@%s does not exist",
                source, base.getRevision());
        return persist(new Changes() {
            @Override
            public void with(Commit c) {
                store.copyNode(src, target, c);
            }
        }, base, null);
    }

    @Override
    protected DocumentNodeState move(final String source,
                                  final String target,
                                  DocumentNodeState base) {
        final DocumentNodeState src = store.getNode(source, base.getRevision());
        checkState(src != null, "Source node %s@%s does not exist",
                source, base.getRevision());
        return persist(new Changes() {
            @Override
            public void with(Commit c) {
                store.moveNode(src, target, c);
            }
        }, base, null);
    }

    @Nonnull
    @Override
    public NodeState merge(@Nonnull CommitHook hook, @Nonnull CommitInfo info)
            throws CommitFailedException {
        try {
            return super.merge(hook, info);
        } catch (CommitFailedException e) {
            if (!e.isOfType(MERGE)) {
                throw e;
            }
        }
        // retry with exclusive lock, blocking other
        // concurrent writes
        // do not wait forever
        boolean acquired = false;
        try {
            acquired = mergeLock.writeLock()
                    .tryLock(maxLockTryTimeMS, MILLISECONDS);
        } catch (InterruptedException e) {
            // ignore and proceed with shared lock used in base class
        }
        try {
            return super.merge(hook, info);
        } finally {
            if (acquired) {
                mergeLock.writeLock().unlock();
            }
        }
    }

    //------------------------------< internal >--------------------------------

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
        Commit c = store.newCommit(base.getRevision());
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

    private interface Changes {

        void with(Commit c);
    }

    /**
     * Returns the branch instance in use by the current thread or
     * <code>null</code> if there is none.
     * <p>
     * See also {@link AbstractNodeStoreBranch#withCurrentBranch(Callable)}.
     *
     * @return
     */
    @CheckForNull
    static DocumentNodeStoreBranch getCurrentBranch() {
        AbstractNodeStoreBranch b = BRANCHES.get(Thread.currentThread());
        if (b instanceof DocumentNodeStoreBranch) {
            return (DocumentNodeStoreBranch) b;
        }
        return null;
    }
}
