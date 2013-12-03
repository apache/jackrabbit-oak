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
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.base.Objects;

public class SegmentNodeStore implements NodeStore, Observable {

    static final String ROOT = "root";

    private final SegmentStore store;

    private final Journal journal;

    private final ChangeDispatcher changeDispatcher;

    volatile SegmentNodeState head;

    /**
     * Semaphore that controls access to the {@link #head} variable.
     * Only a single local commit is allowed at a time. When such
     * a commit is in progress, no external updates will be seen.
     */
    private final Semaphore commitSemaphore = new Semaphore(1);

    private long maximumBackoff = MILLISECONDS.convert(10, SECONDS);

    public SegmentNodeStore(SegmentStore store, String journal) {
        this.store = store;
        this.journal = store.getJournal(journal);
        this.head = new SegmentNodeState(
                store.getWriter().getDummySegment(), this.journal.getHead());
        this.changeDispatcher = new ChangeDispatcher(getRoot());
    }

    public SegmentNodeStore(SegmentStore store) {
        this(store, "root");
    }

    void setMaximumBackoff(long max) {
        this.maximumBackoff = max;
    }

    /**
     * Refreshes the head state. Does nothing if a concurrent local commit is
     * in progress, as that commit will automatically refresh the head state.
     */
    private void refreshHead() {
        RecordId id = journal.getHead();
        if (!id.equals(head.getRecordId())) {
            head = new SegmentNodeState(
                    store.getWriter().getDummySegment(), id);
            changeDispatcher.contentChanged(head.getChildNode(ROOT), null);
        }
    }

    @Override
    public Closeable addObserver(Observer observer) {
        return changeDispatcher.addObserver(observer);
    }

    @Override @Nonnull
    public NodeState getRoot() {
        if (commitSemaphore.tryAcquire()) {
            try {
                refreshHead();
            } finally {
                commitSemaphore.release();
            }
        }
        return new SegmentRootState(head);
    }

    @Override
    public NodeState merge(
            @Nonnull NodeBuilder builder, @Nonnull CommitHook commitHook,
            @Nullable CommitInfo info) throws CommitFailedException {
        checkNotNull(commitHook);

        NodeState base = builder.getBaseState();
        checkArgument(store.isInstance(base, SegmentRootState.class));
        SegmentNodeState root = ((SegmentRootState) base).getRootState();

        try {
            commitSemaphore.acquire();
            try {
                Commit commit = new Commit(
                        root, builder.getNodeState(), commitHook, info);
                NodeState merged = commit.execute();
                ((SegmentNodeBuilder) builder).reset(merged);
                return merged;
            } finally {
                commitSemaphore.release();
            }
        } catch (InterruptedException e) {
            throw new CommitFailedException(
                    "Segment", 2, "Merge interrupted", e);
        }
    }

    @Override @Nonnull
    public NodeState rebase(@Nonnull NodeBuilder builder) {
        checkArgument(builder instanceof SegmentRootBuilder);
        NodeState newBase = getRoot();
        NodeState oldBase = builder.getBaseState();
        if (!fastEquals(oldBase, newBase)) {
            NodeState head = builder.getNodeState();
            ((SegmentNodeBuilder) builder).reset(newBase);
            head.compareAgainstBaseState(oldBase, new ConflictAnnotatingRebaseDiff(builder));
        }
        return builder.getNodeState();
    }

    private boolean fastEquals(Object a, Object b) {
        return store.isInstance(a, Record.class)
                && store.isInstance(b, Record.class)
                && Objects.equal(
                        ((Record) a).getRecordId(),
                        ((Record) b).getRecordId());
    }

    @Override @Nonnull
    public NodeState reset(@Nonnull NodeBuilder builder) {
        checkArgument(builder instanceof SegmentRootBuilder);
        NodeState state = getRoot();
        ((SegmentNodeBuilder) builder).reset(state);
        return state;
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return store.getWriter().writeStream(stream);
    }

    @Override @Nonnull
    public synchronized String checkpoint(long lifetime) {
        checkArgument(lifetime > 0);
        // TODO: Guard the checkpoint from garbage collection
        return head.getRecordId().toString();
    }

    @Override @CheckForNull
    public synchronized NodeState retrieve(@Nonnull String checkpoint) {
        // TODO: Verify validity of the checkpoint
        RecordId id = RecordId.fromString(checkNotNull(checkpoint));
        SegmentNodeState root =
                new SegmentNodeState(store.getWriter().getDummySegment(), id);
        return root.getChildNode(ROOT);
    }

    private class Commit {

        private final Random random = new Random();

        private SegmentNodeState base;

        private SegmentNodeState head;

        private final CommitHook hook;

        private final CommitInfo info;

        Commit(@Nonnull SegmentNodeState base, @Nonnull NodeState head,
                @Nonnull CommitHook hook, @Nullable CommitInfo info) {
            this.base = checkNotNull(base);
            SegmentRootBuilder builder = base.builder();
            builder.setChildNode(ROOT, checkNotNull(head));
            this.head = builder.getNodeState();

            this.hook = checkNotNull(hook);
            this.info = info;
        }

        private boolean setHead(
                SegmentNodeState base, SegmentNodeState head, CommitInfo info) {
            refreshHead();
            if (journal.setHead(base.getRecordId(), head.getRecordId())) {
                this.head = head;
                changeDispatcher.contentChanged(head.getChildNode(ROOT), info);
                refreshHead();
                return true;
            } else {
                return false;
            }
        }

        private void rebase() {
            SegmentNodeState newBase = SegmentNodeStore.this.head;
            if (!base.getRecordId().equals(newBase.getRecordId())) {
                NodeBuilder builder = newBase.builder();
                head.getChildNode(ROOT).compareAgainstBaseState(
                        base.getChildNode(ROOT),
                        new ConflictAnnotatingRebaseDiff(builder.child(ROOT)));
                base = newBase;
                head = store.getWriter().writeNode(builder.getNodeState());
            }
        }

        private long optimisticMerge(CommitHook hook, CommitInfo info)
                throws CommitFailedException, InterruptedException {
            long timeout = 1;

            SegmentNodeState originalBase = base;
            SegmentNodeState originalHead = head;

            // use exponential backoff in case of concurrent commits
            for (long backoff = 1; backoff < maximumBackoff; backoff *= 2) {
                rebase(); // rebase to latest head, a no-op if already there

                long start = System.nanoTime();

                if (base.hasProperty("token")
                        && base.getLong("timeout") >= System.currentTimeMillis()) {
                    // someone else has a pessimistic lock on the journal,
                    // so we should not try to commit anything
                } else {
                    // apply commit hooks on the rebased changes
                    NodeBuilder builder = head.builder();
                    builder.setChildNode(ROOT, hook.processCommit(
                            base.getChildNode(ROOT), head.getChildNode(ROOT)));
                    SegmentNodeState newHead =
                            store.getWriter().writeNode(builder.getNodeState());

                    // use optimistic locking to update the journal
                    if (setHead(base, newHead, info)) {
                        base = newHead;
                        head = newHead;
                        return -1;
                    }
                }

                // someone else was faster, so restore state and retry later
                base = originalBase;
                head = originalHead;

                Thread.sleep(backoff, random.nextInt(1000000));

                long stop = System.nanoTime();
                if (stop - start > timeout) {
                    timeout = stop - start;
                }
            }

            return MILLISECONDS.convert(timeout, NANOSECONDS);
        }

        private void pessimisticMerge(
                CommitHook hook, long timeout, CommitInfo info)
                throws CommitFailedException, InterruptedException {
            while (true) {
                SegmentNodeState before = head;
                long now = System.currentTimeMillis();
                if (before.hasProperty("token")
                        && before.getLong("timeout") >= now) {
                    // locked by someone else, wait until unlocked or expired
                    Thread.sleep(
                            Math.min(before.getLong("timeout") - now, 1000),
                            random.nextInt(1000000));
                } else {
                    // attempt to acquire the lock
                    NodeBuilder builder = before.builder();
                    builder.setProperty("token", UUID.randomUUID().toString());
                    builder.setProperty("timeout", now + timeout);

                    SegmentNodeState after =
                            store.getWriter().writeNode(builder.getNodeState());
                    if (setHead(before, after, info)) {
                        SegmentNodeState originalBase = base;
                        SegmentNodeState originalHead = head;

                        // lock acquired; rebase, apply commit hooks, and unlock
                        rebase();
                        builder.setChildNode(ROOT, hook.processCommit(
                                base.getChildNode(ROOT), head.getChildNode(ROOT)));
                        builder.removeProperty("token");
                        builder.removeProperty("timeout");

                        // complete the commit
                        SegmentNodeState newHead =
                                store.getWriter().writeNode(builder.getNodeState());
                        if (setHead(after, newHead, info)) {
                            base = newHead;
                            head = newHead;
                            return;
                        } else {
                            // something else happened, perhaps a timeout, so
                            // undo the previous rebase and try again
                            base = originalBase;
                            head = originalHead;
                        }
                    }
                }
            }
        }

        @Nonnull
        SegmentRootState execute()
                throws CommitFailedException, InterruptedException {
            if (base != head) {
                long timeout = optimisticMerge(hook, info);
                if (timeout >= 0) {
                    pessimisticMerge(hook, timeout, info);
                }
            }
            return new SegmentRootState(head);
        }

    }

}
