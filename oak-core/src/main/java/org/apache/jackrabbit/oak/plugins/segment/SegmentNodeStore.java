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

import static com.google.common.base.Objects.equal;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.currentTimeMillis;
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
        return head.getChildNode(ROOT);
    }

    @Override
    public NodeState merge(
            @Nonnull NodeBuilder builder, @Nonnull CommitHook commitHook,
            @Nullable CommitInfo info) throws CommitFailedException {
        checkArgument(builder instanceof SegmentNodeBuilder);
        checkNotNull(commitHook);

        SegmentNodeBuilder snb = (SegmentNodeBuilder) builder;
        checkArgument(store == snb.getBaseState().getStore());

        try {
            commitSemaphore.acquire();
            try {
                Commit commit = new Commit(snb, commitHook, info);
                NodeState merged = commit.execute();
                snb.reset(merged);
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
        checkArgument(builder instanceof SegmentNodeBuilder);
        NodeState newBase = getRoot();
        NodeState oldBase = builder.getBaseState();
        if (!fastEquals(oldBase, newBase)) {
            NodeState head = builder.getNodeState();
            ((SegmentNodeBuilder) builder).reset(newBase);
            head.compareAgainstBaseState(oldBase, new ConflictAnnotatingRebaseDiff(builder));
        }
        return builder.getNodeState();
    }

    @Override @Nonnull
    public NodeState reset(@Nonnull NodeBuilder builder) {
        checkArgument(builder instanceof SegmentNodeBuilder);
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
        String name = UUID.randomUUID().toString();

        // try 5 times
        for (int i = 0; i < 5; i++) {
            if (commitSemaphore.tryAcquire()) {
                try {
                    refreshHead();

                    SegmentNodeState ns = head;
                    RecordId ri = head.getRecordId();

                    SegmentNodeBuilder builder = ns.builder();
                    NodeBuilder cp = builder.child(name);
                    cp.setProperty("timestamp", System.currentTimeMillis()
                            + lifetime);
                    cp.setChildNode(ROOT, ns.getChildNode(ROOT));

                    if (journal.setHead(ri, builder.getNodeState()
                            .getRecordId())) {
                        refreshHead();
                        return name;
                    }

                } finally {
                    commitSemaphore.release();
                }
            }
        }

        return name;
    }

    @Override @CheckForNull
    public synchronized NodeState retrieve(@Nonnull String checkpoint) {
        NodeState cp = head.getChildNode(checkpoint).getChildNode(ROOT);
        if (cp.exists()) {
            return cp;
        }
        return null;
    }

    private class Commit {

        private final Random random = new Random();

        private SegmentNodeState before;

        private SegmentNodeState after;

        private final CommitHook hook;

        private final CommitInfo info;

        Commit(@Nonnull SegmentNodeBuilder builder,
                @Nonnull CommitHook hook, @Nullable CommitInfo info) {
            checkNotNull(builder);
            this.before = builder.getBaseState();
            this.after = builder.getNodeState();

            this.hook = checkNotNull(hook);
            this.info = info;
        }

        private boolean setHead(SegmentNodeBuilder builder) {
            SegmentNodeState base = builder.getBaseState();
            SegmentNodeState head = builder.getNodeState();

            refreshHead();
            if (journal.setHead(base.getRecordId(), head.getRecordId())) {
                SegmentNodeStore.this.head = head;
                changeDispatcher.contentChanged(head.getChildNode(ROOT), info);
                refreshHead();
                return true;
            } else {
                return false;
            }
        }

        private SegmentNodeBuilder prepare() throws CommitFailedException {
            SegmentNodeBuilder builder = head.builder();
            if (fastEquals(before, head.getChildNode(ROOT))) {
                // use a shortcut when there are no external changes
                builder.setChildNode(ROOT, hook.processCommit(before, after));
            } else {
                // there were some external changes, so do the full rebase
                ConflictAnnotatingRebaseDiff diff =
                        new ConflictAnnotatingRebaseDiff(builder.child(ROOT));
                after.compareAgainstBaseState(before, diff);
                // apply commit hooks on the rebased changes
                builder.setChildNode(ROOT, hook.processCommit(
                        builder.getBaseState().getChildNode(ROOT),
                        builder.getNodeState().getChildNode(ROOT)));
            }
            return builder;
        }

        private long optimisticMerge()
                throws CommitFailedException, InterruptedException {
            long timeout = 1;

            // use exponential backoff in case of concurrent commits
            for (long backoff = 1; backoff < maximumBackoff; backoff *= 2) {
                long start = System.nanoTime();

                refreshHead();
                if (head.hasProperty("token")
                        && head.getLong("timeout") >= currentTimeMillis()) {
                    // someone else has a pessimistic lock on the journal,
                    // so we should not try to commit anything yet
                } else {
                    SegmentNodeBuilder builder = prepare();
                    // use optimistic locking to update the journal
                    if (setHead(builder)) {
                        return -1;
                    }
                }

                // someone else was faster, so wait a while and retry later
                Thread.sleep(backoff, random.nextInt(1000000));

                long stop = System.nanoTime();
                if (stop - start > timeout) {
                    timeout = stop - start;
                }
            }

            return MILLISECONDS.convert(timeout, NANOSECONDS);
        }

        private void pessimisticMerge(long timeout)
                throws CommitFailedException, InterruptedException {
            while (true) {
                long now = currentTimeMillis();
                if (head.hasProperty("token")
                        && head.getLong("timeout") >= now) {
                    // locked by someone else, wait until unlocked or expired
                    Thread.sleep(
                            Math.min(head.getLong("timeout") - now, 1000),
                            random.nextInt(1000000));
                } else {
                    // attempt to acquire the lock
                    SegmentNodeBuilder builder = head.builder();
                    builder.setProperty("token", UUID.randomUUID().toString());
                    builder.setProperty("timeout", now + timeout);

                    if (setHead(builder)) {
                         // lock acquired; rebase, apply commit hooks, and unlock
                        builder = prepare();
                        builder.removeProperty("token");
                        builder.removeProperty("timeout");

                        // complete the commit
                        if (setHead(builder)) {
                            return;
                        }
                    }
                }
            }
        }

        @Nonnull
        NodeState execute()
                throws CommitFailedException, InterruptedException {
            // only do the merge if there are some changes to commit
            if (!fastEquals(before, after)) {
                long timeout = optimisticMerge();
                if (timeout >= 0) {
                    pessimisticMerge(timeout);
                }
            }
            return head.getChildNode(ROOT);
        }

    }

    private static boolean fastEquals(Object a, Object b) {
        return a instanceof Record && fastEquals((Record) a, b);
    }

    private static boolean fastEquals(Record a, Object b) {
        return b instanceof Record && fastEquals(a, (Record) b);
    }

    private static boolean fastEquals(Record a, Record b) {
        return equal(a.getRecordId(), b.getRecordId());
    }

}
