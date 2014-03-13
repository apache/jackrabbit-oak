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
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.segment.Record.fastEquals;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
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

    private final ChangeDispatcher changeDispatcher;

    /**
     * Local copy of the head of the journal associated with this store.
     */
    private final AtomicReference<SegmentNodeState> head;

    /**
     * Semaphore that controls access to the {@link #head} variable.
     * Only a single local commit is allowed at a time. When such
     * a commit is in progress, no external updates will be seen.
     */
    private final Semaphore commitSemaphore = new Semaphore(1);

    private long maximumBackoff = MILLISECONDS.convert(10, SECONDS);

    public SegmentNodeStore(SegmentStore store) {
        this.store = store;
        this.head = new AtomicReference<SegmentNodeState>(store.getHead());
        this.changeDispatcher = new ChangeDispatcher(getRoot());
    }

    public SegmentNodeStore() {
        this(new MemoryStore());
    }

    void setMaximumBackoff(long max) {
        this.maximumBackoff = max;
    }

    /**
     * Refreshes the head state. Should only be called while holding a
     * permit from the {@link #commitSemaphore}.
     */
    private void refreshHead() {
        SegmentNodeState state = store.getHead();
        if (!state.getRecordId().equals(head.get().getRecordId())) {
            head.set(state);
            changeDispatcher.contentChanged(state.getChildNode(ROOT), null);
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
        return head.get().getChildNode(ROOT);
    }

    @Override
    public NodeState merge(
            @Nonnull NodeBuilder builder, @Nonnull CommitHook commitHook,
            @Nullable CommitInfo info) throws CommitFailedException {
        checkArgument(builder instanceof SegmentNodeBuilder);
        checkNotNull(commitHook);

        SegmentNodeBuilder snb = (SegmentNodeBuilder) builder;

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

        SegmentNodeBuilder snb = (SegmentNodeBuilder) builder;

        NodeState root = getRoot();
        SegmentNodeState before = snb.getBaseState();
        if (!fastEquals(before, root)) {
            SegmentNodeState after = snb.getNodeState();
            snb.reset(root);
            after.compareAgainstBaseState(
                    before, new ConflictAnnotatingRebaseDiff(snb));
        }

        return snb.getNodeState();
    }

    @Override @Nonnull
    public NodeState reset(@Nonnull NodeBuilder builder) {
        checkArgument(builder instanceof SegmentNodeBuilder);

        SegmentNodeBuilder snb = (SegmentNodeBuilder) builder;

        NodeState root = getRoot();
        snb.reset(root);

        return root;
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return store.getWriter().writeStream(stream);
    }

    @Override
    public Blob getBlob(@Nonnull String reference) {
        return store.readBlob(reference);
    }

    @Override @Nonnull
    public String checkpoint(long lifetime) {
        checkArgument(lifetime > 0);
        String name = UUID.randomUUID().toString();

        // try 5 times
        for (int i = 0; i < 5; i++) {
            if (commitSemaphore.tryAcquire()) {
                try {
                    refreshHead();

                    SegmentNodeState state = head.get();

                    SegmentNodeBuilder builder = state.builder();
                    NodeBuilder cp = builder.child(name);
                    cp.setProperty("timestamp", System.currentTimeMillis()
                            + lifetime);
                    cp.setChildNode(ROOT, state.getChildNode(ROOT));

                    if (store.setHead(state, builder.getNodeState())) {
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
    public NodeState retrieve(@Nonnull String checkpoint) {
        NodeState cp = head.get().getChildNode(checkpoint).getChildNode(ROOT);
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
                @Nonnull CommitHook hook, @Nonnull CommitInfo info) {
            checkNotNull(builder);
            this.before = builder.getBaseState();
            this.after = builder.getNodeState();

            this.hook = checkNotNull(hook);
            this.info = checkNotNull(info);
        }

        private boolean setHead(SegmentNodeBuilder builder) {
            SegmentNodeState before = builder.getBaseState();
            SegmentNodeState after = builder.getNodeState();

            refreshHead();
            if (store.setHead(before, after)) {
                head.set(after);
                changeDispatcher.contentChanged(after.getChildNode(ROOT), info);
                refreshHead();
                return true;
            } else {
                return false;
            }
        }

        private SegmentNodeBuilder prepare() throws CommitFailedException {
            SegmentNodeState state = head.get();
            SegmentNodeBuilder builder = state.builder();
            if (fastEquals(before, state.getChildNode(ROOT))) {
                // use a shortcut when there are no external changes
                builder.setChildNode(
                        ROOT, hook.processCommit(before, after, info));
            } else {
                // there were some external changes, so do the full rebase
                ConflictAnnotatingRebaseDiff diff =
                        new ConflictAnnotatingRebaseDiff(builder.child(ROOT));
                after.compareAgainstBaseState(before, diff);
                // apply commit hooks on the rebased changes
                builder.setChildNode(ROOT, hook.processCommit(
                        builder.getBaseState().getChildNode(ROOT),
                        builder.getNodeState().getChildNode(ROOT),
                        info));
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
                SegmentNodeState state = head.get();
                if (state.hasProperty("token")
                        && state.getLong("timeout") >= currentTimeMillis()) {
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
                SegmentNodeState state = head.get();
                if (state.hasProperty("token")
                        && state.getLong("timeout") >= now) {
                    // locked by someone else, wait until unlocked or expired
                    Thread.sleep(
                            Math.min(state.getLong("timeout") - now, 1000),
                            random.nextInt(1000000));
                } else {
                    // attempt to acquire the lock
                    SegmentNodeBuilder builder = state.builder();
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
            return head.get().getChildNode(ROOT);
        }

    }

}
