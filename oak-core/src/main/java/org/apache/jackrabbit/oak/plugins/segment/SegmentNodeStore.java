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
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

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
     * Whether a thread is currently processing a local commit.
     */
    private boolean inLocalCommit = false;

    /**
     * Number of threads waiting to make a local commit.
     * Used to avoid extra {@link #notifyAll()} calls when nobody is waiting.
     */
    private long waitingToCommit = 0;

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
     *
     * @param commit whether this refresh is a part of a local commit
     */
    private synchronized void refreshHead(boolean commit) {
        if (commit || !inLocalCommit) {
            RecordId id = journal.getHead();
            if (!id.equals(head.getRecordId())) {
                head = new SegmentNodeState(
                        store.getWriter().getDummySegment(), id);
                changeDispatcher.contentChanged(head.getChildNode(ROOT), null);
            }
        }
    }

    private synchronized void refreshHeadInCommit(boolean start)
            throws InterruptedException {
        if (start) {
            while (inLocalCommit) {
                waitingToCommit++;
                try {
                    wait();
                } finally {
                    waitingToCommit--;
                }
            }
            inLocalCommit = true;
        } else {
            checkState(inLocalCommit);
        }

        try {
            refreshHead(true);
        } finally {
            if (!start) {
                inLocalCommit = false;
                if (waitingToCommit > 0) {
                    notifyAll();
                }
            }
        }
    }

    boolean setHead(
            SegmentNodeState base, SegmentNodeState head, CommitInfo info)
            throws InterruptedException {
        refreshHeadInCommit(true);
        try {
            if (journal.setHead(base.getRecordId(), head.getRecordId())) {
                this.head = head;
                changeDispatcher.contentChanged(head.getChildNode(ROOT), info);
                return true;
            } else {
                return false;
            }
        } finally {
            refreshHeadInCommit(false);
        }
    }

    @Override
    public Closeable addObserver(Observer observer) {
        return changeDispatcher.addObserver(observer);
    }

    @Override @Nonnull
    public NodeState getRoot() {
        refreshHead(false);
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

        SegmentNodeStoreBranch branch = new SegmentNodeStoreBranch(
                this, store.getWriter(), root, maximumBackoff);
        branch.setRoot(builder.getNodeState());
        NodeState merged = branch.merge(commitHook, info);
        ((SegmentNodeBuilder) builder).reset(merged);
        return merged;
    }

    @Override @Nonnull
    public NodeState rebase(@Nonnull NodeBuilder builder) {
        return rebase(builder, getRoot());
    }

    private NodeState rebase(@Nonnull NodeBuilder builder, NodeState newBase) {
        checkArgument(builder instanceof SegmentRootBuilder);
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
}
