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
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyObserver;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.commit.PostCommitHook;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class SegmentNodeStore implements NodeStore {

    static final String ROOT = "root";

    private final SegmentStore store;

    private final Journal journal;

    private final SegmentReader reader;

    private final Observer observer;

    private SegmentNodeState head;

    private long maximumBackoff = MILLISECONDS.convert(10, SECONDS);

    public SegmentNodeStore(SegmentStore store, String journal) {
        this.store = store;
        this.journal = store.getJournal(journal);
        this.reader = new SegmentReader(store);
        this.observer = EmptyObserver.INSTANCE;
        this.head = new SegmentNodeState(store, this.journal.getHead());
    }

    public SegmentNodeStore(SegmentStore store) {
        this(store, "root");
    }

    void setMaximumBackoff(long max) {
        this.maximumBackoff = max;
    }

    synchronized SegmentNodeState getHead() {
        NodeState before = head.getChildNode(ROOT);
        head = new SegmentNodeState(store, journal.getHead());
        NodeState after = head.getChildNode(ROOT);
        observer.contentChanged(before, after);
        return head;
    }

    boolean setHead(SegmentNodeState base, SegmentNodeState head) {
        return journal.setHead(base.getRecordId(), head.getRecordId());
    }

    @Override @Nonnull
    public synchronized NodeState getRoot() {
        return getHead().getChildNode(ROOT);
    }

    @Override
    public synchronized NodeState merge(
            @Nonnull NodeBuilder builder,
            @Nonnull CommitHook commitHook, PostCommitHook committed)
            throws CommitFailedException {
        checkArgument(builder instanceof SegmentNodeBuilder);
        checkNotNull(commitHook);
        SegmentNodeState head = getHead();
        rebase(builder, head.getChildNode(ROOT)); // TODO: can we avoid this?
        SegmentNodeStoreBranch branch = new SegmentNodeStoreBranch(
                this, store.getWriter(), head, maximumBackoff);
        branch.setRoot(builder.getNodeState());
        NodeState merged = branch.merge(commitHook, committed);
        ((SegmentNodeBuilder) builder).reset(merged);
        return merged;
    }

    @Override @Nonnull
    public NodeState rebase(@Nonnull NodeBuilder builder) {
        return rebase(builder, getRoot());
    }

    private NodeState rebase(@Nonnull NodeBuilder builder, NodeState newBase) {
        checkArgument(builder instanceof SegmentNodeBuilder);
        NodeState oldBase = builder.getBaseState();
        if (!SegmentNodeState.fastEquals(oldBase, newBase)) {
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
        SegmentWriter writer = store.getWriter();
        RecordId recordId = writer.writeStream(stream);
        writer.flush();
        return new SegmentBlob(reader, recordId);
    }

    @Override @Nonnull
    public synchronized String checkpoint(long lifetime) {
        checkArgument(lifetime > 0);
        // TODO: Guard the checkpoint from garbage collection
        return getHead().getRecordId().toString();
    }

    @Override @CheckForNull
    public synchronized NodeState retrieve(@Nonnull String checkpoint) {
        // TODO: Verify validity of the checkpoint
        RecordId id = RecordId.fromString(checkNotNull(checkpoint));
        return new SegmentNodeState(store, id).getChildNode(ROOT);
    }

}
