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

package org.apache.jackrabbit.oak.segment.scheduler;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@code Commit} instance represents a set of related changes, which when
 * applied to a base node state result in a new node state.
 */
public class Commit {
    static final String ROOT = "root";

    private final SegmentNodeBuilder changes;
    private final CommitHook hook;
    private final CommitInfo info;

    private volatile GCGeneration gcGeneration;

    public Commit(@NotNull NodeBuilder changes, @NotNull CommitHook hook, @NotNull CommitInfo info) {
        requireNonNull(changes);
        checkArgument(changes instanceof SegmentNodeBuilder);
        this.changes = (SegmentNodeBuilder) changes;

        this.hook = requireNonNull(hook);
        this.info = requireNonNull(info);
    }

    /**
     * This method makes a best effort on getting the gc generation of the current commit.
     * However it avoids causing a write ahead action by calling {@link NodeBuilder#getName(String)}
     * on the changes in this commit.
     *
     * @return  the gc generation of this commit or {@code null} if not yet available.
     */
    @Nullable
    public GCGeneration getGCGeneration() {
        return gcGeneration;
    }

    @NotNull
    private NodeState getBeforeState() {
        return changes.getBaseState();
    }

    @NotNull
    private SegmentNodeState getAfterState() {
        SegmentNodeState after = changes.getNodeState();
        gcGeneration = after.getGcGeneration();
        return after;
    }

    /**
     * Apply the changes represented by this commit to the passed {@code base}
     * node state.
     *
     * @param base
     *            the base node state to apply this commit to
     * @return the resulting state from applying this commit to {@code base}.
     * @throws CommitFailedException
     *             if the commit cannot be applied to {@code base}. (e.g.
     *             because of a conflict.)
     */
    public SegmentNodeState apply(SegmentNodeState base) throws CommitFailedException {
        SegmentNodeBuilder builder = base.builder();
        if (SegmentNodeState.fastEquals(getBeforeState(), base.getChildNode(ROOT))) {
            // use a shortcut when there are no external changes
            NodeState before = getBeforeState();
            NodeState after = getAfterState();

            builder.setChildNode(ROOT, hook.processCommit(before, after, info));
        } else {
            // there were some external changes, so do the full rebase
            ConflictAnnotatingRebaseDiff diff = new ConflictAnnotatingRebaseDiff(builder.child(ROOT));
            getAfterState().compareAgainstBaseState(getBeforeState(), diff);
            // apply commit hooks on the rebased changes
            builder.setChildNode(ROOT, hook.processCommit(builder.getBaseState().getChildNode(ROOT),
                                                          builder.getNodeState().getChildNode(ROOT), info));
        }
        return builder.getNodeState();
    }

    /**
     * Does housekeeping work needed after applying the commit.
     * @param merged
     *            the current head node state, after applying the changes in the commit.
     */
    public void applied(SegmentNodeState merged) {
        changes.reset(merged);
    }

    /**
     * Checks if the commit contains any changes. This is a shallow check, with
     * the same semantics as
     * {@link SegmentNodeState#fastEquals(NodeState, NodeState)}, which cannot
     * guarantee against false negatives.
     *
     * @return {@code true}, if the commit has changes.
     */
    public boolean hasChanges() {
        return !SegmentNodeState.fastEquals(getBeforeState(), getAfterState());
    }

    public CommitInfo info() {
        return info;
    }
}
