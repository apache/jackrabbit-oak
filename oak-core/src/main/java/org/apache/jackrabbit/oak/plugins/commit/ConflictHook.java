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
package org.apache.jackrabbit.oak.plugins.commit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandlers;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This commit hook implementation is responsible for resolving
 * conflicts. It does so by detecting the presence of conflict
 * markers added by the Microkernel and delegating to a
 * {@link org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler}
 * for resolving the conflicts.
 *
 * @see org.apache.jackrabbit.oak.spi.state.NodeStore#rebase(org.apache.jackrabbit.oak.spi.state.NodeBuilder)
 */
public class ConflictHook implements CommitHook {
    private final ThreeWayConflictHandler conflictHandler;

    /**
     * @deprecated Use {@link #of(ThreeWayConflictHandler)} instead.
     */
    @Deprecated
    public static final ConflictHook of(ConflictHandler handler) {
        return of(ConflictHandlers.wrap(handler));
    }

    public static final ConflictHook of(ThreeWayConflictHandler handler) {
        return new ConflictHook(handler);
    }

    /**
     * Create a new instance of the conflict hook using the
     * passed conflict handler for resolving conflicts.
     * @param conflictHandler  a conflict handler
     */
    public ConflictHook(ThreeWayConflictHandler conflictHandler) {
        this.conflictHandler = conflictHandler;
    }

    @Nonnull
    @Override
    public NodeState processCommit(
            NodeState before, NodeState after, CommitInfo info)
            throws CommitFailedException {
        return MergingNodeStateDiff.merge(before, after, conflictHandler);
    }
}
