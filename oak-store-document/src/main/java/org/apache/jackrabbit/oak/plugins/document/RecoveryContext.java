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

import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A revision context that represents a cluster node with an expired lease for
 * which recovery is performed.
 */
final class RecoveryContext implements RevisionContext {

    private final NodeDocument root;
    private final Clock clock;
    private final int clusterId;
    private final CommitValueResolver resolver;

    /**
     * A new recovery context.
     *
     * @param root the current root document.
     * @param clock the clock.
     * @param clusterId the clusterId for which to run recovery.
     * @param resolver a commit resolver.
     */
    RecoveryContext(NodeDocument root,
                    Clock clock,
                    int clusterId,
                    CommitValueResolver resolver) {
        this.root = root;
        this.clock = clock;
        this.clusterId = clusterId;
        this.resolver = resolver;
    }

    @Override
    public UnmergedBranches getBranches() {
        // an expired cluster node does not have active unmerged branches
        return new UnmergedBranches();
    }

    @Override
    public UnsavedModifications getPendingModifications() {
        // an expired cluster node does not have
        // pending in-memory _lastRev updates
        return new UnsavedModifications();
    }

    @Override
    public int getClusterId() {
        return clusterId;
    }

    @NotNull
    @Override
    public RevisionVector getHeadRevision() {
        return new RevisionVector(root.getLastRev().values());
    }

    @NotNull
    @Override
    public Revision newRevision() {
        return Revision.newRevision(clusterId);
    }

    @NotNull
    @Override
    public Clock getClock() {
        return clock;
    }

    @Nullable
    @Override
    public String getCommitValue(@NotNull Revision changeRevision,
                                 @NotNull NodeDocument doc) {
        return resolver.resolve(changeRevision, doc);
    }
}
