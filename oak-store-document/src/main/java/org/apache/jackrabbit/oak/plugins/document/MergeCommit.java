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

import java.util.Set;
import java.util.SortedSet;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

/**
 * A merge commit containing multiple commit revisions. One for each branch
 * commit to merge.
 */
class MergeCommit extends Commit {

    private final SortedSet<Revision> mergeRevs;
    private final Set<Revision> branchCommits = Sets.newHashSet();

    MergeCommit(DocumentNodeStore nodeStore,
                RevisionVector baseRevision,
                SortedSet<Revision> revisions) {
        super(nodeStore, revisions.last(), baseRevision);
        this.mergeRevs = revisions;
    }

    SortedSet<Revision> getMergeRevisions() {
        return mergeRevs;
    }

    void addBranchCommits(@Nonnull Branch branch) {
        for (Revision r : branch.getCommits()) {
            if (!branch.getCommit(r).isRebase()) {
                branchCommits.add(r);
            }
        }
    }

    @Override
    public void applyToCache(RevisionVector before, boolean isBranchCommit) {
        // do nothing for a merge commit, only notify node
        // store about merged revisions
        nodeStore.revisionsMerged(branchCommits);
    }
}
