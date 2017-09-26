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

import java.lang.ref.ReferenceQueue;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.plugins.document.Branch.BranchCommit;
import org.apache.jackrabbit.oak.plugins.document.Branch.BranchReference;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <code>UnmergedBranches</code> contains all un-merged branches of a DocumentMK
 * instance.
 */
class UnmergedBranches {

    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * List of active branches.
     */
    private final List<Branch> branches = new CopyOnWriteArrayList<Branch>();

    /**
     * Queue for {@link BranchReference} when the
     * {@link DocumentNodeStoreBranch} associated with a {@link Branch} is only
     * weakly reachable.
     */
    private final ReferenceQueue<Object> queue = 
            new ReferenceQueue<Object>();

    /**
     * Set to <code>true</code> once initialized.
     */
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * Initialize with un-merged branches from <code>store</code> for this
     * <code>clusterId</code>.
     *
     * @param store the document store.
     * @param context the revision context.
     */
    void init(DocumentStore store, RevisionContext context) {
        if (!initialized.compareAndSet(false, true)) {
            throw new IllegalStateException("already initialized");
        }
        NodeDocument doc = store.find(Collection.NODES, Utils.getIdFromPath("/"));
        if (doc == null) {
            return;
        }
        int purgeCount = doc.purgeUncommittedRevisions(context);
        if (purgeCount > 0) {
            log.info("Purged [{}] uncommitted branch revision entries", purgeCount);
        }
        purgeCount = doc.purgeCollisionMarkers(context);
        if (purgeCount > 0) {
            log.info("Purged [{}] collision markers", purgeCount);
        }
    }

    /**
     * Create a branch with an initial commit revision.
     *
     * @param base the base revision of the branch (must be a non-branch revision).
     * @param initial the initial commit to the branch (must be a branch revision).
     * @param guard an optional guard object controlling the life time of this
     *              branch. When {@code guard} becomes weakly reachable, the
     *              branch will at some point later be considered orphaned.
     *              Orphaned branches can be retrieved with
     *              {@link #pollOrphanedBranch()}.
     * @return the branch.
     * @throws IllegalArgumentException if {@code base} is a branch revision
     *          or {@code initial} is not a branch revision.
     */
    @Nonnull
    Branch create(@Nonnull RevisionVector base,
                  @Nonnull Revision initial,
                  @Nullable Object guard) {
        checkArgument(!checkNotNull(base).isBranch(),
                "base is not a trunk revision: %s", base);
        checkArgument(checkNotNull(initial).isBranch(),
                "initial is not a branch revision: %s", initial);
        SortedSet<Revision> commits = new TreeSet<Revision>(StableRevisionComparator.INSTANCE);
        commits.add(initial);
        Branch b = new Branch(commits, base, queue, guard);
        branches.add(b);
        return b;
    }

    /**
     * Returns the branch, which contains the given revision or <code>null</code>
     * if there is no such branch.
     *
     * @param r a revision.
     * @return the branch containing the given revision or <code>null</code>.
     */
    @CheckForNull
    Branch getBranch(@Nonnull RevisionVector r) {
        if (!r.isBranch()) {
            return null;
        }
        Revision branchRev = r.getBranchRevision();
        for (Branch b : branches) {
            if (b.containsCommit(branchRev)) {
                return b;
            }
        }
        return null;
    }

    /**
     * Returns {@code true} if the given revision is the base of an unmerged
     * branch.
     *
     * @param r the base revision of a branch.
     * @return {@code true} if such a branch exists, {@code false} otherwise.
     */
    boolean isBranchBase(@Nonnull RevisionVector r) {
        if (!r.isBranch()) {
            return false;
        }
        RevisionVector base = r.asTrunkRevision();
        for (Branch b : branches) {
            if (b.getBase().equals(base)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the branch commit with the given revision or {@code null} if
     * it doesn't exists.
     *
     * @param r a revision.
     * @return the branch commit or {@code null} if it doesn't exist.
     */
    @CheckForNull
    BranchCommit getBranchCommit(@Nonnull Revision r) {
        for (Branch b : branches) {
            BranchCommit c = b.getCommit(r);
            if (c != null) {
                return c;
            }
        }
        return null;
    }

    /**
     * Removes the given branch.
     * @param b the branch to remove.
     */
    void remove(Branch b) {
        branches.remove(b);
    }

    /**
     * Count of currently unmerged branches
     */
    int size(){
        return branches.size();
    }

    /**
     * Polls for an orphaned branch. The implementation will remove the branch
     * from the internal list of unmerged branches.
     *
     * @return an orphaned branch or {@code null} if there is none at this time.
     */
    Branch pollOrphanedBranch() {
        BranchReference ref = (BranchReference) queue.poll();
        if (ref != null) {
            if (branches.remove(ref.getBranch())) {
                return ref.getBranch();
            }
        }
        return null;
    }
}
