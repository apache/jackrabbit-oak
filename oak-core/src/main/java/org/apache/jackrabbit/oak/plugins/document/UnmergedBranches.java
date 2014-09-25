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

import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.document.Branch.BranchCommit;
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
     * Map of branches with the head of the branch as key.
     */
    private final List<Branch> branches = new CopyOnWriteArrayList<Branch>();

    /**
     * Set to <code>true</code> once initialized.
     */
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * The revision comparator.
     */
    private final Comparator<Revision> comparator;

    UnmergedBranches(@Nonnull Comparator<Revision> comparator) {
        this.comparator = checkNotNull(comparator);
    }

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
    }

    /**
     * Create a branch with an initial commit revision.
     *
     * @param base the base revision of the branch.
     * @param initial the initial commit to the branch.
     * @return the branch.
     * @throws IllegalArgumentException if
     */
    @Nonnull
    Branch create(@Nonnull Revision base, @Nonnull Revision initial) {
        checkArgument(!checkNotNull(base).isBranch(),
                "base is not a trunk revision: %s", base);
        checkArgument(checkNotNull(initial).isBranch(),
                "initial is not a branch revision: %s", initial);
        SortedSet<Revision> commits = new TreeSet<Revision>(comparator);
        commits.add(initial);
        Branch b = new Branch(commits, base);
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
    Branch getBranch(@Nonnull Revision r) {
        for (Branch b : branches) {
            if (b.containsCommit(r)) {
                return b;
            }
        }
        return null;
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
        b.dispose();
    }
}
