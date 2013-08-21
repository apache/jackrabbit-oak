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
package org.apache.jackrabbit.oak.plugins.mongomk;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.mongomk.util.Utils;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <code>UnmergedBranches</code> contains all un-merged branches of a MongoMK
 * instance.
 */
class UnmergedBranches {

    /**
     * Map of branches with the head of the branch as key.
     */
    private final List<Branch> branches = new ArrayList<Branch>();

    /**
     * Set to <code>true</code> once initialized.
     */
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * The revision comparator.
     */
    private final Revision.RevisionComparator comparator;

    UnmergedBranches(@Nonnull Revision.RevisionComparator comparator) {
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
        SortedMap<Revision, Revision> revisions = doc.getUncommittedRevisions(context);
        while (!revisions.isEmpty()) {
            SortedSet<Revision> commits = new TreeSet<Revision>(comparator);
            Revision head = revisions.lastKey();
            commits.add(head);
            Revision base = revisions.remove(head);
            while (revisions.containsKey(base)) {
                commits.add(base);
                base = revisions.remove(base);
            }
            branches.add(new Branch(commits, base, comparator));
        }
    }

    /**
     * Create a branch with an initial commit revision.
     *
     * @param base the base revision of the branch.
     * @param initial the initial commit to the branch.
     * @return the branch.
     */
    @Nonnull
    Branch create(@Nonnull Revision base, @Nonnull Revision initial) {
        SortedSet<Revision> commits = new TreeSet<Revision>(comparator);
        commits.add(checkNotNull(initial));
        Branch b = new Branch(commits, checkNotNull(base), comparator);
        synchronized (branches) {
            branches.add(b);
        }
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
        synchronized (branches) {
            for (Branch b : branches) {
                if (b.containsCommit(r)) {
                    return b;
                }
            }
        }
        return null;
    }

    /**
     * Removes the given branch.
     * @param b the branch to remove.
     */
    void remove(Branch b) {
        synchronized (branches) {
            for (int i = 0; i < branches.size(); i++) {
                if (branches.get(i) == b) {
                    branches.remove(i);
                    return;
                }
            }
        }
    }
}
