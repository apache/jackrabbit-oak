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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * Contains commit information about a branch and its base revision.
 */
class Branch {

    /**
     * The commits to the branch
     */
    private final ConcurrentSkipListMap<Revision, BranchCommit> commits;

    /**
     * The initial base revision of this branch.
     */
    private final Revision base;

    /**
     * Create a new branch instance with an initial set of commits and a given
     * base revision.
     *
     * @param commits the initial branch commits.
     * @param base the base commit.
     * @throws IllegalArgumentException if base is a branch revision.
     */
    Branch(@Nonnull SortedSet<Revision> commits,
           @Nonnull Revision base) {
        checkArgument(!checkNotNull(base).isBranch(), "base is not a trunk revision: %s", base);
        this.base = base;
        this.commits = new ConcurrentSkipListMap<Revision, BranchCommit>(commits.comparator());
        for (Revision r : commits) {
            this.commits.put(r.asBranchRevision(), new BranchCommit(base));
        }
    }

    /**
     * @return the initial base of this branch.
     */
    @Nonnull
    Revision getBase() {
        return base;
    }

    /**
     * Returns the base revision for the given branch revision <code>r</code>.
     *
     * @param r revision of a commit in this branch.
     * @return the base revision for <code>r</code>.
     * @throws IllegalArgumentException if <code>r</code> is not a commit of
     *                                  this branch.
     */
    @Nonnull
    Revision getBase(@Nonnull Revision r) {
        BranchCommit c = commits.get(checkNotNull(r).asBranchRevision());
        if (c == null) {
            throw new IllegalArgumentException(
                    "Revision " + r + " is not a commit in this branch");
        }
        return c.getBase();
    }

    /**
     * Rebases the last commit of this branch to the given revision.
     *
     * @param head the new head of the branch.
     * @param base rebase to this revision.
     * @throws IllegalArgumentException if head is a trunk revision or base is a
     *                                  branch revision.
     */
    void rebase(@Nonnull Revision head, @Nonnull Revision base) {
        checkArgument(checkNotNull(head).isBranch(), "Not a branch revision: %s", head);
        checkArgument(!checkNotNull(base).isBranch(), "Not a trunk revision: %s", base);
        Revision last = commits.lastKey();
        checkArgument(commits.comparator().compare(head, last) > 0);
        BranchCommit bc = new BranchCommit(base);
        // set all previously touched paths as modified
        for (BranchCommit c : commits.values()) {
            c.getModifications().applyTo(bc.getModifications(), head);
        }
        commits.put(head, bc);
    }

    /**
     * Adds a new commit with revision <code>r</code> to this branch.
     *
     * @param r the revision of the branch commit to add.
     * @throws IllegalArgumentException if r is not a branch revision.
     */
    void addCommit(@Nonnull Revision r) {
        checkArgument(checkNotNull(r).isBranch(), "Not a branch revision: %s", r);
        Revision last = commits.lastKey();
        checkArgument(commits.comparator().compare(r, last) > 0);
        commits.put(r, new BranchCommit(commits.get(last).getBase()));
    }

    /**
     * @return the commits to this branch.
     */
    SortedSet<Revision> getCommits() {
        return commits.keySet();
    }

    /**
     * @return <code>true</code> if this branch contains any commits;
     *         <code>false</code> otherwise.
     */
    boolean hasCommits() {
        return !commits.isEmpty();
    }

    /**
     * Checks if this branch contains a commit with the given revision.
     *
     * @param r the revision of a commit.
     * @return <code>true</code> if this branch contains a commit with the given
     *         revision; <code>false</code> otherwise.
     */
    boolean containsCommit(@Nonnull Revision r) {
        return commits.containsKey(checkNotNull(r).asBranchRevision());
    }

    /**
     * Removes the commit with the given revision <code>r</code>. Does nothing
     * if there is no such commit.
     *
     * @param r the revision of the commit to remove.
     * @throws IllegalArgumentException if r is not a branch revision.
     */
    public void removeCommit(@Nonnull Revision r) {
        checkArgument(checkNotNull(r).isBranch(), "Not a branch revision: %s", r);
        commits.remove(r);
    }

    /**
     * Gets the unsaved modifications for the given branch commit revision.
     *
     * @param r a branch commit revision.
     * @return the unsaved modification for the given branch commit.
     * @throws IllegalArgumentException r is not a branch revision or if there
     *                                  is no commit with the given revision.
     */
    @Nonnull
    public UnsavedModifications getModifications(@Nonnull Revision r) {
        checkArgument(checkNotNull(r).isBranch(), "Not a branch revision: %s", r);
        BranchCommit c = commits.get(r);
        if (c == null) {
            throw new IllegalArgumentException(
                    "Revision " + r + " is not a commit in this branch");
        }
        return c.getModifications();
    }

    /**
     * Applies all unsaved modification of this branch to the given collection
     * of unsaved trunk modifications with the given merge commit revision.
     *
     * @param trunk the unsaved trunk modifications.
     * @param mergeCommit the revision of the merge commit.
     */
    public void applyTo(@Nonnull UnsavedModifications trunk,
                                     @Nonnull Revision mergeCommit) {
        checkNotNull(trunk);
        for (BranchCommit c : commits.values()) {
            c.getModifications().applyTo(trunk, mergeCommit);
        }
    }

    /**
     * Gets the most recent unsaved last revision at <code>readRevision</code>
     * or earlier in this branch for the given <code>path</code>.
     *
     * @param path         the path of a node.
     * @param readRevision the read revision.
     * @return the most recent unsaved last revision or <code>null</code> if
     *         there is none in this branch.
     */
    @CheckForNull
    public Revision getUnsavedLastRevision(String path,
                                           Revision readRevision) {
        readRevision = readRevision.asBranchRevision();
        for (Revision r : commits.descendingKeySet()) {
            if (readRevision.compareRevisionTime(r) < 0) {
                continue;
            }
            BranchCommit c = commits.get(r);
            Revision modRevision = c.getModifications().get(path);
            if (modRevision != null) {
                return modRevision;
            }
        }
        return null;
    }

    /**
     * Information about a commit within a branch.
     */
    private static final class BranchCommit {

        private final UnsavedModifications modifications = new UnsavedModifications();
        private final Revision base;

        BranchCommit(Revision base) {
            this.base = base;
        }

        Revision getBase() {
            return base;
        }

        UnsavedModifications getModifications() {
            return modifications;
        }
    }
}
