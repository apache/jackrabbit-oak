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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
            this.commits.put(r.asBranchRevision(),
                    new BranchCommitImpl(base, r.asBranchRevision()));
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
        commits.put(head, new RebaseCommit(base, head, commits));
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
        commits.put(r, new BranchCommitImpl(commits.get(last).getBase(), r));
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
     * Returns the branch commit with the given or {@code null} if it does not
     * exist.
     *
     * @param r the revision of a commit.
     * @return the branch commit or {@code null} if it doesn't exist.
     */
    @CheckForNull
    BranchCommit getCommit(@Nonnull Revision r) {
        return commits.get(checkNotNull(r).asBranchRevision());
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
            c.applyTo(trunk, mergeCommit);
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
            if (c.isModified(path)) {
                return r;
            }
        }
        return null;
    }

    /**
     * @param rev the revision to check.
     * @return {@code true} if the given revision is the head of this branch,
     *          {@code false} otherwise.
     */
    public boolean isHead(@Nonnull Revision rev) {
        checkArgument(checkNotNull(rev).isBranch(),
                "Not a branch revision: %s", rev);
        return checkNotNull(rev).equals(commits.lastKey());
    }

    /**
     * Information about a commit within a branch.
     */
    abstract static class BranchCommit implements LastRevTracker {

        protected final Revision base;
        protected final Revision commit;

        BranchCommit(Revision base, Revision commit) {
            this.base = base;
            this.commit = commit;
        }

        Revision getBase() {
            return base;
        }

        abstract void applyTo(UnsavedModifications trunk, Revision commit);

        abstract boolean isModified(String path);

        abstract Iterable<String> getModifiedPaths();
    }

    /**
     * Implements a regular branch commit.
     */
    private static class BranchCommitImpl extends BranchCommit {

        private final Set<String> modifications = Sets.newHashSet();

        BranchCommitImpl(Revision base, Revision commit) {
            super(base, commit);
        }

        @Override
        void applyTo(UnsavedModifications trunk, Revision commit) {
            for (String p : modifications) {
                trunk.put(p, commit);
            }
        }

        @Override
        boolean isModified(String path) { // TODO: rather pass NodeDocument?
            return modifications.contains(path);
        }

        @Override
        Iterable<String> getModifiedPaths() {
            return modifications;
        }

        //------------------< LastRevTracker >----------------------------------

        @Override
        public void track(String path) {
            modifications.add(path);
        }
    }

    static class RebaseCommit extends BranchCommit {

        private final NavigableMap<Revision, BranchCommit> previous;

        RebaseCommit(Revision base, Revision commit,
                     NavigableMap<Revision, BranchCommit> previous) {
            super(base, commit);
            this.previous = Maps.newTreeMap(previous);
        }

        @Override
        void applyTo(UnsavedModifications trunk, Revision commit) {
            for (BranchCommit c : previous.values()) {
                c.applyTo(trunk, commit);
            }
        }

        @Override
        boolean isModified(String path) {
            for (BranchCommit c : previous.values()) {
                if (c.isModified(path)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        Iterable<String> getModifiedPaths() {
            Iterable<Iterable<String>> paths = transform(previous.values(),
                    new Function<BranchCommit, Iterable<String>>() {
                @Override
                public Iterable<String> apply(BranchCommit branchCommit) {
                    return branchCommit.getModifiedPaths();
                }
            });
            return Iterables.concat(paths);
        }

        //------------------< LastRevTracker >----------------------------------

        @Override
        public void track(String path) {
            throw new UnsupportedOperationException("RebaseCommit is read-only");
        }
    }
}
