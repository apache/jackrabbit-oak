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

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.guava.common.collect.Iterables.filter;
import static org.apache.jackrabbit.guava.common.collect.Iterables.transform;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;

import org.apache.jackrabbit.guava.common.collect.Iterables;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    private final RevisionVector base;

    /**
     * The branch reference.
     */
    private final BranchReference ref;

    /**
     * Create a new branch instance with an initial set of commits and a given
     * base revision. The life time of this branch can be controlled with
     * the {@code guard} parameter. Once the {@code guard} object becomes weakly
     * reachable, the {@link BranchReference} for this branch is appended to
     * the passed {@code queue}. No {@link BranchReference} is appended if the
     * passed {@code guard} is {@code null}.
     *
     * @param commits the initial branch commits.
     * @param base the base commit.
     * @param queue a {@link BranchReference} to this branch will be appended to
     *              this queue when {@code guard} becomes weakly reachable.
     * @param guard controls the life time of this branch.
     * @throws IllegalArgumentException if base is a branch revision.
     */
    Branch(@NotNull SortedSet<Revision> commits,
           @NotNull RevisionVector base,
           @NotNull ReferenceQueue<Object> queue,
           @Nullable Object guard) {
        checkArgument(!requireNonNull(base).isBranch(), "base is not a trunk revision: %s", base);
        this.base = base;
        this.commits = new ConcurrentSkipListMap<Revision, BranchCommit>(commits.comparator());
        for (Revision r : commits) {
            this.commits.put(r.asBranchRevision(),
                    new BranchCommitImpl(base, r.asBranchRevision()));
        }
        if (guard != null) {
            this.ref = new BranchReference(queue, this, guard);
        } else {
            this.ref = null;
        }
    }

    /**
     * @return the initial base of this branch. This is a trunk revision.
     */
    @NotNull
    RevisionVector getBase() {
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
    @NotNull
    RevisionVector getBase(@NotNull Revision r) {
        BranchCommit c = commits.get(requireNonNull(r).asBranchRevision());
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
    void rebase(@NotNull Revision head, @NotNull RevisionVector base) {
        checkArgument(requireNonNull(head).isBranch(), "Not a branch revision: %s", head);
        checkArgument(!requireNonNull(base).isBranch(), "Not a trunk revision: %s", base);
        Revision last = commits.lastKey();
        checkArgument(head.compareRevisionTime(last) > 0);
        commits.put(head, new RebaseCommit(base, head, commits));
    }

    /**
     * Adds a new commit with revision <code>r</code> to this branch.
     *
     * @param r the revision of the branch commit to add.
     * @throws IllegalArgumentException if r is not a branch revision.
     */
    void addCommit(@NotNull Revision r) {
        checkArgument(requireNonNull(r).isBranch(), "Not a branch revision: %s", r);
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
    boolean containsCommit(@NotNull Revision r) {
        return commits.containsKey(requireNonNull(r).asBranchRevision());
    }

    /**
     * Returns the branch commit with the given or {@code null} if it does not
     * exist.
     *
     * @param r the revision of a commit.
     * @return the branch commit or {@code null} if it doesn't exist.
     */
    @Nullable
    BranchCommit getCommit(@NotNull Revision r) {
        return commits.get(requireNonNull(r).asBranchRevision());
    }

    /**
     * @return the branch reference or {@code null} if no guard object was
     *         passed to the constructor of this branch. 
     */
    @Nullable
    BranchReference getRef() {
        return ref;
    }

    /**
     * Removes the commit with the given revision <code>r</code>. Does nothing
     * if there is no such commit.
     *
     * @param r the revision of the commit to remove.
     * @throws IllegalArgumentException if r is not a branch revision.
     */
    public void removeCommit(@NotNull Revision r) {
        checkArgument(requireNonNull(r).isBranch(), "Not a branch revision: %s", r);
        commits.remove(r);
    }

    /**
     * Applies all unsaved modification of this branch to the given collection
     * of unsaved trunk modifications with the given merge commit revision.
     *
     * @param trunk the unsaved trunk modifications.
     * @param mergeCommit the revision of the merge commit.
     */
    public void applyTo(@NotNull UnsavedModifications trunk,
                        @NotNull Revision mergeCommit) {
        requireNonNull(trunk);
        for (BranchCommit c : commits.values()) {
            c.applyTo(trunk, mergeCommit);
        }
    }

    /**
     * Gets the most recent unsaved last revision at <code>readRevision</code>
     * or earlier in this branch for the given <code>path</code>. Documents with
     * explicit updates are not tracked and this method may return {@code null}.
     *
     * @param path         the path of a node.
     * @param readRevision the read revision.
     * @return the most recent unsaved last revision or <code>null</code> if
     *         there is none in this branch.
     */
    @Nullable
    public Revision getUnsavedLastRevision(Path path,
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
    public boolean isHead(@NotNull Revision rev) {
        checkArgument(requireNonNull(rev).isBranch(),
                "Not a branch revision: %s", rev);
        return requireNonNull(rev).equals(commits.lastKey());
    }

    /**
     * Returns the modified paths since the base revision of this branch until
     * the given branch revision {@code r} (inclusive).
     *
     * @param r a commit on this branch.
     * @return modified paths until {@code r}.
     * @throws IllegalArgumentException if r is not a branch revision.
     */
    Iterable<Path> getModifiedPathsUntil(@NotNull final Revision r) {
        checkArgument(requireNonNull(r).isBranch(),
                "Not a branch revision: %s", r);
        if (!commits.containsKey(r)) {
            return Collections.emptyList();
        }
        Iterable<Iterable<Path>> paths = transform(filter(commits.entrySet(),
                new Predicate<Map.Entry<Revision, BranchCommit>>() {
            @Override
            public boolean test(Map.Entry<Revision, BranchCommit> input) {
                return !input.getValue().isRebase()
                        && input.getKey().compareRevisionTime(r) <= 0;
            }
        }::test), input -> input.getValue().getModifiedPaths());
        return Iterables.concat(paths);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        base.toStringBuilder(sb).append("[");
        String separator = "";
        for (Map.Entry<Revision, BranchCommit> c : commits.entrySet()) {
            sb.append(separator);
            separator = ",";
            sb.append(c.getKey()).append("->").append(c.getValue());
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Information about a commit within a branch.
     */
    abstract static class BranchCommit implements LastRevTracker {

        protected final RevisionVector base;
        protected final Revision commit;

        BranchCommit(RevisionVector base, Revision commit) {
            this.base = base;
            this.commit = commit;
        }

        /**
         * @return the branch base for this branch commit.
         */
        RevisionVector getBase() {
            return base;
        }

        abstract void applyTo(UnsavedModifications trunk, Revision commit);

        abstract boolean isModified(Path path);

        abstract Iterable<Path> getModifiedPaths();

        protected abstract boolean isRebase();
    }

    /**
     * Implements a regular branch commit.
     */
    private static class BranchCommitImpl extends BranchCommit {

        private final Set<Path> modifications = new HashSet<>();

        BranchCommitImpl(RevisionVector base, Revision commit) {
            super(base, commit);
        }

        @Override
        void applyTo(UnsavedModifications trunk, Revision commit) {
            for (Path p : modifications) {
                trunk.put(p, commit);
            }
        }

        @Override
        boolean isModified(Path path) { // TODO: rather pass NodeDocument?
            return modifications.contains(path);
        }

        @Override
        Iterable<Path> getModifiedPaths() {
            return modifications;
        }

        @Override
        protected boolean isRebase() {
            return false;
        }

        //------------------< LastRevTracker >----------------------------------

        @Override
        public void track(Path path) {
            modifications.add(path);
        }

        @Override
        public String toString() {
            return "B (" + modifications.size() + ")";
        }
    }

    private static class RebaseCommit extends BranchCommit {

        private final NavigableMap<Revision, BranchCommit> previous;

        RebaseCommit(RevisionVector base, Revision commit,
                     NavigableMap<Revision, BranchCommit> previous) {
            super(base, commit);
            this.previous = squash(previous);
        }

        @Override
        void applyTo(UnsavedModifications trunk, Revision commit) {
            for (BranchCommit c : previous.values()) {
                c.applyTo(trunk, commit);
            }
        }

        @Override
        boolean isModified(Path path) {
            for (BranchCommit c : previous.values()) {
                if (c.isModified(path)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected boolean isRebase() {
            return true;
        }

        @Override
        Iterable<Path> getModifiedPaths() {
            Iterable<Iterable<Path>> paths = transform(previous.values(),
                    branchCommit -> branchCommit.getModifiedPaths());
            return Iterables.concat(paths);
        }

        /**
         * Filter out the RebaseCommits as they are just container of previous BranchCommit
         *
         * @param previous branch commit history
         * @return filtered branch history only containing non rebase commits
         */
        private static NavigableMap<Revision, BranchCommit> squash(NavigableMap<Revision, BranchCommit> previous) {
            NavigableMap<Revision, BranchCommit> result = new TreeMap<Revision, BranchCommit>(previous.comparator());
            for (Map.Entry<Revision, BranchCommit> e : previous.entrySet()){
                if (!e.getValue().isRebase()){
                    result.put(e.getKey(), e.getValue());
                }
            }
            return result;
        }

        //------------------< LastRevTracker >----------------------------------

        @Override
        public void track(Path path) {
            throw new UnsupportedOperationException("RebaseCommit is read-only");
        }

        @Override
        public String toString() {
            return "R (" + previous.size() + ")";
        }
    }

    final static class BranchReference extends WeakReference<Object> {

        private final Branch branch;

        private BranchReference(@NotNull ReferenceQueue<Object> queue,
                                @NotNull Branch branch,
                                @NotNull Object referent) {
            super(requireNonNull(referent), queue);
            this.branch = requireNonNull(branch);
        }

        Branch getBranch() {
            return branch;
        }
    }
}
