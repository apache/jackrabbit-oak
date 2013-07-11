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

import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Contains commit information about a branch and its base revision.
 * TODO document
 */
class Branch {

    /**
     * The commits to the branch
     */
    private final TreeMap<Revision, Commit> commits;

    private final Revision base;

    Branch(@Nonnull SortedSet<Revision> commits,
           @Nonnull Revision base,
           @Nonnull Revision.RevisionComparator comparator) {
        this.base = checkNotNull(base);
        this.commits = new TreeMap<Revision, Commit>(
                checkNotNull(comparator));
        for (Revision r : commits) {
            this.commits.put(r, new Commit(base));
        }
    }

    /**
     * @return the initial base of this branch.
     */
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
    synchronized Revision getBase(Revision r) {
        Commit c = commits.get(r);
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
     */
    synchronized void rebase(Revision head, Revision base) {
        Revision last = commits.lastKey();
        checkArgument(commits.comparator().compare(head, last) > 0);
        commits.put(head, new Commit(base));
    }

    synchronized void addCommit(@Nonnull Revision r) {
        Revision last = commits.lastKey();
        checkArgument(commits.comparator().compare(r, last) > 0);
        commits.put(r, new Commit(commits.get(last).getBase()));
    }

    synchronized SortedSet<Revision> getCommits() {
        SortedSet<Revision> revisions = new TreeSet<Revision>(commits.comparator());
        revisions.addAll(commits.keySet());
        return revisions;
    }

    synchronized boolean hasCommits() {
        return !commits.isEmpty();
    }

    synchronized boolean containsCommit(@Nonnull Revision r) {
        return commits.containsKey(r);
    }

    public synchronized void removeCommit(@Nonnull Revision rev) {
        commits.remove(rev);
    }

    /**
     * Gets the unsaved modifications for the given branch commit revision.
     *
     * @param r a branch commit revision.
     * @return the unsaved modification for the given branch commit.
     * @throws IllegalArgumentException if there is no commit with the given
     *                                  revision.
     */
    @Nonnull
    public synchronized UnsavedModifications getModifications(@Nonnull Revision r) {
        Commit c = commits.get(r);
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
    public synchronized void applyTo(@Nonnull UnsavedModifications trunk,
                                     @Nonnull Revision mergeCommit) {
        checkNotNull(trunk);
        for (Commit c : commits.values()) {
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
    public synchronized Revision getUnsavedLastRevision(String path,
                                                        Revision readRevision) {
        for (Revision r : commits.descendingKeySet()) {
            if (readRevision.compareRevisionTime(r) < 0) {
                continue;
            }
            Commit c = commits.get(r);
            Revision modRevision = c.getModifications().get(path);
            if (modRevision != null) {
                return modRevision;
            }
        }
        return null;
    }

    private static final class Commit {

        private final UnsavedModifications modifications = new UnsavedModifications();
        private final Revision base;

        Commit(Revision base) {
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
