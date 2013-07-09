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
    private final TreeMap<Revision, UnsavedModifications> commits;

    private volatile Revision base;

    Branch(@Nonnull SortedSet<Revision> commits,
           @Nonnull Revision base,
           @Nonnull Revision.RevisionComparator comparator) {
        this.base = checkNotNull(base);
        this.commits = new TreeMap<Revision, UnsavedModifications>(
                checkNotNull(comparator));
        for (Revision r : commits) {
            this.commits.put(r, new UnsavedModifications());
        }
    }

    Revision getBase() {
        return base;
    }

    public void setBase(Revision base) {
        this.base = base;
    }
    synchronized void addCommit(@Nonnull Revision r) {
        checkArgument(commits.comparator().compare(r, commits.lastKey()) > 0);
        commits.put(r, new UnsavedModifications());
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
        UnsavedModifications modifications = commits.get(r);
        if (modifications == null) {
            throw new IllegalArgumentException(
                    "Revision " + r + " is not a commit in this branch");
        }
        return modifications;
    }

    /**
     * Applies all unsaved modification of this branch to the given collection
     * of unsaved trunk modifications. A modification is only applied if there
     * is no modification in <code>trunk</code> for a given path or if the
     * <code>trunk</code> modification is earlier.
     *
     * @param trunk the unsaved trunk modifications.
     */
    public synchronized void applyTo(@Nonnull UnsavedModifications trunk) {
        checkNotNull(trunk);
        for (UnsavedModifications modifications : commits.values()) {
            modifications.applyTo(trunk);
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
            UnsavedModifications modifications = commits.get(r);
            Revision modRevision = modifications.get(path);
            if (modRevision != null) {
                return modRevision;
            }
        }
        return null;
    }
}
