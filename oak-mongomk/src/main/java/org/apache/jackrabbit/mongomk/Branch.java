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
package org.apache.jackrabbit.mongomk;

import java.util.SortedSet;
import java.util.TreeSet;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Contains commit information about a branch and its base revision.
 */
class Branch {

    /**
     * The commits to the branch
     */
    private final SortedSet<Revision> commits;

    private final Revision base;

    Branch(@Nonnull SortedSet<Revision> commits, @Nonnull Revision base) {
        this.commits = checkNotNull(commits);
        this.base = base;
    }

    synchronized Revision getHead() {
        return commits.last();
    }

    Revision getBase() {
        return base;
    }

    synchronized void addCommit(@Nonnull Revision r) {
        checkArgument(commits.comparator().compare(r, commits.last()) > 0);
        commits.add(r);
    }

    synchronized SortedSet<Revision> getCommits() {
        return new TreeSet<Revision>(commits);
    }

    synchronized boolean containsCommit(@Nonnull Revision r) {
        return commits.contains(r);
    }

    public synchronized void removeCommit(@Nonnull Revision rev) {
        commits.remove(rev);
    }
}
