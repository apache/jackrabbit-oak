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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;

/**
 * Helper class to track when a node was last modified.
 */
final class LastRevs implements Iterable<Revision> {

    private final Map<Integer, Revision> revs;

    private final RevisionVector readRevision;

    private final Branch branch;

    private Revision branchRev;

    LastRevs(RevisionVector readRevision) {
        this(Collections.<Integer, Revision>emptyMap(), readRevision, null);
    }

    LastRevs(Map<Integer, Revision> revs,
             RevisionVector readRevision,
             Branch branch) {
        this.revs = new HashMap<Integer, Revision>(revs);
        this.readRevision = readRevision;
        this.branch = branch;
    }

    void update(@Nullable Revision rev) {
        if (rev == null) {
            return;
        }
        Revision r = revs.get(rev.getClusterId());
        if (r == null || rev.compareRevisionTime(r) > 0) {
            revs.put(rev.getClusterId(), rev);
        }
    }

    void updateBranch(@Nullable Revision rev) {
        if (rev == null) {
            return;
        }
        rev = rev.asBranchRevision();
        if (branch != null && branch.containsCommit(rev)
                && readRevision.getBranchRevision().compareRevisionTime(rev) >= 0) {
            branchRev = Utils.max(branchRev, rev);
        }
    }

    @CheckForNull
    Revision getBranchRevision() {
        return branchRev;
    }

    @Override
    public Iterator<Revision> iterator() {
        return revs.values().iterator();
    }
}
