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

import java.io.IOException;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.sort.StringSort;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.JournalEntry.asId;
import static org.apache.jackrabbit.oak.plugins.document.JournalEntry.fillExternalChanges;

/**
 * A DiffCache loader reading from journal entries.
 */
class JournalDiffLoader implements DiffCache.Loader {

    private static final Logger LOG = LoggerFactory.getLogger(JournalDiffLoader.class);

    private final AbstractDocumentNodeState base;

    private final AbstractDocumentNodeState node;

    private final DocumentNodeStore ns;

    JournalDiffLoader(@Nonnull AbstractDocumentNodeState base,
                      @Nonnull AbstractDocumentNodeState node,
                      @Nonnull DocumentNodeStore ns) {
        this.base = base;
        this.node = node;
        this.ns = ns;
    }

    @Override
    public String call() {
        RevisionVector afterRev = node.getRootRevision();
        RevisionVector beforeRev = base.getRootRevision();

        JournalEntry localPending = ns.getCurrentJournalEntry();
        DocumentStore store = ns.getDocumentStore();
        NodeDocument root = Utils.getRootDocument(store);
        Map<Integer, Revision> lastRevs = root.getLastRev();
        int clusterId = ns.getClusterId();
        Revision localLastRev = lastRevs.get(clusterId);
        if (localLastRev == null) {
            throw new IllegalStateException("Root document does not have a " +
                    "lastRev entry for local clusterId " + clusterId);
        }

        StringSort changes = JournalEntry.newSorter();
        try {
            readTrunkChanges(beforeRev, afterRev, localPending, localLastRev, changes);

            readBranchChanges(beforeRev, changes);
            readBranchChanges(afterRev, changes);

            changes.sort();
            DiffCache df = ns.getDiffCache();
            WrappedDiffCache wrappedCache = new WrappedDiffCache(node.getPath(), df);
            JournalEntry.applyTo(changes, wrappedCache, beforeRev, afterRev);

            return wrappedCache.changes;
        } catch (IOException e) {
            throw DocumentStoreException.convert(e);
        } finally {
            Utils.closeIfCloseable(changes);
        }
    }

    private void readBranchChanges(RevisionVector rv,
                                   StringSort changes) throws IOException {
        if (!rv.isBranch() || ns.isDisableBranches()) {
            return;
        }
        Branch b = ns.getBranches().getBranch(rv);
        if (b == null) {
            if (!ns.getBranches().isBranchBase(rv)) {
                missingBranch(rv);
            }
            return;
        }
        DocumentStore store = ns.getDocumentStore();
        for (Revision br : b.getCommits()) {
            Branch.BranchCommit bc = b.getCommit(br);
            if (!bc.isRebase()) {
                JournalEntry entry = store.find(Collection.JOURNAL, asId(br));
                if (entry != null) {
                    entry.addTo(changes);
                } else {
                    LOG.warn("Missing journal entry for {}", asId(br));
                }
            }
        }
    }

    private void readTrunkChanges(RevisionVector beforeRev,
                                  RevisionVector afterRev,
                                  JournalEntry localPending,
                                  Revision localLastRev,
                                  StringSort changes) throws IOException {
        if (ns.isDisableBranches()) {
            beforeRev = beforeRev.asTrunkRevision();
            afterRev = afterRev.asTrunkRevision();
        } else {
            beforeRev = getBaseRevision(beforeRev);
            afterRev = getBaseRevision(afterRev);
        }
        if (beforeRev.equals(afterRev)) {
            return;
        }

        int clusterId = ns.getClusterId();
        RevisionVector max = beforeRev.pmax(afterRev);
        RevisionVector min = beforeRev.pmin(afterRev);
        for (Revision to : max) {
            Revision from = min.getRevision(to.getClusterId());
            if (from == null) {
                // there is no min revision with this clusterId
                // use revision with a timestamp of zero
                from = new Revision(0, 0, to.getClusterId());
            }
            fillExternalChanges(changes, from, to, ns.getDocumentStore());
        }
        // do we need to include changes from pending local changes?
        if (!max.isRevisionNewer(localLastRev)
                && !localLastRev.equals(max.getRevision(clusterId))) {
            // journal does not contain all local changes
            localPending.addTo(changes);
        }
    }

    @Nonnull
    private RevisionVector getBaseRevision(RevisionVector rv) {
        if (!rv.isBranch()) {
            return rv;
        }
        Branch b = ns.getBranches().getBranch(rv);
        if (b != null) {
            rv = b.getBase(rv.getBranchRevision());
        } else if (ns.getBranches().isBranchBase(rv)) {
            rv = rv.asTrunkRevision();
        } else {
            missingBranch(rv);
        }
        return rv;
    }

    private static void missingBranch(RevisionVector rv) {
        throw new IllegalStateException("Missing branch for revision " + rv);
    }

    private static class WrappedDiffCache extends DiffCache {

        private final String path;
        private String changes = "";
        private final DiffCache cache;

        WrappedDiffCache(String path, DiffCache cache) {
            this.path = path;
            this.cache = cache;
        }

        @CheckForNull
        String getChanges() {
            return changes;
        }

        @Override
        String getChanges(@Nonnull RevisionVector from,
                          @Nonnull RevisionVector to,
                          @Nonnull String path,
                          @Nullable Loader loader) {
            return cache.getChanges(from, to, path, loader);
        }

        @Nonnull
        @Override
        Entry newEntry(@Nonnull RevisionVector from,
                       @Nonnull RevisionVector to,
                       boolean local) {
            final Entry entry = cache.newEntry(from, to, local);
            return new Entry() {
                @Override
                public void append(@Nonnull String path,
                                   @Nonnull String changes) {
                    entry.append(path, changes);
                    if (path.equals(WrappedDiffCache.this.path)) {
                        WrappedDiffCache.this.changes = changes;
                    }
                }

                @Override
                public boolean done() {
                    return entry.done();
                }
            };
        }

        @Nonnull
        @Override
        Iterable<CacheStats> getStats() {
            return cache.getStats();
        }
    }
}
