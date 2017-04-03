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
import java.util.concurrent.TimeUnit;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.sort.StringSort;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
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

    private Stats stats;

    JournalDiffLoader(@Nonnull AbstractDocumentNodeState base,
                      @Nonnull AbstractDocumentNodeState node,
                      @Nonnull DocumentNodeStore ns) {
        this.base = checkNotNull(base);
        this.node = checkNotNull(node);
        this.ns = checkNotNull(ns);
        checkArgument(base.getPath().equals(node.getPath()),
                "nodes must have matching paths: {} != {}",
                base.getPath(), node.getPath());
    }

    @Override
    public String call() {
        String path = node.getPath();
        RevisionVector afterRev = node.getRootRevision();
        RevisionVector beforeRev = base.getRootRevision();
        stats = new Stats(path, beforeRev, afterRev);

        StringSort changes = JournalEntry.newSorter();
        try {
            readTrunkChanges(path, beforeRev, afterRev, changes);

            readBranchChanges(path, beforeRev, changes);
            readBranchChanges(path, afterRev, changes);

            changes.sort();
            DiffCache df = ns.getDiffCache();
            WrappedDiffCache wrappedCache = new WrappedDiffCache(node.getPath(), df, stats);
            JournalEntry.applyTo(changes, wrappedCache, path, beforeRev, afterRev);

            return wrappedCache.changes;
        } catch (IOException e) {
            throw DocumentStoreException.convert(e);
        } finally {
            Utils.closeIfCloseable(changes);
            logStats();
        }
    }

    private void readBranchChanges(String path,
                                   RevisionVector rv,
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
                    entry.addTo(changes, path);
                    stats.numJournalEntries++;
                } else {
                    LOG.warn("Missing journal entry for {}", asId(br));
                }
            }
        }
    }

    private void readTrunkChanges(String path,
                                  RevisionVector beforeRev,
                                  RevisionVector afterRev,
                                  StringSort changes) throws IOException {
        JournalEntry localPending = ns.getCurrentJournalEntry();
        DocumentStore store = ns.getDocumentStore();
        NodeDocument root = Utils.getRootDocument(store);
        int clusterId = ns.getClusterId();
        Map<Integer, Revision> lastRevs = root.getLastRev();
        Revision localLastRev;
        if (clusterId == 0) {
            // read-only node store
            localLastRev = afterRev.getRevision(clusterId);
        } else {
            localLastRev = lastRevs.get(clusterId);
        }
        if (localLastRev == null) {
            throw new IllegalStateException("Root document does not have a " +
                    "lastRev entry for local clusterId " + clusterId);
        }

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

        RevisionVector max = beforeRev.pmax(afterRev);
        RevisionVector min = beforeRev.pmin(afterRev);

        // do we need to include changes from pending local changes?
        if (!max.isRevisionNewer(localLastRev)
                && !localLastRev.equals(max.getRevision(clusterId))) {
            // journal does not contain all local changes
            localPending.addTo(changes, path);
            stats.numJournalEntries++;
        }

        for (Revision to : max) {
            Revision from = min.getRevision(to.getClusterId());
            if (from == null) {
                // there is no min revision with this clusterId
                // use revision with a timestamp of zero
                from = new Revision(0, 0, to.getClusterId());
            }
            StringSort invalidateOnly = JournalEntry.newSorter();
            try {
                stats.numJournalEntries += fillExternalChanges(changes, invalidateOnly,
                        path, from, to, ns.getDocumentStore(), null, null);
            } finally {
                invalidateOnly.close();
            }
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

    private void logStats() {
        stats.sw.stop();
        long timeInSec = stats.sw.elapsed(TimeUnit.SECONDS);
        if (timeInSec > 60) {
            LOG.warn(stats.toString());
        } else if (timeInSec > 10) {
            LOG.info(stats.toString());
        } else {
            LOG.debug(stats.toString());
        }
    }

    private static class Stats {

        private final Stopwatch sw = Stopwatch.createStarted();
        private final String path;
        private final RevisionVector from, to;
        private long numJournalEntries;
        private long numDiffEntries;
        private long keyMemory;
        private long valueMemory;

        Stats(String path, RevisionVector from, RevisionVector to) {
            this.path = path;
            this.from = from;
            this.to = to;
        }

        @Override
        public String toString() {
            String msg = "%d diffs for %s (%s/%s) loaded from %d journal entries in %s. " +
                    "Keys: %s, values: %s, total: %s";
            return String.format(msg, numDiffEntries, path, from, to,
                    numJournalEntries, sw,
                    byteCountToDisplaySize(keyMemory),
                    byteCountToDisplaySize(valueMemory),
                    byteCountToDisplaySize(keyMemory + valueMemory));
        }
    }

    private static class WrappedDiffCache extends DiffCache {

        private final String path;
        private String changes = "";
        private final DiffCache cache;
        private Stats stats;

        WrappedDiffCache(String path,
                         DiffCache cache,
                         Stats stats) {
            this.path = path;
            this.cache = cache;
            this.stats = stats;
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
        Entry newEntry(@Nonnull final RevisionVector from,
                       @Nonnull final RevisionVector to,
                       boolean local) {
            final Entry entry = cache.newEntry(from, to, local);
            return new Entry() {
                @Override
                public void append(@Nonnull String path,
                                   @Nonnull String changes) {
                    trackStats(path, from, to, changes);
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

        private void trackStats(String path,
                                RevisionVector from,
                                RevisionVector to,
                                String changes) {
            stats.numDiffEntries++;
            stats.keyMemory += new StringValue(path).getMemory();
            stats.keyMemory += from.getMemory();
            stats.keyMemory += to.getMemory();
            stats.valueMemory += new StringValue(changes).getMemory();
        }

        @Nonnull
        @Override
        Iterable<CacheStats> getStats() {
            return cache.getStats();
        }
    }
}
