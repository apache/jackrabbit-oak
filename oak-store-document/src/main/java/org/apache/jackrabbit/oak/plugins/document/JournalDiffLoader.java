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

import org.apache.jackrabbit.guava.common.base.Stopwatch;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.sort.StringSort;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;
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

    JournalDiffLoader(@NotNull AbstractDocumentNodeState base,
                      @NotNull AbstractDocumentNodeState node,
                      @NotNull DocumentNodeStore ns) {
        this.base = requireNonNull(base);
        this.node = requireNonNull(node);
        this.ns = requireNonNull(ns);
        checkArgument(base.getPath().equals(node.getPath()),
                "nodes must have matching paths: %s != %s",
                base.getPath(), node.getPath());
    }

    @Override
    public String call() {
        RevisionVector afterRev = node.getRootRevision();
        RevisionVector beforeRev = base.getRootRevision();
        stats = new Stats(node.getPath(), beforeRev, afterRev);

        StringSort changes = JournalEntry.newSorter();
        try {
            Path path = node.getPath();
            readTrunkChanges(path, beforeRev, afterRev, changes);

            readBranchChanges(path, beforeRev, changes);
            readBranchChanges(path, afterRev, changes);

            changes.sort();
            DiffCache df = ns.getDiffCache();
            WrappedDiffCache wrappedCache = new WrappedDiffCache(node.getPath(), df, stats);
            JournalEntry.applyTo(changes, wrappedCache, node.getPath(), beforeRev, afterRev);

            return wrappedCache.changes;
        } catch (IOException e) {
            throw DocumentStoreException.convert(e);
        } finally {
            Utils.closeIfCloseable(changes);
            logStats();
        }
    }

    private void readBranchChanges(Path path,
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

    private void readTrunkChanges(Path path,
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
            stats.numJournalEntries += fillExternalChanges(changes, null,
                    path, from, to, ns.getDocumentStore(), entry -> {}, null, null);
        }
    }

    @NotNull
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
        private final Path path;
        private final RevisionVector from, to;
        private long numJournalEntries;
        private long numDiffEntries;
        private long keyMemory;
        private long valueMemory;

        Stats(Path path, RevisionVector from, RevisionVector to) {
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

        private final Path path;
        private String changes = "";
        private final DiffCache cache;
        private Stats stats;

        WrappedDiffCache(Path path,
                         DiffCache cache,
                         Stats stats) {
            this.path = path;
            this.cache = cache;
            this.stats = stats;
        }

        @Nullable
        String getChanges() {
            return changes;
        }

        @Override
        String getChanges(@NotNull RevisionVector from,
                          @NotNull RevisionVector to,
                          @NotNull Path path,
                          @Nullable Loader loader) {
            return cache.getChanges(from, to, path, loader);
        }

        @NotNull
        @Override
        Entry newEntry(@NotNull final RevisionVector from,
                       @NotNull final RevisionVector to,
                       boolean local) {
            final Entry entry = cache.newEntry(from, to, local);
            return new Entry() {
                @Override
                public void append(@NotNull Path path,
                                   @NotNull String changes) {
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

        private void trackStats(Path path,
                                RevisionVector from,
                                RevisionVector to,
                                String changes) {
            stats.numDiffEntries++;
            stats.keyMemory += path.getMemory();
            stats.keyMemory += from.getMemory();
            stats.keyMemory += to.getMemory();
            stats.valueMemory += new StringValue(changes).getMemory();
        }

        @NotNull
        @Override
        Iterable<CacheStats> getStats() {
            return cache.getStats();
        }

        @Override
        public void invalidateAll() {
            cache.invalidateAll();
        }
    }
}
