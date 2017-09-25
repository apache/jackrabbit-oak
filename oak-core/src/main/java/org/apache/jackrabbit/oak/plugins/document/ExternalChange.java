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
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.sort.StringSort;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.observation.ChangeSetBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.JournalEntry.fillExternalChanges;
import static org.apache.jackrabbit.oak.plugins.document.JournalEntry.newSorter;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.alignWithExternalRevisions;

/**
 * Utility class to pull in external changes in the DocumentNodeStore and
 * process journal entries.
 */
abstract class ExternalChange {

    private static final Logger LOG = LoggerFactory.getLogger(ExternalChange.class);

    private final DocumentNodeStore store;

    protected final BackgroundReadStats stats;

    private ChangeSetBuilder changeSetBuilder;

    private final JournalPropertyHandler journalPropertyHandler;

    ExternalChange(DocumentNodeStore store) {
        this.store = store;
        this.stats = new BackgroundReadStats();
        this.journalPropertyHandler = store.getJournalPropertyHandlerFactory().newHandler();
    }

    /**
     * Called when when cache entries related to nodes with the given paths
     * must be invalidated.
     *
     * @param paths the paths of affected nodes.
     */
    abstract void invalidateCache(@Nonnull Iterable<String> paths);

    /**
     * Called when all cache entries must be invalidated.
     */
    abstract void invalidateCache();

    /**
     * Called when the current head should be updated with revisions of external
     * changes.
     *
     * @param externalChanges the head revision of other cluster nodes that
     *                        changed and should now be considered visible.
     * @param sweepRevisions the current sweep revisions.
     * @param changedPaths paths of nodes that are affected by those external
     *                     changes.
     */
    abstract void updateHead(@Nonnull Set<Revision> externalChanges,
                             @Nonnull RevisionVector sweepRevisions,
                             @Nullable Iterable<String> changedPaths);

    /**
     * Processes external changes if there are any.
     *
     * @return statistics about the background read operation.
     */
    BackgroundReadStats process() {
        Clock clock = store.getClock();
        int clusterId = store.getClusterId();
        long time = clock.getTime();
        String id = Utils.getIdFromPath("/");
        NodeDocument doc = store.getDocumentStore().find(NODES, id, store.getAsyncDelay());
        if (doc == null) {
            return stats;
        }
        try {
            alignWithExternalRevisions(doc, clock, clusterId);
        } catch (InterruptedException e) {
            throw new RuntimeException("Background read interrupted", e);
        }

        StringSort externalSort = newSorter();
        StringSort invalidate = newSorter();

        Map<Integer, Revision> lastRevMap = doc.getLastRev();
        try {
            changeSetBuilder = new ChangeSetBuilder(
                    store.getChangeSetMaxItems(), store.getChangeSetMaxDepth());
            RevisionVector headRevision = store.getHeadRevision();
            Set<Revision> externalChanges = newHashSet();
            for (Map.Entry<Integer, Revision> e : lastRevMap.entrySet()) {
                int machineId = e.getKey();
                if (machineId == clusterId) {
                    // ignore own lastRev
                    continue;
                }
                Revision r = e.getValue();
                Revision last = headRevision.getRevision(machineId);
                if (last == null) {
                    // make sure we see all changes when a cluster node joins
                    last = new Revision(0, 0, machineId);
                }
                if (r.compareRevisionTime(last) > 0) {
                    // OAK-2345
                    // only consider as external change if
                    // the revision changed for the machineId
                    externalChanges.add(r);
                    // collect external changes
                    if (externalSort != null) {
                        // add changes for this particular clusterId to the externalSort
                        try {
                            fillExternalChanges(externalSort, invalidate,
                                    PathUtils.ROOT_PATH, last, r,
                                    store.getDocumentStore(),
                                    changeSetBuilder, journalPropertyHandler);
                        } catch (Exception e1) {
                            LOG.error("backgroundRead: Exception while reading external changes from journal: " + e1, e1);
                            closeQuietly(externalSort);
                            closeQuietly(invalidate);
                            externalSort = null;
                            invalidate = null;
                        }
                    }
                }
            }

            stats.readHead = clock.getTime() - time;
            time = clock.getTime();

            // invalidate cache
            if (cacheInvalidationNeeded(externalSort, invalidate)) {
                // invalidate caches
                if (externalSort == null) {
                    // if no externalSort available, then invalidate everything
                    invalidateCache();
                } else {
                    stats.numExternalChanges = externalSort.getSize();
                    try {
                        sortAndInvalidate(externalSort);
                        sortAndInvalidate(invalidate);
                    } catch (Exception ioe) {
                        LOG.error("backgroundRead: got IOException during external sorting/cache invalidation (as a result, invalidating entire cache): "+ioe, ioe);
                        invalidateCache();
                    }
                }
                stats.cacheInvalidationTime = clock.getTime() - time;
            }

            // update head
            if (!externalChanges.isEmpty()) {
                updateHead(externalChanges, doc.getSweepRevisions(), externalSort);
            }
        } finally {
            closeQuietly(externalSort);
            closeQuietly(invalidate);
        }

        return stats;
    }

    ChangeSetBuilder getChangeSetBuilder() {
        return changeSetBuilder;
    }

    JournalPropertyHandler getJournalPropertyHandler() {
        return journalPropertyHandler;
    }

    //-------------------------< internal >-------------------------------------

    private boolean cacheInvalidationNeeded(StringSort externalSort,
                                            StringSort invalidate) {
        return externalSort == null || invalidate == null
                || !externalSort.isEmpty() || !invalidate.isEmpty();
    }

    private void sortAndInvalidate(StringSort paths) throws IOException {
        if (paths.isEmpty()) {
            return;
        }
        paths.sort();
        invalidateCache(paths);
    }
}
