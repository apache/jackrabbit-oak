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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

import org.apache.jackrabbit.oak.plugins.document.util.MapFactory;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jackrabbit.guava.common.base.Stopwatch;

import org.apache.jackrabbit.guava.common.collect.Iterables;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Commit.createUpdateOp;

/**
 * Keeps track of when nodes where last modified. To be persisted later by
 * a background thread.
 */
class UnsavedModifications {

    private static final Logger LOG = LoggerFactory.getLogger(UnsavedModifications.class);

    /**
     * The maximum number of document to update at once in a multi update.
     */
    static final int BACKGROUND_MULTI_UPDATE_LIMIT = 100;

    private final ConcurrentMap<Path, Revision> map = MapFactory.getInstance().create();

    /**
     * Puts a revision for the given path. The revision for the given path is
     * only put if there is no modification present for the revision or if the
     * current modification revision is older than the passed revision.
     *
     * @param path the path of the modified node.
     * @param revision the revision of the modification.
     * @return the previously set revision for the given path or null if there
     *          was none or the current revision is newer.
     */
    @Nullable
    public Revision put(@NotNull Path path, @NotNull Revision revision) {
        requireNonNull(path);
        requireNonNull(revision);
        for (;;) {
            Revision previous = map.get(path);
            if (previous == null) {
                if (map.putIfAbsent(path, revision) == null) {
                    return null;
                }
            } else {
                if (previous.compareRevisionTime(revision) < 0) {
                    if (map.replace(path, previous, revision)) {
                        return previous;
                    }
                } else {
                    // revision is earlier, do not update
                    return null;
                }
            }
        }
    }

    @Nullable
    public Revision get(Path path) {
        return map.get(path);
    }

    @NotNull
    public Collection<Path> getPaths() {
        return map.keySet();
    }

    /**
     * Returns all paths of nodes with modifications at the start revision
     * (inclusive) or later.
     *
     * @param start the start revision (inclusive).
     * @return matching paths with pending modifications.
     */
    @NotNull
    public Iterable<Path> getPaths(@NotNull final Revision start) {
        if (map.isEmpty()) {
            return Collections.emptyList();
        } else {
            return Iterables.transform(Iterables.filter(map.entrySet(),
                    input ->start.compareRevisionTime(input.getValue()) < 1),
                    input -> input.getKey());
        }
    }

    /**
     * Persist the pending changes to _lastRev to the given store. This method
     * will persist a snapshot of the pending revisions and current sweep
     * revision by acquiring the passed lock for a short period of time.
     *
     * @param store the document store.
     * @param sweepRevision supplier for the current sweep revision.
     * @param snapshot callback when the snapshot of the pending changes is
     *                 acquired.
     * @param lock the lock to acquire to get a consistent snapshot of the
     *             revisions to write back.
     * @return stats about the write operation.
     */
    public BackgroundWriteStats persist(@NotNull DocumentStore store,
                                        @NotNull Supplier<Revision> sweepRevision,
                                        @NotNull Snapshot snapshot,
                                        @NotNull Lock lock) {
        BackgroundWriteStats stats = new BackgroundWriteStats();
        if (map.size() == 0) {
            return stats;
        }
        requireNonNull(store);
        requireNonNull(sweepRevision);
        requireNonNull(snapshot);
        requireNonNull(lock);

        Stopwatch sw = Stopwatch.createStarted();
        // get a copy of the map while holding the lock
        lock.lock();
        stats.lock = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset().start();
        Revision sweepRev;
        Map<Path, Revision> pending;
        try {
            snapshot.acquiring(getMostRecentRevision());
            pending = new TreeMap<>(PathComparator.INSTANCE);
            pending.putAll(map);
            sweepRev = sweepRevision.get();
        } finally {
            lock.unlock();
        }
        stats.num = pending.size();
        List<UpdateOp> updates = new ArrayList<>();
        Map<Path, Revision> pathToRevision = new HashMap<>();
        for (Iterable<Map.Entry<Path, Revision>> batch : Iterables.partition(
                pending.entrySet(), BACKGROUND_MULTI_UPDATE_LIMIT)) {
            for (Map.Entry<Path, Revision> entry : batch) {
                Path p = entry.getKey();
                Revision r = entry.getValue();
                if (p.isRoot()) {
                    // update root individually at the end
                    continue;
                }
                updates.add(newUpdateOp(p, r));
                pathToRevision.put(entry.getKey(), r);
            }
            if (!updates.isEmpty()) {
                store.createOrUpdate(NODES, updates);
                stats.calls++;
                for (Map.Entry<Path, Revision> entry : pathToRevision.entrySet()) {
                    map.remove(entry.getKey(), entry.getValue());
                    LOG.debug("Updated _lastRev to {} on {}", entry.getValue(), entry.getKey());
                }
                // clean up for next batch
                updates.clear();
                pathToRevision.clear();
            }
        }
        // finally update remaining root document
        Revision rootRev = pending.get(Path.ROOT);
        if (rootRev != null) {
            UpdateOp rootUpdate = newUpdateOp(Path.ROOT, rootRev);
            // also update to most recent sweep revision
            if (sweepRev != null) {
                NodeDocument.setSweepRevision(rootUpdate, sweepRev);
                LOG.debug("Updating _sweepRev to {}", sweepRev);
            }
            // ensure lastRev is not updated by someone else in the meantime
            Revision lastRev = Utils.getRootDocument(store).getLastRev().get(rootRev.getClusterId());
            if (lastRev != null) {
                NodeDocument.hasLastRev(rootUpdate, lastRev);
            }
            if (store.findAndUpdate(NODES, rootUpdate) == null) {
                throw new DocumentStoreException("Update of root document to _lastRev " +
                        rootRev + " failed. Detected concurrent update");
            }
            stats.calls++;
            map.remove(Path.ROOT, rootRev);
            LOG.debug("Updated _lastRev to {} on {}", rootRev, Path.ROOT);

            int cid = rootRev.getClusterId();
            UpdateOp update = new UpdateOp(String.valueOf(cid), false);
            update.set(ClusterNodeInfo.LAST_WRITTEN_ROOT_REV_KEY, rootRev.toString());
            store.findAndUpdate(CLUSTER_NODES, update);
        }

        stats.write = sw.elapsed(TimeUnit.MILLISECONDS);
        return stats;
    }

    @Override
    public String toString() {
        return map.toString();
    }

    private static UpdateOp newUpdateOp(Path path, Revision r) {
        UpdateOp updateOp = createUpdateOp(path, r, false);
        NodeDocument.setLastRev(updateOp, r);
        return updateOp;
    }

    private Revision getMostRecentRevision() {
        // use revision of root document
        Revision rev = map.get(Path.ROOT);
        // otherwise find most recent
        if (rev == null) {
            for (Revision r : map.values()) {
                rev = Utils.max(rev, r);
            }
        }
        return rev;
    }

    public interface Snapshot {

        Snapshot IGNORE = new Snapshot() {
            @Override
            public void acquiring(Revision mostRecent) {
            }
        };

        void acquiring(Revision mostRecent);
    }
}
