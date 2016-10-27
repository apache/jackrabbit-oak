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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MapFactory;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;

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

    private final ConcurrentMap<String, Revision> map = MapFactory.getInstance().create();

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
    @CheckForNull
    public Revision put(@Nonnull String path, @Nonnull Revision revision) {
        checkNotNull(path);
        checkNotNull(revision);
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

    @CheckForNull
    public Revision get(String path) {
        return map.get(path);
    }

    @Nonnull
    public Collection<String> getPaths() {
        return map.keySet();
    }

    /**
     * Returns all paths of nodes with modifications at the start revision
     * (inclusive) or later.
     *
     * @param start the start revision (inclusive).
     * @return matching paths with pending modifications.
     */
    @Nonnull
    public Iterable<String> getPaths(@Nonnull final Revision start) {
        if (map.isEmpty()) {
            return Collections.emptyList();
        } else {
            return Iterables.transform(Iterables.filter(map.entrySet(),
                    new Predicate<Map.Entry<String, Revision>>() {
                @Override
                public boolean apply(Map.Entry<String, Revision> input) {
                    return start.compareRevisionTime(input.getValue()) < 1;
                }
            }), new Function<Map.Entry<String, Revision>, String>() {
                @Override
                public String apply(Map.Entry<String, Revision> input) {
                    return input.getKey();
                }
            });
        }
    }

    /**
     * Persist the pending changes to _lastRev to the given store. This method
     * will persist a snapshot of the pending revisions by acquiring the passed
     * lock for a short period of time.
     *
     * @param store the document node store.
     * @param snapshot callback when the snapshot of the pending changes is
     *                 acquired.
     * @param lock the lock to acquire to get a consistent snapshot of the
     *             revisions to write back.
     * @return stats about the write operation.
     */
    public BackgroundWriteStats persist(@Nonnull DocumentNodeStore store,
                                        @Nonnull Snapshot snapshot,
                                        @Nonnull Lock lock) {
        BackgroundWriteStats stats = new BackgroundWriteStats();
        if (map.size() == 0) {
            return stats;
        }
        checkNotNull(store);
        checkNotNull(lock);

        Clock clock = store.getClock();

        long time = clock.getTime();
        // get a copy of the map while holding the lock
        lock.lock();
        stats.lock = clock.getTime() - time;
        time = clock.getTime();
        Map<String, Revision> pending;
        try {
            snapshot.acquiring(getMostRecentRevision());
            pending = Maps.newTreeMap(PathComparator.INSTANCE);
            pending.putAll(map);
        } finally {
            lock.unlock();
        }
        stats.num = pending.size();
        List<UpdateOp> updates = Lists.newArrayList();
        Map<String, Revision> pathToRevision = Maps.newHashMap();
        for (Iterable<Map.Entry<String, Revision>> batch : Iterables.partition(
                pending.entrySet(), BACKGROUND_MULTI_UPDATE_LIMIT)) {
            for (Map.Entry<String, Revision> entry : batch) {
                String p = entry.getKey();
                Revision r = entry.getValue();
                if (PathUtils.denotesRoot(entry.getKey())) {
                    // update root individually at the end
                    continue;
                }
                updates.add(newUpdateOp(store, p, r));
                pathToRevision.put(p, r);
            }
            if (!updates.isEmpty()) {
                store.getDocumentStore().createOrUpdate(NODES, updates);
                stats.calls++;
                for (Map.Entry<String, Revision> entry : pathToRevision.entrySet()) {
                    map.remove(entry.getKey(), entry.getValue());
                    LOG.debug("Updated _lastRev to {} on {}", entry.getValue(), entry.getKey());
                }
                // clean up for next batch
                updates.clear();
                pathToRevision.clear();
            }
        }
        // finally update remaining root document
        Revision rootRev = pending.get(ROOT_PATH);
        if (rootRev != null) {
            store.getDocumentStore().findAndUpdate(NODES, newUpdateOp(store, ROOT_PATH, rootRev));
            stats.calls++;
            map.remove(ROOT_PATH, rootRev);
            LOG.debug("Updated _lastRev to {} on {}", rootRev, ROOT_PATH);

            int cid = rootRev.getClusterId();
            if (store.getDocumentStore().find(CLUSTER_NODES, String.valueOf(cid)) != null) {
                UpdateOp update = new UpdateOp(String.valueOf(cid), false);
                update.equals(Document.ID, null, String.valueOf(cid));
                update.set(ClusterNodeInfo.LAST_WRITTEN_ROOT_REV_KEY, rootRev.toString());
                store.getDocumentStore().findAndUpdate(CLUSTER_NODES, update);
            }
        }

        stats.write = clock.getTime() - time;
        return stats;
    }

    @Override
    public String toString() {
        return map.toString();
    }

    private static UpdateOp newUpdateOp(DocumentNodeStore store,
                                        String path, Revision r) {
        Commit commit = new Commit(store, r, null);
        UpdateOp updateOp = commit.getUpdateOperationForNode(path);
        NodeDocument.setLastRev(updateOp, r);
        return updateOp;
    }

    private Revision getMostRecentRevision() {
        // use revision of root document
        Revision rev = map.get(ROOT_PATH);
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
