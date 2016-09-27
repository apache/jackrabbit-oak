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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.document.util.MapFactory;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;

import static com.google.common.base.Preconditions.checkNotNull;
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
    static final int BACKGROUND_MULTI_UPDATE_LIMIT = 10000;

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
        UpdateOp updateOp = null;
        Revision lastRev = null;
        PeekingIterator<String> paths = Iterators.peekingIterator(
                pending.keySet().iterator());
        int i = 0;
        ArrayList<String> pathList = new ArrayList<String>();
        while (paths.hasNext()) {
            String p = paths.peek();
            Revision r = pending.get(p);

            int size = pathList.size();
            if (updateOp == null) {
                // create UpdateOp
                Commit commit = new Commit(store, r, null);
                updateOp = commit.getUpdateOperationForNode(p);
                NodeDocument.setLastRev(updateOp, r);
                lastRev = r;
                pathList.add(p);
                paths.next();
                i++;
            } else if (r.equals(lastRev)) {
                // use multi update when possible
                pathList.add(p);
                paths.next();
                i++;
            }
            // call update if any of the following is true:
            // - this is the second-to-last or last path (update last path, the
            //   root document, individually)
            // - revision is not equal to last revision (size of ids didn't change)
            // - the update limit is reached
            if (i + 2 > pending.size()
                    || size == pathList.size()
                    || pathList.size() >= BACKGROUND_MULTI_UPDATE_LIMIT) {
                List<String> ids = new ArrayList<String>();
                for (String path : pathList) {
                    ids.add(Utils.getIdFromPath(path));
                }
                store.getDocumentStore().update(NODES, ids, updateOp);
                LOG.debug("Updated _lastRev to {} on {}", lastRev, ids);
                for (String path : pathList) {
                    map.remove(path, lastRev);
                }
                pathList.clear();
                updateOp = null;
                lastRev = null;
            }
        }
        Revision writtenRootRev = pending.get("/");
        if (writtenRootRev != null) {
            int cid = writtenRootRev.getClusterId();
            if (store.getDocumentStore().find(org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES, String.valueOf(cid)) != null) {
                UpdateOp update = new UpdateOp(String.valueOf(cid), false);
                update.equals(Document.ID, null, String.valueOf(cid));
                update.set(ClusterNodeInfo.LAST_WRITTEN_ROOT_REV_KEY, writtenRootRev.toString());
                store.getDocumentStore().findAndUpdate(org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES, update);
            }
        }

        stats.write = clock.getTime() - time;
        return stats;
    }

    @Override
    public String toString() {
        return map.toString();
    }

    private Revision getMostRecentRevision() {
        // use revision of root document
        Revision rev = map.get("/");
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
