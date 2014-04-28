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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;

/**
 * Keeps track of when nodes where last modified. To be persisted later by
 * a background thread.
 */
class UnsavedModifications {

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
     * Applies all modifications from this instance to the <code>other</code>.
     * A modification is only applied if there is no modification in other
     * for a given path or if the other modification is earlier than the
     * {@code commit} revision.
     *
     * @param other the other <code>UnsavedModifications</code>.
     * @param commit the commit revision.
     */
    public void applyTo(UnsavedModifications other, Revision commit) {
        for (Map.Entry<String, Revision> entry : map.entrySet()) {
            other.put(entry.getKey(), commit);
        }
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
     * @param lock the lock to acquire to get a consistent snapshot of the
     *             revisions to write back.
     */
    public void persist(@Nonnull DocumentNodeStore store,
                        @Nonnull Lock lock) {
        if (map.isEmpty()) {
            return;
        }
        checkNotNull(store);
        checkNotNull(lock);

        // get a copy of the map while holding the lock
        lock.lock();
        Map<String, Revision> pending;
        try {
            pending = Maps.newHashMap(map);
        } finally {
            lock.unlock();
        }
        ArrayList<String> paths = new ArrayList<String>(pending.keySet());
        // sort by depth (high depth first), then path
        Collections.sort(paths, PathComparator.INSTANCE);

        UpdateOp updateOp = null;
        Revision lastRev = null;
        ArrayList<String> pathList = new ArrayList<String>();
        for (int i = 0; i < paths.size();) {
            String p = paths.get(i);
            Revision r = pending.get(p);
            if (r == null) {
                i++;
                continue;
            }
            int size = pathList.size();
            if (updateOp == null) {
                // create UpdateOp
                Commit commit = new Commit(store, null, r);
                updateOp = commit.getUpdateOperationForNode(p);
                NodeDocument.setLastRev(updateOp, r);
                lastRev = r;
                pathList.add(p);
                i++;
            } else if (r.equals(lastRev)) {
                // use multi update when possible
                pathList.add(p);
                i++;
            }
            // call update if any of the following is true:
            // - this is the second-to-last or last path (update last path, the
            //   root document, individually)
            // - revision is not equal to last revision (size of ids didn't change)
            // - the update limit is reached
            if (i + 2 > paths.size()
                    || size == pathList.size()
                    || pathList.size() >= BACKGROUND_MULTI_UPDATE_LIMIT) {
                List<String> ids = new ArrayList<String>();
                for (String path : pathList) {
                    ids.add(Utils.getIdFromPath(path));
                }
                store.getDocumentStore().update(NODES, ids, updateOp);
                for (String path : pathList) {
                    map.remove(path, lastRev);
                }
                pathList.clear();
                updateOp = null;
                lastRev = null;
            }
        }
    }
}
