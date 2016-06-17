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
package org.apache.jackrabbit.oak.plugins.document.mongo.replica;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isGreaterOrEquals;

/**
 * This class maintains a list of local changes (paths+revisions), which
 * shouldn't be read from the secondary Mongo, as we are not sure if they have
 * been already replicated from primary. Once we get this confidence, the entry
 * will be removed from the map.
 */
public class LocalChanges implements ReplicaSetInfoListener {

    private static final Logger LOG = LoggerFactory.getLogger(LocalChanges.class);

    /**
     * How many paths should be stored in the {@link #localChanges} map. If
     * there's more paths added (and not removed in the
     * {@link #gotRootRevisions(RevisionVector)}), only the latest changed
     * revision will be remembered.
     */
    private static final int SIZE_LIMIT = 100;

    /**
     * This map contains document paths and revisions in which they have been
     * changed. Paths in this collection hasn't been replicated to secondary
     * instances yet.
     */
    final Map<String, RevisionVector> localChanges = new HashMap<String, RevisionVector>();

    /**
     * If there's more than {@link #SIZE_LIMIT} paths in the
     * {@link #localChanges}, the class will clear the above map and update this
     * variable with the latest changed revision. {@code true} will be returned
     * for all {@link #mayContainChildrenOf(String)} and {@link #mayContain(String)}
     * invocations until this revision is replicated to all secondary instances.
     * <p>
     * This is a safety mechanism, so the {@link #localChanges} won't grow too much.
     */
    private volatile RevisionVector latestChange;

    /**
     * True if the current Mongo installation is an working replica. Otherwise
     * there's no need to store the local changes.
     */
    private volatile boolean replicaActive;

    private volatile RevisionVector rootRevision;

    public void add(String id, Collection<Revision> revs) {
        RevisionVector revsV = new RevisionVector(revs);
        RevisionVector localRootRev = rootRevision;
        if (localRootRev != null && isGreaterOrEquals(localRootRev, revsV)) {
            return;
        }

        synchronized (this) {
            if (latestChange != null && isGreaterOrEquals(latestChange, revsV)) {
                return;
            }

            if (replicaActive) {
                localChanges.put(id, revsV);
                if (localChanges.size() >= SIZE_LIMIT) {
                    localChanges.clear();
                    latestChange = revsV;
                    LOG.debug(
                            "The local changes count == {}. Clearing the list and switching to the 'latest change' mode: {}",
                            SIZE_LIMIT, latestChange);
                }
            } else {
                latestChange = revsV;
            }
        }
    }

    /**
     * Check if it's possible that the given document hasn't been replicated to
     * the secondary yet.
     *
     * @param documentId
     * @return {@code true} if it's possible that the document is still in the
     *         Mongo replication queue
     */
    public boolean mayContain(String documentId) {
        if (!replicaActive || latestChange != null) {
            return true;
        }

        synchronized (this) {
            return localChanges.containsKey(documentId);
        }
    }

    /**
     * Check if it's possible that the children of the given document hasn't
     * been replicated to the secondary yet.
     *
     * @param documentId
     * @return {@code true} if it's possible that the children of given document
     *         are still in the Mongo replication queue
     */
    public boolean mayContainChildrenOf(String parentId) {
        if (!replicaActive || latestChange != null) {
            return true;
        }

        synchronized (this) {
            for (String key : localChanges.keySet()) {
                if (parentId.equals(Utils.getParentId(key))) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public void gotRootRevisions(RevisionVector rootRevision) {
        if (rootRevision == null) {
            return;
        }

        this.rootRevision = rootRevision;

        if (!replicaActive) {
            replicaActive = true;
            LOG.info("Replica set became active");
        }

        synchronized (this) {
            if (latestChange != null && latestChange.compareTo(rootRevision) <= 0) {
                latestChange = null;
            }

            Iterator<RevisionVector> it = localChanges.values().iterator();
            while (it.hasNext()) {
                if (it.next().compareTo(rootRevision) <= 0) {
                    it.remove();
                }
            }
        }
    }
}
