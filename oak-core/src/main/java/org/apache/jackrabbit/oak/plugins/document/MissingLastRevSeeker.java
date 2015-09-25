/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.RecoverLockState;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.getModifiedInSecs;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getSelectedDocuments;

/**
 * Utilities to retrieve _lastRev missing update candidates.
 */
public class MissingLastRevSeeker {

    private static final Logger LOG = LoggerFactory.getLogger(MissingLastRevSeeker.class);

    protected final String ROOT_PATH = "/";

    private final DocumentStore store;

    public MissingLastRevSeeker(DocumentStore store) {
        this.store = store;
    }

    /**
     * Gets the clusters which potentially need _lastRev recovery.
     *
     * @return the clusters
     */
    public Iterable<ClusterNodeInfoDocument> getAllClusters() {
        return ClusterNodeInfoDocument.all(store);
    }

    /**
     * Gets the cluster node info for the given cluster node id.
     *
     * @param clusterId the cluster id
     * @return the cluster node info
     */
    public ClusterNodeInfoDocument getClusterNodeInfo(final int clusterId) {
        // Fetch all documents.
        return store.find(Collection.CLUSTER_NODES, String.valueOf(clusterId));
    }

    /**
     * Get the candidates with modified time after the specified
     * {@code startTime}.
     *
     * @param startTime the start time.
     * @return the candidates
     */
    public Iterable<NodeDocument> getCandidates(final long startTime) {
        // Fetch all documents where lastmod >= startTime
        Iterable<NodeDocument> nodes = getSelectedDocuments(store,
                MODIFIED_IN_SECS, getModifiedInSecs(startTime));
        return Iterables.filter(nodes, new Predicate<NodeDocument>() {
            @Override
            public boolean apply(NodeDocument input) {
                Long modified = (Long) input.get(MODIFIED_IN_SECS);
                return (modified != null && (modified >= getModifiedInSecs(startTime)));
            }
        });
    }

    public boolean acquireRecoveryLock(int clusterId, int recoveredBy) {
        try {
            // This approach has a race condition where two different cluster
            // nodes
            // can acquire the lock simultaneously.
            UpdateOp update = new UpdateOp(Integer.toString(clusterId), true);
            update.set(ClusterNodeInfo.REV_RECOVERY_LOCK, RecoverLockState.ACQUIRED.name());
            if (recoveredBy != 0) {
                update.set(ClusterNodeInfo.REV_RECOVERY_BY, recoveredBy);
            }
            store.createOrUpdate(Collection.CLUSTER_NODES, update);
            return true;
        } catch (RuntimeException ex) {
            LOG.error("Failed to acquire the recovery lock for clusterNodeId " + clusterId, ex);
            throw (ex);
        }
    }

    public void releaseRecoveryLock(int clusterId) {
        try {
            UpdateOp update = new UpdateOp(Integer.toString(clusterId), true);
            update.set(ClusterNodeInfo.REV_RECOVERY_LOCK, null);
            update.set(ClusterNodeInfo.REV_RECOVERY_BY, null);
            update.set(ClusterNodeInfo.STATE, null);
            store.createOrUpdate(Collection.CLUSTER_NODES, update);
        } catch (RuntimeException ex) {
            LOG.error("Failed to release the recovery lock for clusterNodeId " + clusterId, ex);
            throw (ex);
        }
    }

    public NodeDocument getRoot() {
        return store.find(Collection.NODES, Utils.getIdFromPath(ROOT_PATH));
    }

    public boolean isRecoveryNeeded(long currentTime) {
        for(ClusterNodeInfoDocument nodeInfo : getAllClusters()){
            // Check if _lastRev recovery needed for this cluster node
            // state is Active && currentTime past the leaseEnd time && recoveryLock not held by someone
            if (nodeInfo.isActive()
                    && currentTime > nodeInfo.getLeaseEndTime()
                    && !nodeInfo.isBeingRecovered()) {
                return true;
            }
        }
        return false;
    }
}
