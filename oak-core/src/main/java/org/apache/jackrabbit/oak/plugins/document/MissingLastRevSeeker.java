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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;

import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.RecoverLockState;

/**
 * Utils to retrieve _lastRev missing update candidates.
 */
public class MissingLastRevSeeker {
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
        return store.query(Collection.CLUSTER_NODES, ClusterNodeInfoDocument.MIN_ID_VALUE,
                ClusterNodeInfoDocument.MAX_ID_VALUE, Integer.MAX_VALUE);
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
        Iterable<NodeDocument> nodes = new MissingLastRevCandidateIterable(store, NodeDocument.getModifiedInSecs(startTime));
        return Iterables.filter(nodes, new Predicate<NodeDocument>() {
            @Override
            public boolean apply(NodeDocument input) {
                Long modified = (Long) input.get(NodeDocument.MODIFIED_IN_SECS);
                return (modified != null && (modified >= NodeDocument.getModifiedInSecs(startTime)));
            }
        });
    }

    /**
     * Query the document store for nodes newer than a specific timestamp; implement
     * this as an Iterator that gets relatively small chunks from the store.
     */
    private class MissingLastRevCandidateIterable implements Iterable<NodeDocument> {

        private final DocumentStore store;
        private final long startTime;

        public MissingLastRevCandidateIterable(DocumentStore store, long startTime) {
            this.store = store;
            this.startTime = startTime;
        }

        @Override
        public Iterator<NodeDocument> iterator() {
            return new MissingLastRevCandidateIterator(store, startTime);
        }

        private class MissingLastRevCandidateIterator extends AbstractIterator<NodeDocument> {

            private static final int CHUNKSIZE = 256;
            private final DocumentStore store;
            private final long startTime;
            private String startIndex = NodeDocument.MIN_ID_VALUE;

            private List<NodeDocument> results = new LinkedList<NodeDocument>();

            public MissingLastRevCandidateIterator(DocumentStore store, long startTime) {
                this.store = store;
                this.startTime = startTime;
            }

            private boolean refill() {
                if (startIndex == null) {
                    return false;
                } else {
                    List<NodeDocument> found = store.query(Collection.NODES, startIndex, NodeDocument.MAX_ID_VALUE,
                            NodeDocument.MODIFIED_IN_SECS, startTime, CHUNKSIZE);
                    if (found.isEmpty()) {
                        startIndex = null;
                        return false;
                    } else {
                        results.addAll(found);
                        return true;
                    }
                }
            }

            @Override
            protected NodeDocument computeNext() {
                if (!results.isEmpty()) {
                    NodeDocument result = results.remove(0);
                    startIndex = result.getId();
                    return result;
                } else {
                    // need to refill
                    if (refill()) {
                        return computeNext();
                    } else {
                        return endOfData();
                    }
                }
            }
        }
    }

    public boolean acquireRecoveryLock(int clusterId){
        //This approach has a race condition where two different cluster nodes
        //can acquire the lock simultaneously.
        UpdateOp update = new UpdateOp(Integer.toString(clusterId), true);
        update.set(ClusterNodeInfo.REV_RECOVERY_LOCK, RecoverLockState.ACQUIRED.name());
        store.createOrUpdate(Collection.CLUSTER_NODES, update);
        return true;
    }

    public void releaseRecoveryLock(int clusterId){
        UpdateOp update = new UpdateOp(Integer.toString(clusterId), true);
        update.set(ClusterNodeInfo.REV_RECOVERY_LOCK, null);
        update.set(ClusterNodeInfo.STATE, null);
        store.createOrUpdate(Collection.CLUSTER_NODES, update);
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

