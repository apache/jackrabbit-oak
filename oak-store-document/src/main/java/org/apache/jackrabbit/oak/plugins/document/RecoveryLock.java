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

import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.ClusterNodeState.ACTIVE;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.DEFAULT_LEASE_DURATION_MILLIS;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.LEASE_END_KEY;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.REV_RECOVERY_BY;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.REV_RECOVERY_LOCK;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.RecoverLockState.ACQUIRED;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.STATE;
import static org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES;

/**
 * Utility class to acquire and release a recovery lock on an entry in the
 * {@link Collection#CLUSTER_NODES} collection.
 */
class RecoveryLock {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveryLock.class);

    private final DocumentStore store;

    private final Clock clock;

    private final int clusterId;

    /**
     * Prepare a recovery lock on the document store for an entry with the given
     * {@code clusterId}. Constructing the lock does not check whether an entry
     * actually exists for the {@code clusterId} and is indeed valid.
     *
     * @param store the store where the cluster node entries are stored.
     * @param clock the clock used to check whether an entry's lease expired.
     * @param clusterId the {@code clusterId} this lock is created for.
     */
    RecoveryLock(DocumentStore store, Clock clock, int clusterId) {
        this.store = store;
        this.clock = clock;
        this.clusterId = clusterId;
    }

    /**
     * Acquire a recovery lock for the cluster node info entry with the
     * {@code clusterId} specified in the constructor of this recovery lock.
     * This method may break a lock when it determines the cluster node holding
     * the recovery lock is no more active or its lease expired.
     *
     * @param recoveredBy id of cluster doing the recovery
     * @return whether the lock has been acquired
     */
    boolean acquireRecoveryLock(int recoveredBy) {
        ClusterNodeInfoDocument doc = store.find(CLUSTER_NODES, String.valueOf(clusterId));
        if (doc == null) {
            // this is unexpected...
            return false;
        }
        if (!doc.isRecoveryNeeded(clock.getTime())) {
            return false;
        }
        if (tryAcquireRecoveryLock(doc, recoveredBy)) {
            return true;
        }
        // either we already own the lock or were able to break the lock
        return doc.isBeingRecoveredBy(recoveredBy)
                || tryBreakRecoveryLock(doc, recoveredBy);
    }

    /**
     * Releases the recovery lock on the given {@code clusterId}. If
     * {@code success} is {@code true}, the state of the cluster node entry
     * is reset, otherwise it is left as is. That is, for a cluster node which
     * requires recovery and the recovery process failed, the state will still
     * be active, when this release method is called with {@code success} set
     * to {@code false}.
     *
     * @param success whether recovery was successful.
     */
    void releaseRecoveryLock(boolean success) {
        try {
            UpdateOp update = new UpdateOp(Integer.toString(clusterId), false);
            update.set(REV_RECOVERY_LOCK, ClusterNodeInfo.RecoverLockState.NONE.name());
            update.set(REV_RECOVERY_BY, null);
            if (success) {
                update.set(STATE, null);
                update.set(LEASE_END_KEY, null);
            } else {
                // make sure lease is expired
                update.set(LEASE_END_KEY, clock.getTime() - 1);
            }
            ClusterNodeInfoDocument old = store.findAndUpdate(CLUSTER_NODES, update);
            if (old == null) {
                throw new DocumentStoreException("ClusterNodeInfo document for " + clusterId + " does not exist.");
            }
            LOG.info("Released recovery lock for cluster id {} (recovery successful: {})",
                    clusterId, success);
        } catch (RuntimeException ex) {
            LOG.error("Failed to release the recovery lock for clusterNodeId " + clusterId, ex);
            throw (ex);
        }
    }

    //-------------------------------< internal >-------------------------------

    /**
     * Acquire a recovery lock for the given cluster node info document
     *
     * @param info
     *            info document of the cluster that is going to be recovered
     * @param recoveredBy
     *            id of cluster doing the recovery ({@code 0} when unknown)
     * @return whether the lock has been acquired
     */
    private boolean tryAcquireRecoveryLock(ClusterNodeInfoDocument info,
                                           int recoveredBy) {
        int clusterId = info.getClusterId();
        try {
            UpdateOp update = new UpdateOp(Integer.toString(clusterId), false);
            update.equals(STATE, ACTIVE.name());
            update.equals(LEASE_END_KEY, info.getLeaseEndTime());
            update.notEquals(REV_RECOVERY_LOCK, ACQUIRED.name());
            update.set(REV_RECOVERY_LOCK, ACQUIRED.name());
            // Renew the lease once to give the recovery some time to finish
            // in case recovery is done by the same clusterId. In this scenario
            // the lease is not updated by a background thread.
            update.set(LEASE_END_KEY, clock.getTime() + DEFAULT_LEASE_DURATION_MILLIS);
            if (recoveredBy != 0) {
                update.set(REV_RECOVERY_BY, recoveredBy);
            }
            ClusterNodeInfoDocument old = store.findAndUpdate(CLUSTER_NODES, update);
            if (old != null) {
                LOG.info("Acquired recovery lock for cluster id {}", clusterId);
            }
            return old != null;
        } catch (RuntimeException ex) {
            LOG.error("Failed to acquire the recovery lock for clusterNodeId " + clusterId, ex);
            throw (ex);
        }
    }

    /**
     * Checks if the recovering cluster node is inactive and then tries to
     * break the recovery lock.
     *
     * @param doc the cluster node info document of the cluster node to acquire
     *            the recovery lock for.
     * @param recoveredBy id of cluster doing the recovery.
     * @return whether the lock has been acquired.
     */
    private boolean tryBreakRecoveryLock(ClusterNodeInfoDocument doc,
                                         int recoveredBy) {
        Long recoveryBy = doc.getRecoveryBy();
        if (recoveryBy == null) {
            // cannot determine current lock owner
            return false;
        }
        ClusterNodeInfoDocument recovering = store.find(CLUSTER_NODES, String.valueOf(recoveryBy));
        if (recovering == null) {
            // cannot determine current lock owner
            return false;
        }
        long now = clock.getTime();
        long leaseEnd = recovering.getLeaseEndTime();
        if (recovering.isActive() && leaseEnd > now) {
            // still active, cannot break lock
            return false;
        }
        // try to break the lock
        try {
            UpdateOp update = new UpdateOp(Integer.toString(doc.getClusterId()), false);
            update.equals(STATE, ACTIVE.name());
            update.equals(REV_RECOVERY_LOCK, ACQUIRED.name());
            update.equals(REV_RECOVERY_BY, recoveryBy);
            update.set(REV_RECOVERY_BY, recoveredBy);
            // Renew the lease once to give the recovery some time to finish
            // in case recovery is done by the same clusterId. In this scenario
            // the lease is not updated by a background thread.
            update.set(LEASE_END_KEY, clock.getTime() + DEFAULT_LEASE_DURATION_MILLIS);
            ClusterNodeInfoDocument old = store.findAndUpdate(CLUSTER_NODES, update);
            if (old != null) {
                LOG.info("Acquired (broke) recovery lock for cluster id {}. " +
                        "Previous lock owner: {}", doc.getClusterId(), recoveryBy);
            }
            return old != null;
        } catch (RuntimeException ex) {
            LOG.error("Failed to break the recovery lock for clusterNodeId " +
                    doc.getClusterId(), ex);
            throw (ex);
        }
    }
}
