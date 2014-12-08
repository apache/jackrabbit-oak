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

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.mergeSorted;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.CheckForNull;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoMissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.util.MapFactory;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for recovering potential missing _lastRev updates of nodes due to crash of a node.
 */
public class LastRevRecoveryAgent {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DocumentNodeStore nodeStore;

    private final MissingLastRevSeeker missingLastRevUtil;

    public LastRevRecoveryAgent(DocumentNodeStore nodeStore) {
        this.nodeStore = nodeStore;

        if (nodeStore.getDocumentStore() instanceof MongoDocumentStore) {
            this.missingLastRevUtil =
                    new MongoMissingLastRevSeeker((MongoDocumentStore) nodeStore.getDocumentStore());
        } else {
            this.missingLastRevUtil = new MissingLastRevSeeker(nodeStore.getDocumentStore());
        }
    }

    /**
     * Recover the correct _lastRev updates for potentially missing candidate nodes.
     * 
     * @param clusterId the cluster id for which the _lastRev are to be recovered
     * @return the int the number of restored nodes
     */
    public int recover(int clusterId) {
        ClusterNodeInfoDocument nodeInfo = missingLastRevUtil.getClusterNodeInfo(clusterId);

        //TODO Currently leaseTime remains same per cluster node. If this
        //is made configurable then it should be read from DB entry
        final long leaseTime = ClusterNodeInfo.DEFAULT_LEASE_DURATION_MILLIS;
        final long asyncDelay = nodeStore.getAsyncDelay();

        if (nodeInfo != null) {
            // Check if _lastRev recovery needed for this cluster node
            // state is Active && recoveryLock not held by someone
            if (isRecoveryNeeded(nodeInfo)) {
                long leaseEnd = nodeInfo.getLeaseEndTime();

                // retrieve the root document's _lastRev
                NodeDocument root = missingLastRevUtil.getRoot();
                Revision lastRev = root.getLastRev().get(clusterId);

                // start time is the _lastRev timestamp of this cluster node
                final long startTime;
                //lastRev can be null if other cluster node did not got
                //chance to perform lastRev rollup even once
                if (lastRev != null) {
                    startTime = lastRev.getTimestamp();
                } else {
                    startTime = leaseEnd - leaseTime - asyncDelay;
                }

                log.info("Recovering candidates modified after: [{}] for clusterId [{}]",
                        Utils.timestampToString(startTime), clusterId);

                return recoverCandidates(clusterId, startTime);
            }
        }

        log.debug("No recovery needed for clusterId {}", clusterId);
        return 0;
    }

    /**
     * Recover the correct _lastRev updates for the given candidate nodes.
     *
     * @param suspects the potential suspects
     * @param clusterId the cluster id for which _lastRev recovery needed
     * @return the number of documents that required recovery.
     */
    public int recover(Iterator<NodeDocument> suspects, int clusterId) {
        return recover(suspects, clusterId, false);
    }

    /**
     * Recover the correct _lastRev updates for the given candidate nodes.
     * 
     * @param suspects the potential suspects
     * @param clusterId the cluster id for which _lastRev recovery needed
     * @param dryRun if {@code true}, this method will only perform a check
     *               but not apply the changes to the _lastRev fields.
     * @return the number of documents that required recovery. This method
     *          returns the number of the affected documents even if
     *          {@code dryRun} is set true and no document was changed.
     */
    public int recover(Iterator<NodeDocument> suspects,
                       int clusterId, boolean dryRun) {
        UnsavedModifications unsaved = new UnsavedModifications();
        UnsavedModifications unsavedParents = new UnsavedModifications();

        //Map of known last rev of checked paths
        Map<String, Revision> knownLastRevs = MapFactory.getInstance().create();

        long count = 0;
        while (suspects.hasNext()) {
            NodeDocument doc = suspects.next();
            count++;
            if (count % 100000 == 0) {
                log.info("Scanned {} suspects so far...", count);
            }

            Revision currentLastRev = doc.getLastRev().get(clusterId);
            if (currentLastRev != null) {
                knownLastRevs.put(doc.getPath(), currentLastRev);
            }
            // 1. determine last committed modification on document
            Revision lastModifiedRev = determineLastModification(doc, clusterId);

            Revision lastRevForParents = Utils.max(lastModifiedRev, currentLastRev);

            //If both currentLastRev and lostLastRev are null it means
            //that no change is done by suspect cluster on this document
            //so nothing needs to be updated. Probably it was only changed by
            //other cluster nodes. If this node is parent of any child node which
            //has been modified by cluster then that node roll up would
            //add this node path to unsaved

            //2. Update lastRev for parent paths aka rollup
            if (lastRevForParents != null) {
                String path = doc.getPath();
                while (true) {
                    if (PathUtils.denotesRoot(path)) {
                        break;
                    }
                    path = PathUtils.getParentPath(path);
                    unsavedParents.put(path, lastRevForParents);
                }
            }
        }

        for (String parentPath : unsavedParents.getPaths()) {
            Revision calcLastRev = unsavedParents.get(parentPath);
            Revision knownLastRev = knownLastRevs.get(parentPath);

            //Copy the calcLastRev of parent only if they have changed
            //In many case it might happen that parent have consistent lastRev
            //This check ensures that unnecessary updates are not made
            if (knownLastRev == null
                    || calcLastRev.compareRevisionTime(knownLastRev) > 0) {
                unsaved.put(parentPath, calcLastRev);
            }
        }

        //Note the size before persist as persist operation
        //would empty the internal state
        int size = unsaved.getPaths().size();
        String updates = unsaved.toString();

        if (dryRun) {
            log.info("Dry run of lastRev recovery identified [{}] documents for " +
                    "cluster node [{}]: {}", size, clusterId, updates);
        } else {
            //UnsavedModifications is designed to be used in concurrent
            //access mode. For recovery case there is no concurrent access
            //involve so just pass a new lock instance
            unsaved.persist(nodeStore, new ReentrantLock());

            log.info("Updated lastRev of [{}] documents while performing lastRev recovery for " +
                    "cluster node [{}]: {}", size, clusterId, updates);
        }

        return size;
    }

    /**
     * Retrieves possible candidates which have been modified after the given
     * {@code startTime} and recovers the missing updates.
     * 
     * @param clusterId the cluster id
     * @param startTime the start time
     * @return the int the number of restored nodes
     */
    private int recoverCandidates(final int clusterId, final long startTime) {
        boolean lockAcquired = missingLastRevUtil.acquireRecoveryLock(clusterId);

        //TODO What if recovery is being performed for current clusterNode by some other node
        //should we halt the startup
        if(!lockAcquired){
            log.info("Last revision recovery already being performed by some other node. " +
                    "Would not attempt recovery");
            return 0;
        }

        Iterable<NodeDocument> suspects = missingLastRevUtil.getCandidates(startTime);
        log.debug("Performing Last Revision recovery for cluster {}", clusterId);

        try {
            return recover(suspects.iterator(), clusterId);
        } finally {
            Utils.closeIfCloseable(suspects);
            // Relinquish the lock on the recovery for the cluster on the clusterInfo
            missingLastRevUtil.releaseRecoveryLock(clusterId);
        }
    }

    /**
     * Determines the last committed modification to the given document by
     * a {@code clusterId}.
     * 
     * @param doc a document.
     * @param clusterId clusterId for which the last committed modification is
     *                  looked up.
     * @return the commit revision of the last modification by {@code clusterId}
     *          to the given document.
     */
    @CheckForNull
    private Revision determineLastModification(NodeDocument doc, int clusterId) {
        ClusterPredicate cp = new ClusterPredicate(clusterId);

        // Merge sort the revs for which changes have been made
        // to this doc

        // localMap always keeps the most recent valid commit entry
        // per cluster node so looking into that should be sufficient
        Iterable<Revision> revs = mergeSorted(of(
                filter(doc.getLocalCommitRoot().keySet(), cp),
                filter(doc.getLocalRevisions().keySet(), cp)),
                StableRevisionComparator.REVERSE
                );

        Revision lastModified = null;
        // Look for latest valid revision
        for (Revision rev : revs) {
            lastModified = Utils.max(lastModified, doc.getCommitRevision(rev));
        }
        return lastModified;
    }

    /**
     * Determines if any of the cluster node failed to renew its lease and
     * did not properly shutdown. If any such cluster node is found then are potential
     * candidates for last rev recovery
     *
     * @return true if last rev recovery needs to be performed for any of the cluster nodes
     */
    public boolean isRecoveryNeeded(){
        return missingLastRevUtil.isRecoveryNeeded(nodeStore.getClock().getTime());
    }

    public void performRecoveryIfNeeded(){
        if(isRecoveryNeeded()){
            List<Integer> clusterIds = getRecoveryCandidateNodes();
            log.info("Starting last revision recovery for following clusterId {}", clusterIds);
            for(int clusterId : clusterIds){
                recover(clusterId);
            }
        }
    }
    
    /**
     * Gets the _lastRev recovery candidate cluster nodes.
     *
     * @return the recovery candidate nodes
     */
    public List<Integer> getRecoveryCandidateNodes() {
        Iterable<ClusterNodeInfoDocument> clusters = missingLastRevUtil.getAllClusters();
        List<Integer> candidateClusterNodes = Lists.newArrayList();
        
        for (ClusterNodeInfoDocument nodeInfo : clusters) {
            if (isRecoveryNeeded(nodeInfo)) {
                candidateClusterNodes.add(Integer.valueOf(nodeInfo.getId()));
            }
        }
        
        return candidateClusterNodes;
    }
    
    private boolean isRecoveryNeeded(ClusterNodeInfoDocument nodeInfo) {
        if (nodeInfo != null) {
            // Check if _lastRev recovery needed for this cluster node
            // state is Active && currentTime past the leaseEnd time && recoveryLock not held by someone
            if (nodeInfo.isActive()
                    && nodeStore.getClock().getTime() > nodeInfo.getLeaseEndTime()
                    && !nodeInfo.isBeingRecovered()) {
                return true;
            }
        }
        return false;
    }
    
    private static class ClusterPredicate implements Predicate<Revision> {
        private final int clusterId;

        private ClusterPredicate(int clusterId) {
            this.clusterId = clusterId;
        }

        @Override
        public boolean apply(Revision input) {
            return clusterId == input.getClusterId();
        }
    }
}

