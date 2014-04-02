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
import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoMissingLastRevSeeker;
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

    LastRevRecoveryAgent(DocumentNodeStore nodeStore) {
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

        if (nodeInfo != null) {
            long leaseEnd = nodeInfo.getLeaseEndTime();

            // Check if _lastRev recovery needed for this cluster node
            // state is Active && recoveryLock not held by someone
            if (isRecoveryNeeded(nodeInfo)) {            
                // retrieve the root document's _lastRev
                NodeDocument root = missingLastRevUtil.getRoot();
                Revision lastRev = root.getLastRev().get(clusterId);

                // start time is the _lastRev timestamp of this cluster node
                long startTime = lastRev.getTimestamp();

                // Endtime is the leaseEnd + the asyncDelay
                long endTime = leaseEnd + nodeStore.getAsyncDelay();

                log.info("Recovering candidates modified in time range : [{},{}] for clusterId [{}]",
                        Utils.timestampToString(startTime),
                        Utils.timestampToString(endTime), clusterId);

                return recoverCandidates(clusterId, startTime, endTime);
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
     * @return the int
     */
    public int recover(Iterator<NodeDocument> suspects, int clusterId) {
        UnsavedModifications unsaved = new UnsavedModifications();
        UnsavedModifications unsavedParents = new UnsavedModifications();

        //Map of known last rev of checked paths
        Map<String, Revision> knownLastRevs = Maps.newHashMap();

        while (suspects.hasNext()) {
            NodeDocument doc = suspects.next();

            Revision currentLastRev = doc.getLastRev().get(clusterId);
            if (currentLastRev != null) {
                knownLastRevs.put(doc.getPath(), currentLastRev);
            }
            Revision lostLastRev = determineMissedLastRev(doc, clusterId);

            //1. Update lastRev for this doc
            if (lostLastRev != null) {
                unsaved.put(doc.getPath(), lostLastRev);
            }

            Revision lastRevForParents = lostLastRev != null ? lostLastRev : currentLastRev;

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

        if (log.isDebugEnabled()) {
            log.debug("Last revision for following documents would be updated {}", unsaved
                    .getPaths());
        }

        //UnsavedModifications is designed to be used in concurrent
        //access mode. For recovery case there is no concurrent access
        //involve so just pass a new lock instance
        unsaved.persist(nodeStore, new ReentrantLock());

        log.info("Updated lastRev of [{}] documents while performing lastRev recovery for " +
                "cluster node [{}]", size, clusterId);

        return size;
    }

    /**
     * Retrieves possible candidates which have been modifed in the time range and recovers the
     * missing updates.
     * 
     * @param clusterId the cluster id
     * @param startTime the start time
     * @param endTime the end time
     * @return the int the number of restored nodes
     */
    private int recoverCandidates(final int clusterId, final long startTime, final long endTime) {
        boolean lockAcquired = missingLastRevUtil.acquireRecoveryLock(clusterId);

        //TODO What if recovery is being performed for current clusterNode by some other node
        //should we halt the startup
        if(!lockAcquired){
            log.info("Last revision recovery already being performed by some other node. " +
                    "Would not attempt recovery");
            return 0;
        }

        Iterable<NodeDocument> suspects = missingLastRevUtil.getCandidates(startTime, endTime);
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
     * Determines the last revision value which needs to set for given clusterId
     * on the passed document. If the last rev entries are consisted
     * 
     * @param doc NodeDocument where lastRev entries needs to be fixed
     * @param clusterId clusterId for which lastRev has to be checked
     * @return lastRev which needs to be updated. <tt>null</tt> if no
     *         updated is required i.e. lastRev entries are valid
     */
    @CheckForNull
    private Revision determineMissedLastRev(NodeDocument doc, int clusterId) {
        Revision currentLastRev = doc.getLastRev().get(clusterId);
        if (currentLastRev == null) {
            currentLastRev = new Revision(0, 0, clusterId);
        }

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

        // Look for latest valid revision > currentLastRev
        // if found then lastRev needs to be fixed
        for (Revision rev : revs) {
            if (rev.compareRevisionTime(currentLastRev) > 0) {
                if (doc.isCommitted(rev)) {
                    return rev;
                }
            } else {
                // No valid revision found > currentLastRev
                // indicates that lastRev is valid for given clusterId
                // and no further checks are required
                break;
            }
        }
        return null;
    }
    
    /**
     * Gets the _lastRev recovery candidate cluster nodes.
     *
     * @return the recovery candidate nodes
     */
    public List<String> getRecoveryCandidateNodes() {
        Iterable<ClusterNodeInfoDocument> clusters = missingLastRevUtil.getAllClusters();
        List<String> candidateClusterNodes = Lists.newArrayList();
        
        for (ClusterNodeInfoDocument nodeInfo : clusters) {
            if (isRecoveryNeeded(nodeInfo)) {
                candidateClusterNodes.add(nodeInfo.getId());
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

