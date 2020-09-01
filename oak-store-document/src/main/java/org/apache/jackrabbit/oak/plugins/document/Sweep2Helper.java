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

import java.util.Map;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods for sweep2 functionality introduced with OAK-9176.
 * Kept separate from DocumentNodeStore to limit its size.
 */
public class Sweep2Helper {

    private static final Logger LOG = LoggerFactory.getLogger(Sweep2Helper.class);

    static boolean isSweep2Necessary(DocumentStore store) {
        NodeDocument rootNodeDoc = store.find(Collection.NODES, Utils.getIdFromPath("/"));
        if (rootNodeDoc == null) {
            // that'd be very weird
            LOG.warn("isSweep2Necessary : cannot get root node - assuming no sweep2 needed");
            return false;
        }
    
        if (rootNodeDoc.get("_sweepRev") == null) {
            // this indicates a pre 1.8 repository upgrade (case 1).
            // no sweep2 is needed as it is embedded in the normal sweep[1].
            return false;
        }
    
        // in this case we have a post (>=) 1.8 repository
        // which might or might not have previously been a pre (<) 1.8
        // and we need to distinguish those 2 cases - which, to repeat, are:
        // 2) Oak >= 1.8 which never did an Oak <= 1.6 upgrade:
        //    -> no sweep2 is needed as OAK-9176 doesn't apply (the repository
        //       never ran <= 1.6)
        // 3) Oak >= 1.8 which was previously doing an Oak <= 1.6 upgrade:
        //    -> A (full) sweep2 is needed. This is the main case of OAK-9176.
        Map<Revision, String> bcValueMap = rootNodeDoc.getValueMap("_bc");
        Map<Revision, String> valueMap = rootNodeDoc.getValueMap(NodeDocument.REVISIONS);
        for (Map.Entry<Revision, String> entry : valueMap.entrySet()) {
            Revision rev = entry.getKey();
    
            // consider all clusterIds..
            String rawCommitValue = entry.getValue();
            String cv = rawCommitValue;
            if (cv == null) {
                // skip
                continue;
            }
            Revision cRev = Utils.resolveCommitRevision(rev, cv);
            if (cRev.equals(rev)) {
                // fine
                continue;
            } else {
                // then rev should be in the branch commit list
                if (bcValueMap.containsKey(rev)) {
                    // all good
                    continue;
                }
                // otherwise the "_bc" does *not* contain a branch commit
                // which we suspect it should contain.
                // that is an indicator of requiring a sweep2
                // it might not, however, be sufficient, ie it might (really?)
                // be that it got garbage collected away - in which
                // case we'd be doing an unnecessary sweep2.
                // so this is case 3)
                return true;
            }
        }
    
        // this is case 2
        return false;
    }

    /**
     * Acquires a cluster singleton lock for doing a sweep2 if a sweep2 is necessary.
     * If instructed this will check whether a sweep2 is necessary at all by
     * inspecting the relevant node document(s).
     * @param checkIfSweep2Necessary true permits this method to check
     * whether sweep2 is necessary, false instructs this method to assume a sweep2 is necessary
     * @return <ul>
     * <li>
     * &gt;0: the lock was successfully acquired and a sweep2 must now be done
     * by the local instance. The returned value represents a simple lock value
     * which needs to be provided for a successful unlock later on.
     * </li>
     * <li>
     * 0: a sweep2 maybe must be done, but cannot at this point. A later retry should be done.
     * This can happen when another instance is busy doing a sweep2 (and we
     * monitor that other instance until it is done) - or because no discovery-lite
     * status is available yet (so we don't know if the current owner of the sweep2 status
     * crashed or not and how the local instance fits into the picture)
     * </li>
     * <li>
     * -1: no sweep2 is necessary
     * </li>
     * </ul>
     */
    static long acquireSweep2LockIfNecessary(DocumentNodeStore ns, int clusterId, boolean checkIfSweep2Necessary) {
        Sweep2StatusDocument status = Sweep2StatusDocument.readFrom(ns.getDocumentStore());
        if (status != null && status.isSwept()) {
            // nothing left to do.
            // this should be the most frequent case.
            return -1;
        }
    
        if (status == null || !status.isSweeping()) {
            // unknown or invalid status, derive from root document if told to check
            if (checkIfSweep2Necessary && !isSweep2Necessary(ns.getDocumentStore())) {
                // if no sweep2 is necessary, then mark it so in the settings collection.
                if (!Sweep2StatusDocument.forceReleaseSweep2LockAndMarkSwept(ns.getDocumentStore(), clusterId)) {
                    LOG.error("acquireSweep2LockIfNecessary: could not set the sweep2 status. Sweep2 not necessary though.");
                }
                // if we concluded that no sweep2 is necessary but failed to update
                // the status in the settings collection we should continue anyway
                // (without doing a sweep2 in the background)
                return -1;
            } else {
                return Sweep2StatusDocument.acquireSweep2Lock(ns, clusterId);
            }
        }
        // otherwise the status is "sweeping", which could be by ourselves or by another instance
    
        int lockClusterId = status.getLockClusterId();
        if (lockClusterId == clusterId) {
            // the local instance was the originator of the sweeping lock, but likely crashed
            // hence we need to redo the work from scratch as we can't know if we finished it properly
            LOG.info("acquireSweep2LockIfNecessary : sweep2 status was sweeping, locked by own instance ({}). "
                    + "Another sweep2 is required.",
                    clusterId);
            return status.getLockValue();
        }
    
        // another instance marked as sweeping - check to see if it is still active or it might have crashed
        if (ClusterNodeInfoDocument.all(ns.getDocumentStore()).stream()
                .anyMatch(info -> info.getClusterId() == lockClusterId && info.isActive())) {
            // then another instance is busy sweep2-ing, which is fine.
            // but we should continue monitoring until that other instance is done
            LOG.debug("acquireSweep2LockIfNecessary : another instance (id {}) is (still) busy doing a sweep2.",
                    lockClusterId);
            return 0;
        }
    
        // otherwise the lockClusterId is no longer active, so we
        // try to overwrite/reacquire the lock for us
        return Sweep2StatusDocument.acquireSweep2Lock(ns, clusterId);
    }

}
