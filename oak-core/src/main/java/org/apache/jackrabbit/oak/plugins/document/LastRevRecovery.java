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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.CheckForNull;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.mergeSorted;

public class LastRevRecovery {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DocumentNodeStore nodeStore;

    public LastRevRecovery(DocumentNodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    public void recover(Iterator<NodeDocument> suspects, int clusterId) {
        UnsavedModifications unsaved = new UnsavedModifications();

        //Set of parent path whose lastRev has been updated based on
        //last rev information obtained from suspects. Its possible
        //that lastRev for such parents present in DS has
        //higher value. So before persisting the changes for these
        //paths we need to ensure that there actual lastRev is lesser
        //than one being set via unsaved
        Set<String> unverifiedParentPaths = Sets.newHashSet();

        //Map of known last rev of checked paths
        Map<String, Revision> knownLastRevs = Maps.newHashMap();

        while (suspects.hasNext()) {
            NodeDocument doc = suspects.next();

            Revision currentLastRev = doc.getLastRev().get(clusterId);
            if (currentLastRev != null) {
                knownLastRevs.put(doc.getPath(), currentLastRev);
            }

            Revision lostLastRev = determineMissedLastRev(doc, clusterId);

            //lastRev is consistent
            if (lostLastRev == null) {
                continue;
            }

            //1. Update lastRev for this doc
            unsaved.put(doc.getPath(), lostLastRev);

            //2. Update lastRev for parent paths
            String path = doc.getPath();
            while (true) {
                if (PathUtils.denotesRoot(path)) {
                    break;
                }
                path = PathUtils.getParentPath(path);
                unsaved.put(path, lostLastRev);
                unverifiedParentPaths.add(path);
            }
        }

        //By now we have iterated over all suspects so remove entries for paths
        //whose lastRev have been determined on the basis of state obtained from
        //DS
        Iterator<String> unverifiedParentPathsItr = unverifiedParentPaths.iterator();
        while (unverifiedParentPathsItr.hasNext()) {
            String unverifiedParentPath = unverifiedParentPathsItr.next();
            Revision knownRevision = knownLastRevs.get(unverifiedParentPath);
            if (knownRevision != null) {
                unverifiedParentPathsItr.remove();
                unsaved.put(unverifiedParentPath, knownRevision);
            }
        }

        //Now for the left over unverifiedParentPaths determine the lastRev
        //from DS and add them to unsaved. This ensures that we do not set lastRev
        //to a lower value

        //TODO For Mongo case we can fetch such documents more efficiently
        //via batch fetch
        for (String path : unverifiedParentPaths) {
            NodeDocument doc = getDocument(path);
            if (doc != null) {
                Revision lastRev = doc.getLastRev().get(clusterId);
                unsaved.put(path, lastRev);
            }
        }

        int size = unsaved.getPaths().size();

        if (log.isDebugEnabled()) {
            log.debug("Last revision for following documents would be updated {}", unsaved.getPaths());
        }

        //UnsavedModifications is designed to be used in concurrent
        //access mode. For recovery case there is no concurrent access
        //involve so just pass a new lock instance
        unsaved.persist(nodeStore, new ReentrantLock());

        log.info("Updated lastRev of [{}] documents while performing lastRev recovery for " +
                "cluster node [{}]", size, clusterId);
    }

    /**
     * Determines the last revision value which needs to set for given clusterId
     * on the passed document. If the last rev entries are consisted
     *
     * @param doc       NodeDocument where lastRev entries needs to be fixed
     * @param clusterId clusterId for which lastRev has to be checked
     * @return lastRev which needs to be updated. <tt>null</tt> if no
     * updated is required i.e. lastRev entries are valid
     */
    @CheckForNull
    private Revision determineMissedLastRev(NodeDocument doc, int clusterId) {
        Revision currentLastRev = doc.getLastRev().get(clusterId);
        if (currentLastRev == null) {
            currentLastRev = new Revision(0, 0, clusterId);
        }

        ClusterPredicate cp = new ClusterPredicate(clusterId);

        //Merge sort the revs for which changes have been made
        //to this doc

        //TODO Would looking into the Local map be sufficient
        //Probably yes as entries for a particular cluster node
        //are split by that cluster only
        Iterable<Revision> revs = mergeSorted(of(
                        filter(doc.getLocalCommitRoot().keySet(), cp),
                        filter(doc.getLocalRevisions().keySet(), cp)),
                StableRevisionComparator.REVERSE
        );

        //Look for latest valid revision > currentLastRev
        //if found then lastRev needs to be fixed
        for (Revision rev : revs) {
            if (rev.compareRevisionTime(currentLastRev) > 0) {
                if (doc.isCommitted(rev)) {
                    return rev;
                }
            } else {
                //No valid revision found > currentLastRev
                //indicates that lastRev is valid for given clusterId
                //and no further checks are required
                break;
            }
        }
        return null;
    }

    private NodeDocument getDocument(String path) {
        return nodeStore.getDocumentStore().find(Collection.NODES, Utils.getIdFromPath(path));
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
