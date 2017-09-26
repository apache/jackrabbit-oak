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

package org.apache.jackrabbit.oak.plugins.document.secondary;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.EvictingQueue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStateCache;
import org.apache.jackrabbit.oak.plugins.document.NodeStateDiffer;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SecondaryStoreCache implements DocumentNodeStateCache, SecondaryStoreRootObserver {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private static final AbstractDocumentNodeState[] EMPTY = new AbstractDocumentNodeState[0];
    private final NodeStore store;
    private final PathFilter pathFilter;
    private final NodeStateDiffer differ;
    private final MeterStats unknownPaths;
    private final MeterStats knownMissed;
    private final MeterStats knownMissedOld;
    private final MeterStats knownMissedNew;
    private final MeterStats knownMissedInRange;
    private final MeterStats headRevMatched;
    private final MeterStats prevRevMatched;
    private final int maxSize = 10000;
    private final EvictingQueue<AbstractDocumentNodeState> queue;
    private volatile AbstractDocumentNodeState[] previousRoots = EMPTY;

    public SecondaryStoreCache(NodeStore nodeStore, NodeStateDiffer differ, PathFilter pathFilter,
                               StatisticsProvider statisticsProvider) {
        this.differ = differ;
        this.store = nodeStore;
        this.pathFilter = pathFilter;
        this.unknownPaths = statisticsProvider.getMeter("DOCUMENT_CACHE_SEC_UNKNOWN", StatsOptions.DEFAULT);
        this.knownMissed = statisticsProvider.getMeter("DOCUMENT_CACHE_SEC_KNOWN_MISSED", StatsOptions.DEFAULT);
        this.knownMissedOld = statisticsProvider.getMeter("DOCUMENT_CACHE_SEC_KNOWN_MISSED_OLD", StatsOptions.DEFAULT);
        this.knownMissedNew = statisticsProvider.getMeter("DOCUMENT_CACHE_SEC_KNOWN_MISSED_NEW", StatsOptions.DEFAULT);
        this.knownMissedInRange = statisticsProvider.getMeter("DOCUMENT_CACHE_SEC_KNOWN_MISSED_IN_RANGE", StatsOptions
                .DEFAULT);
        this.headRevMatched = statisticsProvider.getMeter("DOCUMENT_CACHE_SEC_HEAD", StatsOptions.DEFAULT);
        this.prevRevMatched = statisticsProvider.getMeter("DOCUMENT_CACHE_SEC_OLD", StatsOptions.DEFAULT);
        this.queue = EvictingQueue.create(maxSize);
    }

    @CheckForNull
    @Override
    public AbstractDocumentNodeState getDocumentNodeState(String path, RevisionVector rootRevision,
                                                    RevisionVector lastRev) {
        //TODO We might need skip the calls if they occur due to SecondaryStoreObserver
        //doing the diff or in the startup when we try to sync the state
        PathFilter.Result result = pathFilter.filter(path);
        if (result != PathFilter.Result.INCLUDE) {
            unknownPaths.mark();
            return null;
        }

        if (!DelegatingDocumentNodeState.hasMetaProps(store.getRoot())){
            return null;
        }

        AbstractDocumentNodeState currentRoot = DelegatingDocumentNodeState.wrap(store.getRoot(), differ);

        //If the root rev is < lastRev then secondary store is lagging and would
        //not have the matching result
        if (lastRev.compareTo(currentRoot.getLastRevision()) > 0){
            return null;
        }

        AbstractDocumentNodeState nodeState = findByMatchingLastRev(currentRoot, path, lastRev);
        if (nodeState != null){
            headRevMatched.mark();
            return nodeState;
        }

        AbstractDocumentNodeState matchingRoot = findMatchingRoot(rootRevision);
        if (matchingRoot != null){
            NodeState state = NodeStateUtils.getNode(matchingRoot, path);
            if (state.exists()){
                AbstractDocumentNodeState docState = asDocState(state);
                prevRevMatched.mark();
                return docState;
            }
        }

        knownMissed.mark();
        return null;
    }

    @Override
    public boolean isCached(String path) {
        return pathFilter.filter(path) == PathFilter.Result.INCLUDE;
    }

    @CheckForNull
    private AbstractDocumentNodeState findByMatchingLastRev(AbstractDocumentNodeState root, String path,
                                                      RevisionVector lastRev){
        NodeState state = root;

        for (String name : PathUtils.elements(path)) {
            state = state.getChildNode(name);

            if (!state.exists()){
                return null;
            }

            //requested lastRev is > current node lastRev then no need to check further
            if (lastRev.compareTo(asDocState(state).getLastRevision()) > 0){
                return null;
            }
        }

        AbstractDocumentNodeState docState = asDocState(state);
        if (lastRev.equals(docState.getLastRevision())) {
            headRevMatched.mark();
            return docState;
        }

        return null;
    }

    @CheckForNull
    private AbstractDocumentNodeState findMatchingRoot(RevisionVector rr) {
        if (isEmpty()){
            return null;
        }

        //Use a local variable as the array can get changed in process
        AbstractDocumentNodeState[] roots = previousRoots;
        AbstractDocumentNodeState latest = roots[roots.length - 1];
        AbstractDocumentNodeState oldest = roots[0];

        if (rr.compareTo(latest.getRootRevision()) > 0){
            knownMissedNew.mark();
            return null;
        }

        if (rr.compareTo(oldest.getRootRevision()) < 0){
            knownMissedOld.mark();
            return null;
        }

        AbstractDocumentNodeState result = findMatchingRoot(roots, rr);
        if (result != null){
            return result;
        }
        knownMissedInRange.mark();
        return null;
    }

    @Override
    public void contentChanged(@Nonnull AbstractDocumentNodeState root) {
        synchronized (queue){
            //TODO Possibly can be improved
            queue.add(root);
            previousRoots = queue.toArray(EMPTY);
        }
    }

    private boolean isEmpty() {
        return previousRoots.length == 0;
    }


    static AbstractDocumentNodeState findMatchingRoot(AbstractDocumentNodeState[] roots, RevisionVector key) {
        int low = 0;
        int high = roots.length - 1;

        //Perform a binary search as the array is sorted in ascending order
        while (low <= high) {
            int mid = (low + high) >>> 1;
            AbstractDocumentNodeState midVal = roots[mid];
            int cmp = midVal.getRootRevision().compareTo(key);

            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return midVal; // key found
            }
        }
        return null;  // key not found.
    }

    private static AbstractDocumentNodeState asDocState(NodeState state) {
        return (AbstractDocumentNodeState)state;
    }

}
