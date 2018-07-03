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

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES;

/**
 * Implements the recovery handler on startup of a {@link DocumentNodeStore}.
 */
class RecoveryHandlerImpl implements RecoveryHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveryHandlerImpl.class);

    private static final int COMMIT_VALUE_CACHE_SIZE = 10000;

    /**
     * The timeout in milliseconds to wait for the recovery performed by
     * another cluster node.
     */
    private long recoveryWaitTimeoutMS =
            Long.getLong("oak.recoveryWaitTimeoutMS", 60000);

    private final DocumentStore store;
    private final Clock clock;
    private final MissingLastRevSeeker lastRevSeeker;

    RecoveryHandlerImpl(DocumentStore store,
                        Clock clock,
                        MissingLastRevSeeker lastRevSeeker) {
        this.store = store;
        this.clock = clock;
        this.lastRevSeeker = lastRevSeeker;
    }

    @Override
    public boolean recover(int clusterId) {
        try {
            return recoverInternal(clusterId);
        } catch (DocumentStoreException e) {
            LOG.warn("Recovery failed for cluster node {}", clusterId, e);
            return false;
        }
    }

    private boolean recoverInternal(int clusterId)
            throws DocumentStoreException {
        NodeDocument root = Utils.getRootDocument(store);
        // prepare a context for recovery
        RevisionContext context = new RecoveryContext(
                root, clock, clusterId,
                new CachingCommitValueResolver(COMMIT_VALUE_CACHE_SIZE, root::getSweepRevisions));
        LastRevRecoveryAgent agent = new LastRevRecoveryAgent(
                store, context, lastRevSeeker, id -> {});
        long timeout = context.getClock().getTime() + recoveryWaitTimeoutMS;
        int numRecovered = agent.recover(clusterId, timeout);
        if (numRecovered == -1) {
            ClusterNodeInfoDocument doc = store.find(CLUSTER_NODES, String.valueOf(clusterId));
            String otherId = "n/a";
            if (doc != null) {
                otherId = String.valueOf(doc.get(ClusterNodeInfo.REV_RECOVERY_BY));
            }
            String msg = "This cluster node (" + clusterId + ") requires " +
                    "_lastRev recovery which is currently performed by " +
                    "another cluster node (" + otherId + "). Recovery is " +
                    "still ongoing after " + recoveryWaitTimeoutMS + " ms.";
            LOG.info(msg);
            return false;
        }

        return true;
    }

}
