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

package org.apache.jackrabbit.oak.plugins.index.importer;

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lock implementation for clustered scenario. The locking is done
 * by setting the lease time for the lane to distant future which
 * prevent AsyncIndexUpdate from  running.
 */
public class ClusterNodeStoreLock implements AsyncIndexerLock<ClusteredLockToken> {
    /**
     * Use a looong lease time to ensure that async indexer does not start
     * in between the import process which can take some time
     */
    private static final long LOCK_TIMEOUT = TimeUnit.DAYS.toMillis(100);
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final NodeStore nodeStore;
    private final Clock clock;

    public ClusterNodeStoreLock(NodeStore nodeStore){
        this(nodeStore, Clock.SIMPLE);
    }

    public ClusterNodeStoreLock(NodeStore nodeStore, Clock clock) {
        this.nodeStore = nodeStore;
        this.clock = clock;
    }

    @Override
    public ClusteredLockToken lock(String asyncIndexerLane) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        NodeBuilder async = builder.child(":async");

        String leaseName = AsyncIndexUpdate.leasify(asyncIndexerLane);
        long leaseEndTime = clock.getTime() + LOCK_TIMEOUT;

        if (async.hasProperty(leaseName)){
            log.info("AsyncIndexer found to be running currently. Lease update would cause its" +
                    "commit to fail. Such a failure should be ignored");
        }

        //TODO Attempt few times if merge failure due to current running indexer cycle
        async.setProperty(leaseName, leaseEndTime);
        async.setProperty(lockName(asyncIndexerLane), true);
        NodeStoreUtils.mergeWithConcurrentCheck(nodeStore, builder);

        log.info("Acquired the lock for async indexer lane [{}]", asyncIndexerLane);

        return new ClusteredLockToken(asyncIndexerLane, leaseEndTime);
    }

    @Override
    public void unlock(ClusteredLockToken token) throws CommitFailedException {
        String leaseName = AsyncIndexUpdate.leasify(token.laneName);

        NodeBuilder builder = nodeStore.getRoot().builder();
        NodeBuilder async = builder.child(":async");
        async.removeProperty(leaseName);
        async.removeProperty(lockName(token.laneName));
        NodeStoreUtils.mergeWithConcurrentCheck(nodeStore, builder);
        log.info("Remove the lock for async indexer lane [{}]", token.laneName);
    }

    public boolean isLocked(String asyncIndexerLane) {
        NodeState async = nodeStore.getRoot().getChildNode(":async");
        String leaseName = lockName(asyncIndexerLane);
        return async.hasProperty(leaseName);
    }

    private static String lockName(String asyncIndexerLane) {
        return asyncIndexerLane + "-lock";
    }
}

class ClusteredLockToken implements AsyncIndexerLock.LockToken {
    final String laneName;
    final long timeout;

    ClusteredLockToken(String laneName, long timeout) {
        this.laneName = laneName;
        this.timeout = timeout;
    }
}
