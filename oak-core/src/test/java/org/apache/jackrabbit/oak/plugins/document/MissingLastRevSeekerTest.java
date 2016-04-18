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

import com.google.common.collect.Iterables;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.DEFAULT_LEASE_DURATION_MILLIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MissingLastRevSeekerTest {

    private Clock clock;
    private DocumentStore store;
    private MissingLastRevSeeker seeker;

    @Before
    public void before() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
        store = new MemoryDocumentStore();
        seeker = new MissingLastRevSeeker(store, clock);
    }

    @After
    public void after() throws Exception {
        ClusterNodeInfo.resetClockToDefault();
        Revision.resetClockToDefault();
    }

    @Test
    public void acquireRecoveryLockOnActiveClusterNode() {
        ClusterNodeInfo nodeInfo1 = ClusterNodeInfo.getInstance(store, 1);
        nodeInfo1.renewLease();

        assertFalse(seeker.acquireRecoveryLock(1, 2));
    }

    @Test
    public void acquireRecoveryLockOnInactiveClusterNode() {
        ClusterNodeInfo nodeInfo1 = ClusterNodeInfo.getInstance(store, 1);
        nodeInfo1.renewLease();
        nodeInfo1.dispose();

        assertFalse(seeker.acquireRecoveryLock(1, 2));
    }

    @Test
    public void acquireRecoveryLockOnExpiredLease() throws Exception {
        ClusterNodeInfo nodeInfo1 = ClusterNodeInfo.getInstance(store, 1);
        nodeInfo1.renewLease();
        // expire the lease
        clock.waitUntil(clock.getTime() + DEFAULT_LEASE_DURATION_MILLIS + 1);

        assertTrue(seeker.acquireRecoveryLock(1, 2));
    }

    @Test
    public void acquireRecoveryLockOnAlreadyLocked() throws Exception {
        ClusterNodeInfo nodeInfo1 = ClusterNodeInfo.getInstance(store, 1);
        nodeInfo1.renewLease();
        // expire the lease
        clock.waitUntil(clock.getTime() + DEFAULT_LEASE_DURATION_MILLIS + 1);

        ClusterNodeInfo nodeInfo2 = ClusterNodeInfo.getInstance(store, 2);
        nodeInfo2.renewLease();

        assertTrue(seeker.acquireRecoveryLock(1, 2));
        assertFalse(seeker.acquireRecoveryLock(1, 3));
    }

    @Test
    public void acquireRecoveryLockAgain() throws Exception {
        ClusterNodeInfo nodeInfo1 = ClusterNodeInfo.getInstance(store, 1);
        nodeInfo1.renewLease();
        // expire the lease
        clock.waitUntil(clock.getTime() + DEFAULT_LEASE_DURATION_MILLIS + 1);

        assertTrue(seeker.acquireRecoveryLock(1, 2));
        assertTrue(seeker.acquireRecoveryLock(1, 2));
    }

    @Test
    public void releaseRecoveryLockSuccessTrue() throws Exception {
        ClusterNodeInfo nodeInfo1 = ClusterNodeInfo.getInstance(store, 1);
        nodeInfo1.renewLease();
        // expire the lease
        clock.waitUntil(clock.getTime() + DEFAULT_LEASE_DURATION_MILLIS + 1);

        assertTrue(seeker.acquireRecoveryLock(1, 2));
        assertTrue(getClusterNodeInfo(1).isBeingRecovered());
        assertTrue(getClusterNodeInfo(1).isActive());
        seeker.releaseRecoveryLock(1, true);
        assertFalse(getClusterNodeInfo(1).isBeingRecovered());
        assertFalse(getClusterNodeInfo(1).isActive());
        // recovery not needed anymore
        assertFalse(seeker.isRecoveryNeeded(getClusterNodeInfo(1)));
        assertFalse(seeker.acquireRecoveryLock(1, 2));
    }

    @Test
    public void releaseRecoveryLockSuccessFalse() throws Exception {
        ClusterNodeInfo nodeInfo1 = ClusterNodeInfo.getInstance(store, 1);
        nodeInfo1.renewLease();
        // expire the lease
        clock.waitUntil(clock.getTime() + DEFAULT_LEASE_DURATION_MILLIS + 1);

        assertTrue(seeker.acquireRecoveryLock(1, 2));
        assertTrue(getClusterNodeInfo(1).isBeingRecovered());
        assertTrue(getClusterNodeInfo(1).isActive());
        seeker.releaseRecoveryLock(1, false);
        assertFalse(getClusterNodeInfo(1).isBeingRecovered());
        assertTrue(getClusterNodeInfo(1).isActive());
        // recovery still needed
        assertTrue(seeker.isRecoveryNeeded(getClusterNodeInfo(1)));
        assertTrue(seeker.acquireRecoveryLock(1, 2));
    }

    @Test
    public void isRecoveryNeeded() throws Exception {
        ClusterNodeInfo nodeInfo1 = ClusterNodeInfo.getInstance(store, 1);
        nodeInfo1.renewLease();
        // expire the lease
        clock.waitUntil(clock.getTime() + DEFAULT_LEASE_DURATION_MILLIS + 1);

        ClusterNodeInfo nodeInfo2 = ClusterNodeInfo.getInstance(store, 2);
        nodeInfo2.renewLease();

        assertTrue(seeker.isRecoveryNeeded());
        assertTrue(seeker.isRecoveryNeeded(getClusterNodeInfo(1)));
        assertFalse(seeker.isRecoveryNeeded(getClusterNodeInfo(2)));

        assertTrue(seeker.acquireRecoveryLock(1, 2));
        seeker.releaseRecoveryLock(1, true);

        assertFalse(seeker.isRecoveryNeeded());
        assertFalse(seeker.isRecoveryNeeded(getClusterNodeInfo(1)));
        assertFalse(seeker.isRecoveryNeeded(getClusterNodeInfo(2)));
    }

    @Test
    public void getAllClusterNodes() {
        assertEquals(0, Iterables.size(seeker.getAllClusters()));

        ClusterNodeInfo nodeInfo1 = ClusterNodeInfo.getInstance(store, 1);
        nodeInfo1.renewLease();

        assertEquals(1, Iterables.size(seeker.getAllClusters()));

        ClusterNodeInfo nodeInfo2 = ClusterNodeInfo.getInstance(store, 2);
        nodeInfo2.renewLease();

        assertEquals(2, Iterables.size(seeker.getAllClusters()));
    }

    @Test
    public void getClusterNodeInfo() {
        assertNull(getClusterNodeInfo(1));

        ClusterNodeInfo nodeInfo1 = ClusterNodeInfo.getInstance(store, 1);
        nodeInfo1.renewLease();

        assertNotNull(getClusterNodeInfo(1));
    }

    private ClusterNodeInfoDocument getClusterNodeInfo(int clusterId) {
        return seeker.getClusterNodeInfo(clusterId);
    }
}
