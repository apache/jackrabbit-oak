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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS;
import static org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RecoveryLockTest {

    private DocumentStore store = new MemoryDocumentStore();

    private Clock clock = new Clock.Virtual();

    private ExecutorService executor = Executors.newCachedThreadPool();

    private RecoveryLock lock1 = new RecoveryLock(store, clock, 1);
    private RecoveryLock lock2 = new RecoveryLock(store, clock, 2);

    private ClusterNodeInfo info1;
    private ClusterNodeInfo info2;

    @Before
    public void before() throws Exception {
        clock.waitUntil(System.currentTimeMillis());
        ClusterNodeInfo.setClock(clock);
        info1 = ClusterNodeInfo.getInstance(store, RecoveryHandler.NOOP,
                null, "node1", 1);
    }

    @After
    public void after() {
        ClusterNodeInfo.resetClockToDefault();
    }

    @Test
    public void recoveryNotNeeded() {
        assertFalse(lock1.acquireRecoveryLock(2));
    }

    @Test
    public void acquireUnknown() {
        assertFalse(lock2.acquireRecoveryLock(1));
    }

    @Test
    public void releaseRemovedClusterNodeInfo() throws Exception {
        clock.waitUntil(info1.getLeaseEndTime() + DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS);
        assertTrue(lock1.acquireRecoveryLock(2));
        store.remove(CLUSTER_NODES, String.valueOf(info1.getId()));
        try {
            lock1.releaseRecoveryLock(false);
            fail("Must fail with DocumentStoreException");
        } catch (DocumentStoreException e) {
            assertThat(e.getMessage(), containsString("does not exist"));
        }
    }

    @Test
    public void acquireAfterLeaseEnd() throws Exception {
        clock.waitUntil(info1.getLeaseEndTime() + DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS);
        assertTrue(lock1.acquireRecoveryLock(2));
        ClusterNodeInfoDocument c = infoDocument(1);
        assertTrue(c.isActive());
        assertTrue(c.isBeingRecovered());
        assertEquals(Long.valueOf(2), c.getRecoveryBy());
        assertNotNull(c.get(ClusterNodeInfo.LEASE_END_KEY));
    }

    @Test
    public void successfulRecovery() throws Exception {
        clock.waitUntil(info1.getLeaseEndTime() + DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS);
        assertTrue(lock1.acquireRecoveryLock(2));
        lock1.releaseRecoveryLock(true);
        ClusterNodeInfoDocument c = infoDocument(1);
        assertFalse(c.isActive());
        assertFalse(c.isBeingRecovered());
        assertFalse(c.isBeingRecoveredBy(2));
        assertNull(c.get(ClusterNodeInfo.LEASE_END_KEY));
    }

    @Test
    public void unsuccessfulRecovery() throws Exception {
        clock.waitUntil(info1.getLeaseEndTime() + DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS);
        assertTrue(lock1.acquireRecoveryLock(2));
        lock1.releaseRecoveryLock(false);
        ClusterNodeInfoDocument c = infoDocument(1);
        assertTrue(c.isActive());
        assertFalse(c.isBeingRecovered());
        assertFalse(c.isBeingRecoveredBy(2));
        assertNotNull(c.get(ClusterNodeInfo.LEASE_END_KEY));
        assertThat(c.getLeaseEndTime(), lessThan(clock.getTime()));
    }

    @Test
    public void inactive() {
        info1.dispose();
        assertFalse(lock1.acquireRecoveryLock(1));
        assertFalse(lock1.acquireRecoveryLock(2));
    }

    @Test
    public void selfRecoveryWithinDeadline() throws Exception {
        // expire clusterId 1
        clock.waitUntil(info1.getLeaseEndTime() + DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS);
        ClusterNodeInfoDocument c = infoDocument(1);
        MissingLastRevSeeker seeker = new MissingLastRevSeeker(store, clock);
        assertTrue(c.isRecoveryNeeded(clock.getTime()));
        assertFalse(c.isBeingRecovered());

        Semaphore recovering = new Semaphore(0);
        Semaphore recovered = new Semaphore(0);
        // simulate new startup and get info again
        Future<ClusterNodeInfo> infoFuture = executor.submit(() ->
                ClusterNodeInfo.getInstance(store, clusterId -> {
                    assertTrue(lock1.acquireRecoveryLock(1));
                    recovering.release();
                    recovered.acquireUninterruptibly();
                    lock1.releaseRecoveryLock(true);
                    return true;
        }, null, "node1", 1));
        // wait until submitted task is in recovery
        recovering.acquireUninterruptibly();

        // check state again
        c = infoDocument(1);
        assertTrue(c.isRecoveryNeeded(clock.getTime()));
        assertTrue(c.isBeingRecovered());
        assertTrue(c.isBeingRecoveredBy(1));
        // clusterId 2 must not be able to acquire (break) the recovery lock
        assertFalse(lock1.acquireRecoveryLock(2));

        // signal recovery to continue
        recovered.release();
        ClusterNodeInfo info1 = infoFuture.get();
        assertEquals(1, info1.getId());

        // check state again
        c = infoDocument(1);
        assertFalse(c.isRecoveryNeeded(clock.getTime()));
        assertFalse(c.isBeingRecovered());
        assertFalse(c.isBeingRecoveredBy(1));

        // neither must be able to acquire a recovery lock on
        // an active entry with a valid lease
        assertFalse(lock1.acquireRecoveryLock(1));
        assertFalse(lock1.acquireRecoveryLock(2));
    }

    private ClusterNodeInfoDocument infoDocument(int clusterId) {
        ClusterNodeInfoDocument doc = store.find(CLUSTER_NODES, String.valueOf(clusterId));
        assertNotNull(doc);
        return doc;
    }
}
