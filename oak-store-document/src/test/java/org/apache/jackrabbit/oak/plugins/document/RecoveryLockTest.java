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

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RecoveryLockTest {

    private DocumentStore store = new MemoryDocumentStore();

    private Clock clock = new Clock.Virtual();

    private ExecutorService executor = Executors.newCachedThreadPool();

    private RecoveryLock lock1 = new RecoveryLock(store, clock, 1);
    private RecoveryLock lock2 = new RecoveryLock(store, clock, 2);

    private ClusterNodeInfo info1;

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
        new ExecutorCloser(executor).close();
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

    @Test
    // OAK-9401: Reproduce a bug that happens when the cluster is not active having a null leaseEndTime
    public void breakRecoveryLockOfNotActiveCluster() throws Exception {
        // Create a mocked version of the DocumentStore, to replace a method later during the test
        DocumentStore store = spy(new MemoryDocumentStore());

        info1 = ClusterNodeInfo.getInstance(store, RecoveryHandler.NOOP,
                null, "node1", 1);
        RecoveryLock recLock = new RecoveryLock(store, clock, 1);

        // expire clusterId 1
        clock.waitUntil(info1.getLeaseEndTime() + DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS);

        Semaphore recovering = new Semaphore(0);
        Semaphore recovered = new Semaphore(0);
        // simulate new startup and get info again
        executor.submit(() ->
                ClusterNodeInfo.getInstance(store, clusterId -> {
                    assertTrue(recLock.acquireRecoveryLock(1));
                    recovering.release();
                    recovered.acquireUninterruptibly();
                    recLock.releaseRecoveryLock(true);
                    return true;
        }, null, "node1", 1));
        // wait until submitted task is in recovery
        recovering.acquireUninterruptibly();

        // OAK-9401: Reproduce a bug that happens when the cluster is not active having a null leaseEndTime
        // create a mocked copy of the ClusterNodeInfoDocument to be able to edit it, as the original is immutable
        ClusterNodeInfoDocument realClusterInfo = spy(store.find(CLUSTER_NODES, String.valueOf(1)));
        ClusterNodeInfoDocument mockClusterInfo = spy(CLUSTER_NODES.newDocument(store));
        realClusterInfo.deepCopy(mockClusterInfo);

        mockClusterInfo.put(ClusterNodeInfo.LEASE_END_KEY, null);
        doReturn(false).when(mockClusterInfo).isActive();
        when(store.find(CLUSTER_NODES, String.valueOf(1))).thenCallRealMethod().thenReturn(mockClusterInfo);

        // clusterId 2 should be able to acquire (break) the recovery lock, instead of 
        // throwing "java.lang.NullPointerException: Lease End Time not set"
        assertTrue(recLock.acquireRecoveryLock(2));

        // let submitted task complete
        recovered.release();
    }

    private ClusterNodeInfoDocument infoDocument(int clusterId) {
        ClusterNodeInfoDocument doc = store.find(CLUSTER_NODES, String.valueOf(clusterId));
        assertNotNull(doc);
        return doc;
    }
}
