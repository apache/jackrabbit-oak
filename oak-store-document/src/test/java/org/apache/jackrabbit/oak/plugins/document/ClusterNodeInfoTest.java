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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClusterNodeInfoTest {

    private Clock clock;
    private TestStore store;
    private FailureHandler handler = new FailureHandler();

    @Before
    public void before() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        ClusterNodeInfo.setClock(clock);
        store = new TestStore();
    }

    @After
    public void after() {
        ClusterNodeInfo.resetClockToDefault();
    }

    @Test
    public void renewLease() throws Exception {
        ClusterNodeInfo info = newClusterNodeInfo(1);
        long leaseEnd = info.getLeaseEndTime();
        waitLeaseUpdateInterval();
        assertTrue(info.renewLease());
        assertTrue(info.getLeaseEndTime() > leaseEnd);
        assertFalse(handler.isLeaseFailure());
    }

    @Test
    public void renewLeaseExceptionBefore() throws Exception {
        ClusterNodeInfo info = newClusterNodeInfo(1);
        waitLeaseUpdateInterval();
        store.setFailBeforeUpdate(1);
        try {
            info.renewLease();
            fail("must fail with DocumentStoreException");
        } catch (DocumentStoreException e) {
            // expected
        }
        assertEquals(0, store.getFailBeforeUpdate());

        long leaseEnd = info.getLeaseEndTime();
        // must succeed next time
        waitLeaseUpdateInterval();
        assertTrue(info.renewLease());
        assertTrue(info.getLeaseEndTime() > leaseEnd);
        assertFalse(handler.isLeaseFailure());
    }

    // OAK-4770
    @Test
    public void renewLeaseExceptionAfter() throws Exception {
        ClusterNodeInfo info = newClusterNodeInfo(1);
        waitLeaseUpdateInterval();
        store.setFailAfterUpdate(1);
        try {
            info.renewLease();
            fail("must fail with DocumentStoreException");
        } catch (DocumentStoreException e) {
            // expected
        }
        assertEquals(0, store.getFailAfterUpdate());

        long leaseEnd = info.getLeaseEndTime();
        // must succeed next time
        waitLeaseUpdateInterval();
        assertTrue(info.renewLease());
        assertTrue(info.getLeaseEndTime() > leaseEnd);
        assertFalse(handler.isLeaseFailure());
    }

    @Test
    public void renewLeaseExceptionBeforeWithDelay() throws Exception {
        ClusterNodeInfo info = newClusterNodeInfo(1);
        waitLeaseUpdateInterval();
        store.setFailBeforeUpdate(1);
        // delay operations by half the lease time, this will
        // first delay the update and then delay the subsequent
        // find because of the exception on update. afterwards the
        // lease must be expired
        store.setDelayMillis(info.getLeaseTime() / 2);
        try {
            info.renewLease();
            fail("must throw DocumentStoreException");
        } catch (DocumentStoreException e) {
            // expected
        }
        assertTrue(info.getLeaseEndTime() < clock.getTime());
    }

    @Test
    public void renewLeaseExceptionAfterWithDelay() throws Exception {
        ClusterNodeInfo info = newClusterNodeInfo(1);
        long leaseEnd = info.getLeaseEndTime();
        waitLeaseUpdateInterval();
        store.setFailAfterUpdate(1);
        // delay operations by half the lease time, this will
        // first delay the update and then delay the subsequent
        // find because of the exception on update. afterwards
        // the leaseEnd must reflect the updated value
        store.setDelayMillis(info.getLeaseTime() / 2);
        try {
            info.renewLease();
            fail("must throw DocumentStoreException");
        } catch (DocumentStoreException e) {
            // expected
        }
        assertTrue(info.getLeaseEndTime() > leaseEnd);
    }

    @Test
    public void renewLeaseExceptionAfterFindFails() throws Exception {
        ClusterNodeInfo info = newClusterNodeInfo(1);
        long leaseEnd = info.getLeaseEndTime();
        waitLeaseUpdateInterval();
        store.setFailAfterUpdate(1);
        store.setFailFind(1);
        // delay operations by half the lease time, this will
        // first delay the update and then delay and fail the
        // subsequent find once.
        store.setDelayMillis(info.getLeaseTime() / 2);
        try {
            info.renewLease();
            fail("must throw DocumentStoreException");
        } catch (DocumentStoreException e) {
            // expected
        }
        assertEquals(0, store.getFailFind());
        // must not reflect the updated value, because retries
        // to read the current cluster node info document stops
        // once lease expires
        assertEquals(leaseEnd, info.getLeaseEndTime());
    }

    @Test
    public void renewLeaseExceptionAfterFindSucceedsEventually() throws Exception {
        ClusterNodeInfo info = newClusterNodeInfo(1);
        waitLeaseUpdateInterval();
        // delay operations by a sixth of the lease time, this will
        // first delay the update and then delay and fail the
        // subsequent find calls. find retries should eventually
        // succeed within the lease time
        store.setDelayMillis(info.getLeaseTime() / 6);
        store.setFailAfterUpdate(1);
        store.setFailFind(3);
        try {
            info.renewLease();
            fail("must throw DocumentStoreException");
        } catch (DocumentStoreException e) {
            // expected
        }
        // the three retries must eventually succeed within the lease time
        assertEquals(0, store.getFailFind());
        // must reflect the updated value
        assertTrue(info.getLeaseEndTime() > clock.getTime());
    }

    @Test
    public void renewLeaseDelayed() throws Exception {
        ClusterNodeInfo info = newClusterNodeInfo(1);
        clock.waitUntil(info.getLeaseEndTime() + ClusterNodeInfo.DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS);
        recoverClusterNode(1);
        try {
            info.renewLease();
            fail("must fail with DocumentStoreException");
        } catch (DocumentStoreException e) {
            // expected
        }
        assertLeaseFailure();
    }

    // OAK-4779
    @Test
    public void renewLeaseWhileRecoveryRunning() throws Exception {
        ClusterNodeInfo info = newClusterNodeInfo(1);
        // wait until after lease end
        clock.waitUntil(info.getLeaseEndTime() + ClusterNodeInfo.DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS);
        // simulate a started recovery
        MissingLastRevSeeker seeker = new MissingLastRevSeeker(store.getStore(), clock);
        assertTrue(seeker.acquireRecoveryLock(1, 42));
        // cluster node 1 must not be able to renew the lease now
        try {
            // must either return false
            assertFalse(info.renewLease());
        } catch (DocumentStoreException e) {
            // or throw an exception
        }
    }

    @Test
    public void renewLeaseTimedOutWithCheck() throws Exception {
        ClusterNodeInfo info = newClusterNodeInfo(1);
        // wait until after lease end
        clock.waitUntil(info.getLeaseEndTime() + ClusterNodeInfo.DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS);
        try {
            info.performLeaseCheck();
            fail("lease check must fail with exception");
        } catch (DocumentStoreException e) {
            // expected
        }
        // cluster node 1 must not be able to renew the lease now
        try {
            // must either return false
            assertFalse(info.renewLease());
        } catch (DocumentStoreException e) {
            // or throw an exception
        }
    }

    @Test
    public void readOnlyClusterNodeInfo() {
        ClusterNodeInfo info = ClusterNodeInfo.getReadOnlyInstance(store);
        assertEquals(0, info.getId());
        assertEquals(Long.MAX_VALUE, info.getLeaseEndTime());
        assertFalse(info.renewLease());
    }

    @Test
    public void ignoreEntryWithInvalidID() {
        String instanceId1 = "node1";

        ClusterNodeInfo info1 = newClusterNodeInfo(0, instanceId1);
        assertEquals(1, info1.getId());
        assertEquals(instanceId1, info1.getInstanceId());
        // shut it down
        info1.dispose();

        // sneak in an invalid entry
        UpdateOp op = new UpdateOp("invalid", true);
        store.create(Collection.CLUSTER_NODES, Collections.singletonList(op));

        // acquire again
        info1 = newClusterNodeInfo(0, instanceId1);
        assertEquals(1, info1.getId());
        assertEquals(instanceId1, info1.getInstanceId());
        info1.dispose();
    }

    @Test
    public void acquireInactiveClusterId() {
        // simulate multiple cluster nodes
        String instanceId1 = "node1";
        String instanceId2 = "node2";

        ClusterNodeInfo info1 = newClusterNodeInfo(0, instanceId1);
        assertEquals(1, info1.getId());
        assertEquals(instanceId1, info1.getInstanceId());
        // shut it down
        info1.dispose();

        // simulate start from different location
        ClusterNodeInfo info2 = newClusterNodeInfo(0, instanceId2);
        // must acquire inactive clusterId 1
        assertEquals(1, info2.getId());
        assertEquals(instanceId2, info2.getInstanceId());
        info2.dispose();
    }

    @Test
    public void acquireInactiveClusterIdWithMatchingEnvironment() {
        // simulate multiple cluster nodes
        String instanceId1 = "node1";
        String instanceId2 = "node2";

        ClusterNodeInfo info1 = newClusterNodeInfo(0, instanceId1);
        assertEquals(1, info1.getId());
        assertEquals(instanceId1, info1.getInstanceId());

        // simulate start from different location
        ClusterNodeInfo info2 = newClusterNodeInfo(0, instanceId2);
        assertEquals(2, info2.getId());
        assertEquals(instanceId2, info2.getInstanceId());

        info1.dispose();
        info2.dispose();

        // restart node2
        info2 = newClusterNodeInfo(0, instanceId2);
        // must acquire clusterId 2 again
        assertEquals(2, info2.getId());
        assertEquals(instanceId2, info2.getInstanceId());
    }

    @Test
    public void acquireInactiveClusterIdConcurrently() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        String instanceId1 = "node1";
        String instanceId2 = "node2";
        String instanceId3 = "node3";
        List<String> instanceIds = new ArrayList<>();
        Collections.addAll(instanceIds, instanceId1, instanceId2, instanceId3);

        // create a first clusterNode entry
        ClusterNodeInfo info1 = newClusterNodeInfo(0, instanceId1);
        assertEquals(1, info1.getId());
        assertEquals(instanceId1, info1.getInstanceId());
        // shut it down
        info1.dispose();

        // start multiple instances from different locations competing for
        // the same inactive clusterId
        List<Callable<ClusterNodeInfo>> tasks = new ArrayList<>();
        for (String id : instanceIds) {
            tasks.add(() -> newClusterNodeInfo(0, id));
        }
        Map<Integer, ClusterNodeInfo> clusterNodes = executor.invokeAll(tasks)
                .stream().map(f -> {
                    try {
                        return f.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toMap(ClusterNodeInfo::getId, Function.identity()));

        // must have different clusterIds
        assertEquals(3, clusterNodes.size());
        assertThat(clusterNodes.keySet(), containsInAnyOrder(1, 2, 3));

        clusterNodes.values().forEach(ClusterNodeInfo::dispose);
        executor.shutdown();
    }

    @Test
    public void acquireExpiredClusterId() throws Exception {
        String instanceId1 = "node1";

        ClusterNodeInfo info1 = newClusterNodeInfo(0, instanceId1);
        assertEquals(1, info1.getId());
        assertEquals(instanceId1, info1.getInstanceId());
        expireLease(info1);

        // simulate a restart after a crash and expired lease
        info1 = newClusterNodeInfo(0, instanceId1);
        // must acquire expired clusterId 1
        assertEquals(1, info1.getId());
        assertEquals(instanceId1, info1.getInstanceId());
        info1.dispose();
    }

    @Test
    public void skipExpiredClusterIdWithDifferentInstanceId() throws Exception {
        // simulate multiple cluster nodes
        String instanceId1 = "node1";
        String instanceId2 = "node2";

        ClusterNodeInfo info1 = newClusterNodeInfo(0, instanceId1);
        assertEquals(1, info1.getId());
        assertEquals(instanceId1, info1.getInstanceId());
        expireLease(info1);

        // simulate start from different location
        ClusterNodeInfo info2 = newClusterNodeInfo(0, instanceId2);
        // must not acquire expired clusterId 1
        assertEquals(2, info2.getId());
        assertEquals(instanceId2, info2.getInstanceId());
        info2.dispose();
    }

    @Test
    public void acquireExpiredClusterIdStatic() throws Exception {
        String instanceId1 = "node1";
        String instanceId2 = "node2";

        ClusterNodeInfo info1 = newClusterNodeInfo(0, instanceId1);
        assertEquals(1, info1.getId());
        assertEquals(instanceId1, info1.getInstanceId());
        expireLease(info1);

        // simulate start from different location and
        // acquire with static clusterId
        try {
            newClusterNodeInfo(1, instanceId2);
            fail("Must fail with DocumentStoreException");
        } catch (DocumentStoreException e) {
            assertThat(e.getMessage(), containsString("needs recovery"));
        }
    }

    @Test
    public void acquireExpiredClusterIdConcurrently() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        String instanceId1 = "node1";
        String instanceId2 = "node2";
        String instanceId3 = "node3";
        List<String> instanceIds = new ArrayList<>();
        Collections.addAll(instanceIds, instanceId1, instanceId2, instanceId3);

        ClusterNodeInfo info1 = newClusterNodeInfo(0, instanceId1);
        assertEquals(1, info1.getId());
        assertEquals(instanceId1, info1.getInstanceId());
        expireLease(info1);

        // start multiple instances from different locations competing for
        // the same clusterId with expired lease
        List<Callable<ClusterNodeInfo>> tasks = new ArrayList<>();
        for (String id : instanceIds) {
            tasks.add(() -> newClusterNodeInfo(0, id));
        }
        Map<Integer, ClusterNodeInfo> clusterNodes = executor.invokeAll(tasks)
                .stream().map(f -> {
                    try {
                        return f.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toMap(ClusterNodeInfo::getId, Function.identity()));

        // must have different clusterIds
        assertEquals(3, clusterNodes.size());
        assertThat(clusterNodes.keySet(), containsInAnyOrder(1, 2, 3));

        clusterNodes.values().forEach(ClusterNodeInfo::dispose);
        executor.shutdown();
    }

    @Test
    public void skipClusterIdWithoutStartTime() {
        ClusterNodeInfo info = newClusterNodeInfo(0);
        int id = info.getId();
        assertEquals(1, id);
        // shut it down
        info.dispose();

        // remove startTime field
        UpdateOp op = new UpdateOp(String.valueOf(id), false);
        op.remove(ClusterNodeInfo.START_TIME_KEY);
        assertNotNull(store.findAndUpdate(Collection.CLUSTER_NODES, op));

        // acquire it again
        info = newClusterNodeInfo(0);
        // must not use clusterId 1
        assertNotEquals(1, info.getId());
    }

    @Test
    public void defaultLeaseCheckMode() {
        assertEquals(LeaseCheckMode.STRICT, newClusterNodeInfo(0).getLeaseCheckMode());
    }

    @Test
    public void strictLeaseCheckMode() throws Exception {
        ClusterNodeInfo info = newClusterNodeInfo(1);
        clock.waitUntil(info.getLeaseEndTime());
        // lease renew must fail with exception
        try {
            info.renewLease();
            fail("must fail with DocumentStoreException");
        } catch (DocumentStoreException e) {
            assertThat(e.getMessage(), containsString("failed to update the lease"));
            assertThat(e.getMessage(), containsString("mode: STRICT"));
        }
        assertLeaseFailure();
    }

    @Test
    public void lenientLeaseCheckMode() throws Exception {
        ClusterNodeInfo info = newClusterNodeInfo(1);
        info.setLeaseCheckMode(LeaseCheckMode.LENIENT);
        clock.waitUntil(info.getLeaseEndTime());
        // must still be able to renew
        assertTrue(info.renewLease());
        assertFalse(handler.isLeaseFailure());
    }

    private void assertLeaseFailure() throws Exception {
        for (int i = 0; i < 100; i++) {
            if (handler.isLeaseFailure()) {
                return;
            }
            Thread.sleep(10);
        }
        fail("expected lease failure");
    }

    private void expireLease(ClusterNodeInfo info)
            throws InterruptedException {
        // let lease expire
        clock.waitUntil(info.getLeaseEndTime() +
                ClusterNodeInfo.DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS);
        // check if expired -> recovery is needed
        MissingLastRevSeeker util = new MissingLastRevSeeker(store, clock);
        String key = String.valueOf(info.getId());
        ClusterNodeInfoDocument infoDoc = store.find(Collection.CLUSTER_NODES, key);
        assertNotNull(infoDoc);
        assertTrue(infoDoc.isRecoveryNeeded(clock.getTime()));
    }

    private void recoverClusterNode(int clusterId) throws Exception {
        DocumentNodeStore ns = new DocumentMK.Builder()
                .setDocumentStore(store.getStore()) // use unwrapped store
                .setAsyncDelay(0).setClusterId(42).clock(clock).getNodeStore();
        try {
            LastRevRecoveryAgent recovery = new LastRevRecoveryAgent(ns.getDocumentStore(), ns);
            recovery.recover(clusterId);
        } finally {
            ns.dispose();
        }
    }

    private void waitLeaseUpdateInterval() throws Exception {
        clock.waitUntil(clock.getTime() + ClusterNodeInfo.DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS + 1);
    }

    private ClusterNodeInfo newClusterNodeInfo(int clusterId,
                                               String instanceId) {
        ClusterNodeInfo info = ClusterNodeInfo.getInstance(store,
                new SimpleRecoveryHandler(), null, instanceId, clusterId);
        info.setLeaseFailureHandler(handler);
        return info;
    }

    private class SimpleRecoveryHandler implements RecoveryHandler {

        @Override
        public boolean recover(int clusterId) {
            // simulate recovery by acquiring recovery lock
            RecoveryLock lock = new RecoveryLock(store, clock, clusterId);
            if (lock.acquireRecoveryLock(clusterId)) {
                lock.releaseRecoveryLock(true);
                return true;
            }
            return false;
        }
    }

    private ClusterNodeInfo newClusterNodeInfo(int clusterId) {
        return newClusterNodeInfo(clusterId, null);
    }

    static final class FailureHandler implements LeaseFailureHandler {

        private final AtomicBoolean leaseFailure = new AtomicBoolean();

        @Override
        public void handleLeaseFailure() {
            leaseFailure.set(true);
        }

        public boolean isLeaseFailure() {
            return leaseFailure.get();
        }
    }

    final class TestStore extends DocumentStoreWrapper {

        private final AtomicInteger failBeforeUpdate = new AtomicInteger();
        private final AtomicInteger failAfterUpdate = new AtomicInteger();
        private final AtomicInteger failFind = new AtomicInteger();
        private long delayMillis;

        TestStore() {
            super(new MemoryDocumentStore());
        }

        DocumentStore getStore() {
            return store;
        }

        @Override
        public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                    UpdateOp update) {
            maybeDelay();
            maybeThrow(failBeforeUpdate, "update failed before");
            T doc = super.findAndUpdate(collection, update);
            maybeThrow(failAfterUpdate, "update failed after");
            return doc;
        }

        @Override
        public <T extends Document> T find(Collection<T> collection,
                                           String key) {
            maybeDelay();
            maybeThrow(failFind, "find failed");
            return super.find(collection, key);
        }

        private void maybeDelay() {
            try {
                clock.waitUntil(clock.getTime() + delayMillis);
            } catch (InterruptedException e) {
                throw new DocumentStoreException(e);
            }
        }

        private void maybeThrow(AtomicInteger num, String msg) {
            if (num.get() > 0) {
                num.decrementAndGet();
                throw new DocumentStoreException(msg);
            }
        }

        public int getFailBeforeUpdate() {
            return failBeforeUpdate.get();
        }

        public void setFailBeforeUpdate(int num) {
            failBeforeUpdate.set(num);
        }

        public int getFailAfterUpdate() {
            return failAfterUpdate.get();
        }

        public void setFailAfterUpdate(int num) {
            this.failAfterUpdate.set(num);
        }

        public long getDelayMillis() {
            return delayMillis;
        }

        public void setDelayMillis(long delayMillis) {
            this.delayMillis = delayMillis;
        }

        public int getFailFind() {
            return failFind.get();
        }

        public void setFailFind(int num) {
            this.failFind.set(num);
        }
    }
}
