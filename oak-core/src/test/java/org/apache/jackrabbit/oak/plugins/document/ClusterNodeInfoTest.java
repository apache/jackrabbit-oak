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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    public void after() throws Exception {
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
        for (int i = 0; i < 10; i++) {
            if (handler.isLeaseFailure()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("expected lease failure");
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

    private void recoverClusterNode(int clusterId) throws Exception {
        DocumentNodeStore ns = new DocumentMK.Builder()
                .setDocumentStore(store.getStore()) // use unwrapped store
                .setAsyncDelay(0).setClusterId(42).clock(clock).getNodeStore();
        try {
            LastRevRecoveryAgent recovery = new LastRevRecoveryAgent(ns);
            recovery.recover(clusterId);
        } finally {
            ns.dispose();
        }
    }

    private void waitLeaseUpdateInterval() throws Exception {
        clock.waitUntil(clock.getTime() + ClusterNodeInfo.DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS + 1);
    }

    private ClusterNodeInfo newClusterNodeInfo(int clusterId) {
        ClusterNodeInfo info = ClusterNodeInfo.getInstance(store, clusterId);
        info.setLeaseFailureHandler(handler);
        assertTrue(info.renewLease()); // perform initial lease renew
        return info;
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
