/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.Clock.Virtual;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junitx.util.PrivateAccessor;

public class DocumentLeaseUpdateRetryTest {

    private DocumentNodeStore ns;
    private Virtual clock;
    private TestStore ds;

    @Before
    public void setup() throws Exception {
        clock = new Clock.Virtual();
        ClusterNodeInfo.setClock(clock);
        ds = new TestStore();
        ns = new DocumentMK.Builder().clock(clock).setDocumentStore(ds).setLeaseCheck(true).getNodeStore();
    }

    @After
    public void tearDown() throws Exception {
        ClusterNodeInfo.resetClockToDefault();
        ns.dispose();
    }

    @Test
    public void testLeaseRetryLoop() throws Exception {
        internalTestLeaseRetryLoop(false);
    }

    @Test
    public void testLeaseRetryLoopWithDelay() throws Exception {
        // see OAK-5446
        // (simulates a very slow read access on the clusterNodes collection)
        internalTestLeaseRetryLoop(true);
    }

    private void internalTestLeaseRetryLoop(boolean withDelay) throws Exception {
        ClusterNodeInfo clusterInfo = ns.getClusterInfo();
        long leaseTime = clusterInfo.getLeaseTime();
        long leaseEndTime1 = clusterInfo.getLeaseEndTime();
        // TODO: replace privateAccessor.getField with a test-only
        // package-protected access
        Long leaseUpdateInterval = (Long) PrivateAccessor.getField(clusterInfo, "leaseUpdateInterval");

        // assert that lease is fine at this point
        // do this indirectly by trying to invoke some DocumentNodeStore
        // functionality
        ns.checkpoint(1);

        // forward the virtual clock by more than the leaseUpdateInterval, to
        // trigger a lease update
        clock.waitUntil(clock.getTime() + leaseUpdateInterval + 1000);
        // but also actually sleep more than 1s to give the background tasks of
        // DNS a chance to run
        Thread.sleep(2000);

        // assert leaseEndTime having been updated - ie lease having been
        // updated
        long leaseEndTime2 = clusterInfo.getLeaseEndTime();
        assertTrue(leaseEndTime1 < leaseEndTime2);

        // again assert that lease is fine -> do some dummy ns call
        ns.checkpoint(2);

        if (withDelay) {
            // mark the TestStore as delaying from now on
            ds.setDelaying(true);
            Thread.sleep(1200);
        }

        // now forward the virtual clock by more than the lease time - which
        // should cause lease to time out
        clock.waitUntil(clock.getTime() + leaseTime + leaseUpdateInterval + 1000);

        // so the next call to the lease check wrapper should now run into the
        // retry loop, as the lease has timed out
        ns.checkpoint(3); // should not fail
    }

    final class TestStore extends DocumentStoreWrapper {

        private boolean delaying = false;

        TestStore() {
            super(new MemoryDocumentStore());
        }

        void setDelaying(boolean delaying) {
            this.delaying = delaying;
        }

        @Override
        public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) {
            if (delaying && collection == Collection.CLUSTER_NODES) {
                try {
                    // make the lookup on the clusterNodes collection *really*
                    // slow
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return super.query(collection, fromKey, toKey, limit);
        }
    }
}
