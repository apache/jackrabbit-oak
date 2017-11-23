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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests the restore of potentially missing _lastRev updates.
 */
@RunWith(Parameterized.class)
public class LastRevSingleNodeRecoveryTest {
    private DocumentStoreFixture fixture;

    private Clock clock;

    private DocumentMK mk;

    private DocumentMK mk2;

    public LastRevSingleNodeRecoveryTest(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> fixtures() throws IOException {
        List<Object[]> fixtures = Lists.newArrayList();
        DocumentStoreFixture mongo = new DocumentStoreFixture.MongoFixture();
        if (mongo.isAvailable()) {
            fixtures.add(new Object[] {mongo});
        }
        return fixtures;
    }

    private DocumentMK createMK(int clusterId) throws InterruptedException {
        clock = new Clock.Virtual();
        return openMK(clusterId, fixture.createDocumentStore());
    }

    private DocumentMK openMK(int clusterId, DocumentStore store) throws InterruptedException {
        clock.waitUntil(System.currentTimeMillis());

        // Sets the clock for testing
        ClusterNodeInfo.setClock(clock);
        Revision.setClock(clock);

        DocumentMK.Builder builder = new DocumentMK.Builder();
        builder.setAsyncDelay(0)
                .setClusterId(clusterId)
                .clock(clock)
                .setLeaseCheck(false)
                .setDocumentStore(store);
        mk = builder.open();
        clock.waitUntil(Revision.getCurrentTimestamp());

        return mk;
    }

    @Before
    public void setUp() throws InterruptedException {
        try {
            mk = createMK(0);
            Assume.assumeNotNull(mk);

            // initialize node hierarchy
            mk.commit("/", "+\"x\" : { \"y\": {\"z\":{} } }", null, null);
            mk.commit("/", "+\"a\" : { \"b\": {\"c\": {}} }", null, null);
        } catch (Exception e) {
            Assume.assumeNoException(e);
        }
    }

    @Test
    public void testLastRevRestoreOnNodeStart() throws Exception {
        clock.waitUntil(clock.getTime() + mk.getClusterInfo().getLeaseTime() + 10);

        // pending updates
        setupScenario();

        // renew lease
        clock.waitUntil(clock.getTime() + mk.getClusterInfo().getLeaseTime() + 10);
        mk.getClusterInfo().renewLease();

        // so that the current time is more than the current lease end
        clock.waitUntil(clock.getTime() + mk.getClusterInfo().getLeaseTime() + 1000);
        // Recreate mk instance, to simulate fail condition and recovery on start
        // Make sure to use a different variable for cleanup ; mk should not be disposed here
        mk2 = openMK(0, mk.getNodeStore().getDocumentStore());

        int pendingCount = mk2.getPendingWriteCount();
        // Immediately check again, now should not have done any changes.
        LastRevRecoveryAgent recoveryAgent = mk2.getNodeStore().getLastRevRecoveryAgent();
        /** Now there should have been pendingCount updates **/
        assertEquals(pendingCount, recoveryAgent.recover(mk2.getClusterInfo().getId()));
    }

    @Test
    public void testLastRevRestore() throws Exception {
        clock.waitUntil(clock.getTime() + mk.getClusterInfo().getLeaseTime() + 10);
        setupScenario();

        int pendingCount = mk.getPendingWriteCount();
        // so that the current time is more than the current lease end
        clock.waitUntil(clock.getTime() + mk.getClusterInfo().getLeaseTime() + 1000);
        LastRevRecoveryAgent recoveryAgent = mk.getNodeStore().getLastRevRecoveryAgent();

        /** All pending updates should have been restored **/
        assertEquals(pendingCount, recoveryAgent.recover(mk.getClusterInfo().getId()));
    }


    @Test
    public void testNoMissingUpdates() throws Exception {
        clock.waitUntil(clock.getTime() + mk.getClusterInfo().getLeaseTime() + 10);
        setupScenario();
        mk.backgroundWrite();

        // move the time forward and do another update of the root node so that only 2 nodes are
        // candidates
        clock.waitUntil(clock.getTime() + 5000);
        mk.commit("/", "^\"a/key2\" : \"value2\"", null, null);
        mk.backgroundWrite();

        clock.waitUntil(clock.getTime() + mk.getClusterInfo().getLeaseTime());
        mk.getClusterInfo().renewLease();

        // Should be 0
        int pendingCount = mk.getPendingWriteCount();
        LastRevRecoveryAgent recoveryAgent = mk.getNodeStore().getLastRevRecoveryAgent();

        clock.waitUntil(clock.getTime() + mk.getClusterInfo().getLeaseTime());
        /** There should have been no updates **/
        assertEquals(pendingCount, recoveryAgent.recover(mk.getClusterInfo().getId()));
    }
    
    @Test
    public void testNodeRecoveryNeeded() throws InterruptedException {
        clock.waitUntil(clock.getTime() + mk.getClusterInfo().getLeaseTime() + 10);
        setupScenario();

        // so that the current time is more than the current lease end
        clock.waitUntil(clock.getTime() + mk.getClusterInfo().getLeaseTime() + 1000);

        LastRevRecoveryAgent recoveryAgent = mk.getNodeStore().getLastRevRecoveryAgent();

        // Post OAK-5337, a cluster node won't report itself as a candidate for recovery
        // Recovery agent would still detect that recovery is required and calling
        // recover on self would recover too (testLastRevRestore)
        assertTrue(recoveryAgent.isRecoveryNeeded());
        Iterable<Integer> cids = recoveryAgent.getRecoveryCandidateNodes();
        assertEquals(0, Iterables.size(cids));
    }
    
    private void setupScenario() throws InterruptedException {
        // add some nodes which won't be returned
        mk.commit("/", "+\"u\" : { \"v\": {}}", null, null);
        mk.commit("/u", "^\"v/key1\" : \"value1\"", null, null);

        // move the time forward so that the root gets updated
        clock.waitUntil(clock.getTime() + 5000);
        mk.commit("/", "^\"a/key1\" : \"value1\"", null, null);
        mk.backgroundWrite();

        // move the time forward to have a new node under root
        clock.waitUntil(clock.getTime() + 5000);
        mk.commit("/", "+\"p\":{}", null, null);

        // move the time forward to write all pending changes
        clock.waitUntil(clock.getTime() + 5000);
        mk.backgroundWrite();

        // renew lease one last time
        clock.waitUntil(clock.getTime() + mk.getClusterInfo().getLeaseTime());
        mk.getClusterInfo().renewLease();

        clock.waitUntil(clock.getTime() + 5000);
        // add nodes won't trigger _lastRev updates
        addNodes();
    }

    /**
     * Should have the
     */
    private void addNodes() {
        // change node /a/b/c by adding a property
        mk.commit("/a/b", "^\"c/key1\" : \"value1\"", null, null);
        // add node /a/b/c/d
        mk.commit("/a/b/c", "+\"d\":{}", null, null);
        // add node /a/b/f
        mk.commit("/a/b", "+\"f\" : {}", null, null);
        // add node /a/b/f/e
        mk.commit("/a/b/f", "+\"e\": {}", null, null);
        // change node /x/y/z
        mk.commit("/x/y", "^\"z/key1\" : \"value1\"", null, null);
    }

    @After
    public void tearDown() throws Exception {
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
        mk.dispose();
        if ( mk2 != null ) {
            mk2.dispose();
        }
        fixture.dispose();
    }
}
