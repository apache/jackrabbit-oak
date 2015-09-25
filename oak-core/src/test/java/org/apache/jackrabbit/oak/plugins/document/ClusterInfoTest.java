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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Test;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;

/**
 * Test the ClusterInfo class
 */
public class ClusterInfoTest {

    @Test
    public void readWriteMode() throws InterruptedException {

        MemoryDocumentStore mem = new MemoryDocumentStore();
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        ClusterNodeInfo.setClock(clock);

        DocumentNodeStore ns1 = new DocumentMK.Builder().
                setDocumentStore(mem).
                setAsyncDelay(0).
                setLeaseCheck(false).
                getNodeStore();
        DocumentNodeStore ns2 = new DocumentMK.Builder().
                setDocumentStore(mem).
                setAsyncDelay(0).
                setLeaseCheck(false).
                getNodeStore();
        // Bring the current time forward to after the leaseTime which would have been 
        // updated in the DocumentNodeStore initialization.
        clock.waitUntil(clock.getTime() + ns1.getClusterInfo().getLeaseTime());

        ns1.getClusterInfo().setLeaseTime(0);
        ns1.getClusterInfo().setLeaseUpdateInterval(0);
        ns2.getClusterInfo().setLeaseTime(0);
        ns2.getClusterInfo().setLeaseUpdateInterval(0);

        List<ClusterNodeInfoDocument> list = mem.query(
                Collection.CLUSTER_NODES, "0", "a", Integer.MAX_VALUE);
        assertEquals(2, list.size());

        assertNull(mem.getReadPreference());
        assertNull(mem.getWriteConcern());
        mem.setReadWriteMode("read:primary, write:majority");
        assertEquals(ReadPreference.primary(), mem.getReadPreference());
        assertEquals(WriteConcern.MAJORITY, mem.getWriteConcern());

        UpdateOp op;

        // unknown modes: ignore
        op = new UpdateOp(list.get(0).getId(), false);
        op.set("readWriteMode", "read:xyz, write:abc");
        mem.findAndUpdate(Collection.CLUSTER_NODES, op);
        ns1.renewClusterIdLease();
        assertEquals(ReadPreference.primary(), mem.getReadPreference());
        assertEquals(WriteConcern.MAJORITY, mem.getWriteConcern());

        op = new UpdateOp(list.get(0).getId(), false);
        op.set("readWriteMode", "read:nearest, write:fsynced");
        mem.findAndUpdate(Collection.CLUSTER_NODES, op);
        ns1.renewClusterIdLease();
        assertEquals(ReadPreference.nearest(), mem.getReadPreference());
        assertEquals(WriteConcern.FSYNCED, mem.getWriteConcern());

        ns1.dispose();
        ns2.dispose();
    }

    @Test
    public void renewLease() throws InterruptedException {
        MemoryDocumentStore mem = new MemoryDocumentStore();
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        ClusterNodeInfo.setClock(clock);

        DocumentNodeStore ns = new DocumentMK.Builder().
                setDocumentStore(mem).
                setAsyncDelay(0).
                setLeaseCheck(false).
                getNodeStore();

        ClusterNodeInfo info = ns.getClusterInfo();
        assertNotNull(info);

        // current lease end
        long leaseEnd = getLeaseEndTime(ns);

        // wait a bit, 1sec less than leaseUpdateTime (10sec-1sec by default)
        clock.waitUntil(clock.getTime() + ClusterNodeInfo.DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS - 1000);

        // must not renew lease right now
        ns.renewClusterIdLease();
        assertEquals(leaseEnd, getLeaseEndTime(ns));

        // wait some more time
        clock.waitUntil(clock.getTime() + 2000);

        // now the lease must be renewed
        ns.renewClusterIdLease();
        assertTrue(getLeaseEndTime(ns) > leaseEnd);

        ns.dispose();
    }

    private static long getLeaseEndTime(DocumentNodeStore nodeStore) {
        ClusterNodeInfoDocument doc = nodeStore.getDocumentStore().find(
                Collection.CLUSTER_NODES,
                String.valueOf(nodeStore.getClusterId()));
        assertNotNull(doc);
        return doc.getLeaseEndTime();
    }

    @After
    public void tearDown(){
        ClusterNodeInfo.resetClockToDefault();
    }
}
