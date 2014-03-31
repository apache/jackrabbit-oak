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

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class CheckpointsTest {

    private Clock clock;

    private DocumentNodeStore store;

    @Before
    public void setUp() throws InterruptedException {
        clock = new Clock.Virtual();
        store = new DocumentMK.Builder().clock(clock).getNodeStore();
    }

    @Test
    public void testCheckpointPurge() throws Exception {
        long expiryTime = 1000;
        Revision r1 = Revision.fromString(store.checkpoint(expiryTime));
        assertEquals(r1, store.getCheckpoints().getOldestRevisionToKeep());

        //Trigger expiry by forwarding the clock to future
        clock.waitUntil(clock.getTime() + expiryTime + 1);
        assertNull(store.getCheckpoints().getOldestRevisionToKeep());
    }

    @Test
    public void testCheckpointPurgeByCount() throws Exception {
        long expiryTime = TimeUnit.HOURS.toMillis(1);
        Revision r1 = null;
        for(int i = 0; i < Checkpoints.CLEANUP_INTERVAL; i++){
            r1 = Revision.fromString(store.checkpoint(expiryTime));
            store.setHeadRevision(Revision.newRevision(0));
        }
        assertEquals(r1, store.getCheckpoints().getOldestRevisionToKeep());
        assertEquals(Checkpoints.CLEANUP_INTERVAL, store.getCheckpoints().size());

        //Trigger expiry by forwarding the clock to future
        clock.waitUntil(clock.getTime() + expiryTime);

        //Now creating the next checkpoint should trigger
        //cleanup
        store.checkpoint(expiryTime);
        assertEquals(1, store.getCheckpoints().size());
    }

    @Ignore("OAK-1648")
    @Test
    public void multipleCheckpointOnSameRevision() throws Exception{
        long e1 = TimeUnit.HOURS.toMillis(1);
        long e2 = TimeUnit.HOURS.toMillis(3);

        //Create CP with higher expiry first and then one with
        //lower expiry
        Revision r2 = Revision.fromString(store.checkpoint(e2));
        Revision r1 = Revision.fromString(store.checkpoint(e1));

        //Head revision has not changed so revision must be same
        assertEquals(r1,r2);

        clock.waitUntil(clock.getTime() + e1 + 1);

        //The older checkpoint was for greater duration so checkpoint
        //must not be GC
        assertEquals(r1, store.getCheckpoints().getOldestRevisionToKeep());
    }

    @Test
    public void testGetOldestRevisionToKeep() throws Exception {
        long et1 = 1000, et2 = et1 + 1000;

        Revision r1 = Revision.fromString(store.checkpoint(et1));

        //Do some commit to change headRevision
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("x");
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        Revision r2 = Revision.fromString(store.checkpoint(et2));
        assertNotSame(r1, r2);

        //r2 has the later expiry
        assertEquals(r2, store.getCheckpoints().getOldestRevisionToKeep());

        long starttime = clock.getTime();

        //Trigger expiry by forwarding the clock to future e1
        clock.waitUntil(starttime + et1 + 1);
        assertEquals(r2, store.getCheckpoints().getOldestRevisionToKeep());

        //Trigger expiry by forwarding the clock to future e2
        //This time no valid checkpoint
        clock.waitUntil(starttime + et2 + 1);
        assertNull(store.getCheckpoints().getOldestRevisionToKeep());
    }

}
