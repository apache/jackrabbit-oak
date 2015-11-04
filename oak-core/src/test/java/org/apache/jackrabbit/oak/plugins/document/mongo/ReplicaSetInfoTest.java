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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import static org.apache.jackrabbit.oak.plugins.document.mongo.ReplicaSetInfo.ReplicaSetMemberState.PRIMARY;
import static org.apache.jackrabbit.oak.plugins.document.mongo.ReplicaSetInfo.ReplicaSetMemberState.RECOVERING;
import static org.apache.jackrabbit.oak.plugins.document.mongo.ReplicaSetInfo.ReplicaSetMemberState.SECONDARY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.mongo.ReplicaSetInfo.ReplicaSetMemberState;
import org.bson.BasicBSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.mongodb.DB;

public class ReplicaSetInfoTest {

    private static final long MAX_REPLICATION_LAG = TimeUnit.HOURS.toMillis(6);

    private ReplicaSetInfo replica;

    private ReplicationSetStatusMock replicationSet;

    @Before
    public void resetEstimator() {
        DB db = mock(DB.class);
        when(db.getName()).thenReturn("oak-db");
        when(db.getSisterDB(Mockito.anyString())).thenReturn(db);
        replica = new ReplicaSetInfo(db, null, 0, MAX_REPLICATION_LAG, 0l) {
            @Override
            protected List<Revision> getRootRevisions(String hostName) throws UnknownHostException {
                return replicationSet.revisions(hostName);
            }
        };
    }

    @Test
    public void testMinimumRevision() {
        addInstance(SECONDARY, "m1").addRevisions(20, 18, 3);
        addInstance(SECONDARY, "m2").addRevisions(20, 1, 19);
        updateRevisions();

        assertEquals(20, replica.rootRevisions.get(0).getTimestamp());
        assertEquals( 1, replica.rootRevisions.get(1).getTimestamp());
        assertEquals( 3, replica.rootRevisions.get(2).getTimestamp());
        assertEquals( 1, (long) replica.minRootTimestamp);
    }

    @Test
    public void testIsSafeRevision() {
        addInstance(SECONDARY, "m1").addRevisions(10, 21, 11);
        addInstance(SECONDARY, "m2").addRevisions(15, 14, 13);
        addInstance(SECONDARY, "m3").addRevisions(14, 13, 22);
        updateRevisions();

        assertTrue(replica.isSecondarySafe(lastRev(9, 13, 10)));
        assertFalse(replica.isSecondarySafe(lastRev(11, 14, 10)));
    }

    @Test
    public void testIsSafeAge() {
        addInstance(SECONDARY, "m1").addRevisions(10, 21, 11);
        addInstance(SECONDARY, "m2").addRevisions(15, 14, 13);
        addInstance(SECONDARY, "m3").addRevisions(14, 13, 22);
        updateRevisions();

        assertTrue (replica.isSecondarySafe(21, 30));
        assertFalse(replica.isSecondarySafe(10, 30));
        assertFalse(replica.isSecondarySafe(99, 9)); // current time is before the minimum update 
    }

    @Test
    public void testBranchRevisionIsNotSafe() {
        addInstance(SECONDARY, "m1").addRevisions(10, 21, 11);
        addInstance(SECONDARY, "m2").addRevisions(15, 14, 13);
        addInstance(SECONDARY, "m3").addRevisions(14, 13, 22);
        updateRevisions();

        assertTrue(replica.isSecondarySafe(lastRev(1, 1, 1)));
        RevisionBuilder builder = new RevisionBuilder()
                .addRevision(1, 0, 0, false)
                .addRevision(1, 0, 1, false)
                .addRevision(1, 0, 2, true);
        assertFalse(replica.isSecondarySafe(builder.revsMap));
    }

    @Test
    public void testUnknownStateIsNotSafe() {
        addInstance(SECONDARY, "m1").addRevisions(10, 21, 11);
        addInstance(RECOVERING, "m2");
        updateRevisions();

        assertNull(replica.minRootTimestamp);
        assertNull(replica.rootRevisions);
        assertFalse(replica.isSecondarySafe(lastRev(1, 1, 1)));
        assertFalse(replica.isSecondarySafe(30, 30));
    }

    @Test
    public void testEmptyIsNotSafe() {
        addInstance(PRIMARY, "m1");
        updateRevisions();

        assertNull(replica.minRootTimestamp);
        assertNull(replica.rootRevisions);
        assertFalse(replica.isSecondarySafe(lastRev(1, 1, 1)));
        assertFalse(replica.isSecondarySafe(30, 30));
    }

    @Test
    public void testFallbackToTheMaximumLag() {
        addInstance(RECOVERING, "m1");
        updateRevisions();
        assertTrue(replica.isSecondarySafe(MAX_REPLICATION_LAG, System.currentTimeMillis()));
    }

    private RevisionBuilder addInstance(ReplicaSetMemberState state, String name) {
        if (replicationSet == null) {
            replicationSet = new ReplicationSetStatusMock();
        }
        return replicationSet.addInstance(state, name);
    }

    private void updateRevisions() {
        replica.updateRevisions(replicationSet.getMembers());
        replicationSet = null;
    }

    private static Map<Integer, Revision> lastRev(int... timestamps) {
        return new RevisionBuilder().addRevisions(timestamps).revsMap;
    }

    private class ReplicationSetStatusMock {

        private List<BasicBSONObject> members = new ArrayList<BasicBSONObject>();

        private Map<String, List<Revision>> memberRevisions = new HashMap<String, List<Revision>>();

        private RevisionBuilder addInstance(ReplicaSetMemberState state, String name) {
            BasicBSONObject member = new BasicBSONObject();
            member.put("stateStr", state.name());
            member.put("name", name);
            members.add(member);

            RevisionBuilder builder = new RevisionBuilder();
            memberRevisions.put(name, builder.revs);
            return builder;
        }

        private List<BasicBSONObject> getMembers() {
            return members;
        }

        private List<Revision> revisions(String name) {
            return memberRevisions.get(name);
        }
    }

    private static class RevisionBuilder {

        private final List<Revision> revs = new ArrayList<Revision>();

        private final Map<Integer, Revision> revsMap = new HashMap<Integer, Revision>();

        private RevisionBuilder addRevisions(int... timestamps) {
            for (int i = 0; i < timestamps.length; i++) {
                addRevision(timestamps[i], 0, i, false);
            }
            return this;
        }

        private RevisionBuilder addRevision(int timestamp, int counter, int clusterId, boolean branch) {
            Revision rev = new Revision(timestamp, counter, clusterId, branch);
            revs.add(rev);
            revsMap.put(clusterId, rev);
            return this;
        }
    }
}
