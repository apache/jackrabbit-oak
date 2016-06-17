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
package org.apache.jackrabbit.oak.plugins.document.mongo.replica;

import static com.google.common.collect.Maps.transformValues;
import static org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetInfo.MemberState.PRIMARY;
import static org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetInfo.MemberState.RECOVERING;
import static org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetInfo.MemberState.SECONDARY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetInfo.MemberState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.bson.BasicBSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Function;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;

public class ReplicaSetInfoTest {

    private ReplicaSetInfo replica;

    private ReplicationSetStatusMock replicationSet;

    private Clock.Virtual clock;

    private Clock.Virtual mongoClock;

    @Before
    public void resetEstimator() {
        clock = mongoClock = new Clock.Virtual();

        DB db = mock(DB.class);
        when(db.getName()).thenReturn("oak-db");
        when(db.getSisterDB(Mockito.anyString())).thenReturn(db);
        replica = new ReplicaSetInfo(clock, db, null, 0l, 0l, null) {
            @Override
            protected BasicDBObject getReplicaStatus() {
                BasicDBObject obj = new BasicDBObject();
                obj.put("date", mongoClock.getDate());
                obj.put("members", replicationSet.members);
                return obj;
            }

            @Override
            protected Map<String, Timestamped<RevisionVector>> getRootRevisions(Iterable<String> hosts) {
                return transformValues(replicationSet.memberRevisions,
                        new Function<RevisionBuilder, Timestamped<RevisionVector>>() {
                    @Override
                    public Timestamped<RevisionVector> apply(RevisionBuilder input) {
                        return new Timestamped<RevisionVector>(input.revs, clock.getTime());
                    }
                });
            }
        };
        replica.hiddenMembers = Collections.emptyList();
    }

    @Test
    public void testMinimumRevision() {
        addInstance(PRIMARY, "mp").addRevisions(20, 18, 19);
        addInstance(SECONDARY, "m1").addRevisions(20, 18, 3);
        addInstance(SECONDARY, "m2").addRevisions(20, 1, 17);
        updateRevisions();

        assertEquals(20, replica.getMinimumRootRevisions().getRevision(0).getTimestamp());
        assertEquals( 1, replica.getMinimumRootRevisions().getRevision(1).getTimestamp());
        assertEquals( 3, replica.getMinimumRootRevisions().getRevision(2).getTimestamp());
    }

    @Test
    public void testIsMoreRecentThan() {
        addInstance(PRIMARY, "mp").addRevisions(15, 21, 22);
        addInstance(SECONDARY, "m1").addRevisions(10, 21, 11);
        addInstance(SECONDARY, "m2").addRevisions(15, 14, 13);
        addInstance(SECONDARY, "m3").addRevisions(14, 13, 22);
        updateRevisions();

        assertTrue(replica.isMoreRecentThan(lastRev(9, 13, 10)));
        assertFalse(replica.isMoreRecentThan(lastRev(11, 14, 10)));
    }

    @Test
    public void testUnknownStateIsNotSafe() {
        addInstance(PRIMARY, "mp");
        addInstance(SECONDARY, "m1").addRevisions(10, 21, 11);
        addInstance(RECOVERING, "m2");
        updateRevisions();

        assertNull(replica.getMinimumRootRevisions());
        assertFalse(replica.isMoreRecentThan(lastRev(1, 1, 1)));
    }

    @Test
    public void testEmptyIsNotSafe() {
        addInstance(PRIMARY, "m1");
        updateRevisions();

        assertNull(replica.getMinimumRootRevisions());
        assertFalse(replica.isMoreRecentThan(lastRev(1, 1, 1)));
    }

    @Test
    public void testOldestNotReplicated() {
        addInstance(PRIMARY, "mp").addRevisions(10, 30, 30);
        addInstance(SECONDARY, "m1").addRevisions(10, 5, 30);
        addInstance(SECONDARY, "m2").addRevisions(2, 30, 30);
        updateRevisions();

        assertEquals(10, replica.secondariesSafeTimestamp);
    }

    @Test
    public void testAllSecondariesUpToDate() {
        addInstance(PRIMARY, "mp").addRevisions(10, 30, 30);
        addInstance(SECONDARY, "m1").addRevisions(10, 30, 30);
        addInstance(SECONDARY, "m2").addRevisions(10, 30, 30);

        long before = clock.getTime();
        updateRevisions();
        long after = clock.getTime();

        assertBetween(before, after, replica.secondariesSafeTimestamp);
    }

    @Test
    public void testAllSecondariesUpToDateWithTimediff() {
        addInstance(PRIMARY, "mp").addRevisions(10, 30, 30);
        addInstance(SECONDARY, "m1").addRevisions(10, 30, 30);
        addInstance(SECONDARY, "m2").addRevisions(10, 30, 30);

        mongoClock = new Clock.Virtual();
        mongoClock.waitUntil(100);

        long before = clock.getTime();
        updateRevisions();
        long after = clock.getTime();

        assertBetween(before, after, replica.secondariesSafeTimestamp);
    }

    private RevisionBuilder addInstance(MemberState state, String name) {
        if (replicationSet == null) {
            replicationSet = new ReplicationSetStatusMock();
        }
        return replicationSet.addInstance(state, name);
    }

    private void updateRevisions() {
        replica.updateReplicaStatus();
        replicationSet = null;
    }

    private static RevisionVector lastRev(int... timestamps) {
        return new RevisionBuilder().addRevisions(timestamps).revs;
    }

    private static void assertBetween(long from, long to, long actual) {
        final String msg = String.format("%d <= %d <= %d", from, actual, to);
        assertTrue(msg, from <= actual);
        assertTrue(msg, actual <= to);
    }

    private class ReplicationSetStatusMock {

        private List<BasicBSONObject> members = new ArrayList<BasicBSONObject>();

        private Map<String, RevisionBuilder> memberRevisions = new HashMap<String, RevisionBuilder>();

        private RevisionBuilder addInstance(MemberState state, String name) {
            BasicBSONObject member = new BasicBSONObject();
            member.put("stateStr", state.name());
            member.put("name", name);
            members.add(member);

            RevisionBuilder builder = new RevisionBuilder();
            memberRevisions.put(name, builder);
            return builder;
        }
    }

    private static class RevisionBuilder {

        private RevisionVector revs = new RevisionVector();

        private RevisionBuilder addRevisions(int... timestamps) {
            for (int i = 0; i < timestamps.length; i++) {
                addRevision(timestamps[i], 0, i, false);
            }
            return this;
        }

        private RevisionBuilder addRevision(int timestamp, int counter, int clusterId, boolean branch) {
            Revision rev = new Revision(timestamp, counter, clusterId, branch);
            revs = revs.update(rev);
            return this;
        }
    }
}