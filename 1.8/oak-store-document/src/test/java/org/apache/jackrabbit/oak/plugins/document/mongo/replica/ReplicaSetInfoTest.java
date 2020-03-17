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

import static java.lang.Long.MAX_VALUE;
import static org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetInfo.MemberState.PRIMARY;
import static org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetInfo.MemberState.RECOVERING;
import static org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetInfo.MemberState.SECONDARY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Before;
import org.junit.Test;

public class ReplicaSetInfoTest {

    private ReplicaSetInfoMock replica;

    private Clock.Virtual clock;

    @Before
    public void resetEstimator() {
        clock = new Clock.Virtual();
        replica = ReplicaSetInfoMock.create(clock);
    }

    @Test
    public void testMinimumRevision() {
        replica.addInstance(PRIMARY, "mp").addRevisions(20, 18, 19);
        replica.addInstance(SECONDARY, "m1").addRevisions(20, 18, 3);
        replica.addInstance(SECONDARY, "m2").addRevisions(20, 1, 17);
        replica.updateRevisions();

        assertEquals(20, replica.getMinimumRootRevisions().getRevision(0).getTimestamp());
        assertEquals( 1, replica.getMinimumRootRevisions().getRevision(1).getTimestamp());
        assertEquals( 3, replica.getMinimumRootRevisions().getRevision(2).getTimestamp());

        clock.waitUntil(38);
        assertEquals(20, replica.getLag());
    }

    @Test
    public void testIsMoreRecentThan() {
        replica.addInstance(PRIMARY, "mp").addRevisions(15, 21, 22);
        replica.addInstance(SECONDARY, "m1").addRevisions(10, 21, 11);
        replica.addInstance(SECONDARY, "m2").addRevisions(15, 14, 13);
        replica.addInstance(SECONDARY, "m3").addRevisions(14, 13, 22);
        replica.updateRevisions();

        assertTrue(replica.isMoreRecentThan(lastRev(9, 13, 10)));
        assertFalse(replica.isMoreRecentThan(lastRev(11, 14, 10)));
    }

    @Test
    public void testUnknownStateIsNotSafe() {
        replica.addInstance(PRIMARY, "mp");
        replica.addInstance(SECONDARY, "m1").addRevisions(10, 21, 11);
        replica.addInstance(RECOVERING, "m2");
        replica.updateRevisions();

        assertNull(replica.getMinimumRootRevisions());
        assertFalse(replica.isMoreRecentThan(lastRev(1, 1, 1)));
        assertEquals(MAX_VALUE, replica.getLag());
    }

    @Test
    public void testEmptyIsNotSafe() {
        replica.addInstance(PRIMARY, "m1");
        replica.updateRevisions();

        assertNull(replica.getMinimumRootRevisions());
        assertFalse(replica.isMoreRecentThan(lastRev(1, 1, 1)));
        assertEquals(MAX_VALUE, replica.getLag());
    }

    @Test
    public void testOldestNotReplicated() {
        replica.addInstance(PRIMARY, "mp").addRevisions(10, 30, 30);
        replica.addInstance(SECONDARY, "m1").addRevisions(10, 5, 30);
        replica.addInstance(SECONDARY, "m2").addRevisions(2, 30, 30);
        replica.updateRevisions();

        assertEquals(10, replica.secondariesSafeTimestamp);

        clock.waitUntil(40);
        assertEquals(30, replica.getLag());
        clock.waitUntil(50);
        assertEquals(40, replica.getLag());
    }

    @Test
    public void testAllSecondariesUpToDate() {
        replica.addInstance(PRIMARY, "mp").addRevisions(10, 30, 30);
        replica.addInstance(SECONDARY, "m1").addRevisions(10, 30, 30);
        replica.addInstance(SECONDARY, "m2").addRevisions(10, 30, 30);

        long before = clock.getTime();
        replica.updateRevisions();
        long after = clock.getTime();

        assertBetween(before, after, replica.secondariesSafeTimestamp);
    }

    @Test
    public void testAllSecondariesUpToDateWithTimediff() throws InterruptedException {
        replica.addInstance(PRIMARY, "mp").addRevisions(10, 30, 30);
        replica.addInstance(SECONDARY, "m1").addRevisions(10, 30, 30);
        replica.addInstance(SECONDARY, "m2").addRevisions(10, 30, 30);

        Clock mongoClock = new Clock.Virtual();
        replica.setMongoClock(mongoClock);
        mongoClock.waitUntil(100);

        long before = clock.getTime();
        replica.updateRevisions();
        long after = clock.getTime();

        assertBetween(before, after, replica.secondariesSafeTimestamp);
    }

    private static RevisionVector lastRev(long... timestamps) {
        return new ReplicaSetInfoMock.RevisionBuilder().addRevisions(timestamps).build();
    }

    private static void assertBetween(long from, long to, long actual) {
        final String msg = String.format("%d <= %d <= %d", from, actual, to);
        assertTrue(msg, from <= actual);
        assertTrue(msg, actual <= to);
    }

}