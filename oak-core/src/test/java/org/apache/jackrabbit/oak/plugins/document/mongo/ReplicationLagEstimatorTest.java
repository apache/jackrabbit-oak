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

import static org.apache.jackrabbit.oak.plugins.document.mongo.ReplicationLagEstimator.ReplicaSetMemberState.ARBITER;
import static org.apache.jackrabbit.oak.plugins.document.mongo.ReplicationLagEstimator.ReplicaSetMemberState.DOWN;
import static org.apache.jackrabbit.oak.plugins.document.mongo.ReplicationLagEstimator.ReplicaSetMemberState.PRIMARY;
import static org.apache.jackrabbit.oak.plugins.document.mongo.ReplicationLagEstimator.ReplicaSetMemberState.SECONDARY;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.document.mongo.ReplicationLagEstimator.ReplicaSetMemberState;
import org.bson.BasicBSONObject;
import org.bson.types.BSONTimestamp;
import org.junit.Before;
import org.junit.Test;

public class ReplicationLagEstimatorTest {

    private static final long MAX_REPLICATION_LAG = TimeUnit.HOURS.toSeconds(6);

    private ReplicationLagEstimator estimator;

    private ReplicationSetStatusMock replicationSet;

    @Before
    public void resetEstimator() {
        estimator = new ReplicationLagEstimator(null, MAX_REPLICATION_LAG * 1000, 0);
    }

    @Test
    public void singleSecondaryTest() {
        addInstance(PRIMARY, "mongo-p", 10, 0);
        addInstance(SECONDARY, "mongo-s1", 11, 11);
        updateStats();
        assertEquals(1, getEstimationSec());
    }

    @Test
    public void maximumLagTest() {
        addInstance(PRIMARY, "mongo-p", 10, 0);
        addInstance(SECONDARY, "mongo-s1", 11, 11);
        addInstance(SECONDARY, "mongo-s2", 15, 15);
        updateStats();
        assertEquals(5, getEstimationSec());
    }

    @Test
    public void averageValueTest() {
        addInstance(PRIMARY, "mongo-p", 10, 0);
        addInstance(SECONDARY, "mongo-s1", 11, 11);
        updateStats();
        addInstance(PRIMARY, "mongo-p", 20, 0);
        addInstance(SECONDARY, "mongo-s1", 22, 22);
        updateStats();
        addInstance(PRIMARY, "mongo-p", 30, 0);
        addInstance(SECONDARY, "mongo-s1", 33, 33);
        updateStats();
        assertEquals(2, getEstimationSec());
    }

    @Test
    public void operationNotProcessedYet() {
        addInstance(PRIMARY, "mongo-p", 10, 0);
        addInstance(SECONDARY, "mongo-s1", 11, 11);
        updateStats();
        addInstance(PRIMARY, "mongo-p", 20, 0);
        addInstance(SECONDARY, "mongo-s1", 11, 21);
        updateStats();
        assertEquals(MAX_REPLICATION_LAG, getEstimationSec());
    }

    @Test
    public void secondaryDidntContactSinceOperation() {
        addInstance(PRIMARY, "mongo-p", 10, 0);
        addInstance(SECONDARY, "mongo-s1", 11, 11);
        updateStats();
        addInstance(PRIMARY, "mongo-p", 20, 0);
        addInstance(SECONDARY, "mongo-s1", 21, 15);
        updateStats();
        assertEquals(MAX_REPLICATION_LAG, getEstimationSec());
    }

    @Test
    public void oneSecondaryOutOfSync() {
        addInstance(PRIMARY, "mongo-p", 10, 0);
        addInstance(SECONDARY, "mongo-s1", 11, 11);
        addInstance(SECONDARY, "mongo-s2", 11, 11);
        updateStats();
        assertEquals(1, getEstimationSec());
        addInstance(PRIMARY, "mongo-p", 20, 0);
        addInstance(SECONDARY, "mongo-s1", 21, 21);
        addInstance(SECONDARY, "mongo-s2", 11, 11);
        updateStats();
        assertEquals(MAX_REPLICATION_LAG, getEstimationSec());
    }

    @Test
    public void brokenMember() {
        addInstance(PRIMARY, "mongo-p", 10, 0);
        addInstance(SECONDARY, "mongo-s1", 11, 11);
        addInstance(SECONDARY, "mongo-s2", 11, 11);
        addInstance(DOWN, "mongo-s3", 11, 11);
        updateStats();
        assertEquals(MAX_REPLICATION_LAG, getEstimationSec());
    }

    @Test
    public void noMembers() {
        addInstance(PRIMARY, "mongo-p", 10, 0);
        updateStats();
        assertEquals(MAX_REPLICATION_LAG, getEstimationSec());
    }

    @Test
    public void noPrimary() {
        addInstance(PRIMARY, "mongo-p", 10, 0);
        updateStats();
        assertEquals(MAX_REPLICATION_LAG, getEstimationSec());
    }

    @Test
    public void removedMember() {
        addInstance(PRIMARY, "mongo-p", 10, 0);
        addInstance(SECONDARY, "mongo-s1", 13, 13); // avg = 3
        addInstance(SECONDARY, "mongo-s2", 15, 15); // avg = 5
        updateStats();
        assertEquals(5, getEstimationSec());

        addInstance(PRIMARY, "mongo-p", 20, 0);
        addInstance(SECONDARY, "mongo-s1", 21, 21); // avg = 2
        addInstance(SECONDARY, "mongo-s2", 27, 27); // avg = 6
        updateStats();
        assertEquals(6, getEstimationSec());

        addInstance(PRIMARY, "mongo-p", 30, 0);
        addInstance(SECONDARY, "mongo-s1", 32, 32); // avg = 2
        updateStats();
        assertEquals(2, getEstimationSec());
    }

    @Test
    public void arbiterIsIgnored() {
        addInstance(PRIMARY, "mongo-p", 10, 0);
        addInstance(SECONDARY, "mongo-s1", 13, 13);
        addInstance(SECONDARY, "mongo-s2", 15, 15);
        addInstance(ARBITER, "mongo-s3", 30, 30);
        updateStats();
        assertEquals(5, getEstimationSec());

    }

    private void addInstance(ReplicaSetMemberState state, String name, int optimeSec, int lastHearBeatRecvSec) {
        if (replicationSet == null) {
            replicationSet = new ReplicationSetStatusMock();
        }
        replicationSet.addInstance(state, name, optimeSec, lastHearBeatRecvSec);
    }

    private void updateStats() {
        estimator.updateStats(replicationSet.getMembers());
        replicationSet = null;
    }

    private int getEstimationSec() {
        return (int) (estimator.estimate() / 1000);
    }

    private class ReplicationSetStatusMock {

        private List<BasicBSONObject> members = new ArrayList<BasicBSONObject>();

        private void addInstance(ReplicaSetMemberState state, String name, int optimeSec, int lastHeartbeatRecvSec) {
            BasicBSONObject member = new BasicBSONObject();
            member.put("stateStr", state.name());
            member.put("name", name);
            member.put("optime", new BSONTimestamp(optimeSec, 1));
            member.put("lastHeartbeatRecv", new Date(lastHeartbeatRecvSec * 1000));
            members.add(member);
        }

        private List<BasicBSONObject> getMembers() {
            return members;
        }
    }
}
