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
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import java.lang.reflect.Field;

import io.aeron.cluster.service.Cluster;
import org.junit.After;
import org.junit.Test;

import static io.aeron.cluster.service.Cluster.Role.CANDIDATE;
import static io.aeron.cluster.service.Cluster.Role.LEADER;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AeronHealthServiceTest {

    @After
    public void tearDown() {
        System.clearProperty("oak.cluster.heartbeat.maxAgeMs");
    }

    @Test
    public void heartbeatAgeAndStalenessRespectConfiguredThreshold() throws Exception {
        AeronHealthService service = new AeronHealthService();
        System.setProperty("oak.cluster.heartbeat.maxAgeMs", "10");
        setLastHeartbeatTime(service, System.currentTimeMillis() - 1000);

        assertTrue(service.getLastHeartbeatTime() > 0);
        assertTrue(service.getHeartbeatAgeMs() >= 1000);
        assertTrue(service.isHeartbeatStale());

        service.markHeartbeat();

        assertTrue(service.getLastHeartbeatTime() > 0);
        assertFalse(service.isHeartbeatStale());
    }

    @Test
    public void clusterHealthRejectsNullAndCandidateCluster() {
        AeronHealthService service = new AeronHealthService();
        Cluster candidate = mock(Cluster.class);
        when(candidate.role()).thenReturn(CANDIDATE);

        assertFalse(service.isClusterHealthy(null, null, null));
        assertFalse(service.isClusterHealthy(candidate.role(), null, null));
        assertTrue("cluster_not_initialized".equals(service.getUnhealthyReason(null, null, null)));
        assertTrue("leader_election_in_progress".equals(service.getUnhealthyReason(candidate.role(), null, null)));
    }

    @Test
    public void clusterHealthRejectsMissingQuorum() {
        AeronHealthService service = new AeronHealthService();
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(LEADER);

        assertFalse(service.isClusterHealthy(cluster.role(), () -> false, null));
        assertTrue("no_quorum".equals(service.getUnhealthyReason(cluster.role(), () -> false, null)));
    }

    @Test
    public void clusterHealthAcceptsHealthyLeaderWhenClientIsOpenClosedOrMissing() {
        AeronHealthService service = new AeronHealthService();
        Cluster cluster = mock(Cluster.class);
        io.aeron.cluster.client.AeronCluster openClient = mock(io.aeron.cluster.client.AeronCluster.class);
        io.aeron.cluster.client.AeronCluster closedClient = mock(io.aeron.cluster.client.AeronCluster.class);

        when(cluster.role()).thenReturn(LEADER);
        when(openClient.isClosed()).thenReturn(false);
        when(closedClient.isClosed()).thenReturn(true);

        assertTrue(service.isClusterHealthy(cluster.role(), () -> true, () -> openClient));
        assertTrue(service.isClusterHealthy(cluster.role(), () -> true, () -> closedClient));
        assertTrue(service.isClusterHealthy(cluster.role(), null, null));
        assertNull(service.getUnhealthyReason(cluster.role(), () -> true, () -> openClient));
        assertNull(service.getUnhealthyReason(cluster.role(), () -> true, () -> closedClient));
        assertNull(service.getUnhealthyReason(cluster.role(), null, null));
    }

    private static void setLastHeartbeatTime(AeronHealthService service, long value) throws Exception {
        Field field = AeronHealthService.class.getDeclaredField("lastHeartbeatTime");
        field.setAccessible(true);
        field.setLong(service, value);
    }
}
