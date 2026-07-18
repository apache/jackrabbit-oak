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

import io.aeron.cluster.service.Cluster;
import org.junit.Test;

import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AeronClusterStateViewTest {

    @Test
    public void buildNativeClusterStateIncludesWalletAndPeerRoles() {
        AeronClusterStateView view = createView(
            "http://self:8080",
            List.of("http://peer-1:8081"),
            Map.of(0, "http://self:8080", 1, "http://peer-1:8081")
        );
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        when(cluster.memberId()).thenReturn(0);
        when(cluster.time()).thenReturn(1234L);
        when(cluster.logPosition()).thenReturn(5678L);

        Map<String, Object> state = view.buildNativeClusterState(
            cluster,
            "http://self:8080",
            "0xabc",
            "0xpub",
            7,
            8,
            9
        );

        assertEquals("LEADER", state.get("role"));
        assertEquals(Boolean.TRUE, state.get("isLeader"));
        assertEquals(0, state.get("memberId"));
        assertEquals(7, state.get("term"));
        assertEquals(8, state.get("epoch"));
        assertEquals(9, state.get("ethereumEpoch"));
        assertEquals("http://self:8080", state.get("currentLeader"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> members = (List<Map<String, Object>>) state.get("members");
        assertEquals(2, members.size());
        assertEquals("0xabc", members.get(0).get("walletAddress"));
        assertEquals("0xpub", members.get(0).get("publicKey"));
        assertEquals("FOLLOWER", members.get(1).get("role"));
    }

    @Test
    public void buildNativeClusterStateFallsBackToUrlLookupWhenMemberIdUnknown() {
        AeronClusterStateView view = createView(
            "http://127.0.0.1:8080",
            List.of(),
            Map.of(7, "http://localhost:8080")
        );
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(cluster.memberId()).thenReturn(-1);
        when(cluster.time()).thenReturn(10L);
        when(cluster.logPosition()).thenReturn(11L);

        Map<String, Object> state = view.buildNativeClusterState(
            cluster,
            "http://leader:8081",
            null,
            null,
            1,
            2,
            3
        );

        assertEquals(7, state.get("memberId"));
    }

    @Test
    public void resolveLeaderMemberIdPrefersCurrentClusterMemberWhenLeader() {
        AeronClusterStateView view = createView(
            "http://self:8080",
            List.of("http://peer:8081"),
            Map.of(0, "http://self:8080", 1, "http://peer:8081")
        );
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        when(cluster.memberId()).thenReturn(4);

        assertEquals(4, view.resolveLeaderMemberId(cluster, "http://peer:8081"));
    }

    @Test
    public void resolveLeaderMemberIdMatchesLeaderUrlByPort() {
        AeronClusterStateView view = createView(
            "http://self:8080",
            List.of("http://peer-name:8081"),
            Map.of(1, "http://peer-name:8081")
        );
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);

        assertEquals(1, view.resolveLeaderMemberId(cluster, "http://127.0.0.1:8081"));
    }

    @Test
    public void buildReplicationLagStatusMarksHealthyAndUnknownStates() {
        AeronClusterStateView view = createView("http://self:8080", List.of(), Map.of());
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(cluster.logPosition()).thenReturn(250L);

        Map<String, Object> healthy = view.buildReplicationLagStatus(cluster, 300L, 50L);
        Map<String, Object> unknown = view.buildReplicationLagStatus(cluster, 0L, -1L);

        assertEquals(Boolean.TRUE, healthy.get("healthy"));
        assertEquals(Boolean.TRUE, healthy.get("measurementAvailable"));
        assertEquals("HEALTHY", healthy.get("healthStatus"));
        assertFalse(healthy.containsKey("reason"));
        assertEquals(null, unknown.get("healthy"));
        assertEquals(null, unknown.get("leaderLogPosition"));
        assertEquals(null, unknown.get("replicationLag"));
        assertEquals(Boolean.FALSE, unknown.get("measurementAvailable"));
        assertEquals("UNKNOWN", unknown.get("healthStatus"));
        assertEquals("leader_log_position_unknown", unknown.get("reason"));
    }

    private static AeronClusterStateView createView(String selfUrl,
                                                    List<String> peerUrls,
                                                    Map<Integer, String> nodeIdToUrl) {
        return new AeronClusterStateView(selfUrl, peerUrls, nodeIdToUrl, AeronClusterStateViewTest::isSameUrlByPort);
    }

    private static boolean isSameUrlByPort(String left, String right) {
        try {
            URL first = new URL(left);
            URL second = new URL(right);
            return first.getPort() == second.getPort();
        } catch (Exception e) {
            return left.equals(right);
        }
    }
}
