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
package org.apache.jackrabbit.oak.segment.http.server.handlers;

import com.sun.net.httpserver.HttpServer;
import io.aeron.cluster.service.Cluster;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterLauncher;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronPrometheusMetrics;
import org.apache.jackrabbit.oak.segment.consensus.aeron.CrashHandler;
import org.apache.jackrabbit.oak.segment.consensus.aeron.LeadershipChange;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AeronApiHandlerTest {

    @Test
    public void testGetClusterStateDataEnrichesStateAndSelfMember() {
        ServerContext context = newContext();
        AeronConsensusEngine engine = baseEngine();
        context.aeronConsensusEngine = engine;

        CrashHandler crashHandler = mock(CrashHandler.class);
        when(crashHandler.hasCrashed()).thenReturn(false);
        when(crashHandler.getCrashCount()).thenReturn(0);
        when(crashHandler.shouldForceBootstrap()).thenReturn(false);
        AeronClusterLauncher launcher = mock(AeronClusterLauncher.class);
        when(launcher.getCrashHandler()).thenReturn(crashHandler);
        context.aeronClusterLauncher = launcher;

        AeronApiHandler handler = new AeronApiHandler(context);
        Map<String, Object> state = handler.getClusterStateData();

        assertNotNull(state);
        assertEquals("oak-consensus-cluster", state.get("clusterId"));
        assertEquals(2, state.get("nodeId"));

        @SuppressWarnings("unchecked")
        Map<String, Object> validatorIdentity = (Map<String, Object>) state.get("validatorIdentity");
        assertEquals("0x2222222222222222222222222222222222222222", validatorIdentity.get("walletAddress"));
        assertEquals("0xabc123", validatorIdentity.get("publicKey"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> members = (List<Map<String, Object>>) state.get("members");
        assertEquals("0x2222222222222222222222222222222222222222", members.get(0).get("walletAddress"));
        assertEquals("0xabc123", members.get(0).get("publicKey"));

        @SuppressWarnings("unchecked")
        Map<String, Object> mediaDriver = (Map<String, Object>) state.get("mediaDriver");
        assertEquals("HEALTHY", mediaDriver.get("status"));

        @SuppressWarnings("unchecked")
        Map<String, Object> quorum = (Map<String, Object>) state.get("quorum");
        assertEquals(2, quorum.get("required"));
        assertEquals(3, quorum.get("current"));
        assertEquals(true, quorum.get("hasQuorum"));

        @SuppressWarnings("unchecked")
        Map<String, Object> health = (Map<String, Object>) state.get("health");
        assertEquals("HEALTHY", health.get("status"));
        assertEquals(true, health.get("mediaDriverHealthy"));
    }

    @Test
    public void testHandleClusterStateReturnsServiceUnavailableWithoutEngine() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        AeronApiHandler handler = new AeronApiHandler(newContext());
        handler.handleClusterState(response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        String json = body.toString();
        assertTrue(json.contains("\"code\":\"service_unavailable\""));
        assertTrue(json.contains("\"error\":\"Aeron Cluster consensus not configured\""));
    }

    @Test
    public void testGetClusterStateDataUsesFallbacksWhenNativeStateIsSparse() {
        ServerContext context = newContext();
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        Map<String, Object> nativeState = new HashMap<>();
        nativeState.put("memberId", "not-a-number");
        nativeState.put("memberCount", 3);
        nativeState.put("reachableCount", 7);
        when(engine.getNativeClusterState()).thenReturn(nativeState);
        when(engine.getWalletAddress()).thenReturn(null);
        when(engine.getPublicKeyHex()).thenReturn(null);
        when(engine.getReachableValidatorCount()).thenReturn(1);
        when(engine.getLastHeartbeatTime()).thenReturn(1234L);
        context.aeronConsensusEngine = engine;

        AeronClusterLauncher launcher = mock(AeronClusterLauncher.class);
        when(launcher.getCrashHandler()).thenReturn(null);
        context.aeronClusterLauncher = launcher;
        context.aeronPrometheusMetrics = mock(AeronPrometheusMetrics.class);

        AeronApiHandler handler = new AeronApiHandler(context);
        Map<String, Object> state = handler.getClusterStateData();

        assertNotNull(state);
        assertEquals(2, state.get("nodeId"));
        assertFalse(state.containsKey("validatorIdentity"));
        assertEquals(7, state.get("reachableCount"));

        @SuppressWarnings("unchecked")
        Map<String, Object> mediaDriver = (Map<String, Object>) state.get("mediaDriver");
        assertEquals("UNKNOWN", mediaDriver.get("status"));

        @SuppressWarnings("unchecked")
        Map<String, Object> aeronMetrics = (Map<String, Object>) state.get("aeronMetrics");
        assertEquals(true, aeronMetrics.get("available"));

        @SuppressWarnings("unchecked")
        Map<String, Object> health = (Map<String, Object>) state.get("health");
        assertEquals("DEGRADED", health.get("status"));
        assertEquals(false, health.get("mediaDriverHealthy"));
    }

    @Test
    public void testGetClusterStateDataReturnsNullWhenNativeStateMissing() {
        ServerContext context = newContext();
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        when(engine.getNativeClusterState()).thenReturn(null);
        context.aeronConsensusEngine = engine;

        AeronApiHandler handler = new AeronApiHandler(context);
        assertEquals(null, handler.getClusterStateData());
    }

    @Test
    public void testHandleValidatorIdentitiesReturnsSelfIdentity() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        ServerContext context = newContext();
        context.aeronConsensusEngine = baseEngine();

        AeronApiHandler handler = new AeronApiHandler(context);
        handler.handleValidatorIdentities(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"totalValidators\":1"));
        assertTrue(json.contains("\"knownWallets\":1"));
        assertTrue(json.contains("\"walletAddress\":\"0x2222222222222222222222222222222222222222\""));
        assertTrue(json.contains("\"publicKey\":\"0xabc123\""));
    }

    @Test
    public void testHandleValidatorIdentitiesReturnsServiceUnavailableWithoutEngine() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        AeronApiHandler handler = new AeronApiHandler(newContext());
        handler.handleValidatorIdentities(response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("\"error\":\"Aeron Cluster consensus not configured\""));
    }

    @Test
    public void testGetValidatorIdentitiesDataIncludesRemoteFollowerIdentityAndSkipsBlankFollowers() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/aeron/cluster-state", exchange -> {
            byte[] body = ("{\"memberId\":7,\"role\":\"follower\",\"validatorIdentity\":{"
                + "\"walletAddress\":\"0x3333333333333333333333333333333333333333\","
                + "\"publicKey\":\"0xdef456\"}}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(HttpServletResponse.SC_OK, body.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        server.start();

        try {
            ServerContext context = newContext();
            AeronConsensusEngine engine = baseEngine();
            when(engine.getAllFollowers()).thenReturn(Arrays.asList(null, "", "http://localhost:" + server.getAddress().getPort()));
            context.aeronConsensusEngine = engine;

            AeronApiHandler handler = new AeronApiHandler(context);
            Map<String, Object> payload = handler.getValidatorIdentitiesData();

            assertNotNull(payload);
            assertEquals(2, payload.get("totalValidators"));
            assertEquals(2L, payload.get("knownWallets"));

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> validators = (List<Map<String, Object>>) payload.get("validators");
            assertEquals(2, validators.size());
            Map<String, Object> remote = validators.get(1);
            assertEquals(7, remote.get("memberId"));
            assertEquals("FOLLOWER", remote.get("role"));
            assertEquals("ACTIVE", remote.get("status"));
            assertEquals("0x3333333333333333333333333333333333333333", remote.get("walletAddress"));
            assertEquals("0xdef456", remote.get("publicKey"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testHandleRaftMetricsReturnsElectionReplicationAndCommitMetrics() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        ServerContext context = newContext();
        AeronConsensusEngine engine = baseEngine();
        when(engine.getCurrentTerm()).thenReturn(8);
        when(engine.isLeader()).thenReturn(true);
        when(engine.getCurrentLeader()).thenReturn("http://validator-1:8090");
        when(engine.getAllFollowers()).thenReturn(Arrays.asList("http://validator-1:8090", "http://validator-3:8090"));
        when(engine.getCurrentEpoch()).thenReturn(21);
        when(engine.getCurrentEthereumEpoch()).thenReturn(34);
        context.aeronConsensusEngine = engine;

        AeronApiHandler handler = new AeronApiHandler(context);
        handler.handleRaftMetrics(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"currentTerm\":8"));
        assertTrue(json.contains("\"isLeader\":true"));
        assertTrue(json.contains("\"totalFollowers\":2"));
        assertTrue(json.contains("\"currentEpoch\":21"));
        assertTrue(json.contains("\"ethereumEpoch\":34"));
    }

    @Test
    public void testHandleRaftMetricsReturnsServiceUnavailableWithoutEngine() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        AeronApiHandler handler = new AeronApiHandler(newContext());
        handler.handleRaftMetrics(response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("\"error\":\"Aeron Cluster consensus not configured\""));
    }

    @Test
    public void testHandleNodeStatusUsesNodeIdToSelectLeader() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter("nodeId")).thenReturn("1");

        ServerContext context = newContext();
        AeronConsensusEngine engine = baseEngine();
        when(engine.getCurrentLeader()).thenReturn("http://validator-1:8090");
        when(engine.getAllFollowers()).thenReturn(Arrays.asList("http://validator-1:8090", "http://validator-3:8090"));
        when(engine.getLastHeartbeatTime()).thenReturn(9876L);
        context.aeronConsensusEngine = engine;

        AeronApiHandler handler = new AeronApiHandler(context);
        handler.handleNodeStatus(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"nodeId\":1"));
        assertTrue(json.contains("\"url\":\"http://validator-1:8090\""));
        assertTrue(json.contains("\"role\":\"LEADER\""));
        assertTrue(json.contains("\"isSelf\":false"));
    }

    @Test
    public void testHandleNodeStatusUsesNodeIdToSelectFollower() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter("nodeId")).thenReturn("3");

        ServerContext context = newContext();
        AeronConsensusEngine engine = baseEngine();
        when(engine.getCurrentLeader()).thenReturn("http://validator-1:8090");
        when(engine.getAllFollowers()).thenReturn(Arrays.asList("http://validator-2:8090", "http://validator-3:8090"));
        when(engine.getLastHeartbeatTime()).thenReturn(9876L);
        context.aeronConsensusEngine = engine;

        AeronApiHandler handler = new AeronApiHandler(context);
        handler.handleNodeStatus(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"nodeId\":3"));
        assertTrue(json.contains("\"url\":\"http://validator-3:8090\""));
        assertTrue(json.contains("\"role\":\"FOLLOWER\""));
        assertTrue(json.contains("\"isSelf\":false"));
    }

    @Test
    public void testHandleNodeStatusUsesExplicitUrlParameterAndMarksSelf() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter("url")).thenReturn("http://validator-2:8090");

        ServerContext context = newContext();
        AeronConsensusEngine engine = baseEngine();
        when(engine.getLastHeartbeatTime()).thenReturn(9876L);
        context.aeronConsensusEngine = engine;

        AeronApiHandler handler = new AeronApiHandler(context);
        handler.handleNodeStatus(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"nodeId\":2"));
        assertTrue(json.contains("\"role\":\"LEADER\""));
        assertTrue(json.contains("\"isSelf\":true"));
    }

    @Test
    public void testHandleNodeStatusReturnsServiceUnavailableWithoutEngine() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        AeronApiHandler handler = new AeronApiHandler(newContext());
        handler.handleNodeStatus(mock(HttpServletRequest.class), response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("\"error\":\"Aeron Cluster consensus not configured\""));
    }

    @Test
    public void testHandleLeadershipHistoryCapsLimitAndMarksLeaderRotation() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter("limit")).thenReturn("500");

        ServerContext context = newContext();
        AeronConsensusEngine engine = baseEngine();
        when(engine.getLeadershipHistory(100)).thenReturn(Arrays.asList(
            new LeadershipChange(
                1234L,
                5678L,
                Cluster.Role.LEADER,
                Cluster.Role.FOLLOWER,
                7,
                2,
                "http://validator-2:8090"
            )
        ));
        context.aeronConsensusEngine = engine;

        AeronApiHandler handler = new AeronApiHandler(context);
        handler.handleLeadershipHistory(request, response);

        verify(engine).getLeadershipHistory(100);
        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"contractVersion\":\"aeron.leadership-history.v2\""));
        assertTrue(json.contains("\"limit\":100"));
        assertTrue(json.contains("\"totalEntries\":1"));
        assertTrue(json.contains("\"memberUrl\":\"http://validator-2:8090\""));
        assertTrue(json.contains("\"timestamp\":1234"));
        assertTrue(json.contains("\"observedAtMs\":1234"));
        assertTrue(json.contains("\"clusterTime\":5678"));
        assertTrue(json.contains("\"isLeaderRotation\":true"));
    }

    @Test
    public void testHandleLeadershipHistoryDefaultsInvalidLimitAndUnknownPreviousRole() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter("limit")).thenReturn("0");

        ServerContext context = newContext();
        AeronConsensusEngine engine = baseEngine();
        when(engine.getLeadershipHistory(10)).thenReturn(Arrays.asList(
            new LeadershipChange(
                2234L,
                Cluster.Role.FOLLOWER,
                null,
                9,
                3,
                "http://validator-3:8090"
            )
        ));
        context.aeronConsensusEngine = engine;

        AeronApiHandler handler = new AeronApiHandler(context);
        handler.handleLeadershipHistory(request, response);

        verify(engine).getLeadershipHistory(10);
        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"limit\":10"));
        assertTrue(json.contains("\"previousRole\":\"UNKNOWN\""));
        assertTrue(json.contains("\"newRole\":\"FOLLOWER\""));
        assertTrue(json.contains("\"isLeaderRotation\":false"));
    }

    @Test
    public void testHandleLeadershipHistoryDefaultsOnNonNumericLimit() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter("limit")).thenReturn("abc");

        ServerContext context = newContext();
        AeronConsensusEngine engine = baseEngine();
        when(engine.getLeadershipHistory(10)).thenReturn(new ArrayList<>());
        context.aeronConsensusEngine = engine;

        AeronApiHandler handler = new AeronApiHandler(context);
        handler.handleLeadershipHistory(request, response);

        verify(engine).getLeadershipHistory(10);
        assertTrue(body.toString().contains("\"limit\":10"));
    }

    @Test
    public void testHandleLeadershipHistoryReturnsServiceUnavailableWithoutEngine() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        AeronApiHandler handler = new AeronApiHandler(newContext());
        handler.handleLeadershipHistory(mock(HttpServletRequest.class), response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("\"error\":\"Aeron Cluster consensus not configured\""));
    }

    @Test
    public void testHandleReplicationLagReturnsNotFoundWhenUnavailable() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        ServerContext context = newContext();
        AeronConsensusEngine engine = baseEngine();
        when(engine.getReplicationLagStatus()).thenReturn(null);
        context.aeronConsensusEngine = engine;

        AeronApiHandler handler = new AeronApiHandler(context);
        handler.handleReplicationLag(response);

        verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);
        String json = body.toString();
        assertTrue(json.contains("\"code\":\"not_found\""));
        assertTrue(json.contains("\"error\":\"Replication lag not applicable (cluster not initialized)\""));
    }

    @Test
    public void testHandleReplicationLagReturnsPayloadWhenAvailable() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        ServerContext context = newContext();
        AeronConsensusEngine engine = baseEngine();
        Map<String, Object> lagStatus = new HashMap<>();
        lagStatus.put("role", "FOLLOWER");
        lagStatus.put("replicationLag", 12);
        lagStatus.put("healthy", false);
        when(engine.getReplicationLagStatus()).thenReturn(lagStatus);
        context.aeronConsensusEngine = engine;

        AeronApiHandler handler = new AeronApiHandler(context);
        handler.handleReplicationLag(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"role\":\"FOLLOWER\""));
        assertTrue(json.contains("\"replicationLag\":12"));
        assertTrue(json.contains("\"healthy\":false"));
    }

    @Test
    public void testHandleReplicationLagReturnsServiceUnavailableWithoutEngine() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        AeronApiHandler handler = new AeronApiHandler(newContext());
        handler.handleReplicationLag(response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("\"error\":\"Aeron Cluster consensus not configured\""));
    }

    @Test
    public void testHandleGetOpsClusterSnapshotReturnsServiceUnavailableWithoutCache() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        AeronApiHandler handler = new AeronApiHandler(newContext());
        handler.handleGetOpsClusterSnapshot(response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        String json = body.toString();
        assertTrue(json.contains("\"code\":\"service_unavailable\""));
        assertTrue(json.contains("\"error\":\"Aeron Cluster consensus not configured\""));
    }

    @Test
    public void testHandleGetOpsClusterSnapshotUsesCacheAndStaleFallback() throws Exception {
        ServerContext context = newContext();
        context.aeronConsensusEngine = baseEngine();
        AeronApiHandler handler = new AeronApiHandler(context);

        StringWriter firstBody = new StringWriter();
        HttpServletResponse firstResponse = responseWithBody(firstBody);
        handler.handleGetOpsClusterSnapshot(firstResponse);
        verify(firstResponse).setStatus(HttpServletResponse.SC_OK);
        assertTrue(firstBody.toString().contains("\"contractVersion\":\"ops.v1\""));
        assertTrue(firstBody.toString().contains("\"hit\":false"));

        StringWriter secondBody = new StringWriter();
        HttpServletResponse secondResponse = responseWithBody(secondBody);
        handler.handleGetOpsClusterSnapshot(secondResponse);
        verify(secondResponse).setStatus(HttpServletResponse.SC_OK);
        assertTrue(secondBody.toString().contains("\"hit\":true"));
        assertFalse(secondBody.toString().contains("\"degraded\":true"));

        setLongField(handler, "cachedClusterSnapshotSourceTimestampMs", System.currentTimeMillis() - 5_000L);
        context.aeronConsensusEngine = null;

        StringWriter staleBody = new StringWriter();
        HttpServletResponse staleResponse = responseWithBody(staleBody);
        handler.handleGetOpsClusterSnapshot(staleResponse);
        verify(staleResponse).setStatus(HttpServletResponse.SC_OK);
        assertTrue(staleBody.toString().contains("\"degraded\":true"));
        assertTrue(staleBody.toString().contains("\"degradedReason\":\"STALE_CACHE_FALLBACK\""));
        assertTrue(staleBody.toString().contains("\"hit\":true"));
    }

    @Test
    public void testHandleGetOpsReplicationSnapshotUsesCacheAndStaleFallback() throws Exception {
        ServerContext context = newContext();
        AeronConsensusEngine engine = baseEngine();
        Map<String, Object> lagStatus = new HashMap<>();
        lagStatus.put("healthy", true);
        lagStatus.put("replicationLag", 4);
        when(engine.getReplicationLagStatus()).thenReturn(lagStatus);
        context.aeronConsensusEngine = engine;

        AeronApiHandler handler = new AeronApiHandler(context);

        StringWriter firstBody = new StringWriter();
        HttpServletResponse firstResponse = responseWithBody(firstBody);
        handler.handleGetOpsReplicationSnapshot(firstResponse);
        verify(firstResponse).setStatus(HttpServletResponse.SC_OK);
        assertTrue(firstBody.toString().contains("\"hit\":false"));
        assertTrue(firstBody.toString().contains("\"replicationLag\":4"));

        StringWriter secondBody = new StringWriter();
        HttpServletResponse secondResponse = responseWithBody(secondBody);
        handler.handleGetOpsReplicationSnapshot(secondResponse);
        verify(secondResponse).setStatus(HttpServletResponse.SC_OK);
        assertTrue(secondBody.toString().contains("\"hit\":true"));

        setLongField(handler, "cachedReplicationSnapshotSourceTimestampMs", System.currentTimeMillis() - 5_000L);
        context.aeronConsensusEngine = null;

        StringWriter staleBody = new StringWriter();
        HttpServletResponse staleResponse = responseWithBody(staleBody);
        handler.handleGetOpsReplicationSnapshot(staleResponse);
        verify(staleResponse).setStatus(HttpServletResponse.SC_OK);
        assertTrue(staleBody.toString().contains("\"degraded\":true"));
        assertTrue(staleBody.toString().contains("\"degradedReason\":\"STALE_CACHE_FALLBACK\""));
        assertTrue(staleBody.toString().contains("\"hit\":true"));
    }

    private static ServerContext newContext() {
        return new ServerContext(
            mock(FileStore.class),
            mock(NodeStore.class),
            Paths.get("/tmp/store"),
            "http://validator-2:8090"
        );
    }

    private static AeronConsensusEngine baseEngine() {
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        when(engine.getNativeClusterState()).thenReturn(nativeClusterState());
        when(engine.getWalletAddress()).thenReturn("0x2222222222222222222222222222222222222222");
        when(engine.getPublicKeyHex()).thenReturn("0xabc123");
        when(engine.getReachableValidatorCount()).thenReturn(3);
        when(engine.getLastHeartbeatTime()).thenReturn(1234L);
        when(engine.getCurrentLeader()).thenReturn("http://validator-2:8090");
        when(engine.getAllFollowers()).thenReturn(new ArrayList<>());
        when(engine.isLeader()).thenReturn(false);
        return engine;
    }

    private static Map<String, Object> nativeClusterState() {
        Map<String, Object> state = new HashMap<>();
        state.put("role", "FOLLOWER");
        state.put("memberId", 2);
        state.put("clusterMemberCount", 3);
        List<Map<String, Object>> members = new ArrayList<>();
        Map<String, Object> self = new HashMap<>();
        self.put("url", "http://validator-2:8090");
        self.put("memberId", 2);
        members.add(self);
        Map<String, Object> peer = new HashMap<>();
        peer.put("url", "http://validator-1:8090");
        peer.put("memberId", 1);
        members.add(peer);
        state.put("members", members);
        return state;
    }

    private static HttpServletResponse responseWithBody(StringWriter body) throws Exception {
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));
        return response;
    }

    private static void setLongField(Object target, String fieldName, long value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setLong(target, value);
    }
}
