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

import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Test;

import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DashboardHandlerTest {

    @After
    public void tearDown() {
        System.clearProperty("oak.dashboard.external.url");
        System.clearProperty("oak.blockchain.mode");
        BlockchainConfig.reset();
    }

    @Test
    public void testHandleApiIndexIncludesCoreEndpoints() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));

        ServerContext context = new ServerContext(
            mock(FileStore.class),
            mock(NodeStore.class),
            Paths.get("/tmp/store"),
            "http://localhost:8090"
        );
        DashboardHandler handler = new DashboardHandler(context);

        handler.handleApiIndex(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        verify(response).setContentType("application/json; charset=UTF-8");
        String json = body.toString();
        assertTrue(json.contains("\"contractVersion\":\"index.v1\""));
        assertTrue(json.contains("\"surfaceRole\":\"validator-native\""));
        assertTrue(json.contains("\"surfaceClasses\":[\"source\",\"local-ui\",\"local-diagnostic\",\"internal\"]"));
        assertTrue(json.contains("\"preferredBrowserContract\":\"/ops/v1/* via edge/gateway\""));
        assertTrue(json.contains("\"browserContractNotes\":\"Local HTML routes remain diagnostic-only; upstream UX should consume governed /ops/v1/* surfaces.\""));
        assertTrue(json.contains("\"count\":"));
        assertTrue(json.contains("\"path\":\"/v1/index\""));
        assertTrue(json.contains("\"path\":\"/v1/consensus/leader\""));
        assertTrue(json.contains("\"surfaceClass\":\"source\""));
        assertTrue(json.contains("\"surfaceClass\":\"local-ui\""));
        assertTrue(json.contains("\"surfaceClass\":\"local-diagnostic\""));
        assertTrue(json.contains("\"surfaceClass\":\"internal\""));
        assertTrue(json.contains("\"path\":\"/v1/ops/snapshots/runtime\""));
        assertTrue(json.contains("\"path\":\"/v1/ops/snapshots/storage\""));
        assertTrue(json.contains("\"path\":\"/v1/config/osgi/coverage\""));
        assertTrue(json.contains("\"path\":\"/v1/config/osgi/delta\""));
        assertTrue(json.contains("\"path\":\"/v1/proposals/queue/stats\""));
        assertTrue(json.contains("\"path\":\"/v1/proposals/release-flow\""));
        assertTrue(json.contains("\"path\":\"/v1/settlement/proposals/{proposalId}\""));
        assertTrue(json.contains("\"path\":\"/v1/settlement/transactions/{transactionHash}\""));
        assertFalse(json.contains("\"path\":\"/v1/proposals/epochs\""));
        assertFalse(json.contains("\"path\":\"/v1/explorer/epochs\""));
        assertTrue(json.contains("\"path\":\"/v1/explorer/summary\""));
        assertTrue(json.contains("\"path\":\"/v1/explorer/release-flow\""));
        assertTrue(json.contains("\"path\":\"/v1/explorer/content/nav\""));
        assertTrue(json.contains("\"path\":\"/v1/explorer/content/clusters/{clusterId}/tree\""));
        assertTrue(json.contains("\"path\":\"/v1/explorer/content/clusters/{clusterId}/node\""));
        assertTrue(json.contains("\"path\":\"/v1/explorer/content/clusters/{clusterId}/provenance\""));
        assertTrue(json.contains("\"replacement\":\"/ops/v1/explorer/proposals/{proposalId}\""));
        assertTrue(json.contains("\"path\":\"/v1/consensus/status\""));
        assertTrue(json.contains("\"path\":\"/v1/aeron/cluster-state\""));
        assertTrue(json.contains("\"path\":\"/v1/events/stats\""));
        assertTrue(json.contains("\"path\":\"/v1/gc/status\""));
        assertTrue(json.contains("\"path\":\"/metrics\""));
        assertTrue(json.contains("\"path\":\"/api/cid/stats\",\"description\":\"CID mapping stats\",\"category\":\"Storage\""));
        assertTrue(json.contains("\"path\":\"/api/segments/tars\",\"description\":\"TAR file listing\",\"category\":\"Storage\""));
        assertFalse(json.contains("/api/mock/advance-epoch"));
        assertFalse(json.contains("/api/mock/set-epoch-offset"));
        assertFalse(json.contains("/api/mock/epoch-status"));
    }

    @Test
    public void testHandleDashboardRendersApiFirstLanding() throws Exception {
        String html = renderDashboard(new DashboardHandler(newContext()));

        assertTrue(html.contains("Oak Control Plane Home"));
        assertTrue(html.contains("CRX/OC"));
        assertTrue(html.contains("Local API Browser"));
        assertTrue(html.contains("Reachable"));
        assertTrue(html.contains("Quorum"));
        assertTrue(html.contains("/v1/index"));
        assertTrue(html.contains("/ops/v1/*"));
        assertTrue(html.contains("/v1/proposals/queue/stats"));
        assertTrue(html.contains("/v1/proposals/release-flow"));
        assertTrue(html.contains("API-first runtime"));
        assertSharedHeader(html, "/", "Dashboard");
        assertFalse(html.contains("{{SHARED_HEADER}}"));
        assertFalse(html.contains("{{SHARED_HEADER_STYLES}}"));
    }

    @Test
    public void testHandleDashboardUsesMemberIdWhenLeaderNodeIdMissing() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));

        ServerContext context = new ServerContext(
            mock(FileStore.class),
            mock(NodeStore.class),
            Paths.get("/tmp/store"),
            "http://localhost:8090"
        );

        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        Map<String, Object> nativeState = new HashMap<>();
        nativeState.put("role", "LEADER");
        nativeState.put("memberId", 0);
        nativeState.put("clusterMemberCount", 3);
        nativeState.put("leadershipTermId", 1L);
        when(engine.getNativeClusterState()).thenReturn(nativeState);
        when(engine.getReachableValidatorCount()).thenReturn(3);
        when(engine.getLastHeartbeatTime()).thenReturn(System.currentTimeMillis());
        context.aeronConsensusEngine = engine;

        DashboardHandler handler = new DashboardHandler(context);
        handler.handleDashboard(response);

        String html = body.toString();
        assertTrue(html.contains("<div class=\"k\">Leader</div><div class=\"v\">0</div>"));
        assertTrue(html.contains("<div class=\"k\">Reachable</div><div class=\"v\">3/3</div>"));
        assertTrue(html.contains("<div class=\"k\">Quorum</div><div class=\"v\">YES (2)</div>"));
    }

    @Test
    public void testHandleDashboardRendersAeronTermDegradedPostureAndMembers() throws Exception {
        Map<String, Object> nativeState = new HashMap<>();
        nativeState.put("role", "FOLLOWER");
        nativeState.put("memberId", 0);
        nativeState.put("memberCount", 3);
        nativeState.put("term", 5);
        nativeState.put("currentLeader", "http://127.0.0.1:8092");
        nativeState.put("members", Arrays.asList(
            member(2, "FOLLOWER", "http://127.0.0.1:8094"),
            member(0, "FOLLOWER", "http://localhost:8090"),
            member(1, "LEADER", "http://127.0.0.1:8092")));

        String html = renderDashboard(new DashboardHandler(aeronContext(nativeState, 2)));

        assertTrue(html.contains("<div class=\"k\">Term</div><div class=\"v\">5</div>"));
        assertTrue(html.contains("<div class=\"k\">Reachable</div><div class=\"v\">2/3</div>"));
        assertTrue(html.contains("posture-warn"));
        assertTrue(html.contains("Quorum held, 1 unreachable"));
        assertTrue(html.contains("<li class=\"member is-self\"><a class=\"member-name\" href=\"http://localhost:8090/\""));
        assertTrue(html.contains("<li class=\"member is-leader\"><a class=\"member-name\" href=\"http://127.0.0.1:8092/\""));
        assertTrue(html.contains("<span class=\"member-self\">this node</span>"));
        assertTrue(html.indexOf("Node 0</a>") < html.indexOf("Node 1</a>"));
        assertTrue(html.indexOf("Node 1</a>") < html.indexOf("Node 2</a>"));
    }

    @Test
    public void testHandleDashboardRendersNoQuorumAndDoesNotLinkNonHttpMembers() throws Exception {
        Map<String, Object> nativeState = new HashMap<>();
        nativeState.put("role", "FOLLOWER");
        nativeState.put("memberId", 0);
        nativeState.put("memberCount", 3);
        nativeState.put("members", Arrays.asList(member(0, "FOLLOWER", "javascript:alert(1)")));

        String html = renderDashboard(new DashboardHandler(aeronContext(nativeState, 1)));

        assertTrue(html.contains("<div class=\"k\">Quorum</div><div class=\"v\">NO (2)</div>"));
        assertTrue(html.contains("posture-bad"));
        assertTrue(html.contains("No quorum"));
        assertTrue(html.contains("<span class=\"member-name\">Node 0</span>"));
        assertFalse(html.contains("javascript:alert"));
    }

    @Test
    public void testHandleDashboardDerivesLeaderFromCurrentLeaderUrlWhenIdsMissing() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));

        ServerContext context = new ServerContext(
            mock(FileStore.class),
            mock(NodeStore.class),
            Paths.get("/tmp/store"),
            "http://localhost:8090"
        );

        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        Map<String, Object> nativeState = new HashMap<>();
        nativeState.put("role", "FOLLOWER");
        nativeState.put("memberId", 2);
        nativeState.put("clusterMemberCount", 4);
        nativeState.put("leadershipTermId", 9L);
        nativeState.put("currentLeader", "http://validator-4:8096");
        when(engine.getNativeClusterState()).thenReturn(nativeState);
        when(engine.getReachableValidatorCount()).thenReturn(4);
        when(engine.getLastHeartbeatTime()).thenReturn(System.currentTimeMillis());
        context.aeronConsensusEngine = engine;

        DashboardHandler handler = new DashboardHandler(context);
        handler.handleDashboard(response);

        String html = body.toString();
        assertTrue(html.contains("<div class=\"k\">Role</div><div class=\"v\">FOLLOWER</div>"));
        assertTrue(html.contains("<div class=\"k\">Node</div><div class=\"v\">2</div>"));
        assertTrue(html.contains("<div class=\"k\">Leader</div><div class=\"v\">3</div>"));
        assertTrue(html.contains("<div class=\"k\">Term</div><div class=\"v\">9</div>"));
        assertTrue(html.contains("<div class=\"k\">Members</div><div class=\"v\">4</div>"));
        assertTrue(html.contains("<div class=\"k\">Reachable</div><div class=\"v\">4/4</div>"));
        assertTrue(html.contains("<div class=\"k\">Quorum</div><div class=\"v\">YES (3)</div>"));
    }

    @Test
    public void testHandleDashboardRendersExternalDashboardLinkWhenConfigured() throws Exception {
        System.setProperty("oak.dashboard.external.url", "https://ops.example.invalid/dashboard");

        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));

        DashboardHandler handler = new DashboardHandler(newContext());
        handler.handleDashboard(response);

        String html = body.toString();
        assertTrue(html.contains("External Ops Dashboard"));
        assertTrue(html.contains("https://ops.example.invalid/dashboard"));
    }

    @Test
    public void testHandleDashboardFallsBackWhenClusterProbeFails() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));

        ServerContext context = newContext();
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        when(engine.getNativeClusterState()).thenThrow(new RuntimeException("boom"));
        context.aeronConsensusEngine = engine;

        DashboardHandler handler = new DashboardHandler(context);
        handler.handleDashboard(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String html = body.toString();
        assertTrue(html.contains("<div class=\"k\">Role</div><div class=\"v\">UNKNOWN</div>"));
        assertTrue(html.contains("<div class=\"k\">Node</div><div class=\"v\">UNKNOWN</div>"));
        assertTrue(html.contains("<div class=\"k\">Leader</div><div class=\"v\">UNKNOWN</div>"));
        assertTrue(html.contains("<div class=\"k\">Reachable</div><div class=\"v\">UNKNOWN</div>"));
        assertTrue(html.contains("<div class=\"k\">Quorum</div><div class=\"v\">UNKNOWN</div>"));
        assertTrue(html.contains("posture-unknown"));
    }

    @Test
    public void testHandleDashboardUsesLeaderMemberIdWhenProvided() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));

        ServerContext context = newContext();
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        Map<String, Object> nativeState = new HashMap<>();
        nativeState.put("role", "FOLLOWER");
        nativeState.put("memberId", 2);
        nativeState.put("leaderMemberId", 1);
        nativeState.put("memberCount", 5);
        nativeState.put("leadershipTerm", 7L);
        when(engine.getNativeClusterState()).thenReturn(nativeState);
        context.aeronConsensusEngine = engine;

        DashboardHandler handler = new DashboardHandler(context);
        handler.handleDashboard(response);

        String html = body.toString();
        assertTrue(html.contains("<div class=\"k\">Leader</div><div class=\"v\">1</div>"));
        assertTrue(html.contains("<div class=\"k\">Term</div><div class=\"v\">7</div>"));
        assertTrue(html.contains("<div class=\"k\">Members</div><div class=\"v\">5</div>"));
    }

    @Test
    public void testHandleExplorerUiRendersSepoliaModeBadge() throws Exception {
        System.setProperty("oak.blockchain.mode", "sepolia");
        BlockchainConfig.reset();

        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));

        DashboardHandler handler = new DashboardHandler(newContext());
        handler.handleExplorerUI(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        verify(response).setContentType("text/html; charset=UTF-8");
        String html = body.toString();
        assertTrue(html.contains("Explorer | Blockchain AEM Validator"));
        assertTrue(html.contains("mode-sepolia"));
        assertTrue(html.contains("SEPOLIA"));
        assertTrue(html.contains("CRX/OC Explorer"));
        assertTrue(html.contains("/v1/explorer/content/nav"));
        assertTrue(html.contains("/oak-chain/00/00/00/0x0000000000000000000000000000000000000000/content/genesis"));
        assertSharedHeader(html, "/explorer", "CRX/OC");
        assertFalse(html.contains("{{SHARED_HEADER}}"));
        assertFalse(html.contains("{{SHARED_HEADER_STYLES}}"));
    }

    @Test
    public void testHandleApiBrowserUiRendersMainnetModeBadge() throws Exception {
        System.setProperty("oak.blockchain.mode", "mainnet");
        BlockchainConfig.reset();

        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));

        DashboardHandler handler = new DashboardHandler(newContext());
        handler.handleApiBrowserUI(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        verify(response).setContentType("text/html; charset=UTF-8");
        String html = body.toString();
        assertTrue(html.contains("API Browser | Blockchain AEM Validator"));
        assertTrue(html.contains("mode-mainnet"));
        assertTrue(html.contains("MAINNET"));
        assertTrue(html.contains("CRX/OC"));
        assertTrue(html.contains("Filter endpoints by path, description, or category"));
        assertTrue(html.contains("Single-manifest endpoint catalog powered by"));
        assertFalse(html.contains("/api/mock/advance-epoch"));
        assertFalse(html.contains("Mock Mode"));
        assertSharedHeader(html, "/api-browser", "API Browser");
        assertFalse(html.contains("{{SHARED_HEADER}}"));
        assertFalse(html.contains("{{SHARED_HEADER_STYLES}}"));
    }

    @Test
    public void testSharedHeaderRendersConsistentBrandAcrossAllSurfaces() throws Exception {
        DashboardHandler handler = new DashboardHandler(newContext());

        assertSharedHeader(renderDashboard(handler), "/", "Dashboard");
        assertSharedHeader(renderExplorer(handler), "/explorer", "CRX/OC");
        assertSharedHeader(renderApiBrowser(handler), "/api-browser", "API Browser");
    }

    private static String renderDashboard(DashboardHandler handler) throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));
        handler.handleDashboard(response);
        verify(response).setStatus(HttpServletResponse.SC_OK);
        verify(response).setContentType("text/html; charset=UTF-8");
        return body.toString();
    }

    private static String renderExplorer(DashboardHandler handler) throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));
        handler.handleExplorerUI(response);
        verify(response).setStatus(HttpServletResponse.SC_OK);
        verify(response).setContentType("text/html; charset=UTF-8");
        return body.toString();
    }

    private static String renderApiBrowser(DashboardHandler handler) throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));
        handler.handleApiBrowserUI(response);
        verify(response).setStatus(HttpServletResponse.SC_OK);
        verify(response).setContentType("text/html; charset=UTF-8");
        return body.toString();
    }

    private static void assertSharedHeader(String html, String activeHref, String activeLabel) {
        assertTrue(html.contains("<span class=\"brand-title\">Blockchain AEM</span>"));
        assertTrue(html.contains("href=\"/\""));
        assertTrue(html.contains("href=\"/explorer\""));
        assertTrue(html.contains("href=\"/api-browser\""));
        assertTrue(html.contains(">" + activeLabel + "</a>"));
        assertTrue(html.contains("href=\"" + activeHref + "\" class=\"nav-link active\" aria-current=\"page\">" + activeLabel + "</a>"));
    }

    private static ServerContext aeronContext(Map<String, Object> nativeState, int reachable) {
        ServerContext context = newContext();
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        when(engine.getNativeClusterState()).thenReturn(nativeState);
        when(engine.getReachableValidatorCount()).thenReturn(reachable);
        when(engine.getLastHeartbeatTime()).thenReturn(System.currentTimeMillis());
        context.aeronConsensusEngine = engine;
        return context;
    }

    private static Map<String, Object> member(int memberId, String role, String url) {
        Map<String, Object> member = new HashMap<>();
        member.put("memberId", memberId);
        member.put("role", role);
        member.put("url", url);
        member.put("status", "ACTIVE");
        return member;
    }

    private static ServerContext newContext() {
        return new ServerContext(
            mock(FileStore.class),
            mock(NodeStore.class),
            Paths.get("/tmp/store"),
            "http://localhost:8090"
        );
    }
}
