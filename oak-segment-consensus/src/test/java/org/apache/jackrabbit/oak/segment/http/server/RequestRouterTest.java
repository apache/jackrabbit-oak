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
package org.apache.jackrabbit.oak.segment.http.server;

import io.aeron.cluster.service.Cluster;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.consensus.aeron.LeadershipChange;
import org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig;
import org.apache.jackrabbit.oak.segment.consensus.eth.BeaconChainClient;
import org.apache.jackrabbit.oak.segment.consensus.evm.EvmBridge;
import org.apache.jackrabbit.oak.segment.consensus.evm.PaymentProof;
import org.apache.jackrabbit.oak.segment.consensus.evm.SettlementDetails;
import org.apache.jackrabbit.oak.segment.consensus.fragmentation.FragmentationTracker;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCAccountManager;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCCostEstimate;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCCostEstimator;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCExecutionResult;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCProposal;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCProposalManager;
import org.apache.jackrabbit.oak.segment.consensus.leader.ValidatorRole;
import org.apache.jackrabbit.oak.segment.consensus.queue.DurabilityState;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalQueueManagerOptimized;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalState;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalStatus;
import org.apache.jackrabbit.oak.segment.consensus.sharding.ShardingRuntimeConfig;
import org.apache.jackrabbit.oak.segment.consensus.sharding.ShardRouter;
import org.apache.jackrabbit.oak.segment.consensus.util.WalletPathUtil;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.server.binary.CidMappingService;
import org.apache.jackrabbit.oak.segment.http.server.binary.UploadSession;
import org.apache.jackrabbit.oak.segment.http.server.model.ClientRegistration;
import org.apache.jackrabbit.oak.segment.http.server.model.ValidatorRegistration;
import org.apache.jackrabbit.oak.segment.http.server.sse.ContentEvent;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.file.Files;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.io.IOException;
import java.util.Comparator;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class RequestRouterTest {

    @Test
    public void testCloseShutsDownOwnedServices() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-close-owned-services");
            try {
                ServerContext context = newContext(mock(NodeStore.class), storeDirectory);
                context.cidMappingService = new CidMappingService(storeDirectory);
                RequestRouter router = new RequestRouter(context);

                org.apache.jackrabbit.oak.segment.http.server.binary.UploadSessionManager uploadSessionManager =
                    context.uploadSessionManager;
                uploadSessionManager.createSession("0xabc", 128L, "text/plain", null);
                context.cidMappingService.registerMapping(
                    "ed06f9cbf0fe878013ccb266170e6b3ba676933a6f065675cc0115c840bf1442#22216",
                    "Qmf4F3CWU6Ly958TFiR8BRP18gwvW3Xsj2yXu5DqkonWc3"
                );
                assertTrue(uploadSessionManager.getStats().total > 0);

                router.close();

                assertEquals(0, uploadSessionManager.getStats().total);
                assertNull(context.uploadSessionManager);
                assertNull(context.eventBroadcaster);
                assertNull(context.cidMappingService);
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testApiBrowserDisabledWhenHeadlessToggleIsFalse() throws Exception {
        withRoutingProperties(false, () -> {
            ServerContext context = newContext();
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/api-browser");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_GONE);
            assertTrue(body.toString().contains("Browser UI routes are disabled"));
        });
    }

    @Test
    public void testOsgiConfigRouteReturnsEffectiveConfig() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/v1/config/osgi");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"contractVersion\":\"config.osgi.v1\""));
        });
    }

    @Test
    public void testOsgiConfigDeltaRouteReturnsDeltaPayload() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/v1/config/osgi/delta");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"contractVersion\":\"config.osgi.delta.v1\""));
        });
    }

    @Test
    public void testOsgiConfigSchemaRouteReturnsSchemaPayload() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/v1/config/osgi/schema");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"contractVersion\":\"config.osgi.schema.v1\""));
        });
    }

    @Test
    public void testOsgiConfigSourcesRouteReturnsSourcesPayload() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/v1/config/osgi/sources");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"contractVersion\":\"config.osgi.sources.v1\""));
        });
    }

    @Test
    public void testOsgiConfigCoverageRouteReturnsCoveragePayload() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/v1/config/osgi/coverage");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"contractVersion\":\"config.osgi.coverage.v1\""));
        });
    }

    @Test
    public void testConsensusLeaderRouteReturnsCanonicalLeaderPayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            context.aeronConsensusEngine = mock(AeronConsensusEngine.class);
            when(context.aeronConsensusEngine.getCurrentRole()).thenReturn(ValidatorRole.LEADER);
            when(context.aeronConsensusEngine.isLeader()).thenReturn(true);
            when(context.aeronConsensusEngine.getCurrentLeader()).thenReturn("http://localhost:8090");
            when(context.aeronConsensusEngine.getCurrentTerm()).thenReturn(4);

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/consensus/leader");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"contractVersion\":\"consensus.leader.v1\""));
            assertTrue(body.toString().contains("\"currentLeader\":\"http://localhost:8090\""));
        });
    }

    @Test
    public void testDashboardRouteRendersLandingPage() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/dashboard");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("Oak Control Plane Home"));
        });
    }

    @Test
    public void testExplorerRouteRendersExplorerUiWhenBrowserEnabled() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/explorer");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("Explorer | Blockchain AEM Validator"));
        });
    }

    @Test
    public void testApiBrowserRouteRendersBrowserUiWhenEnabled() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/api-browser");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("API Browser | Blockchain AEM Validator"));
        });
    }

    @Test
    public void testApiIndexRouteReturnsIndexPayload() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/v1/index");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"contractVersion\":\"index.v1\""));
        });
    }

    @Test
    public void testOpsRuntimeSnapshotRouteReturnsRuntimeEnvelope() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            context.aeronConsensusEngine = mock(AeronConsensusEngine.class, RETURNS_DEEP_STUBS);
            when(context.aeronConsensusEngine.isClusterHealthy()).thenReturn(true);
            when(context.aeronConsensusEngine.getCurrentRole()).thenReturn(ValidatorRole.FOLLOWER);
            when(context.aeronConsensusEngine.isLeader()).thenReturn(false);
            when(context.aeronConsensusEngine.getCurrentLeader()).thenReturn("http://localhost:8090");
            when(context.aeronConsensusEngine.getCurrentLeaderHint()).thenReturn("http://localhost:8090");
            when(context.aeronConsensusEngine.getCurrentEpoch()).thenReturn(5);
            when(context.aeronConsensusEngine.getCurrentTerm()).thenReturn(2);
            when(context.aeronConsensusEngine.getCurrentEthereumEpoch()).thenReturn(100);
            when(context.aeronConsensusEngine.getReachableValidatorCount()).thenReturn(3);
            when(context.aeronConsensusEngine.getTotalMemberCount()).thenReturn(3);
            when(context.aeronConsensusEngine.getQuorumSize()).thenReturn(2);
            when(context.aeronConsensusEngine.getHeartbeatAgeMs()).thenReturn(12L);
            when(context.aeronConsensusEngine.getLastHeartbeatTime()).thenReturn(1234L);
            when(context.aeronConsensusEngine.getAllFollowers()).thenReturn(Arrays.asList("http://localhost:8092"));
            when(context.aeronConsensusEngine.getNativeClusterState()).thenReturn(Collections.singletonMap("memberId", 0));
            when(context.aeronConsensusEngine.getReplicationLagStatus()).thenReturn(Collections.singletonMap("healthy", true));

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/ops/snapshots/runtime");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"contractVersion\":\"ops.runtime.v1\""));
        });
    }

    @Test
    public void testFragmentationMetricsRouteReturnsTrackedEntityMetrics() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            FragmentationTracker tracker = new FragmentationTracker();
            tracker.recordWrite("0xabc", "data00001a.tar", 2048L);
            context.fragmentationTracker = tracker;
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/fragmentation/metrics");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"walletAddress\":\"0xabc\""));
            assertTrue(body.toString().contains("\"totalEntities\":1"));
        });
    }

    @Test
    public void testGcAccountRouteHandlesNestedWalletPath() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            GCAccountManager accountManager = new GCAccountManager();
            accountManager.addDebt("0xwallet", "/oak-chain/demo", 5L);
            accountManager.convertAllPendingToExecuted();
            context.gcAccountManager = accountManager;
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/gc/account/0xwallet");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"walletAddress\":\"0xwallet\""));
            assertTrue(body.toString().contains("\"executedDebt\":\"0.50\""));
        });
    }

    @Test
    public void testGcAccountPayRouteUsesNestedWalletPath() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            GCAccountManager accountManager = new GCAccountManager();
            accountManager.addDebt("0xwallet", "/oak-chain/demo", 5L);
            accountManager.convertAllPendingToExecuted();
            context.gcAccountManager = accountManager;
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("POST", "/v1/gc/account/0xwallet/pay");
            when(request.getParameter("amount")).thenReturn("0.25");
            when(request.getParameter("txHash")).thenReturn("0xtx");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"amountPaid\":\"0.25\""));
            assertTrue(body.toString().contains("\"remainingDebt\":\"0.25\""));
        });
    }

    @Test
    public void testGcAccountSetLimitRouteUpdatesDebtLimit() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            context.gcAccountManager = new GCAccountManager();
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("POST", "/v1/gc/account/0xwallet/set-limit");
            when(request.getParameter("limit")).thenReturn("12.5");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"debtLimit\":\"12.5\""));
        });
    }

    @Test
    public void testGcAccountExecutePendingRouteConvertsPendingDebt() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            GCAccountManager accountManager = new GCAccountManager();
            accountManager.addDebt("0xwallet", "/oak-chain/demo", 5L);
            context.gcAccountManager = accountManager;
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("POST", "/v1/gc/account/0xwallet/execute-pending");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"converted\":\"0.50\""));
            assertTrue(body.toString().contains("\"executedDebt\":\"0.50\""));
        });
    }

    @Test
    public void testFragmentationEntityRouteReturns404WhenWalletUnknown() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            context.fragmentationTracker = new FragmentationTracker();
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/fragmentation/metrics/0xmissing");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);
            assertTrue(body.toString().contains("No metrics found for wallet: 0xmissing"));
        });
    }

    @Test
    public void testGcTriggerRouteExecutesCycle() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            GCAccountManager accountManager = new GCAccountManager();
            accountManager.getAccount("0xblocked").totalDebt = new BigDecimal("120.00");
            context.gcAccountManager = accountManager;
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("POST", "/v1/gc/trigger");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"entitiesBlocked\":1"));
            assertTrue(body.toString().contains("\"blockedWallets\":[\"0xblocked\"]"));
        });
    }

    @Test
    public void testClusterHealthRouteReturnsClusterMetrics() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            AeronConsensusEngine engine = baseEngine();
            when(engine.isClusterHealthy()).thenReturn(true);
            when(engine.getReachableValidatorCount()).thenReturn(2);
            when(engine.getTotalMemberCount()).thenReturn(3);
            when(engine.getQuorumSize()).thenReturn(2);
            when(engine.hasQuorum()).thenReturn(true);
            when(engine.getLastHeartbeatTime()).thenReturn(1234L);
            when(engine.getHeartbeatAgeMs()).thenReturn(56L);
            when(engine.getCurrentLeader()).thenReturn("http://validator-2:8090");
            context.aeronConsensusEngine = engine;
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/health/cluster");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"status\":\"UP\""));
            assertTrue(body.toString().contains("\"reachableCount\":2"));
            assertTrue(body.toString().contains("\"hasQuorum\":true"));
        });
    }

    @Test
    public void testOpsHealthSnapshotRouteReturnsOpsEnvelope() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            AeronConsensusEngine engine = baseEngine();
            when(engine.isClusterHealthy()).thenReturn(true);
            when(engine.getReachableValidatorCount()).thenReturn(2);
            when(engine.getTotalMemberCount()).thenReturn(3);
            when(engine.getQuorumSize()).thenReturn(2);
            when(engine.getCurrentRole()).thenReturn(org.apache.jackrabbit.oak.segment.consensus.leader.ValidatorRole.FOLLOWER);
            when(engine.getCurrentLeader()).thenReturn("http://validator-2:8090");
            context.aeronConsensusEngine = engine;
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/ops/snapshots/health");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"contractVersion\":\"ops.v1\""));
            assertTrue(body.toString().contains("\"hit\":false"));
        });
    }

    @Test
    public void testAeronClusterStateRouteReturnsEnrichedState() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            context.selfUrl = "http://validator-2:8090";
            context.aeronConsensusEngine = baseEngine();
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/aeron/cluster-state");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"clusterId\":\"oak-consensus-cluster\""));
            assertTrue(body.toString().contains("\"walletAddress\":\"0x2222222222222222222222222222222222222222\""));
        });
    }

    @Test
    public void testExplorerSummaryRouteReturnsCompactSummaryPayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            context.validatorWalletAddress = "0x1234567890abcdef1234567890abcdef12345678";
            context.clusterWalletAddress = "0x9999999999999999999999999999999999999999";
            context.registeredClients.put("c1", new ClientRegistration("c1", "http://client-1:4502",
                "0x1234567890abcdef1234567890abcdef12345678"));
            context.registeredValidators.put("v1", new ValidatorRegistration("validator-1", "http://validator-1:8090"));
            AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
            when(engine.getClusterSize()).thenReturn(3);
            when(engine.getReachableValidatorCount()).thenReturn(2);
            when(engine.getCurrentRole()).thenReturn(ValidatorRole.FOLLOWER);
            when(engine.isLeader()).thenReturn(false);
            when(engine.getCurrentLeader()).thenReturn("http://validator-1:8090");
            when(engine.getCurrentTerm()).thenReturn(8);
            when(engine.getCurrentEpoch()).thenReturn(13);
            when(engine.getCurrentEthereumEpoch()).thenReturn(21);
            context.aeronConsensusEngine = engine;
            ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
            Map<String, Object> queueStats = new HashMap<>();
            queueStats.put("verifiedCount", 7L);
            queueStats.put("totalFinalizedCount", 5L);
            queueStats.put("batchQueueSize", 2L);
            queueStats.put("pendingCount", 1L);
            queueStats.put("mempoolPendingCount", 1L);
            queueStats.put("rejectedCount", 0L);
            queueStats.put("backpressurePendingCount", 0L);
            queueStats.put("backpressurePendingRawCount", 0L);
            queueStats.put("backpressureMaxPending", 3L);
            queueStats.put("backpressureActive", false);
            queueStats.put("totalProposalsSent", 9L);
            queueStats.put("currentEpoch", 13L);
            queueStats.put("finalizedEpoch", 12L);
            queueStats.put("epochsUntilFinality", 1L);
            when(queueManager.getQueueStats()).thenReturn(queueStats);
            context.proposalQueueManager = queueManager;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/explorer/summary");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"contractVersion\":\"explorer.v1\""));
            assertTrue(body.toString().contains("\"routingDebt\":4"));
            assertTrue(body.toString().contains("\"registeredClients\":1"));
        });
    }

    @Test
    public void testExplorerReleaseFlowRouteReturnsAdaptivePayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
            Map<String, Object> flow = new HashMap<>();
            flow.put("releaseMode", "adaptive-active");
            when(queueManager.getProposalReleaseFlowStats()).thenReturn(flow);
            context.proposalQueueManager = queueManager;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/explorer/release-flow");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"releaseFlow\":{\"releaseMode\":\"adaptive-active\"}"));
        });
    }

    @Test
    public void testProposalReleaseFlowRouteReturnsAdaptivePayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
            Map<String, Object> flow = new HashMap<>();
            flow.put("releaseMode", "adaptive-active");
            when(queueManager.getProposalReleaseFlowStats()).thenReturn(flow);
            context.proposalQueueManager = queueManager;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/proposals/release-flow");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"contractVersion\":\"release-flow.v1\""));
            assertTrue(body.toString().contains("\"releaseMode\":\"adaptive-active\""));
        });
    }

    @Test
    public void testSettlementByProposalRouteReturnsSettlementPayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            EvmBridge evmBridge = mock(EvmBridge.class);
            when(evmBridge.getSettlementDetailsByProposalId("proposal-123")).thenReturn(new SettlementDetails(
                "sepolia",
                "proposal-123",
                "0xtx123",
                12345L,
                "0xabc",
                "0xdef",
                "1000",
                PaymentProof.ProposalKind.WRITE,
                PaymentProof.PaymentToken.ETH,
                0,
                4
            ));
            context.evmBridge = evmBridge;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/settlement/proposals/proposal-123");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"contractVersion\":\"settlement.v1\""));
            assertTrue(body.toString().contains("\"proposalId\":\"proposal-123\""));
            assertTrue(body.toString().contains("\"transactionHash\":\"0xtx123\""));
        });
    }

    @Test
    public void testSettlementByTransactionRouteReturnsSettlementPayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            EvmBridge evmBridge = mock(EvmBridge.class);
            when(evmBridge.getSettlementDetailsByTransactionHash("0xtx999")).thenReturn(new SettlementDetails(
                "sepolia",
                "proposal-999",
                "0xtx999",
                88888L,
                "0xaaa",
                "0xbbb",
                "777",
                PaymentProof.ProposalKind.DELETE,
                PaymentProof.PaymentToken.USDC,
                8,
                9
            ));
            context.evmBridge = evmBridge;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/settlement/transactions/0xtx999");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"lookupType\":\"transactionHash\""));
            assertTrue(body.toString().contains("\"proposalKind\":\"DELETE\""));
            assertTrue(body.toString().contains("\"paymentToken\":\"USDC\""));
        });
    }

    @Test
    public void testExplorerProposalRouteReturnsProposalPayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
            ProposalStatus status = new ProposalStatus(
                "proposal-1",
                ProposalState.CONFIRMED,
                "0xtx",
                123L,
                88L,
                null,
                DurabilityState.PENDING,
                456L,
                null,
                "head-2"
            );
            when(queueManager.getProposalStatus("proposal-1")).thenReturn(status);
            context.proposalQueueManager = queueManager;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/explorer/proposals/proposal-1");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"proposalId\":\"proposal-1\""));
            assertTrue(body.toString().contains("\"state\":\"CONFIRMED\""));
        });
    }

    @Test
    public void testExplorerWalletRouteReturnsWalletMetadata() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-explorer-wallet");
            try {
                String wallet = "0x1234567890abcdef1234567890abcdef12345678";
                MemoryNodeStore nodeStore = new MemoryNodeStore();
                seedWallet(nodeStore, wallet);
                ServerContext context = newContext(nodeStore, storeDirectory);
                RequestRouter router = new RequestRouter(context);
                HttpServletRequest request = request("GET", "/v1/explorer/wallets/" + wallet);
                HttpServletResponse response = responseWithBody();

                router.route(request, response);

                verify(response).setStatus(HttpServletResponse.SC_OK);
                assertTrue(body.toString().contains("\"wallet\":\"" + wallet + "\""));
                assertTrue(body.toString().contains("\"contentCount\":2"));
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testExplorerContentNavRouteReturnsClusterAwarePayload() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-explorer-content-nav");
            try {
                ServerContext context = newContext(new MemoryNodeStore(), storeDirectory);
                context.shardingRuntimeConfig = ShardingRuntimeConfig.fromSpecs(true, "00-7f", "80-ff=http://validator-2:8090");
                RequestRouter router = new RequestRouter(context);
                HttpServletRequest request = request("GET", "/v1/explorer/content/nav");
                HttpServletResponse response = responseWithBody();

                router.route(request, response);

                verify(response).setStatus(HttpServletResponse.SC_OK);
                assertTrue(body.toString().contains("\"contractVersion\":\"explorer.content.v1\""));
                assertTrue(body.toString().contains("\"mountedNeighbors\""));
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testExplorerContentTreeRouteFiltersToRequestedCluster() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-explorer-content-tree");
            try {
                MemoryNodeStore nodeStore = new MemoryNodeStore();
                NodeBuilder root = nodeStore.getRoot().builder();
                root.child("oak-chain").child("12").child("aa").child("local-doc").setProperty("title", "local");
                root.child("oak-chain").child("90").child("bb").child("remote-doc").setProperty("title", "remote");
                nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

                ServerContext context = newContext(nodeStore, storeDirectory);
                context.shardingRuntimeConfig = ShardingRuntimeConfig.fromSpecs(true, "00-7f", "80-ff=http://validator-2:8090");

                RequestRouter router = new RequestRouter(context);
                HttpServletRequest request = request("GET", "/v1/explorer/content/clusters/local-localhost-8090/tree");
                when(request.getParameter("path")).thenReturn("/oak-chain");
                HttpServletResponse response = responseWithBody();

                router.route(request, response);

                verify(response).setStatus(HttpServletResponse.SC_OK);
                assertTrue(body.toString().contains("\"name\":\"12\""));
                assertFalse(body.toString().contains("\"name\":\"90\""));
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testApiExploreRouteReturnsNodeTreePayload() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-api-explore");
            try {
                MemoryNodeStore nodeStore = new MemoryNodeStore();
                NodeBuilder root = nodeStore.getRoot().builder();
                NodeBuilder doc = root.child("content").child("doc");
                doc.child("child-a");
                doc.setProperty("title", "Hello");
                nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                ServerContext context = newContext(nodeStore, storeDirectory);
                RequestRouter router = new RequestRouter(context);
                HttpServletRequest request = request("GET", "/api/explore");
                when(request.getParameter("path")).thenReturn("/content/doc");
                HttpServletResponse response = responseWithBody();

                router.route(request, response);

                verify(response).setStatus(HttpServletResponse.SC_OK);
                assertTrue(body.toString().contains("\"path\":\"/content/doc\""));
                assertTrue(body.toString().contains("\"children\":[\"child-a\"]"));
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testRecentSegmentsRouteReturnsJournalEntries() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-recent-segments");
            try {
                Files.write(storeDirectory.resolve("journal.log"), Arrays.asList(
                    "seg-001 2026-03-20T00:00:00Z",
                    "seg-002 2026-03-20T00:01:00Z"
                ));
                ServerContext context = newContext(mock(NodeStore.class), storeDirectory);
                RequestRouter router = new RequestRouter(context);
                HttpServletRequest request = request("GET", "/api/segments/recent");
                HttpServletResponse response = responseWithBody();

                router.route(request, response);

                verify(response).setStatus(HttpServletResponse.SC_OK);
                assertTrue(body.toString().contains("\"id\":\"seg-002\""));
                assertTrue(body.toString().contains("\"id\":\"seg-001\""));
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testTarFilesRouteReturnsTarMetadata() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-tars");
            try {
                Files.write(storeDirectory.resolve("journal.log"), Arrays.asList(
                    "seg-001 now",
                    "seg-002 later"
                ));
                Files.write(storeDirectory.resolve("data00000a.tar"), new byte[16]);
                Files.write(storeDirectory.resolve("data00001a.tar"), new byte[32]);

                ServerContext context = newContext(mock(NodeStore.class), storeDirectory);
                RequestRouter router = new RequestRouter(context);
                HttpServletRequest request = request("GET", "/api/segments/tars");
                HttpServletResponse response = responseWithBody();

                router.route(request, response);

                verify(response).setStatus(HttpServletResponse.SC_OK);
                assertTrue(body.toString().contains("\"name\":\"data00000a.tar\""));
                assertTrue(body.toString().contains("\"name\":\"data00001a.tar\""));
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testWalletStatsRouteReturnsWalletPayload() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-wallet-stats");
            try {
                String wallet = "0x1234567890abcdef1234567890abcdef12345678";
                MemoryNodeStore nodeStore = new MemoryNodeStore();
                seedWallet(nodeStore, wallet);
                ServerContext context = newContext(nodeStore, storeDirectory);
                RequestRouter router = new RequestRouter(context);
                HttpServletRequest request = request("GET", "/v1/wallets/stats");
                when(request.getParameter("wallet")).thenReturn(wallet);
                HttpServletResponse response = responseWithBody();

                router.route(request, response);

                assertTrue(body.toString().contains("\"wallet\":\"" + wallet + "\""));
                assertTrue(body.toString().contains("\"contentCount\":2"));
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testWalletContentRouteReturnsContentPayload() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-wallet-content");
            try {
                String wallet = "0x1234567890abcdef1234567890abcdef12345678";
                MemoryNodeStore nodeStore = new MemoryNodeStore();
                seedWallet(nodeStore, wallet);
                ServerContext context = newContext(nodeStore, storeDirectory);
                RequestRouter router = new RequestRouter(context);
                HttpServletRequest request = request("GET", "/v1/wallets/content");
                when(request.getParameter("wallet")).thenReturn(wallet);
                HttpServletResponse response = responseWithBody();

                router.route(request, response);

                assertTrue(body.toString().contains("\"content\":["));
                assertTrue(body.toString().contains("\"name\":\"doc-1\""));
                assertTrue(body.toString().contains("\"message\":\"hello\""));
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testPeersRouteReturnsKnownValidators() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            context.selfUrl = "http://validator-1:8090";
            context.registeredValidators.put("validator-2", new ValidatorRegistration("validator-2", "http://validator-2:8090"));
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/peers");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"validatorUrl\":\"http://validator-1:8090\""));
            assertTrue(body.toString().contains("\"validatorUrl\":\"http://validator-2:8090\""));
        });
    }

    @Test
    public void testRegisterClientRouteStoresClientByWallet() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("POST", "/v1/register-client");
            when(request.getParameter("walletAddress")).thenReturn("0x1234567890abcdef1234567890abcdef12345678");
            when(request.getParameter("clientId")).thenReturn("author-1");
            when(request.getParameter("clientUrl")).thenReturn("http://author-1:4502");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"walletAddress\":\"0x1234567890abcdef1234567890abcdef12345678\""));
            assertTrue(context.registeredClients.containsKey("0x1234567890abcdef1234567890abcdef12345678"));
        });
    }

    @Test
    public void testProposeDeleteRouteQueuesDeleteProposal() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-propose-delete");
            try {
                String wallet = "0x1234567890abcdef1234567890abcdef12345678";
                MemoryNodeStore nodeStore = new MemoryNodeStore();
                seedWallet(nodeStore, wallet);
                String contentPath = WalletPathUtil.getShardRoot(wallet) + "/content/doc-1";
                ServerContext context = newContext(nodeStore, storeDirectory);
                AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
                when(engine.isClusterHealthy()).thenReturn(true);
                when(engine.isLeader()).thenReturn(true);
                context.aeronConsensusEngine = engine;
                ClientRegistration registration = new ClientRegistration("author-1", "http://author-1:4502", wallet);
                context.registeredClients.put(wallet, registration);
                context.registeredClients.put("author-1", registration);
                context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
                RequestRouter router = new RequestRouter(context);
                HttpServletRequest request = request("POST", "/v1/propose-delete");
                when(request.getParameter("walletAddress")).thenReturn(wallet);
                when(request.getParameter("signature")).thenReturn("0xabcdef12");
                when(request.getParameter("contentPath")).thenReturn(contentPath);
                when(request.getParameter("ethereumTxHash")).thenReturn("0xabcdef123456789f");
                when(request.getParameter("proposalId")).thenReturn(
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
                HttpServletResponse response = responseWithBody();

                router.route(request, response);

                verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
                assertTrue(body.toString().contains(
                    "\"proposalId\":\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\""));
                assertTrue(body.toString().contains("\"type\":\"DELETE\""));
                assertTrue(body.toString().contains("\"status\":\"accepted\""));
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testCidStatsRouteReturnsMappingStats() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-cid-stats");
            try {
                ServerContext context = newContext(mock(NodeStore.class), storeDirectory);
                context.cidMappingService = new CidMappingService(storeDirectory);
                context.cidMappingService.registerMapping(
                    "ed06f9cbf0fe878013ccb266170e6b3ba676933a6f065675cc0115c840bf1442#22216",
                    "Qmf4F3CWU6Ly958TFiR8BRP18gwvW3Xsj2yXu5DqkonWc3"
                );
                try (RequestRouter router = new RequestRouter(context)) {
                    HttpServletRequest request = request("GET", "/api/cid/stats");
                    HttpServletResponse response = responseWithBody();

                    router.route(request, response);

                    verify(response).setStatus(HttpServletResponse.SC_OK);
                    assertTrue(body.toString().contains("\"totalMappings\":1"));
                    assertTrue(body.toString().contains("\"currentSize\":1"));
                }
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testCidGatewayRouteRedirectsToGatewayUrl() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-cid-gateway");
            try {
                ServerContext context = newContext(mock(NodeStore.class), storeDirectory);
                context.cidMappingService = new CidMappingService(storeDirectory);
                context.cidMappingService.registerMapping(
                    "ed06f9cbf0fe878013ccb266170e6b3ba676933a6f065675cc0115c840bf1442#22216",
                    "Qmf4F3CWU6Ly958TFiR8BRP18gwvW3Xsj2yXu5DqkonWc3"
                );
                try (RequestRouter router = new RequestRouter(context)) {
                    HttpServletRequest request = request(
                        "GET",
                        "/api/cid/gateway/ed06f9cbf0fe878013ccb266170e6b3ba676933a6f065675cc0115c840bf1442#22216"
                    );
                    when(request.getPathInfo()).thenReturn(
                        "/api/cid/gateway/ed06f9cbf0fe878013ccb266170e6b3ba676933a6f065675cc0115c840bf1442#22216"
                    );
                    HttpServletResponse response = responseWithBody();

                    router.route(request, response);

                    verify(response).sendRedirect("https://ipfs.io/ipfs/Qmf4F3CWU6Ly958TFiR8BRP18gwvW3Xsj2yXu5DqkonWc3");
                }
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testCidLookupRouteReturnsMappedCidPayload() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-cid-lookup");
            try {
                ServerContext context = newContext(mock(NodeStore.class), storeDirectory);
                context.cidMappingService = new CidMappingService(storeDirectory);
                context.cidMappingService.registerMapping(
                    "ed06f9cbf0fe878013ccb266170e6b3ba676933a6f065675cc0115c840bf1442#22216",
                    "Qmf4F3CWU6Ly958TFiR8BRP18gwvW3Xsj2yXu5DqkonWc3"
                );
                try (RequestRouter router = new RequestRouter(context)) {
                    HttpServletRequest request = request(
                        "GET",
                        "/api/cid/ed06f9cbf0fe878013ccb266170e6b3ba676933a6f065675cc0115c840bf1442#22216"
                    );
                    when(request.getPathInfo()).thenReturn(
                        "/api/cid/ed06f9cbf0fe878013ccb266170e6b3ba676933a6f065675cc0115c840bf1442#22216"
                    );
                    HttpServletResponse response = responseWithBody();

                    router.route(request, response);

                    verify(response).setStatus(HttpServletResponse.SC_OK);
                    assertTrue(body.toString().contains("\"ipfsCid\":\"Qmf4F3CWU6Ly958TFiR8BRP18gwvW3Xsj2yXu5DqkonWc3\""));
                }
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testCidReverseLookupRouteReturnsOakBlobId() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-cid-reverse");
            try {
                ServerContext context = newContext(mock(NodeStore.class), storeDirectory);
                context.cidMappingService = new CidMappingService(storeDirectory);
                context.cidMappingService.registerMapping(
                    "ed06f9cbf0fe878013ccb266170e6b3ba676933a6f065675cc0115c840bf1442#22216",
                    "Qmf4F3CWU6Ly958TFiR8BRP18gwvW3Xsj2yXu5DqkonWc3"
                );
                try (RequestRouter router = new RequestRouter(context)) {
                    HttpServletRequest request = request("GET", "/api/cid/reverse/Qmf4F3CWU6Ly958TFiR8BRP18gwvW3Xsj2yXu5DqkonWc3");
                    when(request.getPathInfo()).thenReturn("/api/cid/reverse/Qmf4F3CWU6Ly958TFiR8BRP18gwvW3Xsj2yXu5DqkonWc3");
                    HttpServletResponse response = responseWithBody();

                    router.route(request, response);

                    verify(response).setStatus(HttpServletResponse.SC_OK);
                    assertTrue(body.toString().contains("\"oakBlobId\":\"ed06f9cbf0fe878013ccb266170e6b3ba676933a6f065675cc0115c840bf1442\""));
                }
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testBinaryDeclareIntentRouteReturnsIntentToken() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("POST", "/v1/binary/declare-intent");
            when(request.getParameter("walletAddress")).thenReturn("0x1234567890abcdef1234567890abcdef12345678");
            when(request.getParameter("filesize")).thenReturn("123");
            when(request.getParameter("mimeType")).thenReturn("image/png");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"intentToken\":\"intent-"));
        });
    }

    @Test
    public void testBinaryCheckIntentRouteReturnsReadyState() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            UploadSession session = router.getBinaryUploadHandler().getSessionManager()
                .createSession("0x1234567890abcdef1234567890abcdef12345678", 123L, "image/png", null);
            router.getBinaryUploadHandler().getSessionManager().markReadyForUpload(session.getIntentToken(), 17L);
            HttpServletRequest request = request("GET", "/v1/binary/check-intent/" + session.getIntentToken());
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"status\":\"READY_FOR_UPLOAD\""));
            assertTrue(body.toString().contains("\"epochNumber\":17"));
        });
    }

    @Test
    public void testBinaryCompleteUploadRouteReturnsSuccessPayload() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            String wallet = "0x1234567890abcdef1234567890abcdef12345678";
            UploadSession session = router.getBinaryUploadHandler().getSessionManager()
                .createSession(wallet, 456L, "application/pdf", null);
            HttpServletRequest request = request("POST", "/v1/binary/complete-upload");
            when(request.getParameter("intentToken")).thenReturn(session.getIntentToken());
            when(request.getParameter("cid")).thenReturn("Qmf4F3CWU6Ly958TFiR8BRP18gwvW3Xsj2yXu5DqkonWc3");
            when(request.getParameter("walletAddress")).thenReturn(wallet);
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"status\":\"complete\""));
        });
    }

    @Test
    public void testEventStreamRouteReplaysBufferedEvents() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            router.getEventBroadcaster().broadcast(ContentEvent.builder()
                .id("evt-1")
                .timestamp(100L)
                .path("/content/doc-1")
                .wallet("0xwallet")
                .organization("acme")
                .build());
            HttpServletRequest request = request("GET", "/v1/events/stream");
            AsyncContext asyncContext = mock(AsyncContext.class);
            when(request.startAsync()).thenReturn(asyncContext);
            when(request.getParameter("types")).thenReturn("content");
            when(request.getParameter("wallets")).thenReturn("0xwallet");
            when(request.getParameter("organizations")).thenReturn("acme");
            when(request.getParameter("path")).thenReturn("/content");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setContentType("text/event-stream");
            assertTrue(body.toString().contains(": connected to oak-chain event stream"));
            assertTrue(body.toString().contains("id: evt-1"));
        });
    }

    @Test
    public void testOpsEventStreamRouteUsesOpsEnvelope() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            router.getEventBroadcaster().broadcast(ContentEvent.builder()
                .id("10")
                .type(ContentEvent.EventType.CONSENSUS)
                .action(ContentEvent.Action.LEADER_CHANGE)
                .timestamp(100L)
                .message("leader switched")
                .build());
            HttpServletRequest request = request("GET", "/v1/ops/events/stream");
            AsyncContext asyncContext = mock(AsyncContext.class);
            when(request.startAsync()).thenReturn(asyncContext);
            when(request.getHeader("Last-Event-ID")).thenReturn("9");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setContentType("text/event-stream");
            assertTrue(body.toString().contains("event: cluster.leader.changed"));
            assertTrue(body.toString().contains("\"contractVersion\":\"ops.v1\""));
        });
    }

    @Test
    public void testFollowerHeadUpdateRouteReplicatesHead() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
            when(engine.isLeader()).thenReturn(false);
            when(engine.getCurrentLeader()).thenReturn("http://127.0.0.1:8090");
            when(engine.pullSegmentsForHead("abc:r1", "http://localhost:8090")).thenReturn(4);
            context.aeronConsensusEngine = engine;
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("POST", "/v1/follower/head-update");
            when(request.getReader()).thenReturn(new java.io.BufferedReader(
                new java.io.StringReader("{\"head\":\"abc:r1\",\"epoch\":\"12\",\"leaderUrl\":\"http://localhost:8090\"}")
            ));
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"success\":true"));
            assertTrue(body.toString().contains("\"segmentCount\":4"));
        });
    }

    @Test
    public void testJournalRouteStreamsFileContents() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-journal");
            try {
                byte[] payload = "journal-entry".getBytes(StandardCharsets.UTF_8);
                Files.write(storeDirectory.resolve("journal.log"), payload);
                ServerContext context = newContext(mock(NodeStore.class), storeDirectory);
                RequestRouter router = new RequestRouter(context);
                HttpServletRequest request = request("GET", "/journal.log");
                RecordingServletOutputStream output = new RecordingServletOutputStream();
                HttpServletResponse response = mock(HttpServletResponse.class);
                when(response.getOutputStream()).thenReturn(output);

                router.route(request, response);

                verify(response).setStatus(HttpServletResponse.SC_OK);
                verify(response).setContentType("text/plain");
                assertArrayEquals(payload, output.toByteArray());
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testManifestHeadRouteReturnsHeaders() throws Exception {
        withRoutingProperties(true, () -> {
            Path storeDirectory = Files.createTempDirectory("router-manifest");
            try {
                byte[] payload = "manifest-data".getBytes(StandardCharsets.UTF_8);
                Files.write(storeDirectory.resolve("manifest"), payload);
                ServerContext context = newContext(mock(NodeStore.class), storeDirectory);
                RequestRouter router = new RequestRouter(context);
                HttpServletRequest request = request("HEAD", "/manifest");
                HttpServletResponse response = mock(HttpServletResponse.class);

                router.route(request, response);

                verify(response).setStatus(HttpServletResponse.SC_OK);
                verify(response).setContentType("text/plain");
                verify(response).setContentLengthLong(payload.length);
            } finally {
                deleteRecursively(storeDirectory);
            }
        });
    }

    @Test
    public void testManifestRouteRejectsUnsupportedMethod() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("POST", "/manifest");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            assertTrue(body.toString().contains("Method not allowed"));
        });
    }

    @Test
    public void testSegmentRouteRejectsInvalidUuid() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/segments/not-a-uuid");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
            assertTrue(body.toString().contains("Invalid segment UUID"));
        });
    }

    @Test
    public void testNgrokRouteReturnsSelfUrl() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            context.selfUrl = "https://public.ngrok.app";
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/ngrok-url");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("https://public.ngrok.app"));
        });
    }

    @Test
    public void testAeronValidatorIdentitiesRouteReturnsIdentityPayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            context.selfUrl = "http://validator-2:8090";
            context.aeronConsensusEngine = baseEngine();
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/aeron/validator-identities");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"totalValidators\":1"));
            assertTrue(body.toString().contains("\"knownWallets\":1"));
        });
    }

    @Test
    public void testAeronReplicationLagRouteReturnsLagPayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            AeronConsensusEngine engine = baseEngine();
            Map<String, Object> lagStatus = new HashMap<>();
            lagStatus.put("healthy", true);
            lagStatus.put("replicationLag", 4);
            when(engine.getReplicationLagStatus()).thenReturn(lagStatus);
            context.aeronConsensusEngine = engine;
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/aeron/replication-lag");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"replicationLag\":4"));
            assertTrue(body.toString().contains("\"healthy\":true"));
        });
    }

    @Test
    public void testOpsClusterSnapshotRouteReturnsOpsEnvelope() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            context.selfUrl = "http://validator-2:8090";
            context.aeronConsensusEngine = baseEngine();
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/ops/snapshots/cluster");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"contractVersion\":\"ops.v1\""));
            assertTrue(body.toString().contains("\"clusterId\":\"oak-consensus-cluster\""));
        });
    }

    @Test
    public void testConsensusStatusRouteReturnsAeronStatusPayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            AeronConsensusEngine engine = baseEngine();
            when(engine.getCurrentRole()).thenReturn(ValidatorRole.FOLLOWER);
            when(engine.getCurrentEpoch()).thenReturn(12);
            when(engine.getCurrentTerm()).thenReturn(8);
            when(engine.getReachableValidatorCount()).thenReturn(3);
            when(engine.getAllFollowers()).thenReturn(Arrays.asList("http://validator-2:8090"));
            when(engine.getCurrentEthereumEpoch()).thenReturn(1024);
            context.aeronConsensusEngine = engine;
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/consensus/status");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"consensusType\":\"aeron-cluster\""));
            assertTrue(body.toString().contains("\"currentEpoch\":12"));
            assertTrue(body.toString().contains("\"ethereumEpoch\":1024"));
        });
    }

    @Test
    public void testDashboardRouteRequiresTokenWhenAuthEnabled() throws Exception {
        withRoutingProperties(true, () -> {
            String previousToken = System.getProperty(AuthTokenValidator.TOKEN_PROPERTY_NAME);
            try {
                System.setProperty(AuthTokenValidator.TOKEN_PROPERTY_NAME, "secret-token");
                RequestRouter router = new RequestRouter(newContext());
                HttpServletRequest request = request("GET", "/dashboard");
                HttpServletResponse response = responseWithBody();

                router.route(request, response);

                verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                assertTrue(body.toString().contains("Missing Authorization header"));
            } finally {
                if (previousToken == null) {
                    System.clearProperty(AuthTokenValidator.TOKEN_PROPERTY_NAME);
                } else {
                    System.setProperty(AuthTokenValidator.TOKEN_PROPERTY_NAME, previousToken);
                }
            }
        });
    }

    @Test
    public void testConfigRouteReturns429WhenRateLimited() throws Exception {
        String previousBrowser = System.getProperty("oak.http.browser.ui.enabled");
        String previousRateLimit = System.getProperty("rate.limit.enabled");
        String previousClientRps = System.getProperty(RateLimiter.PROP_REQUESTS_PER_SECOND);
        String previousBurst = System.getProperty(RateLimiter.PROP_BURST_SIZE);
        String previousGlobalRps = System.getProperty(RateLimiter.PROP_GLOBAL_RPS);
        String previousToken = System.getProperty(AuthTokenValidator.TOKEN_PROPERTY_NAME);
        try {
            System.setProperty("oak.http.browser.ui.enabled", "true");
            System.setProperty("rate.limit.enabled", "true");
            System.setProperty(RateLimiter.PROP_REQUESTS_PER_SECOND, "1");
            System.setProperty(RateLimiter.PROP_BURST_SIZE, "1");
            System.setProperty(RateLimiter.PROP_GLOBAL_RPS, "100");
            System.clearProperty(AuthTokenValidator.TOKEN_PROPERTY_NAME);

            RequestRouter router = new RequestRouter(newContext());

            HttpServletResponse firstResponse = responseWithBody();
            router.route(request("GET", "/v1/config/osgi"), firstResponse);
            verify(firstResponse).setStatus(HttpServletResponse.SC_OK);

            HttpServletResponse secondResponse = responseWithBody();
            router.route(request("GET", "/v1/config/osgi"), secondResponse);

            verify(secondResponse).setStatus(429);
            assertTrue(body.toString().contains("rate_limit_exceeded"));
        } finally {
            restoreProperty("oak.http.browser.ui.enabled", previousBrowser);
            restoreProperty("rate.limit.enabled", previousRateLimit);
            restoreProperty(RateLimiter.PROP_REQUESTS_PER_SECOND, previousClientRps);
            restoreProperty(RateLimiter.PROP_BURST_SIZE, previousBurst);
            restoreProperty(RateLimiter.PROP_GLOBAL_RPS, previousGlobalRps);
            restoreProperty(AuthTokenValidator.TOKEN_PROPERTY_NAME, previousToken);
        }
    }

    @Test
    public void testManifestAndJournalBypassRateLimiter() throws Exception {
        String previousRateLimit = System.getProperty("rate.limit.enabled");
        String previousClientRps = System.getProperty(RateLimiter.PROP_REQUESTS_PER_SECOND);
        String previousBurst = System.getProperty(RateLimiter.PROP_BURST_SIZE);
        String previousGlobalRps = System.getProperty(RateLimiter.PROP_GLOBAL_RPS);
        String previousToken = System.getProperty(AuthTokenValidator.TOKEN_PROPERTY_NAME);
        try {
            System.setProperty("rate.limit.enabled", "true");
            System.setProperty(RateLimiter.PROP_REQUESTS_PER_SECOND, "1");
            System.setProperty(RateLimiter.PROP_BURST_SIZE, "1");
            System.setProperty(RateLimiter.PROP_GLOBAL_RPS, "100");
            System.clearProperty(AuthTokenValidator.TOKEN_PROPERTY_NAME);

            Path storeDirectory = Files.createTempDirectory("router-rate-bypass");
            try {
                Files.write(storeDirectory.resolve("manifest"), "manifest-data".getBytes(StandardCharsets.UTF_8));
                Files.write(storeDirectory.resolve("journal.log"), "journal-entry".getBytes(StandardCharsets.UTF_8));

                RequestRouter router = new RequestRouter(newContext(mock(NodeStore.class), storeDirectory));

                HttpServletResponse firstResponse = responseWithBody();
                router.route(request("GET", "/v1/config/osgi"), firstResponse);
                verify(firstResponse).setStatus(HttpServletResponse.SC_OK);

                HttpServletResponse manifestResponse = responseWithBody();
                router.route(request("HEAD", "/manifest"), manifestResponse);
                verify(manifestResponse).setStatus(HttpServletResponse.SC_OK);

                HttpServletResponse journalResponse = responseWithBody();
                router.route(request("GET", "/journal.log"), journalResponse);
                verify(journalResponse).setStatus(HttpServletResponse.SC_OK);
            } finally {
                deleteRecursively(storeDirectory);
            }
        } finally {
            restoreProperty("rate.limit.enabled", previousRateLimit);
            restoreProperty(RateLimiter.PROP_REQUESTS_PER_SECOND, previousClientRps);
            restoreProperty(RateLimiter.PROP_BURST_SIZE, previousBurst);
            restoreProperty(RateLimiter.PROP_GLOBAL_RPS, previousGlobalRps);
            restoreProperty(AuthTokenValidator.TOKEN_PROPERTY_NAME, previousToken);
        }
    }

    @Test
    public void testHeadRouteUsesAeronHeadValuesWhenAvailable() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContextWithHead("file-head");
            AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
            when(engine.getLatestHead()).thenReturn("latest-aeron-head");
            when(engine.getCommittedHead()).thenReturn("committed-aeron-head");
            when(engine.getLatestEpochSeen()).thenReturn(12);
            when(engine.getLastCommittedEpoch()).thenReturn(11);
            context.aeronConsensusEngine = engine;
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/head");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"latestHead\": \"latest-aeron-head\""));
            assertTrue(body.toString().contains("\"committedHead\": \"committed-aeron-head\""));
            assertTrue(body.toString().contains("\"latestEpochSeen\": 12"));
            assertTrue(body.toString().contains("\"committedEpoch\": 11"));
        });
    }

    @Test
    public void testHeadRouteFallsBackToFileStoreHeadWhenAeronStateMissing() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContextWithHead("fallback-file-head");
            AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
            when(engine.getLatestHead()).thenReturn(null);
            when(engine.getCommittedHead()).thenReturn("");
            when(engine.getLatestEpochSeen()).thenReturn(-1);
            when(engine.getLastCommittedEpoch()).thenReturn(-1);
            context.aeronConsensusEngine = engine;
            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/head");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"latestHead\": \"fallback-file-head\""));
            assertTrue(body.toString().contains("\"committedHead\": null"));
        });
    }

    @Test
    public void testHeadRouteDoesNotPromoteLatestAeronHeadToCommitted() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContextWithHead("file-head");
            AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
            when(engine.getLatestHead()).thenReturn("latest-aeron-head");
            when(engine.getCommittedHead()).thenReturn(null);
            when(engine.getLatestEpochSeen()).thenReturn(12);
            when(engine.getLastCommittedEpoch()).thenReturn(-1);
            context.aeronConsensusEngine = engine;
            RequestRouter router = new RequestRouter(context);
            HttpServletResponse response = responseWithBody();

            router.route(request("GET", "/v1/head"), response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"latestHead\": \"latest-aeron-head\""));
            assertTrue(body.toString().contains("\"committedHead\": null"));
            assertFalse(body.toString().contains("\"committedHead\": \"latest-aeron-head\""));
            assertTrue(body.toString().contains("\"latestEpochSeen\": 12"));
            assertFalse(body.toString().contains("\"committedEpoch\""));
        });
    }

    @Test
    public void testApiMetricsRouteReturnsJsonMetricsPayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
            when(engine.getCurrentRole()).thenReturn(ValidatorRole.FOLLOWER);
            when(engine.isLeader()).thenReturn(false);
            when(engine.getCurrentEpoch()).thenReturn(11);
            when(engine.getCurrentTerm()).thenReturn(2);
            when(engine.getReachableValidatorCount()).thenReturn(2);
            when(engine.getTotalMemberCount()).thenReturn(3);
            when(engine.getQuorumSize()).thenReturn(2);
            when(engine.getHeartbeatAgeMs()).thenReturn(15L);
            when(engine.isClusterHealthy()).thenReturn(true);
            Map<String, Object> lagStatus = new HashMap<>();
            lagStatus.put("role", "FOLLOWER");
            lagStatus.put("myLogPosition", 10L);
            lagStatus.put("leaderLogPosition", 12L);
            lagStatus.put("replicationLag", 2L);
            lagStatus.put("lagThreshold", 5L);
            lagStatus.put("healthy", true);
            when(engine.getReplicationLagStatus()).thenReturn(lagStatus);
            context.aeronConsensusEngine = engine;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/api/metrics");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"status\":\"UP\""));
            assertTrue(body.toString().contains("\"replicationLag\":2"));
        });
    }

    @Test
    public void testPrometheusMetricsRouteExportsTextMetrics() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/metrics");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("oak_active_connections"));
        });
    }

    @Test
    public void testBlobRouteStreamsBlobPayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            BlobStore blobStore = mock(BlobStore.class);
            byte[] payload = "blob-demo".getBytes(StandardCharsets.UTF_8);
            when(blobStore.getInputStream("ed06f9cb-demo")).thenReturn(new ByteArrayInputStream(payload));
            context.blobStore = blobStore;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/api/blob/ed06f9cb-demo");
            RecordingServletOutputStream output = new RecordingServletOutputStream();
            HttpServletResponse response = mock(HttpServletResponse.class);
            when(response.getOutputStream()).thenReturn(output);

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            verify(response).setHeader("X-Blob-Id", "ed06f9cb-demo");
            assertArrayEquals(payload, output.toByteArray());
        });
    }

    @Test
    public void testRecentEventsRouteReturnsEmptyPayloadWhenNoEventsBroadcast() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/v1/events/recent");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            assertTrue(body.toString().contains("\"events\":[]"));
            assertTrue(body.toString().contains("\"count\":0"));
        });
    }

    @Test
    public void testEventStatsRouteReturnsEmptyStatsPayload() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/v1/events/stats");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            assertTrue(body.toString().contains("\"connectedClients\":0"));
            assertTrue(body.toString().contains("\"eventBufferSize\":0"));
            assertTrue(body.toString().contains("\"totalEventsBroadcast\":0"));
        });
    }

    @Test
    public void testHealthRouteReturnsUpPayload() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/health");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"status\":\"UP\""));
        });
    }

    @Test
    public void testLocalHealthRouteReturnsLocalPayload() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/health/local");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"scope\":\"local\""));
        });
    }

    @Test
    public void testBlockchainConfigRouteReturnsConfigPayload() throws Exception {
        String previousMode = System.getProperty("oak.blockchain.mode");
        try {
            System.clearProperty("oak.blockchain.mode");
            BlockchainConfig.reset();
            withRoutingProperties(true, () -> {
                RequestRouter router = new RequestRouter(newContext());
                HttpServletRequest request = request("GET", "/v1/blockchain/config");
                HttpServletResponse response = responseWithBody();

                router.route(request, response);

                verify(response).setStatus(HttpServletResponse.SC_OK);
                assertTrue(body.toString().contains("\"mode\":\"mock\""));
                assertTrue(body.toString().contains("\"validatorUrl\":\"http://localhost:8090\""));
            });
        } finally {
            restoreProperty("oak.blockchain.mode", previousMode);
            BlockchainConfig.reset();
        }
    }

    @Test
    public void testExplorerEpochsRouteReturnsNotFound() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/v1/explorer/epochs");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);
            assertTrue(body.toString().contains("\"error\":\"Not found\""));
        });
    }

    @Test
    public void testPendingCountRouteReturnsQueueCount() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
            when(queueManager.getPendingCount()).thenReturn(7);
            context.proposalQueueManager = queueManager;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/proposals/pending/count");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"pendingCount\":7"));
        });
    }

    @Test
    public void testQueueStatsRouteReturnsStatsPayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
            Map<String, Object> stats = new HashMap<>();
            stats.put("pendingCount", 3);
            stats.put("oldestAgeMs", 55L);
            when(queueManager.getQueueStats()).thenReturn(stats);
            context.proposalQueueManager = queueManager;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/proposals/queue/stats");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"pendingCount\":3"));
            assertTrue(body.toString().contains("\"oldestAgeMs\":55"));
        });
    }

    @Test
    public void testProposalEpochsRouteReturnsNotFound() throws Exception {
        withRoutingProperties(true, () -> {
            RequestRouter router = new RequestRouter(newContext());
            HttpServletRequest request = request("GET", "/v1/proposals/epochs");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);
            assertTrue(body.toString().contains("\"error\":\"Not found\""));
        });
    }

    @Test
    public void testOpsQueueSnapshotRouteReturnsOpsEnvelope() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
            Map<String, Object> stats = new HashMap<>();
            stats.put("pendingCount", 7);
            when(queueManager.getQueueStats()).thenReturn(stats);
            context.proposalQueueManager = queueManager;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/ops/snapshots/queue");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"contractVersion\":\"ops.v1\""));
            assertTrue(body.toString().contains("\"pendingCount\":7"));
        });
    }

    @Test
    public void testOpsReplicationSnapshotRouteReturnsOpsEnvelope() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            AeronConsensusEngine engine = baseEngine();
            Map<String, Object> lagStatus = new HashMap<>();
            lagStatus.put("healthy", true);
            lagStatus.put("replicationLag", 4);
            when(engine.getReplicationLagStatus()).thenReturn(lagStatus);
            context.aeronConsensusEngine = engine;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/ops/snapshots/replication");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"contractVersion\":\"ops.v1\""));
            assertTrue(body.toString().contains("\"replicationLag\":4"));
        });
    }

    @Test
    public void testAeronRaftMetricsRouteReturnsPayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            AeronConsensusEngine engine = baseEngine();
            when(engine.getCurrentTerm()).thenReturn(8);
            when(engine.isLeader()).thenReturn(true);
            when(engine.getCurrentLeader()).thenReturn("http://validator-1:8090");
            when(engine.getAllFollowers()).thenReturn(Arrays.asList("http://validator-1:8090", "http://validator-3:8090"));
            when(engine.getCurrentEpoch()).thenReturn(21);
            when(engine.getCurrentEthereumEpoch()).thenReturn(34);
            context.aeronConsensusEngine = engine;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/aeron/raft-metrics");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"currentTerm\":8"));
            assertTrue(body.toString().contains("\"ethereumEpoch\":34"));
        });
    }

    @Test
    public void testAeronNodeStatusRouteReturnsLeaderPayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            AeronConsensusEngine engine = baseEngine();
            when(engine.getCurrentLeader()).thenReturn("http://validator-1:8090");
            when(engine.getAllFollowers()).thenReturn(Arrays.asList("http://validator-1:8090", "http://validator-3:8090"));
            when(engine.getLastHeartbeatTime()).thenReturn(9876L);
            context.aeronConsensusEngine = engine;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/aeron/node-status");
            when(request.getParameter("nodeId")).thenReturn("1");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"nodeId\":1"));
            assertTrue(body.toString().contains("\"role\":\"LEADER\""));
        });
    }

    @Test
    public void testAeronLeadershipHistoryRouteReturnsHistoryPayload() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            AeronConsensusEngine engine = baseEngine();
            when(engine.getLeadershipHistory(100)).thenReturn(Arrays.asList(
                new LeadershipChange(
                    1234L,
                    Cluster.Role.LEADER,
                    Cluster.Role.FOLLOWER,
                    7,
                    2,
                    "http://validator-2:8090"
                )
            ));
            context.aeronConsensusEngine = engine;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/v1/aeron/leadership-history");
            when(request.getParameter("limit")).thenReturn("500");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            assertTrue(body.toString().contains("\"limit\":100"));
            assertTrue(body.toString().contains("\"memberUrl\":\"http://validator-2:8090\""));
        });
    }

    @Test
    public void testMockEpochStatusRouteReturnsBeaconHealthPayloadInMockMode() throws Exception {
        withBlockchainMode("mock", () -> withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
            BeaconChainClient beaconClient = mock(BeaconChainClient.class);
            when(queueManager.getBeaconClient()).thenReturn(beaconClient);
            when(beaconClient.getCachedCurrentEpoch()).thenReturn(1042L);
            when(beaconClient.getCachedFinalizedEpoch()).thenReturn(1040L);
            when(beaconClient.isEpochDataFresh()).thenReturn(true);
            when(beaconClient.getMillisSinceLastUpdate()).thenReturn(15L);
            context.proposalQueueManager = queueManager;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("GET", "/api/mock/epoch-status");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            assertTrue(body.toString().contains("\"mode\":\"MOCK\""));
            assertTrue(body.toString().contains("\"currentEpoch\":1042"));
            assertTrue(body.toString().contains("\"chainContext\":\"Sepolia\""));
            assertFalse(body.toString().contains("\"mockEpochOffset\""));
        }));
    }

    @Test
    public void testMockAdvanceEpochRouteReturnsGoneInMockMode() throws Exception {
        withBlockchainMode("mock", () -> withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
            BeaconChainClient beaconClient = mock(BeaconChainClient.class);
            when(queueManager.getBeaconClient()).thenReturn(beaconClient);
            context.proposalQueueManager = queueManager;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("POST", "/api/mock/advance-epoch");
            when(request.getParameter("epochs")).thenReturn("3");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_GONE);
            verifyNoInteractions(beaconClient);
            assertTrue(body.toString().contains("Synthetic mock epoch control was removed by ADR 080"));
        }));
    }

    @Test
    public void testMockSetEpochOffsetRouteReturnsGoneInMockMode() throws Exception {
        withBlockchainMode("mock", () -> withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
            BeaconChainClient beaconClient = mock(BeaconChainClient.class);
            when(queueManager.getBeaconClient()).thenReturn(beaconClient);
            context.proposalQueueManager = queueManager;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("POST", "/api/mock/set-epoch-offset");
            when(request.getParameter("offset")).thenReturn("42");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(response).setStatus(HttpServletResponse.SC_GONE);
            verifyNoInteractions(beaconClient);
            assertTrue(body.toString().contains("Synthetic mock epoch control was removed by ADR 080"));
        }));
    }

    @Test
    public void testProposeWriteRouteUsesWalletFallbackForShardRouting() throws Exception {
        withRoutingProperties(true, () -> {
            ServerContext context = newContext();
            ShardRouter shardRouter = mock(ShardRouter.class);
            when(shardRouter.routeRequest("0xwallet")).thenReturn("http://validator-7:8090");
            context.shardRouter = shardRouter;

            RequestRouter router = new RequestRouter(context);
            HttpServletRequest request = request("POST", "/v1/propose-write");
            when(request.getParameter("walletAddress")).thenReturn(null);
            when(request.getParameter("wallet")).thenReturn("0xwallet");
            HttpServletResponse response = responseWithBody();

            router.route(request, response);

            verify(shardRouter).routeRequest("0xwallet");
        });
    }

    private StringWriter body;

    private ServerContext newContext() {
        return newContext(mock(NodeStore.class), Paths.get("/tmp/store"));
    }

    private ServerContext newContext(NodeStore nodeStore, Path storeDirectory) {
        return new ServerContext(
            mock(FileStore.class),
            nodeStore,
            storeDirectory,
            "http://localhost:8090"
        );
    }

    private ServerContext newContextWithHead(String head) {
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        when(fileStore.getHead().getRecordId().toString10()).thenReturn(head);
        return new ServerContext(
            fileStore,
            mock(NodeStore.class),
            Paths.get("/tmp/store"),
            "http://localhost:8090"
        );
    }

    private HttpServletRequest request(String method, String uri) {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn(uri);
        when(request.getPathInfo()).thenReturn(uri);
        when(request.getMethod()).thenReturn(method);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        return request;
    }

    private HttpServletResponse responseWithBody() throws Exception {
        HttpServletResponse response = mock(HttpServletResponse.class);
        body = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(body));
        return response;
    }

    private AeronConsensusEngine baseEngine() {
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

    private Map<String, Object> nativeClusterState() {
        Map<String, Object> state = new HashMap<>();
        state.put("role", "FOLLOWER");
        state.put("memberId", 2);
        state.put("clusterMemberCount", 3);
        java.util.List<Map<String, Object>> members = new ArrayList<>();
        Map<String, Object> self = new HashMap<>();
        self.put("url", "http://validator-2:8090");
        self.put("memberId", 2);
        members.add(self);
        state.put("members", members);
        return state;
    }

    private void seedWallet(MemoryNodeStore nodeStore, String wallet) throws Exception {
        String[] levels = WalletPathUtil.getShardLevels(wallet);
        NodeBuilder root = nodeStore.getRoot().builder();
        NodeBuilder walletNode = root.child("oak-chain")
            .child(levels[0])
            .child(levels[1])
            .child(levels[2])
            .child(wallet);
        walletNode.setProperty("contentCount", 2L);
        walletNode.setProperty("totalWrites", 7L);
        walletNode.child("content").child("doc-1")
            .setProperty("contentType", "fragment")
            .setProperty("timestamp", 300L)
            .setProperty("message", "hello");
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private void deleteRecursively(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (java.util.stream.Stream<Path> stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static final class RecordingServletOutputStream extends ServletOutputStream {
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();

        @Override
        public void write(int b) {
            output.write(b);
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
        }

        private byte[] toByteArray() {
            return output.toByteArray();
        }
    }

    private void withRoutingProperties(boolean browserUiEnabled, ThrowingRunnable runnable) throws Exception {
        String previousBrowser = System.getProperty("oak.http.browser.ui.enabled");
        String previousRateLimit = System.getProperty("rate.limit.enabled");
        try {
            System.setProperty("oak.http.browser.ui.enabled", Boolean.toString(browserUiEnabled));
            System.setProperty("rate.limit.enabled", "false");
            runnable.run();
        } finally {
            if (previousBrowser == null) {
                System.clearProperty("oak.http.browser.ui.enabled");
            } else {
                System.setProperty("oak.http.browser.ui.enabled", previousBrowser);
            }
            if (previousRateLimit == null) {
                System.clearProperty("rate.limit.enabled");
            } else {
                System.setProperty("rate.limit.enabled", previousRateLimit);
            }
        }
    }

    private void withBlockchainMode(String mode, ThrowingRunnable runnable) throws Exception {
        String previousMode = System.getProperty("oak.blockchain.mode");
        try {
            restoreProperty("oak.blockchain.mode", mode);
            BlockchainConfig.reset();
            runnable.run();
        } finally {
            restoreProperty("oak.blockchain.mode", previousMode);
            BlockchainConfig.reset();
        }
    }

    private void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
