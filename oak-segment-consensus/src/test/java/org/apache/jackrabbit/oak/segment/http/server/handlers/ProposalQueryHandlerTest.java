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

import org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig;
import org.apache.jackrabbit.oak.segment.consensus.evm.EvmBridge;
import org.apache.jackrabbit.oak.segment.consensus.evm.PaymentProof;
import org.apache.jackrabbit.oak.segment.consensus.evm.SettlementDetails;
import org.apache.jackrabbit.oak.segment.consensus.evm.impl.SimpleEvmBridge;
import org.apache.jackrabbit.oak.segment.consensus.queue.DurabilityState;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalQueueManagerOptimized;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalState;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalStatus;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProposalQueryHandlerTest {

    private ProposalQueueManagerOptimized queueManager;
    private EvmBridge evmBridge;
    private ProposalQueryHandler handler;
    private HttpServletRequest request;
    private HttpServletResponse response;
    private StringWriter body;

    @Before
    public void setUp() throws Exception {
        queueManager = mock(ProposalQueueManagerOptimized.class);
        evmBridge = mock(EvmBridge.class);
        handler = newHandler(queueManager, evmBridge);
        request = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);
        body = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(body));
    }

    @Test
    public void testGetProposalStatusRejectsMissingRequestUri() throws Exception {
        when(request.getRequestURI()).thenReturn(null);

        handler.handleGetProposalStatus(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("\"error\":\"Invalid proposal ID\""));
    }

    @Test
    public void testGetProposalStatusReturnsProposalPayload() throws Exception {
        when(request.getRequestURI()).thenReturn("/v1/proposals/proposal-123/status");
        when(queueManager.getProposalStatus("proposal-123")).thenReturn(new ProposalStatus(
            "proposal-123",
            ProposalState.CONFIRMED,
            "0xabc",
            1234L,
            null,
            null,
            null,
            0L,
            null,
            null
        ));

        handler.handleGetProposalStatus(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"proposalId\":\"proposal-123\""));
        assertTrue(json.contains("\"state\":\"CONFIRMED\""));
        assertTrue(json.contains("\"confirmedBlock\":-1"));
        assertTrue(json.contains("\"durabilityState\":\"UNKNOWN\""));
    }

    @Test
    public void testGetProposalStatusReturnsNotFoundWhenProposalMissing() throws Exception {
        when(request.getRequestURI()).thenReturn("/v1/proposals/proposal-404/status");
        when(queueManager.getProposalStatus("proposal-404")).thenReturn(null);

        handler.handleGetProposalStatus(request, response);

        verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);
        assertTrue(body.toString().contains("\"error\":\"Proposal not found\""));
    }

    @Test
    public void testGetProposalStatusRejectsWhenQueueUnavailable() throws Exception {
        ProposalQueryHandler noQueueHandler = newHandler(null, evmBridge);
        when(request.getRequestURI()).thenReturn("/v1/proposals/proposal-123/status");

        noQueueHandler.handleGetProposalStatus(request, response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("\"error\":\"Proposal queue not available\""));
    }

    @Test
    public void testGetSettlementByProposalIdReturnsPayload() throws Exception {
        when(request.getRequestURI()).thenReturn("/v1/settlement/proposals/proposal-123");
        when(evmBridge.getSettlementDetailsByProposalId("proposal-123")).thenReturn(new SettlementDetails(
            "sepolia",
            "proposal-123",
            "0xtx123",
            12345L,
            "0xabc",
            "0xdef",
            "1000000000000000",
            PaymentProof.ProposalKind.DELETE,
            PaymentProof.PaymentToken.USDC,
            7,
            12
        ));

        handler.handleGetSettlementByProposalId(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"contractVersion\":\"settlement.v1\""));
        assertTrue(json.contains("\"lookupType\":\"proposalId\""));
        assertTrue(json.contains("\"lookupValue\":\"proposal-123\""));
        assertTrue(json.contains("\"transactionHash\":\"0xtx123\""));
        assertTrue(json.contains("\"proposalKind\":\"DELETE\""));
        assertTrue(json.contains("\"paymentToken\":\"USDC\""));
    }

    @Test
    public void testGetSettlementByProposalIdReturnsNotFoundWhenUnavailable() throws Exception {
        when(request.getRequestURI()).thenReturn("/v1/settlement/proposals/proposal-missing");
        when(evmBridge.getSettlementDetailsByProposalId("proposal-missing")).thenReturn(null);

        handler.handleGetSettlementByProposalId(request, response);

        verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);
        assertTrue(body.toString().contains("\"error\":\"Settlement details not found\""));
    }

    @Test
    public void testGetSettlementByProposalIdReturnsNotFoundForUnregisteredProposalInMockMode() throws Exception {
        String previousMode = System.getProperty("oak.blockchain.mode");
        System.setProperty("oak.blockchain.mode", "mock");
        BlockchainConfig.reset();
        try {
            SimpleEvmBridge mockBridge = new SimpleEvmBridge("mock", "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0");
            ProposalQueryHandler mockBridgeHandler = newHandler(queueManager, mockBridge);
            when(request.getRequestURI()).thenReturn(
                "/v1/settlement/proposals/0x4cc359f9f42dbddc32d50823371578a70f59502e75f7aa41313bfcdb668affad");

            mockBridgeHandler.handleGetSettlementByProposalId(request, response);

            verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);
            assertTrue(body.toString().contains("\"code\":\"not_found\""));
            assertTrue(body.toString().contains("\"error\":\"Settlement details not found\""));
            assertTrue(!body.toString().contains("SECURITY VIOLATION"));
        } finally {
            if (previousMode == null) {
                System.clearProperty("oak.blockchain.mode");
            } else {
                System.setProperty("oak.blockchain.mode", previousMode);
            }
            BlockchainConfig.reset();
        }
    }

    @Test
    public void testGetSettlementByProposalIdRejectsWhenBridgeUnavailable() throws Exception {
        ProposalQueryHandler noBridgeHandler = newHandler(queueManager, null);
        when(request.getRequestURI()).thenReturn("/v1/settlement/proposals/proposal-123");

        noBridgeHandler.handleGetSettlementByProposalId(request, response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("\"error\":\"Settlement lookup not available\""));
    }

    @Test
    public void testGetSettlementByTransactionHashReturnsPayload() throws Exception {
        when(request.getRequestURI()).thenReturn("/v1/settlement/transactions/0xtxabc");
        when(evmBridge.getSettlementDetailsByTransactionHash("0xtxabc")).thenReturn(new SettlementDetails(
            "sepolia",
            "proposal-456",
            "0xtxabc",
            22222L,
            "0x111",
            "0x222",
            "42",
            PaymentProof.ProposalKind.WRITE,
            PaymentProof.PaymentToken.ETH,
            0,
            3
        ));

        handler.handleGetSettlementByTransactionHash(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"lookupType\":\"transactionHash\""));
        assertTrue(json.contains("\"lookupValue\":\"0xtxabc\""));
        assertTrue(json.contains("\"proposalId\":\"proposal-456\""));
        assertTrue(json.contains("\"paymentToken\":\"ETH\""));
    }

    @Test
    public void testGetSettlementByTransactionHashRejectsMissingUriSegment() throws Exception {
        when(request.getRequestURI()).thenReturn("/v1/settlement/transactions/");

        handler.handleGetSettlementByTransactionHash(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("\"error\":\"Invalid transaction hash\""));
    }

    @Test
    public void testGetOperationStatusReturnsCommittedForAckedProcessedProposal() throws Exception {
        when(request.getRequestURI()).thenReturn("/v1/ops/operations/proposal-123");
        when(queueManager.getProposalStatus("proposal-123")).thenReturn(new ProposalStatus(
            "proposal-123",
            ProposalState.PROCESSED,
            "0xabc",
            1234L,
            42L,
            null,
            DurabilityState.ACKED,
            2345L,
            null,
            "head-123"
        ));

        handler.handleGetOperationStatus(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"contractVersion\":\"ops.v1\""));
        assertTrue(json.contains("\"operationId\":\"proposal-123\""));
        assertTrue(json.contains("\"state\":\"COMMITTED\""));
        assertTrue(json.contains("\"durabilityState\":\"ACKED\""));
        assertTrue(json.contains("\"error\":null"));
    }

    @Test
    public void testGetOperationStatusRejectsInvalidOperationId() throws Exception {
        when(request.getRequestURI()).thenReturn("/v1/ops/operations/");

        handler.handleGetOperationStatus(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("\"error\":\"Invalid operation ID\""));
    }

    @Test
    public void testGetOperationStatusReturnsQueuedForPendingProposal() throws Exception {
        when(request.getRequestURI()).thenReturn("/v1/ops/operations/proposal-pending");
        when(queueManager.getProposalStatus("proposal-pending")).thenReturn(new ProposalStatus(
            "proposal-pending",
            ProposalState.PENDING,
            "0xdef",
            9999L,
            null,
            null,
            null,
            0L,
            null,
            null
        ));

        handler.handleGetOperationStatus(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"state\":\"QUEUED\""));
        assertTrue(json.contains("\"completedAtMs\":null"));
        assertTrue(json.contains("\"error\":null"));
    }

    @Test
    public void testGetOperationStatusReturnsFailedWithDurabilityError() throws Exception {
        when(request.getRequestURI()).thenReturn("/v1/ops/operations/proposal-failed");
        when(queueManager.getProposalStatus("proposal-failed")).thenReturn(new ProposalStatus(
            "proposal-failed",
            ProposalState.PROCESSED,
            "0xabc",
            4321L,
            99L,
            "quorum lost",
            DurabilityState.FAILED,
            9876L,
            "durability write failed",
            "head-err"
        ));

        handler.handleGetOperationStatus(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"state\":\"FAILED\""));
        assertTrue(json.contains("\"error\":{\"code\":\"OPERATION_FAILED\""));
        assertTrue(json.contains("\"message\":\"durability write failed\""));
        assertTrue(json.contains("\"retryable\":false"));
    }

    @Test
    public void testGetOperationStatusReturnsTimedOutRetryableError() throws Exception {
        when(request.getRequestURI()).thenReturn("/v1/ops/operations/proposal-timeout");
        when(queueManager.getProposalStatus("proposal-timeout")).thenReturn(new ProposalStatus(
            "proposal-timeout",
            ProposalState.REJECTED,
            null,
            4567L,
            null,
            "Timeout waiting for transaction confirmation",
            null,
            0L,
            null,
            null
        ));

        handler.handleGetOperationStatus(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"state\":\"TIMED_OUT\""));
        assertTrue(json.contains("\"error\":{\"code\":\"OPERATION_TIMED_OUT\""));
        assertTrue(json.contains("\"message\":\"Timeout waiting for transaction confirmation\""));
        assertTrue(json.contains("\"retryable\":true"));
    }

    @Test
    public void testGetOperationStatusReturnsNotFoundWhenOperationMissing() throws Exception {
        when(request.getRequestURI()).thenReturn("/v1/ops/operations/proposal-missing");
        when(queueManager.getProposalStatus("proposal-missing")).thenReturn(null);

        handler.handleGetOperationStatus(request, response);

        verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);
        assertTrue(body.toString().contains("\"error\":\"Operation not found\""));
    }

    @Test
    public void testGetPendingCountReturnsCurrentCount() throws Exception {
        when(queueManager.getPendingCount()).thenReturn(7);

        handler.handleGetPendingCount(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        assertTrue(body.toString().contains("\"pendingCount\":7"));
    }

    @Test
    public void testGetPendingCountReturnsServerErrorWhenQueueFails() throws Exception {
        when(queueManager.getPendingCount()).thenThrow(new IllegalStateException("broken counter"));

        handler.handleGetPendingCount(response);

        verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        assertTrue(body.toString().contains("\"error\":\"Error: broken counter\""));
    }

    @Test
    public void testGetQueueStatsReturnsQueuePayload() throws Exception {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("pendingCount", 3);
        stats.put("oldestAgeMs", 55L);
        when(queueManager.getQueueStats()).thenReturn(stats);

        handler.handleGetQueueStats(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"pendingCount\":3"));
        assertTrue(json.contains("\"oldestAgeMs\":55"));
    }

    @Test
    public void testGetQueueStatsReturnsServerErrorWhenQueueFails() throws Exception {
        when(queueManager.getQueueStats()).thenThrow(new IllegalStateException("queue stats down"));

        handler.handleGetQueueStats(response);

        verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        assertTrue(body.toString().contains("\"error\":\"Error: queue stats down\""));
    }

    @Test
    public void testGetOpsQueueSnapshotUsesWarmCacheWithinTtl() throws Exception {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("pendingCount", 7);
        when(queueManager.getQueueStats()).thenReturn(stats);

        handler.handleGetOpsQueueSnapshot(response);
        body.getBuffer().setLength(0);

        handler.handleGetOpsQueueSnapshot(response);

        verify(queueManager, times(1)).getQueueStats();
        String json = body.toString();
        assertTrue(json.contains("\"contractVersion\":\"ops.v1\""));
        assertTrue(json.contains("\"cache\":{\"hit\":true,\"ttlMs\":1000}"));
        assertTrue(json.contains("\"degraded\":false"));
        assertTrue(json.contains("\"pendingCount\":7"));
    }

    @Test
    public void testGetOpsQueueSnapshotFallsBackToStaleCacheOnRefreshFailure() throws Exception {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("pendingCount", 11);
        when(queueManager.getQueueStats())
            .thenReturn(stats)
            .thenThrow(new IllegalStateException("boom"));

        handler.handleGetOpsQueueSnapshot(response);
        ageQueueSnapshotCache(handler);
        body.getBuffer().setLength(0);

        handler.handleGetOpsQueueSnapshot(response);

        String json = body.toString();
        assertTrue(json.contains("\"degraded\":true"));
        assertTrue(json.contains("\"degradedReason\":\"STALE_CACHE_FALLBACK\""));
        assertTrue(json.contains("\"cache\":{\"hit\":true,\"ttlMs\":1000}"));
        assertTrue(json.contains("\"pendingCount\":11"));
    }

    @Test
    public void testGetOpsQueueSnapshotReturnsServerErrorWithoutCachedFallback() throws Exception {
        when(queueManager.getQueueStats()).thenThrow(new IllegalStateException("snapshot unavailable"));

        handler.handleGetOpsQueueSnapshot(response);

        verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        assertTrue(body.toString().contains("\"error\":\"Error: snapshot unavailable\""));
    }

    @Test
    public void testGetProposalReleaseFlowReturnsAdaptivePayload() throws Exception {
        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("releaseMode", "adaptive-active");
        flow.put("releaseStages", new LinkedHashMap<String, Object>());
        when(queueManager.getProposalReleaseFlowStats()).thenReturn(flow);

        handler.handleGetProposalReleaseFlow(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"contractVersion\":\"release-flow.v1\""));
        assertTrue(json.contains("\"releaseMode\":\"adaptive-active\""));
        assertTrue(json.contains("\"releaseStages\":{}"));
    }

    @Test
    public void testGetProposalReleaseFlowReturnsServerErrorWhenQueueFails() throws Exception {
        when(queueManager.getProposalReleaseFlowStats()).thenThrow(new IllegalStateException("release flow unavailable"));

        handler.handleGetProposalReleaseFlow(response);

        verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        assertTrue(body.toString().contains("\"error\":\"Error: release flow unavailable\""));
    }

    private static ProposalQueryHandler newHandler(ProposalQueueManagerOptimized queueManager, EvmBridge evmBridge) {
        ServerContext context = new ServerContext(
            mock(FileStore.class),
            mock(NodeStore.class),
            Paths.get("/tmp/store"),
            "http://localhost:8090"
        );
        context.proposalQueueManager = queueManager;
        context.evmBridge = evmBridge;
        return new ProposalQueryHandler(context);
    }

    private static void ageQueueSnapshotCache(ProposalQueryHandler handler) throws Exception {
        Field field = ProposalQueryHandler.class.getDeclaredField("cachedQueueStatsSourceTimestampMs");
        field.setAccessible(true);
        field.setLong(handler, System.currentTimeMillis() - 5000L);
    }
}
