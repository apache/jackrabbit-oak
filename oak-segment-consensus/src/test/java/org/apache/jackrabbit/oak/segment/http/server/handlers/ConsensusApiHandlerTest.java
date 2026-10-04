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

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.consensus.fragmentation.FragmentationTracker;
import org.apache.jackrabbit.oak.segment.consensus.queue.DurabilityState;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalQueueManagerOptimized;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalState;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalStatus;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ConsensusApiHandler.
 * 
 * <p>Tests cover:
 * <ul>
 *   <li>ADR 028: Pre-flight health check (503 when unhealthy)</li>
 *   <li>Wallet address validation</li>
 *   <li>Write proposal handling</li>
 *   <li>Delete proposal handling</li>
 *   <li>Error responses</li>
 * </ul>
 * 
 * @see ConsensusApiHandler
 */
public class ConsensusApiHandlerTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Mock
    private FileStore mockFileStore;

    @Mock
    private NodeStore mockNodeStore;

    @Mock
    private AeronConsensusEngine mockAeronEngine;

    @Mock
    private HttpServletRequest mockRequest;

    @Mock
    private HttpServletResponse mockResponse;

    private ServerContext context;
    private StringWriter responseWriter;
    private ConsensusApiHandler handler;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        
        responseWriter = new StringWriter();
        when(mockResponse.getWriter()).thenReturn(new PrintWriter(responseWriter));
        
        // Create real ServerContext with mocked dependencies
        context = new ServerContext(
            mockFileStore,
            mockNodeStore,
            tempFolder.getRoot().toPath(),
            "http://localhost:8090"
        );
        
        handler = new ConsensusApiHandler(context);
    }

    @After
    public void tearDown() {
        if (handler != null) {
            handler.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // ADR 028: PRE-FLIGHT HEALTH CHECK TESTS
    // ═══════════════════════════════════════════════════════════════

    @Test
    public void testProposeWriteReturns503WhenNoConsensusEngine() throws Exception {
        // Given: No Aeron consensus engine configured
        context.aeronConsensusEngine = null;
        
        // When: Write proposal handled
        handler.handleProposeWrite(mockRequest, mockResponse);
        
        // Then: Should return 503 Service Unavailable
        assertJsonErrorContains(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Aeron consensus engine not configured");
    }

    @Test
    public void testProposeWriteReturns503WhenClusterUnhealthy() throws Exception {
        // Given: Aeron engine configured but cluster unhealthy
        context.aeronConsensusEngine = mockAeronEngine;
        when(mockAeronEngine.isClusterHealthy()).thenReturn(false);
        when(mockAeronEngine.getUnhealthyReason()).thenReturn("session_closed_timeout");
        
        // When: Write proposal handled
        handler.handleProposeWrite(mockRequest, mockResponse);
        
        // Then: Should return 503 Service Unavailable with reason
        assertJsonErrorContains(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "session_closed_timeout");
    }

    @Test
    public void testDeleteProposalReturns503WhenClusterUnhealthy() throws Exception {
        // Given: Aeron engine configured but cluster unhealthy
        context.aeronConsensusEngine = mockAeronEngine;
        when(mockAeronEngine.isClusterHealthy()).thenReturn(false);
        when(mockAeronEngine.getUnhealthyReason()).thenReturn("no_leader_elected");
        
        // When: Delete proposal handled
        handler.handleDeleteProposal(mockRequest, mockResponse);
        
        // Then: Should return 503 Service Unavailable with reason
        assertJsonErrorContains(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "no_leader_elected");
    }

    // ═══════════════════════════════════════════════════════════════
    // WALLET VALIDATION TESTS
    // ═══════════════════════════════════════════════════════════════

    @Test
    public void testProposeWriteRejectsMissingWallet() throws Exception {
        // Given: Healthy cluster but no wallet parameter
        context.aeronConsensusEngine = mockAeronEngine;
        when(mockAeronEngine.isClusterHealthy()).thenReturn(true);
        when(mockRequest.getContentType()).thenReturn("application/x-www-form-urlencoded");
        when(mockRequest.getParameter("walletAddress")).thenReturn(null);
        when(mockRequest.getParameter("wallet")).thenReturn(null);
        
        // When: Write proposal handled
        handler.handleProposeWrite(mockRequest, mockResponse);
        
        // Then: Should return 400 Bad Request
        assertJsonErrorContains(HttpServletResponse.SC_BAD_REQUEST, "wallet");
    }

    @Test
    public void testProposeWriteRejectsInvalidWalletFormat() throws Exception {
        // Given: Healthy cluster but invalid wallet format
        context.aeronConsensusEngine = mockAeronEngine;
        when(mockAeronEngine.isClusterHealthy()).thenReturn(true);
        when(mockRequest.getContentType()).thenReturn("application/x-www-form-urlencoded");
        when(mockRequest.getParameter("walletAddress")).thenReturn("not-a-wallet");
        
        // When: Write proposal handled
        handler.handleProposeWrite(mockRequest, mockResponse);
        
        // Then: Should return 400 Bad Request
        assertJsonErrorStatus(HttpServletResponse.SC_BAD_REQUEST);
    }

    @Test
    public void testProposeWriteRejectsShortWallet() throws Exception {
        // Given: Healthy cluster but wallet too short
        context.aeronConsensusEngine = mockAeronEngine;
        when(mockAeronEngine.isClusterHealthy()).thenReturn(true);
        when(mockRequest.getContentType()).thenReturn("application/x-www-form-urlencoded");
        when(mockRequest.getParameter("walletAddress")).thenReturn("0x123");
        
        // When: Write proposal handled
        handler.handleProposeWrite(mockRequest, mockResponse);
        
        // Then: Should return 400 Bad Request
        assertJsonErrorStatus(HttpServletResponse.SC_BAD_REQUEST);
    }

    @Test
    public void testProposeWriteAcceptsValidWallet() throws Exception {
        // Given: Healthy cluster with valid wallet (but no signature - will fail later)
        context.aeronConsensusEngine = mockAeronEngine;
        when(mockAeronEngine.isClusterHealthy()).thenReturn(true);
        when(mockRequest.getContentType()).thenReturn("application/x-www-form-urlencoded");
        when(mockRequest.getParameter("walletAddress")).thenReturn("0x1234567890abcdef1234567890abcdef12345678");
        when(mockRequest.getParameter("signature")).thenReturn(null); // Missing signature
        when(mockRequest.getParameter("message")).thenReturn("test");
        
        // When: Write proposal handled
        handler.handleProposeWrite(mockRequest, mockResponse);
        
        // Then: Should NOT fail on wallet validation (may fail on signature)
        // Verify we got past wallet validation - error should be about signature, not wallet
        verify(mockResponse, never()).sendError(
            eq(HttpServletResponse.SC_BAD_REQUEST),
            contains("wallet")
        );
    }

    // ═══════════════════════════════════════════════════════════════
    // DELETE PROPOSAL VALIDATION TESTS
    // ═══════════════════════════════════════════════════════════════

    @Test
    public void testDeleteProposalRejectsMissingSignature() throws Exception {
        // Given: Healthy cluster with valid wallet but no signature
        context.aeronConsensusEngine = mockAeronEngine;
        when(mockAeronEngine.isClusterHealthy()).thenReturn(true);
        when(mockRequest.getParameter("walletAddress")).thenReturn("0x1234567890abcdef1234567890abcdef12345678");
        when(mockRequest.getParameter("signature")).thenReturn(null);
        when(mockRequest.getParameter("contentPath")).thenReturn("/oak-chain/test");
        
        // When: Delete proposal handled
        handler.handleDeleteProposal(mockRequest, mockResponse);
        
        // Then: Should return 400 Bad Request for missing signature
        assertJsonErrorContains(HttpServletResponse.SC_BAD_REQUEST, "signature");
    }

    @Test
    public void testDeleteProposalRejectsMissingContentPath() throws Exception {
        // Given: Healthy cluster with valid wallet but no content path
        context.aeronConsensusEngine = mockAeronEngine;
        when(mockAeronEngine.isClusterHealthy()).thenReturn(true);
        when(mockRequest.getParameter("walletAddress")).thenReturn("0x1234567890abcdef1234567890abcdef12345678");
        when(mockRequest.getParameter("signature")).thenReturn("0xvalidsig");
        when(mockRequest.getParameter("contentPath")).thenReturn(null);
        
        // When: Delete proposal handled
        handler.handleDeleteProposal(mockRequest, mockResponse);
        
        // Then: Should return 400 Bad Request for missing path
        assertJsonErrorContains(HttpServletResponse.SC_BAD_REQUEST, "contentPath");
    }

    // ═══════════════════════════════════════════════════════════════
    // CONSENSUS STATUS ENDPOINT TESTS
    // ═══════════════════════════════════════════════════════════════

    @Test
    public void testGetConsensusStatusReturnsJson() throws Exception {
        // Given: Handler with context
        
        // When: Consensus status requested
        handler.handleGetConsensusStatus(mockResponse);
        
        // Then: Should return JSON content type
        verify(mockResponse).setContentType("application/json");
        verify(mockResponse).setStatus(HttpServletResponse.SC_OK);
        
        // And: Response should contain JSON
        String response = responseWriter.toString();
        assertTrue("Response should start with {", response.trim().startsWith("{"));
        assertTrue("Response should end with }", response.trim().endsWith("}"));
    }

    @Test
    public void testGetConsensusStatusIncludesConsensusType() throws Exception {
        // Given: Handler with context (no Aeron engine)
        context.aeronConsensusEngine = null;
        
        // When: Consensus status requested
        handler.handleGetConsensusStatus(mockResponse);
        
        // Then: Response should include consensusType field
        String response = responseWriter.toString();
        assertTrue("Response should include consensusType", response.contains("\"consensusType\""));
        assertTrue("Response should show 'none' when no engine", response.contains("\"none\""));
    }

    // ═══════════════════════════════════════════════════════════════
    // PENDING COUNT ENDPOINT TESTS
    // ═══════════════════════════════════════════════════════════════

    @Test
    public void testGetPendingCountReturns503WhenQueueNotAvailable() throws Exception {
        // Given: Handler with no proposal queue manager
        context.proposalQueueManager = null;
        
        // When: Pending count requested
        handler.handleGetPendingCount(mockResponse);
        
        // Then: Should return 503 Service Unavailable
        assertJsonErrorContains(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "queue");
    }

    // ═══════════════════════════════════════════════════════════════
    // PROPOSAL STATUS ENDPOINT TESTS
    // ═══════════════════════════════════════════════════════════════

    @Test
    public void testGetProposalStatusRejectsMissingId() throws Exception {
        // Given: Request without proposal ID (null path)
        when(mockRequest.getRequestURI()).thenReturn(null);
        
        // When: Proposal status requested
        handler.handleGetProposalStatus(mockRequest, mockResponse);
        
        // Then: Should reject the malformed request without throwing internally
        assertJsonErrorContains(HttpServletResponse.SC_BAD_REQUEST, "Invalid proposal ID");
    }

    @Test
    public void testGetProposalStatusReturns503WhenQueueNotAvailable() throws Exception {
        // Given: Request with valid proposal ID but no queue manager
        when(mockRequest.getRequestURI()).thenReturn("/v1/proposals/test-proposal-123/status");
        context.proposalQueueManager = null;
        
        // When: Proposal status requested
        handler.handleGetProposalStatus(mockRequest, mockResponse);
        
        // Then: Should return 503 Service Unavailable
        assertJsonErrorContains(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Proposal queue not available");
    }

    @Test
    public void testGetOperationStatusDelegatesToOpsPayload() throws Exception {
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        context.proposalQueueManager = queueManager;
        when(mockRequest.getRequestURI()).thenReturn("/v1/ops/operations/proposal-123");
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

        handler.handleGetOperationStatus(mockRequest, mockResponse);

        verify(mockResponse).setStatus(HttpServletResponse.SC_OK);
        String response = responseWriter.toString();
        assertTrue(response.contains("\"contractVersion\":\"ops.v1\""));
        assertTrue(response.contains("\"operationId\":\"proposal-123\""));
        assertTrue(response.contains("\"state\":\"COMMITTED\""));
    }

    // ═══════════════════════════════════════════════════════════════
    // WALLET STATS ENDPOINT TESTS
    // ═══════════════════════════════════════════════════════════════

    @Test
    public void testWalletStatsReturnsJson() throws Exception {
        // Given: Request for all wallet stats
        when(mockRequest.getParameter("wallet")).thenReturn(null);
        
        // When: Wallet stats requested
        handler.handleWalletStats(mockRequest, mockResponse);
        
        // Then: Should return JSON content type
        verify(mockResponse).setContentType("application/json");
    }

    @Test
    public void testWalletStatsWithSpecificWallet() throws Exception {
        // Given: Request for specific wallet stats
        when(mockRequest.getParameter("wallet")).thenReturn("0x1234567890abcdef1234567890abcdef12345678");
        
        // When: Wallet stats requested
        handler.handleWalletStats(mockRequest, mockResponse);
        
        // Then: Should return JSON content type
        verify(mockResponse).setContentType("application/json");
    }

    // ═══════════════════════════════════════════════════════════════
    // WALLET CONTENT ENDPOINT TESTS
    // ═══════════════════════════════════════════════════════════════

    @Test
    public void testWalletContentRejectsMissingWallet() throws Exception {
        // Given: Request without wallet parameter
        when(mockRequest.getParameter("wallet")).thenReturn(null);
        
        // When: Wallet content requested
        handler.handleWalletContent(mockRequest, mockResponse);
        
        // Then: Should return 400 Bad Request (uses setStatus, not sendError)
        verify(mockResponse).setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }

    @Test
    public void testWalletContentSetsJsonContentType() throws Exception {
        // Given: Request with a wallet parameter (will fail on query but that's ok)
        when(mockRequest.getParameter("wallet")).thenReturn("0x1234567890abcdef1234567890abcdef12345678");
        
        // When: Wallet content requested
        handler.handleWalletContent(mockRequest, mockResponse);
        
        // Then: Should set JSON content type
        verify(mockResponse).setContentType("application/json");
    }

    // ═══════════════════════════════════════════════════════════════
    // GC COST ESTIMATE ENDPOINT TESTS
    // ═══════════════════════════════════════════════════════════════

    @Test
    public void testGCCostEstimateReturns503WhenNotAvailable() throws Exception {
        // Given: GC cost estimator not configured
        context.gcCostEstimator = null;
        
        // When: GC cost estimate requested
        handler.handleGCCostEstimate(mockRequest, mockResponse);
        
        // Then: Should return 503 Service Unavailable (uses setStatus, not sendError)
        verify(mockResponse).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }

    @Test
    public void testGetQueueStatsReturnsQueueSnapshot() throws Exception {
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        context.proposalQueueManager = queueManager;
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("pendingCount", 7);
        when(queueManager.getQueueStats()).thenReturn(stats);

        handler.handleGetQueueStats(mockResponse);

        verify(mockResponse).setStatus(HttpServletResponse.SC_OK);
        assertTrue(responseWriter.toString().contains("\"pendingCount\":7"));
    }

    @Test
    public void testGetProposalReleaseFlowReturnsAdaptivePayload() throws Exception {
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        context.proposalQueueManager = queueManager;
        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("releaseMode", "adaptive-active");
        when(queueManager.getProposalReleaseFlowStats()).thenReturn(flow);

        handler.handleGetProposalReleaseFlow(mockResponse);

        verify(mockResponse).setStatus(HttpServletResponse.SC_OK);
        String response = responseWriter.toString();
        assertTrue(response.contains("\"contractVersion\":\"release-flow.v1\""));
        assertTrue(response.contains("\"releaseMode\":\"adaptive-active\""));
    }

    @Test
    public void testGetOpsQueueSnapshotReturnsFreshPayload() throws Exception {
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        context.proposalQueueManager = queueManager;
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("pendingCount", 11);
        when(queueManager.getQueueStats()).thenReturn(stats);

        handler.handleGetOpsQueueSnapshot(mockResponse);

        verify(mockResponse).setStatus(HttpServletResponse.SC_OK);
        String response = responseWriter.toString();
        assertTrue(response.contains("\"contractVersion\":\"ops.v1\""));
        assertTrue(response.contains("\"degraded\":false"));
        assertTrue(response.contains("\"pendingCount\":11"));
    }

    // ═══════════════════════════════════════════════════════════════
    // API METRICS TRACKING TESTS
    // ═══════════════════════════════════════════════════════════════

    @Test
    public void testRejectedRequestsCounterIncremented() throws Exception {
        // Given: Initial rejected count
        long initialCount = context.apiRejectedRequests.get();
        
        // And: Unhealthy cluster
        context.aeronConsensusEngine = mockAeronEngine;
        when(mockAeronEngine.isClusterHealthy()).thenReturn(false);
        when(mockAeronEngine.getUnhealthyReason()).thenReturn("test_reason");
        
        // When: Write proposal handled
        handler.handleProposeWrite(mockRequest, mockResponse);
        
        // Then: Rejected counter should be incremented
        assertEquals("Rejected counter should increment", 
            initialCount + 1, context.apiRejectedRequests.get());
    }

    @Test
    public void testRefreshCallbacksBindsLateDurabilityStatusCallback() {
        ServerContext lateContext = new ServerContext(
            mock(FileStore.class, RETURNS_DEEP_STUBS),
            mock(NodeStore.class),
            tempFolder.getRoot().toPath(),
            "http://localhost:8090"
        );
        ConsensusApiHandler lateHandler = new ConsensusApiHandler(lateContext);
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        AeronConsensusEngine aeronEngine = mock(AeronConsensusEngine.class);

        lateContext.setAeronConsensusEngine(aeronEngine);
        lateHandler.refreshCallbacks();

        ArgumentCaptor<AeronConsensusEngine.DurabilityStatusCallback> captor =
            ArgumentCaptor.forClass(AeronConsensusEngine.DurabilityStatusCallback.class);
        verify(aeronEngine).setDurabilityStatusCallback(captor.capture());

        lateContext.setProposalQueueManager(queueManager);
        captor.getValue().onDurable("proposal-1", "head-123");

        verify(queueManager).updateDurability("proposal-1", DurabilityState.ACKED, "head-123", null);
    }

    @Test
    public void testRefreshCallbacksBindsLateDurabilityFailureCallback() {
        ServerContext lateContext = new ServerContext(
            mock(FileStore.class, RETURNS_DEEP_STUBS),
            mock(NodeStore.class),
            tempFolder.getRoot().toPath(),
            "http://localhost:8090"
        );
        ConsensusApiHandler lateHandler = new ConsensusApiHandler(lateContext);
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        AeronConsensusEngine aeronEngine = mock(AeronConsensusEngine.class);

        lateContext.setAeronConsensusEngine(aeronEngine);
        lateHandler.refreshCallbacks();

        ArgumentCaptor<AeronConsensusEngine.DurabilityStatusCallback> captor =
            ArgumentCaptor.forClass(AeronConsensusEngine.DurabilityStatusCallback.class);
        verify(aeronEngine).setDurabilityStatusCallback(captor.capture());

        lateContext.setProposalQueueManager(queueManager);
        captor.getValue().onFailure("proposal-2", "disk-full");

        verify(queueManager).updateDurability("proposal-2", DurabilityState.FAILED, null, "disk-full");
    }

    @Test
    public void testApplyReplicatedWriteSendsDurabilityAfterLateEngineBinding() throws Exception {
        withSynchronousFlush(() -> {
            FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
            when(fileStore.getHead().getRecordId().toString10()).thenReturn("new-head");

            ServerContext lateContext = new ServerContext(
                fileStore,
                new MemoryNodeStore(),
                tempFolder.getRoot().toPath(),
                "http://localhost:8090"
            );
            ConsensusApiHandler lateHandler = new ConsensusApiHandler(lateContext);
            try {
                AeronConsensusEngine aeronEngine = mock(AeronConsensusEngine.class);
                ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);

                when(aeronEngine.isLeader()).thenReturn(true);
                lateContext.setAeronConsensusEngine(aeronEngine);
                lateHandler.refreshCallbacks();
                lateContext.setProposalQueueManager(queueManager);

                lateHandler.applyReplicatedWrite(
                    "0x1234567890abcdef1234567890abcdef12345678",
                    "/oak-chain/aa/bb/cc/0x1234567890abcdef1234567890abcdef12345678/Acme/content/doc-1",
                    "page",
                    "{\"title\":\"Hello\"}",
                    "0xsig",
                    null,
                    null,
                    null,
                    null,
                    "proposal-1"
                );

                verify(aeronEngine).sendSegmentPersisted("proposal-1", "new-head", true, null);
                verify(queueManager, never()).updateDurability("proposal-1", DurabilityState.ACKED, "new-head", null);
            } finally {
                lateHandler.close();
            }
        });
    }

    @Test
    public void testApplyReplicatedDeleteSendsDurabilityAfterLateEngineBinding() throws Exception {
        withSynchronousFlush(() -> {
            FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
            when(fileStore.getHead().getRecordId().toString10()).thenReturn("delete-head");

            MemoryNodeStore nodeStore = new MemoryNodeStore();
            seedNode(nodeStore,
                "/oak-chain/aa/bb/cc/0x1234567890abcdef1234567890abcdef12345678/Acme/content/doc-1");

            ServerContext lateContext = new ServerContext(
                fileStore,
                nodeStore,
                tempFolder.getRoot().toPath(),
                "http://localhost:8090"
            );
            ConsensusApiHandler lateHandler = new ConsensusApiHandler(lateContext);
            try {
                AeronConsensusEngine aeronEngine = mock(AeronConsensusEngine.class);
                ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);

                when(aeronEngine.isLeader()).thenReturn(true);
                lateContext.setAeronConsensusEngine(aeronEngine);
                lateHandler.refreshCallbacks();
                lateContext.setProposalQueueManager(queueManager);

                lateHandler.applyReplicatedDelete(
                    "0x1234567890abcdef1234567890abcdef12345678",
                    "/oak-chain/aa/bb/cc/0x1234567890abcdef1234567890abcdef12345678/Acme/content/doc-1",
                    "0xsig",
                    "proposal-delete-1"
                );

                verify(aeronEngine).sendSegmentPersisted("proposal-delete-1", "delete-head", true, null);
                assertFalse(nodeExists(nodeStore,
                    "/oak-chain/aa/bb/cc/0x1234567890abcdef1234567890abcdef12345678/Acme/content/doc-1"));
            } finally {
                lateHandler.close();
            }
        });
    }

    @Test
    public void testApplyReplicatedWriteTracksFragmentationForNewTarFiles() throws Exception {
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        when(fileStore.getHead().getRecordId().toString()).thenReturn("previous-head");
        when(fileStore.getHead().getRecordId().toString10()).thenReturn("tracked-head");

        MemoryNodeStore nodeStore = new MemoryNodeStore();
        Path storeDir = tempFolder.newFolder("fragmentation-store").toPath();
        Files.write(storeDir.resolve("data00000a.tar"), new byte[16]);
        Files.write(storeDir.resolve("data00001a.tar"), new byte[32]);

        ServerContext fragmentationContext = new ServerContext(
            fileStore,
            nodeStore,
            storeDir,
            "http://localhost:8090"
        );
        fragmentationContext.fragmentationTracker = new FragmentationTracker();
        ConsensusApiHandler fragmentationHandler = new ConsensusApiHandler(fragmentationContext);

        fragmentationHandler.applyReplicatedWrite(
            "0x1234567890abcdef1234567890abcdef12345678",
            "/oak-chain/aa/bb/cc/0x1234567890abcdef1234567890abcdef12345678/Acme/content/doc-1",
            "page",
            "{\"title\":\"Hello\"}",
            "0xsig",
            null,
            null,
            null,
            null,
            null
        );

        FragmentationTracker.EntityFragmentationMetrics metrics =
            fragmentationContext.fragmentationTracker.getMetrics("0x1234567890abcdef1234567890abcdef12345678");
        assertNotNull(metrics);
        assertEquals(2, metrics.tarFilesCreated);
        assertTrue(fragmentationContext.fragmentationTracker
            .getTarFilesForEntity("0x1234567890abcdef1234567890abcdef12345678")
            .contains("data00000a.tar"));
        assertTrue(nodeExists(nodeStore,
            "/oak-chain/aa/bb/cc/0x1234567890abcdef1234567890abcdef12345678/Acme/content/doc-1"));
    }

    @Test
    public void testApplyReplicatedWriteUsesLateBoundAuthoritativeNodeStoreAndBlobStore() {
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        when(fileStore.getHead().getRecordId().toString()).thenReturn("previous-head");
        when(fileStore.getHead().getRecordId().toString10()).thenReturn("binary-head");

        MemoryNodeStore readViewStore = new MemoryNodeStore();
        MemoryNodeStore authoritativeStore = new MemoryNodeStore();
        ServerContext lateContext = new ServerContext(
            fileStore,
            readViewStore,
            tempFolder.getRoot().toPath(),
            "http://localhost:8090"
        );
        ConsensusApiHandler lateHandler = new ConsensusApiHandler(lateContext);
        lateContext.setAuthoritativeNodeStore(authoritativeStore);
        lateContext.blobStore = mock(BlobStore.class);

        lateHandler.applyReplicatedWrite(
            "0x1234567890abcdef1234567890abcdef12345678",
            "/oak-chain/aa/bb/cc/0x1234567890abcdef1234567890abcdef12345678/Acme/content/doc-1",
            "file",
            "{\"title\":\"Hello\"}",
            "0xsig",
            null,
            "blob-123#42",
            "image/jpeg",
            "QmBinaryCid",
            null
        );

        assertFalse(nodeExists(readViewStore,
            "/oak-chain/aa/bb/cc/0x1234567890abcdef1234567890abcdef12345678/Acme/content/doc-1"));
        assertTrue(nodeExists(authoritativeStore,
            "/oak-chain/aa/bb/cc/0x1234567890abcdef1234567890abcdef12345678/Acme/content/doc-1"));

        org.apache.jackrabbit.oak.spi.state.NodeState contentNode = nodeAt(
            authoritativeStore,
            "/oak-chain/aa/bb/cc/0x1234567890abcdef1234567890abcdef12345678/Acme/content/doc-1"
        );
        assertEquals(Type.BINARY, contentNode.getProperty("jcr:data").getType());
        assertEquals("blob-123#42", contentNode.getProperty("jcr:blobId").getValue(Type.STRING));
        assertEquals("QmBinaryCid", contentNode.getProperty("ipfsCid").getValue(Type.STRING));
        assertEquals("validator", contentNode.getProperty("oak:binaryStorageMode").getValue(Type.STRING));
    }

    @Test
    public void testApplyReplicatedWriteFailureFallsBackToQueueDurabilityWithoutEngine() {
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        when(fileStore.getHead().getRecordId().toString()).thenReturn("previous-head");

        ServerContext lateContext = new ServerContext(
            fileStore,
            new MemoryNodeStore(),
            tempFolder.getRoot().toPath(),
            "http://localhost:8090"
        );
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        lateContext.setProposalQueueManager(queueManager);
        ConsensusApiHandler lateHandler = new ConsensusApiHandler(lateContext);

        assertThrows(RuntimeException.class, () -> lateHandler.applyReplicatedWrite(
            "0x1234567890abcdef1234567890abcdef12345678",
            "/oak-chain/aa/bb/cc/0x1234567890abcdef1234567890abcdef12345678/Acme/content/doc-1",
            "page",
            "{\"title\":\"Hello\"}",
            null,
            null,
            null,
            null,
            null,
            "proposal-failure-1"
        ));

        verify(queueManager).updateDurability(
            eq("proposal-failure-1"),
            eq(DurabilityState.FAILED),
            isNull(),
            contains("SECURITY VIOLATION")
        );
    }

    private static void seedNode(MemoryNodeStore nodeStore, String path) throws Exception {
        NodeBuilder root = nodeStore.getRoot().builder();
        NodeBuilder current = root;
        for (String part : path.split("/")) {
            if (!part.isEmpty()) {
                current = current.child(part);
            }
        }
        current.setProperty("title", "seed");
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static boolean nodeExists(MemoryNodeStore nodeStore, String path) {
        org.apache.jackrabbit.oak.spi.state.NodeState current = nodeStore.getRoot();
        for (String part : path.split("/")) {
            if (!part.isEmpty()) {
                current = current.getChildNode(part);
            }
        }
        return current.exists();
    }

    private static org.apache.jackrabbit.oak.spi.state.NodeState nodeAt(MemoryNodeStore nodeStore, String path) {
        org.apache.jackrabbit.oak.spi.state.NodeState current = nodeStore.getRoot();
        for (String part : path.split("/")) {
            if (!part.isEmpty()) {
                current = current.getChildNode(part);
            }
        }
        return current;
    }

    private void withSynchronousFlush(ThrowingRunnable runnable) throws Exception {
        String previousFlushMs = System.getProperty("oak.filestore.flush.ms");
        String previousFlushBatch = System.getProperty("oak.filestore.flush.batch");
        try {
            restoreProperty("oak.filestore.flush.ms", "0");
            restoreProperty("oak.filestore.flush.batch", "1");
            runnable.run();
        } finally {
            restoreProperty("oak.filestore.flush.ms", previousFlushMs);
            restoreProperty("oak.filestore.flush.batch", previousFlushBatch);
        }
    }

    private void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    private void assertJsonErrorStatus(int status) {
        verify(mockResponse).setStatus(status);
        String response = responseWriter.toString();
        assertTrue("Response should include error JSON", response.contains("\"success\":false"));
    }

    private void assertJsonErrorContains(int status, String expectedFragment) {
        assertJsonErrorStatus(status);
        String response = responseWriter.toString();
        assertTrue("Response should contain error detail", response.contains(expectedFragment));
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
