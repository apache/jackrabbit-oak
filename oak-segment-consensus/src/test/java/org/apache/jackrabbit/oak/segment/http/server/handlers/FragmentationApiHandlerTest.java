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

import io.aeron.cluster.service.Cluster;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.consensus.fragmentation.FragmentationTracker;
import org.apache.jackrabbit.oak.segment.consensus.gc.EntityGCAccount;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCAccountManager;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCExecutionResult;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCProposal;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCProposalManager;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FragmentationApiHandlerTest {

    private ServerContext context;
    private FragmentationApiHandler handler;
    private HttpServletRequest request;
    private HttpServletResponse response;
    private StringWriter body;

    @Before
    public void setUp() throws Exception {
        context = new ServerContext(
            mock(FileStore.class),
            mock(NodeStore.class),
            Paths.get("/tmp/store"),
            "http://localhost:8090"
        );
        handler = new FragmentationApiHandler(context);
        request = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);
        body = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(body));
    }

    @Test
    public void testGetAllMetricsReturns503WhenTrackerMissing() throws Exception {
        handler.handleGetAllMetrics(request, response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("\"error\":\"Fragmentation tracker not initialized\""));
    }

    @Test
    public void testGetAllMetricsSerializesTrackedEntities() throws Exception {
        FragmentationTracker tracker = new FragmentationTracker();
        tracker.recordWrite("0xabc", "data00001a.tar", 1024L);
        context.fragmentationTracker = tracker;

        handler.handleGetAllMetrics(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"totalEntities\":1"));
        assertTrue(json.contains("\"walletAddress\":\"0xabc\""));
        assertTrue(json.contains("\"tarFilesCreated\":1"));
        assertTrue(json.contains("\"fragmentationTaxFormatted\":"));
    }

    @Test
    public void testGetEntityMetricsReturns404WhenWalletUnknown() throws Exception {
        context.fragmentationTracker = new FragmentationTracker();

        handler.handleGetEntityMetrics(request, response, "0xmissing");

        verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);
        assertTrue(body.toString().contains("\"error\":\"No metrics found for wallet: 0xmissing\""));
    }

    @Test
    public void testGetEntityMetricsReturnsTrackedWalletPayload() throws Exception {
        FragmentationTracker tracker = new FragmentationTracker();
        tracker.recordWrite("0xabc", "data00001a.tar", 2048L);
        context.fragmentationTracker = tracker;

        handler.handleGetEntityMetrics(request, response, "0xabc");

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"walletAddress\":\"0xabc\""));
        assertTrue(json.contains("\"tarFiles\":[\"data00001a.tar\"]"));
        assertTrue(json.contains("\"totalBytesWritten\":2048"));
    }

    @Test
    public void testGetTopFragmentedDefaultsInvalidLimitToTen() throws Exception {
        FragmentationTracker tracker = new FragmentationTracker();
        tracker.recordWrite("0xabc", "data00001a.tar", 1024L);
        tracker.recordWrite("0xdef", "data00001b.tar", 2048L);
        context.fragmentationTracker = tracker;
        when(request.getParameter("limit")).thenReturn("not-a-number");

        handler.handleGetTopFragmented(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"limit\":10"));
        assertTrue(json.contains("\"walletAddress\":\"0xabc\""));
        assertTrue(json.contains("\"walletAddress\":\"0xdef\""));
    }

    @Test
    public void testGetTopFragmentedHonorsExplicitLimit() throws Exception {
        FragmentationTracker tracker = new FragmentationTracker();
        tracker.recordWrite("0xabc", "data00001a.tar", 1024L);
        context.fragmentationTracker = tracker;
        when(request.getParameter("limit")).thenReturn("1");

        handler.handleGetTopFragmented(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        assertTrue(body.toString().contains("\"limit\":1"));
    }

    @Test
    public void testGetGcStatusReturns503WhenManagerMissing() throws Exception {
        handler.handleGetGcStatus(request, response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("\"error\":\"GC Proposal Manager not initialized\""));
    }

    @Test
    public void testGetGcStatusReturnsSummary() throws Exception {
        GCProposalManager gcManager = mock(GCProposalManager.class);
        GCProposal proposal = new GCProposal();
        proposal.proposalId = "gc-1";
        GCExecutionResult result = new GCExecutionResult();
        result.timestamp = 12345L;
        result.actualReclaimedSizeMB = 42L;
        result.actualCostUSDC = new BigDecimal("4.25");
        context.gcProposalManager = gcManager;
        when(gcManager.getPendingProposals()).thenReturn(Collections.singletonList(proposal));
        when(gcManager.getGCHistory(1)).thenReturn(Collections.singletonList(result));

        handler.handleGetGcStatus(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"pendingProposals\":1"));
        assertTrue(json.contains("\"lastGcRun\":12345"));
        assertTrue(json.contains("\"lastGcReclaimedMB\":42"));
        assertTrue(json.contains("\"lastGcCostUSDC\":\"4.25\""));
    }

    @Test
    public void testGetCompactionProposalsReturns503WhenManagerMissing() throws Exception {
        handler.handleGetCompactionProposals(request, response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("\"error\":\"GC Proposal Manager not initialized\""));
    }

    @Test
    public void testGetCompactionProposalsSerializesProposalVotes() throws Exception {
        GCProposalManager gcManager = mock(GCProposalManager.class);
        GCProposal proposal = new GCProposal();
        proposal.proposalId = "gc-1";
        proposal.proposerWallet = "0xabc";
        proposal.targetRevision = "rev-1";
        proposal.estimatedReclaimableSizeMB = 64L;
        proposal.estimatedCostUSDC = new BigDecimal("6.40");
        proposal.addVote(1, true, "looks good", 1L);
        context.gcProposalManager = gcManager;
        when(gcManager.getPendingProposals()).thenReturn(Collections.singletonList(proposal));

        handler.handleGetCompactionProposals(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"proposalId\":\"gc-1\""));
        assertTrue(json.contains("\"state\":\"VOTING\""));
        assertTrue(json.contains("\"approveVotes\":1"));
        assertTrue(json.contains("\"votes\":{\"1\":"));
    }

    @Test
    public void testProposeGcRejectsMissingWalletAddress() throws Exception {
        context.gcProposalManager = mock(GCProposalManager.class);
        when(request.getContentType()).thenReturn("application/x-www-form-urlencoded");
        when(request.getParameter("walletAddress")).thenReturn(null);
        when(request.getParameter("wallet")).thenReturn(null);

        handler.handleProposeGC(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("walletAddress parameter required"));
    }

    @Test
    public void testProposeGcAcceptsJsonNullTargetRevision() throws Exception {
        GCProposalManager gcManager = mock(GCProposalManager.class);
        GCProposal proposal = new GCProposal();
        proposal.proposalId = "gc-proposal-null";
        proposal.proposerWallet = "0xabc";
        proposal.targetRevision = null;
        proposal.estimatedReclaimableSizeMB = 32L;
        proposal.estimatedCostUSDC = new BigDecimal("3.20");
        context.gcProposalManager = gcManager;
        when(request.getContentType()).thenReturn("application/json");
        when(request.getReader()).thenReturn(readerFor("{\"walletAddress\":\"0xabc\",\"targetRevision\":null}"));
        when(gcManager.proposeGC("0xabc", null)).thenReturn(proposal);
        context.aeronConsensusEngine = replicatingEngine();

        handler.handleProposeGC(request, response);

        verify(gcManager).proposeGC("0xabc", null);
        verify(response).setStatus(HttpServletResponse.SC_OK);
        assertTrue(body.toString().contains("\"proposalId\":\"gc-proposal-null\""));
    }

    @Test
    public void testProposeGcAcceptsJsonBody() throws Exception {
        GCProposalManager gcManager = mock(GCProposalManager.class);
        GCProposal proposal = new GCProposal();
        proposal.proposalId = "gc-proposal-1";
        proposal.proposerWallet = "0xabc";
        proposal.targetRevision = "rev-1";
        proposal.estimatedReclaimableSizeMB = 64L;
        proposal.estimatedCostUSDC = new BigDecimal("6.40");
        proposal.fragmentationOverheadMB = 12L;
        proposal.fragmentationCostUSDC = new BigDecimal("1.20");
        context.gcProposalManager = gcManager;
        when(request.getContentType()).thenReturn("application/json");
        when(request.getReader()).thenReturn(readerFor("{\"walletAddress\":\"0xabc\",\"targetRevision\":\"rev-1\"}"));
        when(gcManager.proposeGC("0xabc", "rev-1")).thenReturn(proposal);
        context.aeronConsensusEngine = replicatingEngine();

        handler.handleProposeGC(request, response);

        verify(gcManager).proposeGC("0xabc", "rev-1");
        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"proposalId\":\"gc-proposal-1\""));
        assertTrue(json.contains("\"proposerWallet\":\"0xabc\""));
        assertTrue(json.contains("\"targetRevision\":\"rev-1\""));
    }

    @Test
    public void testProposeGcFallsBackToQueryParametersAndReplicatesThroughAeron() throws Exception {
        GCProposalManager gcManager = mock(GCProposalManager.class);
        GCProposal proposal = new GCProposal();
        proposal.proposalId = "gc-proposal-2";
        proposal.proposerWallet = "0xabc";
        proposal.targetRevision = "rev-q";
        proposal.estimatedReclaimableSizeMB = 64L;
        proposal.estimatedCostUSDC = new BigDecimal("6.40");
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        context.gcProposalManager = gcManager;
        context.aeronConsensusEngine = engine;
        when(request.getContentType()).thenReturn("application/x-www-form-urlencoded");
        when(request.getParameter("walletAddress")).thenReturn("0xabc");
        when(request.getParameter("wallet")).thenReturn(null);
        when(request.getParameter("targetRevision")).thenReturn("rev-q");
        when(gcManager.proposeGC("0xabc", "rev-q")).thenReturn(proposal);
        when(engine.sendGCProposalThroughIngress("gc-proposal-2", "0xabc", "rev-q", 64L, "6.40")).thenReturn(true);

        handler.handleProposeGC(request, response);

        verify(gcManager).proposeGC("0xabc", "rev-q");
        verify(engine).sendGCProposalThroughIngress("gc-proposal-2", "0xabc", "rev-q", 64L, "6.40");
        verify(response).setStatus(HttpServletResponse.SC_OK);
        assertTrue(body.toString().contains("\"proposalId\":\"gc-proposal-2\""));
    }

    @Test
    public void testProposeGcRecordsNothingWhenTheClusterLogIsUnavailable() throws Exception {
        GCProposalManager gcManager = mock(GCProposalManager.class);
        GCProposal proposal = new GCProposal();
        proposal.proposalId = "gc-unsent";
        proposal.proposerWallet = "0xabc";
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        context.gcProposalManager = gcManager;
        context.aeronConsensusEngine = engine;
        when(request.getContentType()).thenReturn("application/x-www-form-urlencoded");
        when(request.getParameter("walletAddress")).thenReturn("0xabc");
        when(gcManager.proposeGC("0xabc", null)).thenReturn(proposal);

        handler.handleProposeGC(request, response);

        verify(gcManager, never()).applyReplicatedProposal(any(), any(), any(), anyLong(), any(), anyLong());
        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }

    @Test
    public void testExecuteGcSendsGcExecuteThroughTheLogWithClusterMemberId() throws Exception {
        GCProposalManager gcManager = mock(GCProposalManager.class);
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        Cluster cluster = mock(Cluster.class);
        GCProposal proposal = new GCProposal();
        proposal.proposalId = "gc-proposal-3";
        proposal.state = GCProposal.GCProposalState.APPROVED;
        context.gcProposalManager = gcManager;
        context.aeronConsensusEngine = engine;
        when(engine.getCluster()).thenReturn(cluster);
        when(cluster.memberId()).thenReturn(2);
        when(gcManager.getProposal("gc-proposal-3")).thenReturn(proposal);
        when(engine.sendGCExecuteThroughIngress("gc-proposal-3", 2)).thenReturn(true);
        when(request.getContentType()).thenReturn("application/json");
        when(request.getReader()).thenReturn(readerFor("{\"proposalId\":\"gc-proposal-3\"}"));

        handler.handleExecuteGC(request, response);

        verify(engine).sendGCExecuteThroughIngress("gc-proposal-3", 2);
        verify(gcManager, never()).applyReplicatedExecute(any(), anyInt());
        verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
        String json = body.toString();
        assertTrue(json.contains("\"proposalId\":\"gc-proposal-3\""));
        assertTrue(json.contains("\"executorId\":2"));
        assertTrue(json.contains("\"replicated\":true"));
    }

    @Test
    public void testExecuteGcRejectsMissingProposalId() throws Exception {
        context.gcProposalManager = mock(GCProposalManager.class);
        when(request.getContentType()).thenReturn("application/x-www-form-urlencoded");
        when(request.getParameter("proposalId")).thenReturn(null);

        handler.handleExecuteGC(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("proposalId parameter required"));
    }

    @Test
    public void testExecuteGcReturns404WhenProposalMissing() throws Exception {
        GCProposalManager gcManager = mock(GCProposalManager.class);
        context.gcProposalManager = gcManager;
        when(request.getContentType()).thenReturn("application/x-www-form-urlencoded");
        when(request.getParameter("proposalId")).thenReturn("missing");

        handler.handleExecuteGC(request, response);

        verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);
        assertTrue(body.toString().contains("GC proposal not found: missing"));
    }

    @Test
    public void testExecuteGcReturnsBadRequestWhenProposalNotApproved() throws Exception {
        GCProposalManager gcManager = mock(GCProposalManager.class);
        GCProposal proposal = new GCProposal();
        proposal.proposalId = "gc-pending";
        context.gcProposalManager = gcManager;
        when(request.getContentType()).thenReturn("application/x-www-form-urlencoded");
        when(request.getParameter("proposalId")).thenReturn("gc-pending");
        when(gcManager.getProposal("gc-pending")).thenReturn(proposal);

        handler.handleExecuteGC(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("GC proposal not approved: gc-pending"));
    }

    @Test
    public void testVoteGcRejectsMissingProposalId() throws Exception {
        context.gcProposalManager = mock(GCProposalManager.class);
        when(request.getContentType()).thenReturn("application/x-www-form-urlencoded");
        when(request.getParameter("proposalId")).thenReturn(null);

        handler.handleVoteGC(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("proposalId parameter required"));
    }

    @Test
    public void testVoteGcWithoutTheClusterLogDoesNotChangeTheLocalTally() throws Exception {
        GCProposalManager gcManager = mock(GCProposalManager.class);
        GCProposal proposal = new GCProposal();
        proposal.proposalId = "gc-1";
        context.gcProposalManager = gcManager;
        when(request.getContentType()).thenReturn("application/x-www-form-urlencoded");
        when(request.getParameter("proposalId")).thenReturn("gc-1");
        when(request.getParameter("validatorId")).thenReturn("3");
        when(request.getParameter("approve")).thenReturn("false");
        when(request.getParameter("reason")).thenReturn("too expensive");
        when(gcManager.getProposal("gc-1")).thenReturn(proposal);

        handler.handleVoteGC(request, response);

        verify(gcManager, never()).voteOnProposal(any(), anyInt(), anyBoolean(), any(), anyLong());
        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("nothing was recorded"));
    }

    @Test
    public void testVoteGcAcceptsJsonBodyAndReplicatesThroughAeron() throws Exception {
        GCProposalManager gcManager = mock(GCProposalManager.class);
        GCProposal proposal = new GCProposal();
        proposal.proposalId = "gc-json";
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        Cluster cluster = mock(Cluster.class);
        context.gcProposalManager = gcManager;
        context.aeronConsensusEngine = engine;
        when(engine.getCluster()).thenReturn(cluster);
        when(request.getContentType()).thenReturn("application/json");
        when(request.getReader()).thenReturn(readerFor("{\"proposalId\":\"gc-json\",\"validatorId\":4,\"approve\":true,\"reason\":\"ship it\"}"));
        when(engine.sendGCVoteThroughIngress("gc-json", 4, true, "ship it")).thenReturn(true);
        when(gcManager.getProposal("gc-json")).thenReturn(proposal);

        handler.handleVoteGC(request, response);

        verify(engine).sendGCVoteThroughIngress("gc-json", 4, true, "ship it");
        verify(gcManager, never()).voteOnProposal(any(), anyInt(), anyBoolean(), any(), anyLong());
        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"replicated\":true"));
        assertTrue(json.contains("\"replicationAttempted\":true"));
        assertTrue(json.contains("\"validatorId\":4"));
    }

    @Test
    public void testGetGcAccountReturns503WhenAccountManagerMissing() throws Exception {
        handler.handleGetGCAccount(request, response, "0xwallet");

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("\"error\":\"GC Account Manager not initialized\""));
    }

    @Test
    public void testGetGcAccountReturnsAccountState() throws Exception {
        GCAccountManager accountManager = new GCAccountManager();
        accountManager.addDebt("0xwallet", "/oak-chain/test", 5L);
        accountManager.convertAllPendingToExecuted();
        context.gcAccountManager = accountManager;

        handler.handleGetGCAccount(request, response, "0xwallet");

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"walletAddress\":\"0xwallet\""));
        assertTrue(json.contains("\"totalDebt\":\"0.50\""));
        assertTrue(json.contains("\"executedDebt\":\"0.50\""));
    }

    @Test
    public void testPayGcDebtRecordsPaymentAndReturnsUpdatedBalance() throws Exception {
        GCAccountManager accountManager = new GCAccountManager();
        accountManager.addDebt("0xwallet", "/oak-chain/test", 5L);
        accountManager.convertAllPendingToExecuted();
        context.gcAccountManager = accountManager;
        when(request.getParameter("amount")).thenReturn("0.25");
        when(request.getParameter("txHash")).thenReturn("0xpay");

        handler.handlePayGCDebt(request, response, "0xwallet");

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"success\":true"));
        assertTrue(json.contains("\"amountPaid\":\"0.25\""));
        assertTrue(json.contains("\"remainingDebt\":\"0.25\""));
    }

    @Test
    public void testPayGcDebtRejectsMissingAmount() throws Exception {
        context.gcAccountManager = new GCAccountManager();
        when(request.getParameter("amount")).thenReturn(null);

        handler.handlePayGCDebt(request, response, "0xwallet");

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("\"error\":\"amount parameter required\""));
    }

    @Test
    public void testSetDebtLimitRejectsMissingLimit() throws Exception {
        context.gcAccountManager = new GCAccountManager();
        when(request.getParameter("limit")).thenReturn(null);

        handler.handleSetDebtLimit(request, response, "0xwallet");

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("\"error\":\"limit parameter required\""));
    }

    @Test
    public void testSetDebtLimitUpdatesAccountState() throws Exception {
        context.gcAccountManager = new GCAccountManager();
        when(request.getParameter("limit")).thenReturn("12.5");

        handler.handleSetDebtLimit(request, response, "0xwallet");

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"success\":true"));
        assertTrue(json.contains("\"debtLimit\":\"12.5\""));
    }

    @Test
    public void testExecutePendingDebtMovesPendingBalanceToExecuted() throws Exception {
        GCAccountManager accountManager = new GCAccountManager();
        accountManager.addDebt("0xwallet", "/oak-chain/test", 5L);
        context.gcAccountManager = accountManager;

        handler.handleExecutePendingDebt(request, response, "0xwallet");

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"converted\":\"0.50\""));
        assertTrue(json.contains("\"executedDebt\":\"0.50\""));
    }

    @Test
    public void testTriggerGcReturns503WhenAccountManagerMissing() throws Exception {
        handler.handleTriggerGC(request, response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("\"error\":\"GC Account Manager not initialized\""));
    }

    @Test
    public void testTriggerGcConvertsPendingDebtAndReportsBlockedWallets() throws Exception {
        GCAccountManager accountManager = new GCAccountManager();
        EntityGCAccount account = accountManager.getAccount("0xblocked");
        account.totalDebt = new BigDecimal("150.00");
        account.executedDebt = BigDecimal.ZERO;
        account.debtLimit = new BigDecimal("100.00");
        context.gcAccountManager = accountManager;

        handler.handleTriggerGC(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"success\":true"));
        assertTrue(json.contains("\"entitiesWithExecutedDebt\":1"));
        assertTrue(json.contains("\"entitiesBlocked\":1"));
        assertTrue(json.contains("\"blockedWallets\":[\"0xblocked\"]"));
    }

    private static BufferedReader readerFor(String json) {
        return new BufferedReader(new StringReader(json));
    }

    private static AeronConsensusEngine replicatingEngine() {
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        when(engine.sendGCProposalThroughIngress(any(), any(), any(), anyLong(), any())).thenReturn(true);
        return engine;
    }
}
