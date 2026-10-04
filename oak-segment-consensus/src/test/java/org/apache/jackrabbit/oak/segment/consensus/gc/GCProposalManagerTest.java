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
package org.apache.jackrabbit.oak.segment.consensus.gc;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jackrabbit.oak.segment.consensus.evm.EvmBridge;
import org.apache.jackrabbit.oak.segment.consensus.evm.impl.SimplePaymentProof;
import org.apache.jackrabbit.oak.segment.consensus.fragmentation.FragmentationTracker;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GCProposalManagerTest {

    private final List<GCProposalManager> managers = new ArrayList<>();

    @After
    public void tearDown() {
        System.clearProperty("oak.proposal.confirmation.required");
        for (GCProposalManager manager : managers) {
            manager.shutdown();
        }
        managers.clear();
    }

    @Test
    public void testApplyReplicatedProposalNormalizesValuesAndIsIdempotent() {
        GCProposalManager manager = newManager();

        GCProposal proposal = manager.applyReplicatedProposal("gc-proposal-1", "0xwallet", "HEAD", -15L, "invalid", 1_000L);

        assertNull(proposal.targetRevision);
        assertEquals(0L, proposal.estimatedReclaimableSizeMB);
        assertEquals(BigDecimal.ZERO, proposal.estimatedCostUSDC);
        assertEquals(GCProposal.GCProposalState.PENDING, proposal.state);

        proposal.state = GCProposal.GCProposalState.REJECTED;
        GCProposal duplicate = manager.applyReplicatedProposal("gc-proposal-1", "0xother", "rev-2", 99L, "3.00", 1_000L);

        assertSame(proposal, duplicate);
        assertEquals(GCProposal.GCProposalState.REJECTED, duplicate.state);
        assertEquals("0xwallet", duplicate.proposerWallet);
        assertNull(duplicate.targetRevision);
    }

    @Test
    public void testVoteOnProposalReachesApproveQuorumAndIgnoresDuplicateVote() {
        GCProposalManager manager = newManager(null, null, null, 4, () -> false);
        GCProposal proposal = manager.applyReplicatedProposal("gc-proposal-1", "0xwallet", null, 10L, "1.25", 1_000L);

        manager.voteOnProposal(proposal.proposalId, 1, true, "approve", 2_000L);
        manager.voteOnProposal(proposal.proposalId, 1, true, "duplicate", 2_000L);
        manager.voteOnProposal(proposal.proposalId, 2, true, "approve", 2_000L);
        manager.voteOnProposal(proposal.proposalId, 3, true, "approve", 2_000L);

        assertEquals(GCProposal.GCProposalState.APPROVED, proposal.state);
        assertEquals(3, proposal.getTotalVoteCount());
        assertEquals(3, proposal.getApproveVoteCount());
        assertTrue(manager.hasQuorum(proposal.proposalId));
    }

    @Test
    public void testVoteOnProposalRejectsOnQuorum() {
        GCProposalManager manager = newManager(null, null, null, 4, () -> false);
        GCProposal proposal = manager.applyReplicatedProposal("gc-proposal-2", "0xwallet", null, 10L, "1.25", 1_000L);

        manager.voteOnProposal(proposal.proposalId, 1, false, "reject", 2_000L);
        manager.voteOnProposal(proposal.proposalId, 2, false, "reject", 2_000L);
        manager.voteOnProposal(proposal.proposalId, 3, false, "reject", 2_000L);

        assertEquals(GCProposal.GCProposalState.REJECTED, proposal.state);
        assertEquals(3, proposal.getRejectVoteCount());
        assertFalse(manager.hasQuorum(proposal.proposalId));
    }

    @Test
    public void testVoteOnProposalSkipsExpiredAndNonVotableProposals() {
        GCProposalManager manager = newManager();
        GCProposal expired = manager.applyReplicatedProposal("gc-expired", "0xwallet", null, 10L, "1.25", 1_000L);
        expired.expiresAt = 1_999L;

        GCProposal completed = manager.applyReplicatedProposal("gc-completed", "0xwallet", null, 10L, "1.25", 1_000L);
        completed.state = GCProposal.GCProposalState.COMPLETED;

        manager.voteOnProposal(expired.proposalId, 1, true, "approve", 2_000L);
        manager.voteOnProposal(completed.proposalId, 1, true, "approve", 2_000L);

        assertEquals(0, expired.getTotalVoteCount());
        assertEquals(GCProposal.GCProposalState.PENDING, expired.state);
        assertEquals(0, completed.getTotalVoteCount());
        assertEquals(GCProposal.GCProposalState.COMPLETED, completed.state);
    }

    @Test
    public void testVerifyPaymentReturnsTrueWithoutBridgeInPocMode() {
        GCProposalManager manager = newManager();

        assertTrue(manager.verifyPayment("missing-proposal"));
    }

    @Test
    public void testVerifyPaymentReturnsFalseWhenProposalMissing() {
        EvmBridge evmBridge = mock(EvmBridge.class);
        GCProposalManager manager = newManager(null, null, evmBridge, 3, () -> true);

        assertFalse(manager.verifyPayment("missing-proposal"));
    }

    @Test
    public void testVerifyPaymentHonorsConfiguredConfirmationDepth() {
        System.setProperty("oak.proposal.confirmation.required", "2");

        EvmBridge evmBridge = mock(EvmBridge.class);
        GCProposalManager manager = newManager(null, null, evmBridge, 3, () -> true);

        String proposalId = "gc-proposal-3";
        manager.applyReplicatedProposal(proposalId, "0xwallet", "HEAD", 10L, "1.25", 1_000L);
        when(evmBridge.verifyPayment(proposalId)).thenReturn(new SimplePaymentProof(
            "0xtx1",
            100L,
            "0xwallet",
            "0xcontract",
            proposalId,
            "1000000",
            1
        ));

        assertFalse(manager.verifyPayment(proposalId));
    }

    @Test
    public void testVerifyPaymentAcceptsProofOnceConfiguredDepthSatisfied() {
        System.setProperty("oak.proposal.confirmation.required", "2");

        EvmBridge evmBridge = mock(EvmBridge.class);
        GCProposalManager manager = newManager(null, null, evmBridge, 3, () -> true);

        String proposalId = "gc-proposal-4";
        GCProposal proposal = manager.applyReplicatedProposal(proposalId, "0xwallet", "HEAD", 10L, "1.25", 1_000L);
        when(evmBridge.verifyPayment(proposalId)).thenReturn(new SimplePaymentProof(
            "0xtx2",
            100L,
            "0xwallet",
            "0xcontract",
            proposalId,
            "1000000",
            2
        ));

        assertTrue(manager.verifyPayment(proposalId));
        assertEquals("0xtx2", proposal.paymentProof);
    }

    @Test
    public void testVerifyPaymentReturnsEarlyForConfirmedStoredProof() {
        System.setProperty("oak.proposal.confirmation.required", "2");

        EvmBridge evmBridge = mock(EvmBridge.class);
        GCProposalManager manager = newManager(null, null, evmBridge, 3, () -> true);

        GCProposal proposal = manager.applyReplicatedProposal("gc-proposal-5", "0xwallet", null, 10L, "1.25", 1_000L);
        proposal.paymentProof = "0xexisting";
        when(evmBridge.verifyPayment(proposal.proposalId)).thenReturn(new SimplePaymentProof(
            "0xexisting",
            101L,
            "0xwallet",
            "0xcontract",
            proposal.proposalId,
            "1000000",
            2
        ));

        assertTrue(manager.verifyPayment(proposal.proposalId));
        verify(evmBridge, times(1)).verifyPayment(proposal.proposalId);
        assertEquals("0xexisting", proposal.paymentProof);
    }

    @Test
    public void testExecuteGCCompletesApprovedProposalAndAddsHistory() throws IOException {
        FileStore fileStore = mock(FileStore.class);
        GCProposalManager manager = newManager(fileStore, null, null, 3, () -> true);
        GCProposal proposal = manager.applyReplicatedProposal("gc-proposal-6", "0xwallet", null, 10L, "1.25", 1_000L);
        proposal.state = GCProposal.GCProposalState.APPROVED;

        GCExecutionResult result = manager.executeGC(proposal.proposalId, 7);

        verify(fileStore).cleanup();
        assertEquals(GCProposal.GCProposalState.COMPLETED, proposal.state);
        assertTrue(result.success);
        assertEquals(0L, result.actualReclaimedSizeMB);
        assertEquals(new BigDecimal("0.00"), result.actualCostUSDC);
        assertSame(result, proposal.executionResult);
        assertEquals(1, manager.getGCHistory(10).size());
    }

    @Test
    public void testExecuteGCMarksProposalFailedWhenCleanupThrows() {
        FileStore fileStore = mock(FileStore.class);
        GCProposalManager manager = newManager(fileStore, null, null, 3, () -> true);
        GCProposal proposal = manager.applyReplicatedProposal("gc-proposal-7", "0xwallet", null, 10L, "1.25", 1_000L);
        proposal.state = GCProposal.GCProposalState.APPROVED;
        try {
            doThrow(new IOException("disk")).when(fileStore).cleanup();
        } catch (IOException e) {
            throw new AssertionError(e);
        }

        try {
            manager.executeGC(proposal.proposalId, 9);
            fail("Expected IOException");
        } catch (IOException e) {
            assertEquals("GC execution failed", e.getMessage());
            assertTrue(e.getCause() instanceof IOException);
            assertEquals("disk", e.getCause().getMessage());
        }

        assertEquals(GCProposal.GCProposalState.FAILED, proposal.state);
        assertNotNull(proposal.executionResult);
        assertFalse(proposal.executionResult.success);
        assertEquals("disk", proposal.executionResult.errorMessage);
        assertTrue(manager.getGCHistory(10).isEmpty());
    }

    @Test
    public void testExecuteGCRejectsMissingAndUnapprovedProposals() throws IOException {
        GCProposalManager manager = newManager();

        try {
            manager.executeGC("missing-proposal", 1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("GC proposal not found"));
        }

        GCProposal proposal = manager.applyReplicatedProposal("gc-proposal-8", "0xwallet", null, 10L, "1.25", 1_000L);
        try {
            manager.executeGC(proposal.proposalId, 1);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("GC proposal not approved"));
        }
    }

    @Test
    public void testProposeGCIncludesEstimateAndFragmentationOverhead() throws IOException {
        GCCostEstimator estimator = mock(GCCostEstimator.class);
        FragmentationTracker fragmentationTracker = new FragmentationTracker();
        fragmentationTracker.recordWrite("0xfragmented", "data00001a.tar", 1024L);
        when(estimator.estimateCost("rev-1")).thenReturn(new GCCostEstimate(
            5L,
            512L * 1024 * 1024,
            10L,
            1024L * 1024 * 1024,
            new BigDecimal("12.50"),
            Collections.singletonMap("data00001a.tar", 512L * 1024 * 1024)
        ));

        GCProposalManager manager = newManager(null, estimator, null, 3, () -> true, fragmentationTracker);

        GCProposal proposal = manager.proposeGC("0xwallet", "rev-1");

        assertNotNull(proposal.proposalId);
        assertEquals("0xwallet", proposal.proposerWallet);
        assertEquals("rev-1", proposal.targetRevision);
        assertEquals(512L, proposal.estimatedReclaimableSizeMB);
        assertEquals(new BigDecimal("12.50"), proposal.estimatedCostUSDC);
        assertTrue(proposal.fragmentationOverheadMB > 0L);
        assertTrue(proposal.fragmentationCostUSDC.compareTo(BigDecimal.ZERO) > 0);
        assertEquals(GCProposal.GCProposalState.PENDING, proposal.state);
    }

    @Test
    public void testPendingProposalsAndHistoryAreSortedAndFiltered() throws IOException {
        FileStore fileStore = mock(FileStore.class);
        GCProposalManager manager = newManager(fileStore, null, null, 3, () -> true);

        GCProposal newestPending = manager.applyReplicatedProposal("gc-proposal-9", "0xwallet", null, 10L, "1.25", 1_000L);
        newestPending.createdAt = 30L;
        GCProposal approved = manager.applyReplicatedProposal("gc-proposal-10", "0xwallet", null, 10L, "1.25", 1_000L);
        approved.createdAt = 20L;
        approved.state = GCProposal.GCProposalState.APPROVED;
        GCProposal voting = manager.applyReplicatedProposal("gc-proposal-11", "0xwallet", null, 10L, "1.25", 1_000L);
        voting.createdAt = 10L;
        voting.state = GCProposal.GCProposalState.VOTING;
        GCProposal rejected = manager.applyReplicatedProposal("gc-proposal-12", "0xwallet", null, 10L, "1.25", 1_000L);
        rejected.state = GCProposal.GCProposalState.REJECTED;

        List<GCProposal> pending = manager.getPendingProposals();

        assertEquals(3, pending.size());
        assertEquals("gc-proposal-9", pending.get(0).proposalId);
        assertEquals("gc-proposal-10", pending.get(1).proposalId);
        assertEquals("gc-proposal-11", pending.get(2).proposalId);

        GCProposal completedOne = manager.applyReplicatedProposal("gc-proposal-13", "0xwallet", null, 10L, "1.25", 1_000L);
        completedOne.state = GCProposal.GCProposalState.APPROVED;
        GCExecutionResult resultOne = manager.executeGC(completedOne.proposalId, 1);
        resultOne.timestamp = 10L;

        GCProposal completedTwo = manager.applyReplicatedProposal("gc-proposal-14", "0xwallet", null, 10L, "1.25", 1_000L);
        completedTwo.state = GCProposal.GCProposalState.APPROVED;
        GCExecutionResult resultTwo = manager.executeGC(completedTwo.proposalId, 2);
        resultTwo.timestamp = 20L;

        List<GCExecutionResult> history = manager.getGCHistory(5);

        assertEquals(2, history.size());
        assertEquals("gc-proposal-14", history.get(0).proposalId);
        assertEquals("gc-proposal-13", history.get(1).proposalId);
        assertEquals(1, manager.getGCHistory(1).size());
        assertEquals("gc-proposal-14", manager.getGCHistory(1).get(0).proposalId);
    }

    private GCProposalManager newManager() {
        return newManager(null, null, null, 3, () -> true, new FragmentationTracker());
    }

    private GCProposalManager newManager(FileStore fileStore,
                                         GCCostEstimator estimator,
                                         EvmBridge evmBridge,
                                         int totalValidators,
                                         java.util.function.Supplier<Boolean> isLeaderSupplier) {
        return newManager(fileStore, estimator, evmBridge, totalValidators, isLeaderSupplier, new FragmentationTracker());
    }

    private GCProposalManager newManager(FileStore fileStore,
                                         GCCostEstimator estimator,
                                         EvmBridge evmBridge,
                                         int totalValidators,
                                         java.util.function.Supplier<Boolean> isLeaderSupplier,
                                         FragmentationTracker fragmentationTracker) {
        GCProposalManager manager = new GCProposalManager(
            fileStore != null ? fileStore : mock(FileStore.class),
            estimator != null ? estimator : mock(GCCostEstimator.class),
            fragmentationTracker,
            evmBridge,
            totalValidators,
            () -> 7,
            isLeaderSupplier
        );
        managers.add(manager);
        return manager;
    }
}
