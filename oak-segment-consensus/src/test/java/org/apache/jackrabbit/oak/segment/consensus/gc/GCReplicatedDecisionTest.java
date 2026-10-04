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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.segment.consensus.evm.EvmBridge;
import org.apache.jackrabbit.oak.segment.consensus.evm.impl.SimplePaymentProof;
import org.apache.jackrabbit.oak.segment.consensus.fragmentation.FragmentationTracker;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * GC proposal, vote and execute state changes only from log entries: cluster timestamps decide expiry, the
 * configured membership decides quorum, and applying GC_EXECUTE never blocks the service thread.
 */
public class GCReplicatedDecisionTest {

    private static final long T0 = 1_000L;

    private final List<GCProposalManager> managers = new ArrayList<>();

    @After
    public void tearDown() {
        managers.forEach(GCProposalManager::shutdown);
    }

    @Test
    public void membersAndReplayReachIdenticalTallyAndExpiryDecisions() {
        List<String> member0 = applyLog(newFollower());
        List<String> member1 = applyLog(newFollower());
        List<String> replay = applyLog(newFollower());

        assertEquals(Arrays.asList(
            "gc-a:APPROVED:votes=[0, 1, 2]:expiresAt=" + (T0 + GCProposal.DEFAULT_TTL_MS),
            "gc-b:VOTING:votes=[0]:expiresAt=" + (T0 + GCProposal.DEFAULT_TTL_MS)), member0);
        assertEquals(member0, member1);
        assertEquals(member0, replay);
    }

    @Test
    public void quorumComesFromTheConfiguredClusterMembership() {
        GCProposalManager manager = newFollower();
        manager.setClusterMembership(() -> 5);
        manager.applyReplicatedProposal("gc-q", "0xw", null, 1L, "1", T0);
        for (int member = 0; member < 3; member++) {
            manager.voteOnProposal("gc-q", member, true, "", T0 + 1);
        }
        assertEquals("3 of 5 is below the 2/3+ quorum of 4", GCProposal.GCProposalState.VOTING,
            manager.getProposal("gc-q").state);
        manager.voteOnProposal("gc-q", 3, true, "", T0 + 1);
        assertEquals(GCProposal.GCProposalState.APPROVED, manager.getProposal("gc-q").state);
    }

    @Test
    public void applyingGcExecuteNeverRunsPaymentOrCleanupOnTheServiceThread() throws Exception {
        Thread serviceThread = Thread.currentThread();
        List<Thread> cleanupThreads = new CopyOnWriteArrayList<>();
        FileStore fileStore = mock(FileStore.class);
        doAnswer(invocation -> {
            failIfOn(serviceThread, "fileStore.cleanup()");
            cleanupThreads.add(Thread.currentThread());
            return null;
        }).when(fileStore).cleanup();
        EvmBridge evmBridge = mock(EvmBridge.class);
        when(evmBridge.verifyPayment(anyString())).thenAnswer(invocation -> {
            failIfOn(serviceThread, "evmBridge.verifyPayment()");
            return new SimplePaymentProof("0xtx", 1L, "0xw", "0xc", invocation.getArgument(0), "1", 99);
        });
        GCProposalManager manager = newManager(fileStore, evmBridge, true);
        List<String> requested = new CopyOnWriteArrayList<>();
        manager.setExecutionRequester(proposalId -> requested.add(proposalId));

        manager.applyReplicatedProposal("gc-x", "0xw", null, 1L, "1", T0);
        for (int member = 0; member < 3; member++) {
            manager.voteOnProposal("gc-x", member, true, "", T0 + 1);
        }
        awaitTrue(() -> requested.contains("gc-x"));

        assertTrue(manager.applyReplicatedExecute("gc-x", 0));
        assertFalse("a duplicate GC_EXECUTE is ignored", manager.applyReplicatedExecute("gc-x", 1));
        awaitTrue(() -> manager.getProposal("gc-x").state == GCProposal.GCProposalState.COMPLETED);

        assertEquals(1, cleanupThreads.size());
        assertNotSame(serviceThread, cleanupThreads.get(0));
    }

    private static List<String> applyLog(GCProposalManager manager) {
        manager.applyReplicatedProposal("gc-a", "0xw", null, 10L, "1.00", T0);
        manager.applyReplicatedProposal("gc-b", "0xw", null, 10L, "1.00", T0);
        manager.applyReplicatedProposal("gc-a", "0xother", null, 99L, "9.00", T0 + 5);   // duplicate entry
        manager.voteOnProposal("gc-a", 0, true, "", T0 + 10);
        manager.voteOnProposal("gc-a", 0, true, "", T0 + 11);                           // duplicate vote
        manager.voteOnProposal("gc-a", 7, true, "", T0 + 12);                           // not a member
        manager.voteOnProposal("gc-a", 1, true, "", T0 + 13);
        manager.voteOnProposal("gc-a", 2, true, "", T0 + GCProposal.DEFAULT_TTL_MS);    // last valid instant
        manager.voteOnProposal("gc-b", 0, true, "", T0 + 20);
        manager.voteOnProposal("gc-b", 1, true, "", T0 + GCProposal.DEFAULT_TTL_MS + 1); // expired
        manager.voteOnProposal("gc-b", 2, true, "", T0 + GCProposal.DEFAULT_TTL_MS + 2); // expired
        List<String> state = new ArrayList<>();
        for (String id : Arrays.asList("gc-a", "gc-b")) {
            GCProposal proposal = manager.getProposal(id);
            state.add(id + ":" + proposal.state + ":votes=" + new java.util.TreeSet<>(proposal.votes.keySet())
                + ":expiresAt=" + proposal.expiresAt);
        }
        return state;
    }

    private GCProposalManager newFollower() {
        return newManager(mock(FileStore.class), null, false);
    }

    private GCProposalManager newManager(FileStore fileStore, EvmBridge evmBridge, boolean leader) {
        GCProposalManager manager = new GCProposalManager(fileStore, mock(GCCostEstimator.class),
            new FragmentationTracker(), evmBridge, 3, () -> 0, () -> leader);
        managers.add(manager);
        return manager;
    }

    private static void failIfOn(Thread serviceThread, String call) {
        if (Thread.currentThread() == serviceThread) {
            throw new AssertionError(call + " ran on the service thread");
        }
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            assertTrue("condition not reached in time", System.nanoTime() < deadline);
            Thread.sleep(10L);
        }
    }
}
