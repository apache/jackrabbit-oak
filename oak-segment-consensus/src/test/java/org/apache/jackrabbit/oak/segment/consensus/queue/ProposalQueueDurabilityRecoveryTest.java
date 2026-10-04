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
package org.apache.jackrabbit.oak.segment.consensus.queue;

import org.apache.jackrabbit.oak.segment.consensus.economics.ValidatorEarningsTracker;
import org.apache.jackrabbit.oak.segment.consensus.eth.BeaconChainClient;
import org.apache.jackrabbit.oak.segment.consensus.evm.EvmBridge;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.List;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Recovery of processed proposals whose durability ack is still pending.
 */
public class ProposalQueueDurabilityRecoveryTest {

    private static final String WALLET = "0x742d35cc6634c0532925a3b844bc9e7595f0beb0";
    private static final long HOUR_MS = 3_600_000L;

    private RaftAppendCallback raftAppendCallback;
    private ProposalQueueManagerOptimized queueManager;
    private QueuedProposal proposal;

    @Before
    public void setUp() {
        queueManager = new ProposalQueueManagerOptimized(
            mock(EvmBridge.class),
            raftAppendCallback = mock(RaftAppendCallback.class),
            new BackpressureManager(),
            mock(BeaconChainClient.class)
        );
        proposal = queueManager.queueProposal(
            "0xp-durability",
            "0xtx-durability",
            WALLET,
            "/oak-chain/74/2d/35/" + WALLET + "/content/page-1",
            "page",
            "payload",
            "0xsig",
            ValidatorEarningsTracker.PaymentTier.STANDARD,
            null
        );
        proposal.setState(ProposalState.PROCESSED);
    }

    @After
    public void tearDown() {
        queueManager.stop();
    }

    @Test
    public void ackAfterReplayEndsRecoveryWithoutRejection() throws Exception {
        long now = System.currentTimeMillis() + HOUR_MS;
        sweep(now);
        assertEquals(ProposalState.VERIFIED, proposal.getState());
        assertEquals(1, proposal.getRetryCount());

        queueManager.updateDurability(proposal.getProposalId(), DurabilityState.ACKED, "h1", null);
        proposal.setState(ProposalState.PROCESSED);
        sweep(now + HOUR_MS);

        assertEquals(ProposalState.PROCESSED, proposal.getState());
        assertEquals(DurabilityState.ACKED, proposal.getDurabilityState());
        assertEquals(1, proposal.getRetryCount());
    }

    @Test
    public void exhaustedRetriesKeepProcessedAndFailDurability() throws Exception {
        for (int i = 0; i < maxRetryCount(); i++) {
            proposal.incrementRetryCount();
        }

        sweep(System.currentTimeMillis() + HOUR_MS);

        assertProcessedWithFailedDurability("Exceeded max retry count");
    }

    @Test
    public void missingPayloadSidecarKeepsProcessedAndFailsDurability() throws Exception {
        proposal.setPayloadRef("missing-sidecar");

        sweep(System.currentTimeMillis() + HOUR_MS);

        assertProcessedWithFailedDurability("payload sidecar missing");
    }

    @Test
    public void exhaustedSendFailuresOfReplayedProposalKeepProcessedAndFailDurability() throws Exception {
        sweep(System.currentTimeMillis() + HOUR_MS);
        assertEquals(ProposalState.VERIFIED, proposal.getState());

        failSendsUntilRetriesExhausted();

        assertProcessedWithFailedDurability("Exceeded max retry count");
    }

    @Test
    public void exhaustedSendFailuresOfNeverSentProposalStillReject() throws Exception {
        proposal.setState(ProposalState.VERIFIED);

        failSendsUntilRetriesExhausted();

        assertEquals(ProposalState.REJECTED, proposal.getState());
        assertTrue(proposal.getRejectionReason().contains("Aeron send failures"));
    }

    @Test
    public void appendedToLogMarkerSurvivesPersistence() throws Exception {
        sweep(System.currentTimeMillis() + HOUR_MS);
        ProposalPersistenceStore store =
            new ProposalPersistenceStore(Files.createTempDirectory("proposal-appended-marker"));
        QueuedProposal neverSent = new QueuedProposal(
            "0xp-never-sent", "0xtx", null, 1L, 2L, ProposalState.VERIFIED);

        store.save(List.of(proposal, neverSent));
        List<QueuedProposal> restored = store.load();

        assertTrue(restored.get(0).isAppendedToLog());
        assertFalse(restored.get(1).isAppendedToLog());
    }

    private void failSendsUntilRetriesExhausted() throws Exception {
        when(raftAppendCallback.tryAppendProposalWithId(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any()
        )).thenThrow(new IllegalStateException("ingress down"));
        while (proposal.getRetryCount() < maxRetryCount()) {
            proposal.incrementRetryCount();
        }
        Field running = ProposalQueueManagerOptimized.class.getDeclaredField("running");
        running.setAccessible(true);
        running.setBoolean(queueManager, true);
        Field batchQueueField = ProposalQueueManagerOptimized.class.getDeclaredField("batchQueue");
        batchQueueField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Queue<List<QueuedProposal>> batchQueue = (Queue<List<QueuedProposal>>) batchQueueField.get(queueManager);
        batchQueue.offer(List.of(proposal));

        Class<?> senderClass = Class.forName(ProposalQueueManagerOptimized.class.getName() + "$AeronSenderAgent");
        Constructor<?> constructor = senderClass.getDeclaredConstructor(ProposalQueueManagerOptimized.class);
        constructor.setAccessible(true);
        org.agrona.concurrent.Agent sender = (org.agrona.concurrent.Agent) constructor.newInstance(queueManager);
        sender.doWork();
        running.setBoolean(queueManager, false);
    }

    private void assertProcessedWithFailedDurability(String expectedError) {
        ProposalStatus status = queueManager.getProposalStatus(proposal.getProposalId());
        assertEquals(ProposalState.PROCESSED, status.getState());
        assertEquals(DurabilityState.FAILED, status.getDurabilityState());
        assertTrue(status.getDurabilityError(), status.getDurabilityError().contains(expectedError));
        assertNull(status.getRejectionReason());
    }

    private void sweep(long nowMs) throws Exception {
        Method method = ProposalQueueManagerOptimized.class
            .getDeclaredMethod("recoverStaleProcessedProposals", long.class);
        method.setAccessible(true);
        method.invoke(queueManager, nowMs);
    }

    private int maxRetryCount() throws Exception {
        Field field = ProposalQueueManagerOptimized.class.getDeclaredField("maxRetryCount");
        field.setAccessible(true);
        return field.getInt(queueManager);
    }
}
