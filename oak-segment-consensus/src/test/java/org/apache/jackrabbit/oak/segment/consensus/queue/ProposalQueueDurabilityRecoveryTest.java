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

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Recovery of processed proposals whose durability ack is still pending.
 */
public class ProposalQueueDurabilityRecoveryTest {

    private static final String WALLET = "0x742d35cc6634c0532925a3b844bc9e7595f0beb0";
    private static final long HOUR_MS = 3_600_000L;

    private ProposalQueueManagerOptimized queueManager;
    private QueuedProposal proposal;

    @Before
    public void setUp() {
        queueManager = new ProposalQueueManagerOptimized(
            mock(EvmBridge.class),
            mock(RaftAppendCallback.class),
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
