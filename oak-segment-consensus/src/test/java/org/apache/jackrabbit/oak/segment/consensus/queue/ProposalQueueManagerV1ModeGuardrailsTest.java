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

import org.apache.jackrabbit.oak.segment.consensus.eth.BeaconChainClient;
import org.apache.jackrabbit.oak.segment.consensus.economics.ValidatorEarningsTracker;
import org.apache.jackrabbit.oak.segment.consensus.evm.EvmBridge;
import org.apache.jackrabbit.oak.segment.consensus.evm.PaymentProof;
import org.apache.jackrabbit.oak.segment.consensus.evm.impl.SimplePaymentProof;
import org.apache.jackrabbit.oak.segment.consensus.util.WalletPathUtil;
import org.junit.After;
import org.junit.Test;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.Sign;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class ProposalQueueManagerV1ModeGuardrailsTest {

    @After
    public void tearDown() {
        System.clearProperty("oak.blockchain.mode");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();
    }

    @Test
    public void testVerifierRejectsNonMockProposalIdThatIsNotBytes32() throws Exception {
        System.setProperty("oak.blockchain.mode", "sepolia");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();

        EvmBridge evmBridge = mock(EvmBridge.class);
        when(evmBridge.getContractAddress()).thenReturn("0x1111111111111111111111111111111111111111");
        when(evmBridge.getCurrentBlockNumber()).thenReturn(123L);

        BeaconChainClient beaconClient = mock(BeaconChainClient.class);
        when(beaconClient.getCachedCurrentEpoch()).thenReturn(10L);
        when(beaconClient.getCachedFinalizedEpoch()).thenReturn(8L);

        ProposalQueueManagerOptimized queueManager = new ProposalQueueManagerOptimized(
            evmBridge,
            new NoopRaftAppendCallback(),
            new BackpressureManager(),
            beaconClient
        );
        queueManager.start();
        try {
            queueManager.queueProposal(
                "123e4567-e89b-12d3-a456-426614174000",
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0x1234567890abcdef1234567890abcdef12345678",
                "/oak-chain/12/34/56/0x1234567890abcdef1234567890abcdef12345678/content/page-1",
                "page",
                "message",
                "",
                ValidatorEarningsTracker.PaymentTier.STANDARD,
                null
            );

            assertTrue(waitForCondition(() -> rejectedCount(queueManager) == 1L, 5_000L));
            verifyNoInteractions(evmBridge);
        } finally {
            queueManager.stop();
        }
    }

    @Test
    public void testVerifierRejectsMismatchedDeclaredTransactionHashInChainBackedMode() throws Exception {
        System.setProperty("oak.blockchain.mode", "sepolia");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();

        String proposalId = "0x1111111111111111111111111111111111111111111111111111111111111111";
        String declaredTxHash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        String confirmedTxHash = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        String walletAddress = "0x1234567890abcdef1234567890abcdef12345678";

        EvmBridge evmBridge = mock(EvmBridge.class);
        when(evmBridge.getContractAddress()).thenReturn("0x1111111111111111111111111111111111111111");
        when(evmBridge.getCurrentBlockNumber()).thenReturn(123L);
        when(evmBridge.verifyPayment(proposalId)).thenReturn(new SimplePaymentProof(
            confirmedTxHash,
            123L,
            walletAddress,
            "0x1111111111111111111111111111111111111111",
            proposalId,
            "1",
            ValidatorEarningsTracker.PaymentTier.STANDARD,
            12
        ));

        BeaconChainClient beaconClient = mock(BeaconChainClient.class);
        when(beaconClient.getCachedCurrentEpoch()).thenReturn(10L);
        when(beaconClient.getCachedFinalizedEpoch()).thenReturn(8L);

        ProposalQueueManagerOptimized queueManager = new ProposalQueueManagerOptimized(
            evmBridge,
            new NoopRaftAppendCallback(),
            new BackpressureManager(),
            beaconClient
        );
        queueManager.start();
        try {
            queueManager.queueProposal(
                proposalId,
                declaredTxHash,
                walletAddress,
                "/oak-chain/12/34/56/0x1234567890abcdef1234567890abcdef12345678/content/page-2",
                "page",
                "message",
                "",
                ValidatorEarningsTracker.PaymentTier.STANDARD,
                null
            );

            assertTrue(waitForCondition(() -> rejectedCount(queueManager) == 1L, 5_000L));
            assertEquals(1L, rejectedCount(queueManager));
            ProposalStatus status = queueManager.getProposalStatus(proposalId);
            assertNotNull(status);
            assertEquals(ProposalState.REJECTED, status.getState());
            assertTrue(status.getRejectionReason().contains("does not match declared ethereumTxHash"));
        } finally {
            queueManager.stop();
        }
    }

    @Test
    public void testVerifierRejectsDeleteWhenProofKindIsWriteInChainBackedMode() throws Exception {
        System.setProperty("oak.blockchain.mode", "sepolia");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();

        String proposalId = "0x2222222222222222222222222222222222222222222222222222222222222222";
        String declaredTxHash = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        String walletAddress = "0x1234567890abcdef1234567890abcdef12345678";

        EvmBridge evmBridge = mock(EvmBridge.class);
        when(evmBridge.getContractAddress()).thenReturn("0x1111111111111111111111111111111111111111");
        when(evmBridge.getCurrentBlockNumber()).thenReturn(123L);
        when(evmBridge.verifyPayment(proposalId)).thenReturn(new SimplePaymentProof(
            declaredTxHash,
            123L,
            walletAddress,
            "0x1111111111111111111111111111111111111111",
            proposalId,
            "1",
            ValidatorEarningsTracker.PaymentTier.STANDARD,
            PaymentProof.ProposalKind.WRITE,
            PaymentProof.PaymentToken.ETH,
            0,
            12
        ));

        BeaconChainClient beaconClient = mock(BeaconChainClient.class);
        when(beaconClient.getCachedCurrentEpoch()).thenReturn(10L);
        when(beaconClient.getCachedFinalizedEpoch()).thenReturn(8L);

        ProposalQueueManagerOptimized queueManager = new ProposalQueueManagerOptimized(
            evmBridge,
            new NoopRaftAppendCallback(),
            new BackpressureManager(),
            beaconClient
        );
        queueManager.start();
        try {
            queueManager.queueDeleteProposal(
                proposalId,
                declaredTxHash,
                walletAddress,
                "/oak-chain/12/34/56/0x1234567890abcdef1234567890abcdef12345678/content/page-delete",
                "",
                ValidatorEarningsTracker.PaymentTier.STANDARD
            );

            assertTrue(waitForCondition(() -> rejectedCount(queueManager) == 1L, 5_000L));
            ProposalStatus status = queueManager.getProposalStatus(proposalId);
            assertNotNull(status);
            assertEquals(ProposalState.REJECTED, status.getState());
            assertTrue(status.getRejectionReason().contains("proposal kind does not match"));
        } finally {
            queueManager.stop();
        }
    }

    @Test
    public void testVerifierRejectsValidatorHostedBinaryWriteWithoutCapabilityInChainBackedMode() throws Exception {
        System.setProperty("oak.blockchain.mode", "sepolia");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();

        String proposalId = "0x3333333333333333333333333333333333333333333333333333333333333333";
        String declaredTxHash = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        String walletAddress = "0x1234567890abcdef1234567890abcdef12345678";

        EvmBridge evmBridge = mock(EvmBridge.class);
        when(evmBridge.getContractAddress()).thenReturn("0x1111111111111111111111111111111111111111");
        when(evmBridge.getCurrentBlockNumber()).thenReturn(123L);
        when(evmBridge.verifyPayment(proposalId)).thenReturn(new SimplePaymentProof(
            declaredTxHash,
            123L,
            walletAddress,
            "0x1111111111111111111111111111111111111111",
            proposalId,
            "1",
            ValidatorEarningsTracker.PaymentTier.STANDARD,
            PaymentProof.ProposalKind.WRITE,
            PaymentProof.PaymentToken.ETH,
            0,
            12
        ));

        BeaconChainClient beaconClient = mock(BeaconChainClient.class);
        when(beaconClient.getCachedCurrentEpoch()).thenReturn(10L);
        when(beaconClient.getCachedFinalizedEpoch()).thenReturn(8L);

        ProposalQueueManagerOptimized queueManager = new ProposalQueueManagerOptimized(
            evmBridge,
            new NoopRaftAppendCallback(),
            new BackpressureManager(),
            beaconClient
        );
        queueManager.start();
        try {
            queueManager.queueProposal(
                proposalId,
                declaredTxHash,
                walletAddress,
                "/oak-chain/12/34/56/0x1234567890abcdef1234567890abcdef12345678/content/page-binary",
                "page",
                "message",
                "",
                ValidatorEarningsTracker.PaymentTier.STANDARD,
                null,
                "blob-1",
                "application/octet-stream",
                null
            );

            assertTrue(waitForCondition(() -> rejectedCount(queueManager) == 1L, 5_000L));
            ProposalStatus status = queueManager.getProposalStatus(proposalId);
            assertNotNull(status);
            assertEquals(ProposalState.REJECTED, status.getState());
            assertTrue(status.getRejectionReason().contains("CAPABILITY_VALIDATOR_HOSTED_BINARY"));
        } finally {
            queueManager.stop();
        }
    }

    @Test
    public void testVerifierRejectsPaymentToWrongContractInChainBackedMode() throws Exception {
        System.setProperty("oak.blockchain.mode", "sepolia");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();

        String proposalId = "0x4444444444444444444444444444444444444444444444444444444444444444";
        String declaredTxHash = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
        String walletAddress = "0x1234567890abcdef1234567890abcdef12345678";

        EvmBridge evmBridge = mock(EvmBridge.class);
        when(evmBridge.getContractAddress()).thenReturn("0x1111111111111111111111111111111111111111");
        when(evmBridge.getCurrentBlockNumber()).thenReturn(123L);
        when(evmBridge.verifyPayment(proposalId)).thenReturn(new SimplePaymentProof(
            declaredTxHash,
            123L,
            walletAddress,
            "0x2222222222222222222222222222222222222222",
            proposalId,
            "1",
            ValidatorEarningsTracker.PaymentTier.STANDARD,
            PaymentProof.ProposalKind.WRITE,
            PaymentProof.PaymentToken.ETH,
            0,
            12
        ));

        BeaconChainClient beaconClient = mock(BeaconChainClient.class);
        when(beaconClient.getCachedCurrentEpoch()).thenReturn(10L);
        when(beaconClient.getCachedFinalizedEpoch()).thenReturn(8L);

        ProposalQueueManagerOptimized queueManager = new ProposalQueueManagerOptimized(
            evmBridge,
            new NoopRaftAppendCallback(),
            new BackpressureManager(),
            beaconClient
        );
        queueManager.start();
        try {
            queueManager.queueProposal(
                proposalId,
                declaredTxHash,
                walletAddress,
                "/oak-chain/12/34/56/0x1234567890abcdef1234567890abcdef12345678/content/page-contract",
                "page",
                "message",
                "",
                ValidatorEarningsTracker.PaymentTier.STANDARD,
                null
            );

            assertTrue(waitForCondition(() -> rejectedCount(queueManager) == 1L, 5_000L));
            ProposalStatus status = queueManager.getProposalStatus(proposalId);
            assertNotNull(status);
            assertEquals(ProposalState.REJECTED, status.getState());
            assertTrue(status.getRejectionReason().contains("Payment to wrong contract"));
        } finally {
            queueManager.stop();
        }
    }

    @Test
    public void testVerifierRejectsProofFromDifferentWalletInChainBackedMode() throws Exception {
        System.setProperty("oak.blockchain.mode", "sepolia");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();

        String proposalId = "0x5555555555555555555555555555555555555555555555555555555555555555";
        String declaredTxHash = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        String walletAddress = "0x1234567890abcdef1234567890abcdef12345678";

        EvmBridge evmBridge = mock(EvmBridge.class);
        when(evmBridge.getContractAddress()).thenReturn("0x1111111111111111111111111111111111111111");
        when(evmBridge.getCurrentBlockNumber()).thenReturn(123L);
        when(evmBridge.verifyPayment(proposalId)).thenReturn(new SimplePaymentProof(
            declaredTxHash,
            123L,
            "0x9999999999999999999999999999999999999999",
            "0x1111111111111111111111111111111111111111",
            proposalId,
            "1",
            ValidatorEarningsTracker.PaymentTier.STANDARD,
            PaymentProof.ProposalKind.WRITE,
            PaymentProof.PaymentToken.ETH,
            0,
            12
        ));

        BeaconChainClient beaconClient = mock(BeaconChainClient.class);
        when(beaconClient.getCachedCurrentEpoch()).thenReturn(10L);
        when(beaconClient.getCachedFinalizedEpoch()).thenReturn(8L);

        ProposalQueueManagerOptimized queueManager = new ProposalQueueManagerOptimized(
            evmBridge,
            new NoopRaftAppendCallback(),
            new BackpressureManager(),
            beaconClient
        );
        queueManager.start();
        try {
            queueManager.queueProposal(
                proposalId,
                declaredTxHash,
                walletAddress,
                "/oak-chain/12/34/56/0x1234567890abcdef1234567890abcdef12345678/content/page-wallet",
                "page",
                "message",
                "",
                ValidatorEarningsTracker.PaymentTier.STANDARD,
                null
            );

            assertTrue(waitForCondition(() -> rejectedCount(queueManager) == 1L, 5_000L));
            ProposalStatus status = queueManager.getProposalStatus(proposalId);
            assertNotNull(status);
            assertEquals(ProposalState.REJECTED, status.getState());
            assertTrue(status.getRejectionReason().contains("does not match proposal wallet"));
        } finally {
            queueManager.stop();
        }
    }

    @Test
    public void testVerifierRejectsNonPositivePaymentAmountInChainBackedMode() throws Exception {
        System.setProperty("oak.blockchain.mode", "sepolia");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();

        String proposalId = "0x6666666666666666666666666666666666666666666666666666666666666666";
        String declaredTxHash = "0x1212121212121212121212121212121212121212121212121212121212121212";
        String walletAddress = "0x1234567890abcdef1234567890abcdef12345678";

        EvmBridge evmBridge = mock(EvmBridge.class);
        when(evmBridge.getContractAddress()).thenReturn("0x1111111111111111111111111111111111111111");
        when(evmBridge.getCurrentBlockNumber()).thenReturn(123L);
        when(evmBridge.verifyPayment(proposalId)).thenReturn(new SimplePaymentProof(
            declaredTxHash,
            123L,
            walletAddress,
            "0x1111111111111111111111111111111111111111",
            proposalId,
            "0",
            ValidatorEarningsTracker.PaymentTier.STANDARD,
            PaymentProof.ProposalKind.WRITE,
            PaymentProof.PaymentToken.ETH,
            0,
            12
        ));

        BeaconChainClient beaconClient = mock(BeaconChainClient.class);
        when(beaconClient.getCachedCurrentEpoch()).thenReturn(10L);
        when(beaconClient.getCachedFinalizedEpoch()).thenReturn(8L);

        ProposalQueueManagerOptimized queueManager = new ProposalQueueManagerOptimized(
            evmBridge,
            new NoopRaftAppendCallback(),
            new BackpressureManager(),
            beaconClient
        );
        queueManager.start();
        try {
            queueManager.queueProposal(
                proposalId,
                declaredTxHash,
                walletAddress,
                "/oak-chain/12/34/56/0x1234567890abcdef1234567890abcdef12345678/content/page-amount-zero",
                "page",
                "message",
                "",
                ValidatorEarningsTracker.PaymentTier.STANDARD,
                null
            );

            assertTrue(waitForCondition(() -> rejectedCount(queueManager) == 1L, 5_000L));
            ProposalStatus status = queueManager.getProposalStatus(proposalId);
            assertNotNull(status);
            assertEquals(ProposalState.REJECTED, status.getState());
            assertTrue(status.getRejectionReason().contains("Insufficient payment amount"));
        } finally {
            queueManager.stop();
        }
    }

    @Test
    public void testVerifierRejectsInvalidPaymentAmountFormatInChainBackedMode() throws Exception {
        System.setProperty("oak.blockchain.mode", "sepolia");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();

        String proposalId = "0x7777777777777777777777777777777777777777777777777777777777777777";
        String declaredTxHash = "0x3434343434343434343434343434343434343434343434343434343434343434";
        String walletAddress = "0x1234567890abcdef1234567890abcdef12345678";

        EvmBridge evmBridge = mock(EvmBridge.class);
        when(evmBridge.getContractAddress()).thenReturn("0x1111111111111111111111111111111111111111");
        when(evmBridge.getCurrentBlockNumber()).thenReturn(123L);
        when(evmBridge.verifyPayment(proposalId)).thenReturn(new SimplePaymentProof(
            declaredTxHash,
            123L,
            walletAddress,
            "0x1111111111111111111111111111111111111111",
            proposalId,
            "not-a-number",
            ValidatorEarningsTracker.PaymentTier.STANDARD,
            PaymentProof.ProposalKind.WRITE,
            PaymentProof.PaymentToken.ETH,
            0,
            12
        ));

        BeaconChainClient beaconClient = mock(BeaconChainClient.class);
        when(beaconClient.getCachedCurrentEpoch()).thenReturn(10L);
        when(beaconClient.getCachedFinalizedEpoch()).thenReturn(8L);

        ProposalQueueManagerOptimized queueManager = new ProposalQueueManagerOptimized(
            evmBridge,
            new NoopRaftAppendCallback(),
            new BackpressureManager(),
            beaconClient
        );
        queueManager.start();
        try {
            queueManager.queueProposal(
                proposalId,
                declaredTxHash,
                walletAddress,
                "/oak-chain/12/34/56/0x1234567890abcdef1234567890abcdef12345678/content/page-amount-bad",
                "page",
                "message",
                "",
                ValidatorEarningsTracker.PaymentTier.STANDARD,
                null
            );

            assertTrue(waitForCondition(() -> rejectedCount(queueManager) == 1L, 5_000L));
            ProposalStatus status = queueManager.getProposalStatus(proposalId);
            assertNotNull(status);
            assertEquals(ProposalState.REJECTED, status.getState());
            assertTrue(status.getRejectionReason().contains("Invalid payment amount format"));
        } finally {
            queueManager.stop();
        }
    }

    @Test
    public void testVerifierRejectsWhenFullSignatureVerificationUnavailableInChainBackedMode() throws Exception {
        System.setProperty("oak.blockchain.mode", "sepolia");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();

        String proposalId = "0x8888888888888888888888888888888888888888888888888888888888888888";
        String declaredTxHash = "0x5656565656565656565656565656565656565656565656565656565656565656";
        String walletAddress = "0x1234567890abcdef1234567890abcdef12345678";

        EvmBridge evmBridge = mock(EvmBridge.class);
        when(evmBridge.getContractAddress()).thenReturn("0x1111111111111111111111111111111111111111");
        when(evmBridge.getCurrentBlockNumber()).thenReturn(123L);
        when(evmBridge.verifyPayment(proposalId)).thenReturn(new SimplePaymentProof(
            declaredTxHash,
            123L,
            walletAddress,
            "0x1111111111111111111111111111111111111111",
            proposalId,
            "1",
            ValidatorEarningsTracker.PaymentTier.STANDARD,
            PaymentProof.ProposalKind.WRITE,
            PaymentProof.PaymentToken.ETH,
            0,
            12
        ));

        BeaconChainClient beaconClient = mock(BeaconChainClient.class);
        when(beaconClient.getCachedCurrentEpoch()).thenReturn(10L);
        when(beaconClient.getCachedFinalizedEpoch()).thenReturn(8L);

        ProposalQueueManagerOptimized queueManager = new ProposalQueueManagerOptimized(
            evmBridge,
            new NoopRaftAppendCallback(),
            new BackpressureManager(),
            beaconClient
        );
        queueManager.start();
        try {
            withForcedSignatureVerifierUnavailable("simulated verifier outage", () -> {
                queueManager.queueProposal(
                    proposalId,
                    declaredTxHash,
                    walletAddress,
                    "/oak-chain/12/34/56/0x1234567890abcdef1234567890abcdef12345678/content/page-signature-unavailable",
                    "page",
                    "message",
                    "0x" + "1".repeat(130),
                    ValidatorEarningsTracker.PaymentTier.STANDARD,
                    null
                );
                assertTrue(waitForCondition(() -> rejectedCount(queueManager) == 1L, 5_000L));
                ProposalStatus status = queueManager.getProposalStatus(proposalId);
                assertNotNull(status);
                assertEquals(ProposalState.REJECTED, status.getState());
                assertTrue(status.getRejectionReason().contains("Full Ethereum signature verification unavailable"));
                assertTrue(status.getRejectionReason().contains("simulated verifier outage"));
            });
        } finally {
            queueManager.stop();
        }
    }

    @Test
    public void testVerifierAcceptsDeleteSignedOverContentPathInChainBackedMode() throws Exception {
        String proposalId = "0x9999999999999999999999999999999999999999999999999999999999999991";
        String contentPath = shardPath("content/page-delete-signed");
        assertSignatureCheckpointPasses(proposalId, PaymentProof.ProposalKind.DELETE, queueManager ->
            queueManager.queueDeleteProposal(proposalId, SIGNED_TX_HASH, signerWallet(), contentPath,
                personalSign(contentPath), ValidatorEarningsTracker.PaymentTier.STANDARD));
    }

    @Test
    public void testVerifierRejectsDeleteSignedOverEmptyMessageInChainBackedMode() throws Exception {
        String proposalId = "0x9999999999999999999999999999999999999999999999999999999999999992";
        String contentPath = shardPath("content/page-delete-empty");
        assertSignatureCheckpointRejects(proposalId, PaymentProof.ProposalKind.DELETE, queueManager ->
            queueManager.queueDeleteProposal(proposalId, SIGNED_TX_HASH, signerWallet(), contentPath,
                personalSign(""), ValidatorEarningsTracker.PaymentTier.STANDARD));
    }

    @Test
    public void testVerifierRejectsDeleteSignedOverDifferentPathInChainBackedMode() throws Exception {
        String proposalId = "0x9999999999999999999999999999999999999999999999999999999999999993";
        String contentPath = shardPath("content/page-delete-target");
        assertSignatureCheckpointRejects(proposalId, PaymentProof.ProposalKind.DELETE, queueManager ->
            queueManager.queueDeleteProposal(proposalId, SIGNED_TX_HASH, signerWallet(), contentPath,
                personalSign(shardPath("content/page-delete-other")), ValidatorEarningsTracker.PaymentTier.STANDARD));
    }

    @Test
    public void testVerifierAcceptsWriteSignedOverMessageInChainBackedMode() throws Exception {
        String proposalId = "0x9999999999999999999999999999999999999999999999999999999999999994";
        assertSignatureCheckpointPasses(proposalId, PaymentProof.ProposalKind.WRITE, queueManager ->
            queueManager.queueProposal(proposalId, SIGNED_TX_HASH, signerWallet(), shardPath("content/page-write-signed"),
                "page", "signed message", personalSign("signed message"),
                ValidatorEarningsTracker.PaymentTier.STANDARD, null));
    }

    private static final String SIGNER_PRIVATE_KEY = "4c0883a6910395bda8e1ab1b5f9f1cc0aa1f4b3f8718abf3483c796f9649b7fd";
    private static final String SIGNED_TX_HASH = "0x7878787878787878787878787878787878787878787878787878787878787878";

    private static String signerWallet() {
        return Credentials.create(SIGNER_PRIVATE_KEY).getAddress();
    }

    private static String shardPath(String relativePath) {
        return WalletPathUtil.getShardRoot(signerWallet()) + "/" + relativePath;
    }

    private static String personalSign(String message) {
        Sign.SignatureData signatureData = Sign.signPrefixedMessage(
            message.getBytes(StandardCharsets.UTF_8),
            Credentials.create(SIGNER_PRIVATE_KEY).getEcKeyPair()
        );
        StringBuilder builder = new StringBuilder("0x");
        for (byte[] part : new byte[][] {signatureData.getR(), signatureData.getS(), signatureData.getV()}) {
            for (byte b : part) {
                builder.append(String.format("%02x", b & 0xff));
            }
        }
        return builder.toString();
    }

    private static void assertSignatureCheckpointPasses(String proposalId, PaymentProof.ProposalKind kind,
                                                        QueueAction enqueue) throws Exception {
        runChainBackedVerification(proposalId, kind, queueManager -> {
            enqueue.run(queueManager);
            assertTrue(waitForCondition(() -> verifierSuccessCount(queueManager) == 1L, 5_000L));
            assertEquals(0L, rejectedCount(queueManager));
        });
    }

    private static void assertSignatureCheckpointRejects(String proposalId, PaymentProof.ProposalKind kind,
                                                         QueueAction enqueue) throws Exception {
        runChainBackedVerification(proposalId, kind, queueManager -> {
            enqueue.run(queueManager);
            assertTrue(waitForCondition(() -> rejectedCount(queueManager) == 1L, 5_000L));
            ProposalStatus status = queueManager.getProposalStatus(proposalId);
            assertNotNull(status);
            assertEquals(ProposalState.REJECTED, status.getState());
            assertTrue(status.getRejectionReason().contains("Cryptographic signature verification failed"));
            assertEquals(0L, verifierSuccessCount(queueManager));
        });
    }

    private static void runChainBackedVerification(String proposalId, PaymentProof.ProposalKind kind,
                                                   QueueAction scenario) throws Exception {
        System.setProperty("oak.blockchain.mode", "sepolia");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();

        EvmBridge evmBridge = mock(EvmBridge.class);
        when(evmBridge.getContractAddress()).thenReturn("0x1111111111111111111111111111111111111111");
        when(evmBridge.getCurrentBlockNumber()).thenReturn(123L);
        when(evmBridge.verifyPayment(proposalId)).thenReturn(new SimplePaymentProof(
            SIGNED_TX_HASH,
            123L,
            signerWallet(),
            "0x1111111111111111111111111111111111111111",
            proposalId,
            "1",
            ValidatorEarningsTracker.PaymentTier.STANDARD,
            kind,
            PaymentProof.PaymentToken.ETH,
            0,
            12
        ));

        BeaconChainClient beaconClient = mock(BeaconChainClient.class);
        when(beaconClient.getCachedCurrentEpoch()).thenReturn(10L);
        when(beaconClient.getCachedFinalizedEpoch()).thenReturn(8L);

        ProposalQueueManagerOptimized queueManager = new ProposalQueueManagerOptimized(
            evmBridge,
            new NoopRaftAppendCallback(),
            new BackpressureManager(),
            beaconClient
        );
        queueManager.start();
        try {
            scenario.run(queueManager);
        } finally {
            queueManager.stop();
        }
    }

    private static long verifierSuccessCount(ProposalQueueManagerOptimized queueManager) {
        Object value = queueManager.getQueueStats().get("verifierSuccessCount");
        return value instanceof Number ? ((Number) value).longValue() : 0L;
    }

    @FunctionalInterface
    private interface QueueAction {
        void run(ProposalQueueManagerOptimized queueManager) throws Exception;
    }

    private static long rejectedCount(ProposalQueueManagerOptimized queueManager) {
        Map<String, Object> stats = queueManager.getQueueStats();
        Object value = stats.get("totalRejectedCount");
        return value instanceof Number ? ((Number) value).longValue() : 0L;
    }

    private static boolean waitForCondition(java.util.function.BooleanSupplier condition, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(25L);
        }
        return condition.getAsBoolean();
    }

    private static void withForcedSignatureVerifierUnavailable(String reason, ThrowingRunnable runnable)
            throws Exception {
        boolean originalAvailable = readVerifierAvailability();
        String originalReason = readVerifierReason();
        setVerifierAvailability(false);
        setVerifierReason(reason);
        try {
            runnable.run();
        } finally {
            setVerifierAvailability(originalAvailable);
            setVerifierReason(originalReason);
        }
    }

    private static boolean readVerifierAvailability() throws Exception {
        Field field = org.apache.jackrabbit.oak.segment.consensus.security.EthereumSignatureVerifier.class
            .getDeclaredField("bouncyCastleAvailable");
        field.setAccessible(true);
        return field.getBoolean(null);
    }

    private static void setVerifierAvailability(boolean available) throws Exception {
        Field field = org.apache.jackrabbit.oak.segment.consensus.security.EthereumSignatureVerifier.class
            .getDeclaredField("bouncyCastleAvailable");
        field.setAccessible(true);
        field.setBoolean(null, available);
    }

    private static String readVerifierReason() throws Exception {
        Field field = org.apache.jackrabbit.oak.segment.consensus.security.EthereumSignatureVerifier.class
            .getDeclaredField("availabilityReason");
        field.setAccessible(true);
        return (String) field.get(null);
    }

    private static void setVerifierReason(String reason) throws Exception {
        Field field = org.apache.jackrabbit.oak.segment.consensus.security.EthereumSignatureVerifier.class
            .getDeclaredField("availabilityReason");
        field.setAccessible(true);
        field.set(null, reason);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static final class NoopRaftAppendCallback implements RaftAppendCallback {
        @Override
        public void appendProposal(String walletAddress, String path, String contentType, String message, String signature) {
        }

        @Override
        public void appendProposal(String walletAddress, String path, String contentType,
                                   String message, String signature, String blobId, String mimeType) {
        }

        @Override
        public void appendDeleteProposal(String walletAddress, String path, String signature) {
        }
    }
}
