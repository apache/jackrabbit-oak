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
package org.apache.jackrabbit.oak.segment.consensus.evm;

import org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig;
import org.apache.jackrabbit.oak.segment.consensus.evm.impl.SimpleEvmBridge;
import org.apache.jackrabbit.oak.segment.consensus.evm.impl.SimplePaymentProof;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.*;

/**
 * Tests for the EVM bridge payment verifier.
 */
public class EvmBridgeTest {
    
    private SimpleEvmBridge bridge;
    
    @Before
    public void setUp() {
        System.clearProperty("oak.blockchain.mode");
        BlockchainConfig.reset();
        bridge = new SimpleEvmBridge("sepolia", "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0");
        bridge.start();
    }
    
    @After
    public void tearDown() {
        bridge.stop();
        System.clearProperty("oak.blockchain.mode");
        BlockchainConfig.reset();
    }
    
    @Test
    public void testBridgeConfiguration() {
        assertEquals("Network name should match", "sepolia", bridge.getNetworkName());
        assertEquals("Contract address should match", 
                "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0", 
                bridge.getContractAddress());
        assertTrue("Bridge should be running", bridge.isRunning());
    }
    
    @Test
    public void testCalculatePayment() {
        // Test basic write with 1 segment, 1KB, 0 blobs
        String cost1 = bridge.calculateRequiredPayment(1, 1024, 0);
        assertNotNull("Cost should not be null", cost1);
        
        BigInteger amount1 = new BigInteger(cost1);
        assertTrue("Cost should be positive", amount1.compareTo(BigInteger.ZERO) > 0);
        
        // Test larger write with more segments
        String cost2 = bridge.calculateRequiredPayment(10, 10240, 5);
        BigInteger amount2 = new BigInteger(cost2);
        
        // Larger write should cost more
        assertTrue("Larger write should cost more", amount2.compareTo(amount1) > 0);
    }
    
    @Test
    public void testPaymentCalculationFormula() {
        // Base fee: 0.001 ETH = 1,000,000,000,000,000 wei
        // Segment fee: 0.0001 ETH = 100,000,000,000,000 wei per segment
        // Storage fee: 0.00001 ETH = 10,000,000,000,000 wei per KB
        // Blob fee: 0.00005 ETH = 50,000,000,000,000 wei per blob
        
        // Test: 5 segments, 10KB, 2 blobs
        // Expected: 1000000000000000 + (5 * 100000000000000) + (10 * 10000000000000) + (2 * 50000000000000)
        //         = 1000000000000000 + 500000000000000 + 100000000000000 + 100000000000000
        //         = 1700000000000000 wei
        
        String cost = bridge.calculateRequiredPayment(5, 10 * 1024, 2);
        BigInteger amount = new BigInteger(cost);
        BigInteger expected = new BigInteger("1700000000000000");
        
        assertEquals("Cost should match expected formula", expected, amount);
    }
    
    @Test
    public void testPaymentVerification() {
        String proposalId = "proposal-123";
        String walletAddress = "0x1234567890123456789012345678901234567890";
        
        // Register wallet for proposal (required in MOCK mode before verifyPayment)
        bridge.registerProposalWallet(proposalId, walletAddress);
        
        // Simulate a payment
        PaymentProof payment = new SimplePaymentProof(
                "0xabc123",
                1000000,
                walletAddress,
                bridge.getContractAddress(),
                proposalId,
                "1000000000000000",
                12
        );
        bridge.simulatePayment(payment);
        
        // Verify payment
        PaymentProof proof2 = bridge.verifyPayment(proposalId);
        assertNotNull("Should find payment", proof2);
        assertEquals("Transaction hash should match", "0xabc123", proof2.getTransactionHash());
        assertEquals("Proposal ID should match", proposalId, proof2.getProposalId());
    }

    @Test
    public void testSettlementDetailsByProposalIdExposeBasicFields() {
        String proposalId = "proposal-settlement";
        String walletAddress = "0x1234567890123456789012345678901234567890";
        bridge.registerProposalWallet(proposalId, walletAddress);

        PaymentProof payment = new SimplePaymentProof(
            "0xsettlementtx",
            1001234,
            walletAddress,
            bridge.getContractAddress(),
            proposalId,
            "1000000000000000",
            12
        );
        bridge.simulatePayment(payment);

        SettlementDetails details = bridge.getSettlementDetailsByProposalId(proposalId);

        assertNotNull("Settlement details should be available by proposal id", details);
        assertEquals("sepolia", details.getNetworkName());
        assertEquals(proposalId, details.getProposalId());
        assertEquals("0xsettlementtx", details.getTransactionHash());
        assertEquals(walletAddress, details.getFromAddress());
    }

    @Test
    public void testSettlementDetailsByTransactionHashResolveFromCachedPayment() {
        PaymentProof payment = new SimplePaymentProof(
            "0xlookuptx",
            1002000,
            "0x1234567890123456789012345678901234567890",
            bridge.getContractAddress(),
            "proposal-lookup",
            "2000000000000000",
            6
        );
        bridge.simulatePayment(payment);

        SettlementDetails details = bridge.getSettlementDetailsByTransactionHash("0xlookuptx");

        assertNotNull("Settlement details should be available by transaction hash", details);
        assertEquals("proposal-lookup", details.getProposalId());
        assertEquals("0xlookuptx", details.getTransactionHash());
        assertEquals(1002000, details.getBlockNumber());
    }
    
    @Test
    public void testPaymentConfirmations() {
        PaymentProof payment = new SimplePaymentProof(
                "0xdef456",
                1000000,
                "0x1234567890123456789012345678901234567890",
                bridge.getContractAddress(),
                "proposal-456",
                "2000000000000000",
                6
        );
        
        // Should not be confirmed with 12 required
        assertFalse("Should not be confirmed with 6 confirmations", payment.isConfirmed(12));
        
        // Should be confirmed with 5 required
        assertTrue("Should be confirmed with 5 confirmations", payment.isConfirmed(5));
    }
    
    @Test
    public void testWalletMapping() {
        String ethAddress = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
        String walletUuid = "550e8400-e29b-41d4-a716-446655440000";
        
        // Initially no mapping
        String result1 = bridge.getWalletUuidForAddress(ethAddress);
        assertNull("Should have no mapping initially", result1);
        
        // Register mapping
        bridge.registerWallet(ethAddress, walletUuid);
        
        // Verify mapping
        String result2 = bridge.getWalletUuidForAddress(ethAddress);
        assertNotNull("Should find mapping", result2);
        assertEquals("Wallet UUID should match", walletUuid, result2);
    }
    
    @Test
    public void testWalletMappingCaseInsensitive() {
        String ethAddress = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
        String walletUuid = "wallet-uuid-123";
        
        bridge.registerWallet(ethAddress, walletUuid);
        
        // Should work with different case
        String result = bridge.getWalletUuidForAddress("0x742D35CC6634C0532925A3B844BC9E7595F0BEB0");
        assertNotNull("Should find mapping with different case", result);
        assertEquals("Wallet UUID should match", walletUuid, result);
    }
    
    @Test
    public void testBlockProgression() {
        long initialBlock = bridge.getCurrentBlockNumber();
        
        bridge.advanceBlock();
        assertEquals("Block should advance by 1", initialBlock + 1, bridge.getCurrentBlockNumber());
        
        bridge.advanceBlock();
        bridge.advanceBlock();
        assertEquals("Block should advance by 3 total", initialBlock + 3, bridge.getCurrentBlockNumber());
    }
    
    @Test
    public void testMultiplePayments() {
        // Simulate multiple payments
        for (int i = 0; i < 10; i++) {
            String proposalId = "proposal-" + i;
            PaymentProof payment = new SimplePaymentProof(
                    "0xtx" + i,
                    1000000 + i,
                    "0x1234567890123456789012345678901234567890",
                    bridge.getContractAddress(),
                    proposalId,
                    String.valueOf(1000000000000000L * (i + 1)),
                    12
            );
            bridge.simulatePayment(payment);
        }
        
        // Verify all payments
        for (int i = 0; i < 10; i++) {
            String proposalId = "proposal-" + i;
            PaymentProof proof = bridge.verifyPayment(proposalId);
            assertNotNull("Should find payment for " + proposalId, proof);
            assertEquals("Proposal ID should match", proposalId, proof.getProposalId());
            assertTrue("Payment should be confirmed", proof.isConfirmed(12));
        }
    }
    
    @Test
    public void testPaymentProofDetails() {
        PaymentProof payment = new SimplePaymentProof(
                "0x123abc456def",
                2000000,
                "0xFromAddress",
                "0xContractAddress",
                "proposal-789",
                "5000000000000000",
                20
        );
        
        assertEquals("TX hash should match", "0x123abc456def", payment.getTransactionHash());
        assertEquals("Block number should match", 2000000, payment.getBlockNumber());
        assertEquals("From address should match", "0xFromAddress", payment.getFromAddress());
        assertEquals("Contract address should match", "0xContractAddress", payment.getContractAddress());
        assertEquals("Proposal ID should match", "proposal-789", payment.getProposalId());
        assertEquals("Amount should match", "5000000000000000", payment.getAmountWei());
        assertEquals("Proposal kind should default to WRITE", PaymentProof.ProposalKind.WRITE, payment.getProposalKind());
        assertEquals("Payment token should default to UNKNOWN", PaymentProof.PaymentToken.UNKNOWN, payment.getPaymentToken());
        assertEquals("Capability flags should default to zero", 0, payment.getCapabilityFlags());
        assertEquals("Confirmations should match", 20, payment.getConfirmations());
    }
    
    @Test
    public void testBridgeLifecycle() {
        SimpleEvmBridge newBridge = new SimpleEvmBridge();
        assertFalse("Bridge should not be running initially", newBridge.isRunning());
        
        newBridge.start();
        assertTrue("Bridge should be running after start", newBridge.isRunning());
        
        newBridge.stop();
        assertFalse("Bridge should not be running after stop", newBridge.isRunning());
    }

    @Test
    public void testMockModeAutoSimulatesBytes32ProposalIds() {
        bridge.stop();
        System.setProperty("oak.blockchain.mode", "mock");
        BlockchainConfig.reset();
        bridge = new SimpleEvmBridge("mock", "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0");
        bridge.start();

        String proposalId = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        String walletAddress = "0x1234567890123456789012345678901234567890";
        bridge.registerProposalWallet(proposalId, walletAddress);

        PaymentProof proof = bridge.verifyPayment(proposalId);
        assertNotNull("Mock mode should auto-simulate bytes32 proposal ids", proof);
        assertEquals("Proposal id should be preserved", proposalId, proof.getProposalId());
        assertEquals("Bytes32 proposal ids should also work as mock tx hashes", proposalId, proof.getTransactionHash());
        assertEquals("Registered wallet should flow into the mock proof", walletAddress, proof.getFromAddress());
    }

    @Test
    public void testMockModeSettlementLookupForUnknownProposalIsMissNotSecurityViolation() {
        bridge.stop();
        System.setProperty("oak.blockchain.mode", "mock");
        BlockchainConfig.reset();
        bridge = new SimpleEvmBridge("mock", "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0");
        bridge.start();

        String unknownProposalId = "0x4cc359f9f42dbddc32d50823371578a70f59502e75f7aa41313bfcdb668affad";

        assertNull("Read lookup of an unregistered proposal should be a miss",
                bridge.getSettlementDetailsByProposalId(unknownProposalId));
        assertNull("Read lookup of an unknown transaction hash should be a miss",
                bridge.getSettlementDetailsByTransactionHash(unknownProposalId));

        try {
            bridge.verifyPayment(unknownProposalId);
            fail("verifyPayment must still enforce registerProposalWallet() for unregistered proposals");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("SECURITY VIOLATION"));
        }
    }

    @Test
    public void testMockModeSettlementLookupForRegisteredProposalStillResolves() {
        bridge.stop();
        System.setProperty("oak.blockchain.mode", "mock");
        BlockchainConfig.reset();
        bridge = new SimpleEvmBridge("mock", "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0");
        bridge.start();

        String proposalId = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        String walletAddress = "0x1234567890123456789012345678901234567890";
        bridge.registerProposalWallet(proposalId, walletAddress);

        SettlementDetails details = bridge.getSettlementDetailsByProposalId(proposalId);
        assertNotNull("Registered proposal should still resolve settlement details in mock mode", details);
        assertEquals(proposalId, details.getProposalId());
        assertEquals(walletAddress, details.getFromAddress());
    }
}
