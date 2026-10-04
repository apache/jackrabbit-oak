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
package org.apache.jackrabbit.oak.segment.consensus.evm.impl;

import org.apache.jackrabbit.oak.segment.consensus.evm.EvmBridge;
import org.apache.jackrabbit.oak.segment.consensus.evm.PaymentProof;
import org.apache.jackrabbit.oak.segment.consensus.evm.SettlementDetails;
import org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalQueuePolicy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple EVM bridge implementation.
 * <p>
 * This is a POC implementation that simulates blockchain interaction.
 * A production implementation would:
 * - Use Web3j or ethers-java to connect to actual Ethereum nodes
 * - Listen for smart contract events using WebSocket subscriptions
 * - Verify transaction receipts and confirmations
 * - Handle chain reorganizations
 * - Implement retry logic and error handling
 * <p>
 * For now, we simulate payments in memory.
 */
public class SimpleEvmBridge implements EvmBridge {
    
    private static final Logger log = LoggerFactory.getLogger(SimpleEvmBridge.class);
    
    // Pricing constants (all in wei)
    private static final BigInteger BASE_FEE = new BigInteger("1000000000000000"); // 0.001 ETH
    private static final BigInteger SEGMENT_FEE = new BigInteger("100000000000000"); // 0.0001 ETH per segment
    private static final BigInteger STORAGE_FEE_PER_KB = new BigInteger("10000000000000"); // 0.00001 ETH per KB
    private static final BigInteger BLOB_FEE = new BigInteger("50000000000000"); // 0.00005 ETH per blob
    
    private final String networkName;
    private final String contractAddress;
    private final Map<String, PaymentProof> payments = new ConcurrentHashMap<>();
    private final Map<String, String> addressToWalletMapping = new ConcurrentHashMap<>();
    private final Map<String, String> proposalToWalletMapping = new ConcurrentHashMap<>(); // proposalId -> walletAddress
    private long currentBlock = 1000000;
    private boolean running = false;
    private final BlockchainConfig.Mode mode;
    
    /**
     * Create a new EVM bridge.
     *
     * @param networkName the network (e.g., "mainnet", "sepolia", "mock")
     * @param contractAddress the payment verifier contract address
     */
    public SimpleEvmBridge(@NotNull String networkName, @NotNull String contractAddress) {
        this.networkName = networkName;
        this.contractAddress = contractAddress;
        this.mode = BlockchainConfig.getInstance().getMode();
        
        switch (mode) {
            case MOCK:
                log.warn("⚠️  SimpleEvmBridge in MOCK MODE - all payments auto-simulated");
                break;
            case SEPOLIA:
                log.info("✅ SimpleEvmBridge in SEPOLIA MODE - verifying payments on Sepolia testnet");
                break;
            case MAINNET:
                log.info("🔴 SimpleEvmBridge in MAINNET MODE - verifying payments on Ethereum mainnet");
                break;
        }
    }
    
    /**
     * Convenience constructor for testnet.
     */
    public SimpleEvmBridge() {
        this("sepolia", "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0");
    }
    
    @Override
    public PaymentProof verifyPayment(@NotNull String proposalId) {
        // Check if payment already exists
        PaymentProof existing = payments.get(proposalId);
        if (existing != null) {
            return existing;
        }
        
        // In MOCK MODE, auto-simulate payments for any proposal
        if (mode == BlockchainConfig.Mode.MOCK) {
            log.debug("🎭 MOCK MODE: Auto-simulating payment for proposal {}", proposalId);
            
            // Get the wallet address for this proposal (registered earlier)
            String walletAddress = proposalToWalletMapping.get(proposalId);
            if (walletAddress == null) {
                // SECURITY: Wallet address MUST be registered when proposal is queued
                // If it's missing, this is a programming error - fail hard
                String errorMsg = String.format(
                    "SECURITY VIOLATION: No wallet address registered for proposal %s. " +
                    "registerProposalWallet() must be called when queuing. " +
                    "Cannot create payment proof without authenticated wallet.", 
                    proposalId
                );
                log.error("🔒 {}", errorMsg);
                throw new IllegalStateException(errorMsg);
            }
            
            String mockTxHash = buildMockTransactionHash(proposalId);
            
            PaymentProof mockPayment = new SimplePaymentProof(
                mockTxHash, // Mock tx hash (64 chars)
                currentBlock++, // Increment block
                walletAddress, // From address (use registered wallet)
                contractAddress, // To address (contract)
                proposalId,
                "1000000000000000", // 0.001 ETH
                ProposalQueuePolicy.requiredConfirmations()
            );
            
            payments.put(proposalId, mockPayment);
            return mockPayment;
        }
        
        // In SEPOLIA/MAINNET MODE, return null (payment not found - will retry)
        // In production, this would call Web3j to verify the transaction on-chain
        return null;
    }

    @Override
    @Nullable
    public SettlementDetails getSettlementDetailsByProposalId(@NotNull String proposalId) {
        // Read path: a proposal this bridge never registered (or no longer retains)
        // is a lookup miss, not a security violation. verifyPayment() keeps its
        // fail-hard registerProposalWallet() enforcement for the queue/settlement path.
        PaymentProof proof = payments.get(proposalId);
        if (proof == null && proposalToWalletMapping.containsKey(proposalId)) {
            proof = verifyPayment(proposalId);
        }
        return proof != null ? SettlementDetails.fromProof(getNetworkName(), proof) : null;
    }

    @Override
    @Nullable
    public SettlementDetails getSettlementDetailsByTransactionHash(@NotNull String transactionHash) {
        if (transactionHash.trim().isEmpty()) {
            return null;
        }
        for (PaymentProof proof : payments.values()) {
            if (proof != null && transactionHash.equalsIgnoreCase(proof.getTransactionHash())) {
                return SettlementDetails.fromProof(getNetworkName(), proof);
            }
        }
        return null;
    }
    
    /**
     * Register a wallet address for a proposal (used in mock mode).
     * This allows mock payment simulation to use the correct from address.
     * 
     * @param proposalId the proposal ID
     * @param walletAddress the wallet address
     */
    public void registerProposalWallet(@NotNull String proposalId, @NotNull String walletAddress) {
        proposalToWalletMapping.put(proposalId, walletAddress);
    }
    
    @Override
    @NotNull
    public String calculateRequiredPayment(int segmentCount, long byteSize, int blobCount) {
        // write_cost = base_fee + (segment_count * segment_fee) + (byte_size * storage_fee) + (blob_count * blob_fee)
        
        BigInteger total = BASE_FEE;
        
        // Segment cost
        total = total.add(SEGMENT_FEE.multiply(BigInteger.valueOf(segmentCount)));
        
        // Storage cost (per KB)
        long kilobytes = byteSize / 1024;
        total = total.add(STORAGE_FEE_PER_KB.multiply(BigInteger.valueOf(kilobytes)));
        
        // Blob cost
        total = total.add(BLOB_FEE.multiply(BigInteger.valueOf(blobCount)));
        
        return total.toString();
    }
    
    @Override
    public String getWalletUuidForAddress(@NotNull String ethereumAddress) {
        return addressToWalletMapping.get(ethereumAddress.toLowerCase());
    }
    
    @Override
    public long getCurrentBlockNumber() {
        return currentBlock;
    }
    
    @Override
    @NotNull
    public String getNetworkName() {
        return networkName;
    }
    
    @Override
    @NotNull
    public String getContractAddress() {
        return contractAddress;
    }
    
    @Override
    public void start() {
        running = true;
        log.info("EVM Bridge started on {} (contract: {})", networkName, contractAddress);
    }
    
    @Override
    public void stop() {
        running = false;
        log.info("EVM Bridge stopped");
    }
    
    // ========== POC Helper Methods ==========
    
    /**
     * Simulate a payment (for testing).
     * <p>
     * In production, this would be detected by listening to blockchain events.
     *
     * @param payment the payment proof
     */
    public void simulatePayment(@NotNull PaymentProof payment) {
        payments.put(payment.getProposalId(), payment);
        log.debug("Detected payment: {}", payment);
    }
    
    /**
     * Register a wallet mapping (for testing).
     * <p>
     * In production, this would be done through a smart contract or off-chain registry.
     *
     * @param ethereumAddress the Ethereum address
     * @param walletUuid the Oak wallet UUID
     */
    public void registerWallet(@NotNull String ethereumAddress, @NotNull String walletUuid) {
        addressToWalletMapping.put(ethereumAddress.toLowerCase(), walletUuid);
        log.debug("Registered wallet mapping: {} -> {}", ethereumAddress, walletUuid);
    }
    
    /**
     * Simulate block progression (for testing).
     */
    public void advanceBlock() {
        currentBlock++;
    }

    private String buildMockTransactionHash(String proposalId) {
        if (proposalId != null && proposalId.matches("^0x[0-9a-fA-F]{64}$")) {
            return proposalId;
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(String.valueOf(proposalId).getBytes(StandardCharsets.UTF_8));
            return "0x" + toHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 must be available for mock transaction synthesis", e);
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            hex.append(Character.forDigit((value >>> 4) & 0xF, 16));
            hex.append(Character.forDigit(value & 0xF, 16));
        }
        return hex.toString();
    }
    
    /**
     * Check if the bridge is running.
     *
     * @return true if running
     */
    public boolean isRunning() {
        return running;
    }
}
