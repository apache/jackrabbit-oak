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
package org.apache.jackrabbit.oak.segment.consensus.server;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig;
import org.apache.jackrabbit.oak.segment.consensus.config.RuntimeConfigValueResolver;
import org.apache.jackrabbit.oak.segment.consensus.economics.ValidatorEarningsTracker;
import org.apache.jackrabbit.oak.segment.consensus.evm.EvmBridge;
import org.apache.jackrabbit.oak.segment.consensus.evm.impl.EventDrivenEvmBridge;
import org.apache.jackrabbit.oak.segment.consensus.evm.impl.SimpleEvmBridge;
import org.apache.jackrabbit.oak.segment.consensus.queue.BackpressureManager;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalQueueManagerOptimized;
import org.apache.jackrabbit.oak.segment.consensus.queue.QueuedProposal;
import org.apache.jackrabbit.oak.segment.consensus.queue.RaftAppendCallback;
import org.apache.jackrabbit.oak.segment.consensus.service.MutationAuditMetadata;
import org.apache.jackrabbit.oak.segment.consensus.security.EthereumWallet;
import org.apache.jackrabbit.oak.segment.consensus.eth.BeaconChainClient;
import org.apache.jackrabbit.oak.segment.http.server.SegmentHttpServer;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConsensusServicesInitializer {

    private static final Logger log = LoggerFactory.getLogger(ConsensusServicesInitializer.class);
    private static final String ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";
    private static final String EXAMPLE_SEPOLIA_CONTRACT = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";

    private final Supplier<BlockchainConfig> blockchainConfigSupplier;
    private final EvmBridgeFactory evmBridgeFactory;
    private final BeaconChainClientFactory beaconChainClientFactory;
    private final ProposalQueueManagerFactory proposalQueueManagerFactory;
    private final ValidatorEarningsTrackerFactory validatorEarningsTrackerFactory;
    private final RuntimeConfigReader runtimeConfigReader;

    ConsensusServicesInitializer() {
        this(BlockchainConfig::getInstance,
            blockchainConfig -> blockchainConfig.isMockMode()
                ? new SimpleEvmBridge(blockchainConfig.getNetwork(), blockchainConfig.getContractAddress())
                : new EventDrivenEvmBridge(blockchainConfig.getNetwork(), blockchainConfig.getContractAddress(), false),
            BeaconChainClient::new,
            ProposalQueueManagerOptimized::new,
            ValidatorEarningsTracker::new,
            RuntimeConfigValueResolver::readString);
    }

    ConsensusServicesInitializer(
            Supplier<BlockchainConfig> blockchainConfigSupplier,
            EvmBridgeFactory evmBridgeFactory,
            BeaconChainClientFactory beaconChainClientFactory,
            ProposalQueueManagerFactory proposalQueueManagerFactory,
            ValidatorEarningsTrackerFactory validatorEarningsTrackerFactory,
            RuntimeConfigReader runtimeConfigReader) {
        this.blockchainConfigSupplier = blockchainConfigSupplier;
        this.evmBridgeFactory = evmBridgeFactory;
        this.beaconChainClientFactory = beaconChainClientFactory;
        this.proposalQueueManagerFactory = proposalQueueManagerFactory;
        this.validatorEarningsTrackerFactory = validatorEarningsTrackerFactory;
        this.runtimeConfigReader = runtimeConfigReader;
    }

    void initialize(AeronConsensusEngine aeronEngine,
                    SegmentHttpServer httpServer,
                    EthereumWallet wallet,
                    String storeDirectory,
                    String beaconApiUrl,
                    String finalClusterWallet,
                    List<String> hostnamesList) {
        // Initialize Proposal Queue Manager (for Ethereum confirmation tracking)
        BlockchainConfig blockchainConfig = blockchainConfigSupplier.get();
        validateBlockchainRuntime(blockchainConfig);
        EvmBridge evmBridge = evmBridgeFactory.create(blockchainConfig);
        if (blockchainConfig.isMockMode()) {
            log.info("🎭 Using SimpleEvmBridge (mock simulation)");
        } else {
            log.info("🌐 Using EventDrivenEvmBridge (real blockchain event verification)");
        }
        evmBridge.start();

        RaftAppendCallback raftCallback = createRaftAppendCallback(aeronEngine);
        BackpressureManager backpressureManager = resolveBackpressureManager(aeronEngine);
        BeaconChainClient beaconClient = aeronEngine != null ? aeronEngine.getBeaconClient() : null;
        if (beaconClient == null) {
            beaconClient = beaconChainClientFactory.create(beaconApiUrl);
            beaconClient.startBackgroundPolling();
            log.info("✅ Beacon Chain client initialized (tracking Ethereum epochs from {})", beaconApiUrl);
        } else {
            log.info("✅ Reusing Aeron engine Beacon Chain client for proposal queue epoch telemetry");
        }

        String proposalPersistenceDir = runtimeConfigReader.readString(
            "oak.proposal.persistence.dir",
            "OAK_PROPOSAL_PERSISTENCE_DIR",
            new java.io.File(storeDirectory, "proposal-queue").getAbsolutePath()
        );

        ProposalQueueManagerOptimized proposalQueueManager = proposalQueueManagerFactory.create(
            evmBridge,
            raftCallback,
            backpressureManager,
            beaconClient,
            proposalPersistenceDir
        );
        proposalQueueManager.start();
        ServerContext context = httpServer.getContext();
        context.setProposalQueueManager(proposalQueueManager);
        context.evmBridge = evmBridge;
        log.info("✅ Proposal Queue Manager initialized (adaptive packing/release + 3-checkpoint security)");

        // Initialize Validator Earnings Tracker (economic simulation)
        List<String> validatorWallets = buildValidatorWallets(wallet.getWalletAddress(), hostnamesList);
        ValidatorEarningsTracker earningsTracker = validatorEarningsTrackerFactory.create(validatorWallets);
        context.setValidatorEarningsTracker(earningsTracker);
        log.info("   ✅ Validator Earnings Tracker initialized ({} validators)", validatorWallets.size());
        log.info("   - Self wallet: {}", wallet.getWalletAddress());

        context.validatorWalletAddress = wallet.getWalletAddress();
        context.clusterWalletAddress = finalClusterWallet;
        log.info("   - Payments routed to cluster wallet: {}", finalClusterWallet);

        // Aeron Cluster handles membership via Raft consensus.
        // Keep only local self-registration for compatibility state (health/metrics/peer views).
        String validatorId = wallet.getWalletAddress();
        httpServer.registerSelfValidator(validatorId);
        log.info("   - Self registered (local context only): {}", validatorId);
    }

    private static void validateBlockchainRuntime(BlockchainConfig blockchainConfig) {
        if (blockchainConfig == null || blockchainConfig.isMockMode()) {
            return;
        }

        String network = blockchainConfig.getNetwork();
        if ("mainnet".equalsIgnoreCase(network)) {
            throw new IllegalStateException(
                "MAINNET mode is disabled for oak-chain v1. Use MOCK for local simulation or SEPOLIA for testnet validation."
            );
        }

        String rpcUrl = blockchainConfig.getRpcUrl();
        if (rpcUrl == null || rpcUrl.trim().isEmpty()) {
            throw new IllegalStateException(
                "Chain-backed modes require OAK_BLOCKCHAIN_RPC_URL/oak.blockchain.rpcUrl to be configured."
            );
        }

        String contractAddress = blockchainConfig.getContractAddress();
        if (contractAddress == null || contractAddress.trim().isEmpty()) {
            throw new IllegalStateException(
                "Chain-backed modes require an explicit Oak payment contract address."
            );
        }

        if (ZERO_ADDRESS.equalsIgnoreCase(contractAddress)
                || EXAMPLE_SEPOLIA_CONTRACT.equalsIgnoreCase(contractAddress)) {
            throw new IllegalStateException(
                "Chain-backed modes require a deployed Oak payment contract address; example and zero-address defaults are not valid for v1."
            );
        }
    }

    static List<String> buildValidatorWallets(String selfWalletAddress, List<String> hostnamesList) {
        List<String> validatorWallets = new ArrayList<>();
        validatorWallets.add(selfWalletAddress);

        int expectedValidators = hostnamesList != null ? hostnamesList.size() : 1;
        for (int i = 1; i < expectedValidators; i++) {
            validatorWallets.add("0x" + String.format("%040x", i));
        }
        return validatorWallets;
    }

    private static BackpressureManager resolveBackpressureManager(AeronConsensusEngine aeronEngine) {
        BackpressureManager backpressureManager =
            aeronEngine != null ? aeronEngine.getBackpressureManager() : null;

        if (backpressureManager == null) {
            log.warn("⚠️  BackpressureManager not available - using fallback");
            return new BackpressureManager();
        }
        return backpressureManager;
    }

    private static RaftAppendCallback createRaftAppendCallback(AeronConsensusEngine aeronEngine) {
        return new RaftAppendCallback() {
            @Override
            public void appendProposal(String walletAddress, String path, String contentType, String message, String signature) {
                if (aeronEngine == null) {
                    log.error("❌ aeronEngine is NULL in appendProposal!");
                    return;
                }
                log.debug("📤 appendProposal() called - forwarding to Aeron (role: {})", aeronEngine.getCurrentRole());
                boolean success = aeronEngine.sendWriteThroughIngress(walletAddress, path, contentType, message, signature);
                if (!success) {
                    log.error("❌ sendWriteThroughIngress() returned false!");
                }
            }

            @Override
            public void appendProposalWithId(String proposalId, String walletAddress, String path, String contentType,
                                             String message, String signature) {
                tryAppendProposalWithId(proposalId, walletAddress, path, contentType, message, signature);
            }

            @Override
            public boolean tryAppendProposalWithId(String proposalId, String walletAddress, String path, String contentType,
                                                   String message, String signature) {
                if (aeronEngine == null) {
                    log.error("❌ aeronEngine is NULL in appendProposalWithId!");
                    return false;
                }
                boolean success = aeronEngine.sendWriteThroughIngressWithId(
                    walletAddress, path, contentType, message, signature, null, proposalId);
                if (!success) {
                    log.error("❌ sendWriteThroughIngress() returned false!");
                }
                return success;
            }

            @Override
            public boolean tryAppendProposalWithId(String proposalId, String walletAddress, String path, String contentType,
                                                   String message, String signature, MutationAuditMetadata auditMetadata) {
                if (aeronEngine == null) {
                    log.error("❌ aeronEngine is NULL in appendProposalWithId!");
                    return false;
                }
                boolean success = aeronEngine.sendWriteThroughIngressWithId(
                    walletAddress, path, contentType, message, signature, null, auditMetadata);
                if (!success) {
                    log.error("❌ sendWriteThroughIngress() returned false!");
                }
                return success;
            }

            @Override
            public void appendProposal(String walletAddress, String path, String contentType, String message,
                                       String signature, String blobId, String mimeType) {
                if (aeronEngine == null) {
                    log.error("❌ aeronEngine is NULL in appendProposal!");
                    return;
                }
                log.debug("📤 appendProposal() with binary - blobId={} (role: {})", blobId, aeronEngine.getCurrentRole());
                boolean success = aeronEngine.sendWriteThroughIngress(walletAddress, path, contentType, message, signature, blobId, mimeType);
                if (!success) {
                    log.error("❌ sendWriteThroughIngress() with binary returned false!");
                }
            }

            @Override
            public void appendProposalWithId(String proposalId, String walletAddress, String path, String contentType,
                                             String message, String signature, String blobId, String mimeType, String ipfsCid) {
                tryAppendProposalWithId(
                    proposalId, walletAddress, path, contentType, message, signature, blobId, mimeType, ipfsCid);
            }

            @Override
            public boolean tryAppendProposalWithId(String proposalId, String walletAddress, String path, String contentType,
                                                   String message, String signature, String blobId, String mimeType,
                                                   String ipfsCid) {
                if (aeronEngine == null) {
                    log.error("❌ aeronEngine is NULL in appendProposalWithId!");
                    return false;
                }
                boolean success = aeronEngine.sendWriteThroughIngress(
                    walletAddress, path, contentType, message, signature, blobId, mimeType, ipfsCid, proposalId);
                if (!success) {
                    log.error("❌ sendWriteThroughIngress() with binary returned false!");
                }
                return success;
            }

            @Override
            public boolean tryAppendProposalWithId(String proposalId, String walletAddress, String path, String contentType,
                                                   String message, String signature, String blobId, String mimeType,
                                                   String ipfsCid, MutationAuditMetadata auditMetadata) {
                if (aeronEngine == null) {
                    log.error("❌ aeronEngine is NULL in appendProposalWithId!");
                    return false;
                }
                boolean success = aeronEngine.sendWriteThroughIngress(
                    walletAddress, path, contentType, message, signature, blobId, mimeType, ipfsCid, auditMetadata);
                if (!success) {
                    log.error("❌ sendWriteThroughIngress() with binary returned false!");
                }
                return success;
            }

            @Override
            public void appendDeleteProposal(String walletAddress, String path, String signature) {
                if (aeronEngine == null) {
                    log.error("❌ aeronEngine is NULL in appendDeleteProposal!");
                    return;
                }
                log.debug("🗑️  appendDeleteProposal() called - forwarding to Aeron (role: {})", aeronEngine.getCurrentRole());
                boolean success = aeronEngine.sendDeleteThroughIngress(walletAddress, path, signature);
                if (!success) {
                    log.error("❌ sendDeleteThroughIngress() returned false!");
                }
            }

            @Override
            public void appendDeleteProposalWithId(String proposalId, String walletAddress, String path, String signature) {
                tryAppendDeleteProposalWithId(proposalId, walletAddress, path, signature);
            }

            @Override
            public boolean tryAppendDeleteProposalWithId(String proposalId, String walletAddress, String path, String signature) {
                if (aeronEngine == null) {
                    log.error("❌ aeronEngine is NULL in appendDeleteProposalWithId!");
                    return false;
                }
                boolean success = aeronEngine.sendDeleteThroughIngress(walletAddress, path, signature, proposalId);
                if (!success) {
                    log.error("❌ sendDeleteThroughIngress() returned false!");
                }
                return success;
            }

            @Override
            public boolean tryAppendDeleteProposalWithId(String proposalId, String walletAddress, String path,
                                                         String signature, MutationAuditMetadata auditMetadata) {
                if (aeronEngine == null) {
                    log.error("❌ aeronEngine is NULL in appendDeleteProposalWithId!");
                    return false;
                }
                boolean success = aeronEngine.sendDeleteThroughIngress(walletAddress, path, signature, auditMetadata);
                if (!success) {
                    log.error("❌ sendDeleteThroughIngress() returned false!");
                }
                return success;
            }

            @Override
            public int appendProposalBatch(List<QueuedProposal> proposals) {
                log.debug("🔥🔥🔥 OVERRIDE CALLED: appendProposalBatch() - batch size: {}, class: {}",
                    proposals.size(), this.getClass().getName());

                if (aeronEngine == null) {
                    log.error("❌ aeronEngine is NULL in appendProposalBatch!");
                    return 0;
                }
                log.debug("📤 appendProposalBatch() forwarding to aeronEngine.sendWriteBatchThroughIngress() - role: {}",
                    aeronEngine.getCurrentRole());
                int sent = aeronEngine.sendWriteBatchThroughIngress(proposals);
                log.debug("📤 appendProposalBatch() result: {} proposals sent", sent);
                if (sent == 0) {
                    log.error("❌ sendWriteBatchThroughIngress() returned 0 (failed)!");
                }
                return sent;
            }
        };
    }

    interface EvmBridgeFactory {
        EvmBridge create(BlockchainConfig blockchainConfig);
    }

    interface BeaconChainClientFactory {
        BeaconChainClient create(String beaconApiUrl);
    }

    interface ProposalQueueManagerFactory {
        ProposalQueueManagerOptimized create(EvmBridge evmBridge,
                                             RaftAppendCallback raftCallback,
                                             BackpressureManager backpressureManager,
                                             BeaconChainClient beaconClient,
                                             String proposalPersistenceDir);
    }

    interface ValidatorEarningsTrackerFactory {
        ValidatorEarningsTracker create(List<String> validatorWallets);
    }

    interface RuntimeConfigReader {
        String readString(String propertyName, String environmentVariable, String defaultValue);
    }
}
