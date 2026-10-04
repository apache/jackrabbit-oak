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

import org.apache.jackrabbit.oak.segment.consensus.queue.DurabilityState;
import org.apache.jackrabbit.oak.segment.consensus.service.MutationAuditMetadata;
import org.apache.jackrabbit.oak.segment.consensus.service.DeleteApplicationService;
import org.apache.jackrabbit.oak.segment.consensus.service.FileStoreFlushService;
import org.apache.jackrabbit.oak.segment.consensus.service.WriteApplicationService;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Handler for consensus API endpoints (`/v1/propose-write`, `/v1/consensus/status`, `/v1/proposals/*`).
 * This class encapsulates the logic for handling signed write transactions, proposal status queries,
 * and GC cost estimation in the Aeron-based consensus network.
 */
public class ConsensusApiHandler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConsensusApiHandler.class);

    private final ServerContext context;
    private final WriteApplicationService writeApplicationService;
    private final DeleteApplicationService deleteApplicationService;
    private final FileStoreFlushService flushService;
    private final WriteProposalHandler writeProposalHandler;
    private final DeleteProposalHandler deleteProposalHandler;
    private final ConsensusStatusHandler consensusStatusHandler;
    private final ProposalQueryHandler proposalQueryHandler;
    private final GcCostHandler gcCostHandler;
    private final WalletQueryHandler walletQueryHandler;

    public ConsensusApiHandler(ServerContext context) {
        this.context = context;
        
        // Initialize application services
        this.flushService = new FileStoreFlushService(context.fileStore);
        this.writeApplicationService = new WriteApplicationService(
            context.fileStore,
            () -> context.authoritativeNodeStore != null ? context.authoritativeNodeStore : context.nodeStore,
            () -> context.blobStore,
            flushService
        );
        this.deleteApplicationService = new DeleteApplicationService(
            context.fileStore,
            () -> context.authoritativeNodeStore != null ? context.authoritativeNodeStore : context.nodeStore,
            flushService
        );
        
        // Wire callbacks for integration
        wireServiceCallbacks();

        // Initialize domain handlers
        this.writeProposalHandler = new WriteProposalHandler(context);
        this.deleteProposalHandler = new DeleteProposalHandler(context);
        this.consensusStatusHandler = new ConsensusStatusHandler(context);
        this.proposalQueryHandler = new ProposalQueryHandler(context);
        this.gcCostHandler = new GcCostHandler(context);
        this.walletQueryHandler = new WalletQueryHandler(context);
    }
    
    /**
     * Wire callbacks to integrate services with the broader system.
     */
    private void wireServiceCallbacks() {
        // HEAD update callback
        WriteApplicationService.HeadUpdateCallback headCallback = newHead -> {
            if (context.aeronConsensusEngine != null) {
                context.aeronConsensusEngine.updateLatestHead(newHead);
            }
        };
        writeApplicationService.setHeadUpdateCallback(headCallback);
        deleteApplicationService.setHeadUpdateCallback(headCallback::updateHead);
        
        // SSE event callback for writes
        if (context.eventBroadcaster != null) {
            writeApplicationService.setSseEventCallback(new WriteApplicationService.SSEEventCallback() {
                @Override
                public void emitContentWrite(String path, String wallet, String org, 
                                            String message, String signature, String contentType) {
                    try {
                        context.eventBroadcaster.emitContentWrite(path, wallet, org, message, signature, contentType);
                    } catch (Exception e) {
                        log.debug("Failed to emit SSE write event: {}", e.getMessage());
                    }
                }
                
                @Override
                public void emitBinaryUpload(String path, String wallet, String org, 
                                            String message, String ipfsCid, String mimeType) {
                    try {
                        context.eventBroadcaster.emitBinaryUpload(path, wallet, org, message, ipfsCid, null, mimeType);
                    } catch (Exception e) {
                        log.debug("Failed to emit SSE binary event: {}", e.getMessage());
                    }
                }
            });
            
            // SSE event callback for deletes
            deleteApplicationService.setSseEventCallback((path, wallet, org, signature) -> {
                try {
                    context.eventBroadcaster.emitContentDelete(path, wallet, org, signature);
                } catch (Exception e) {
                    log.debug("Failed to emit SSE delete event: {}", e.getMessage());
                }
            });
        }
        
        // Fragmentation tracking callback
        if (context.fragmentationTracker != null) {
            writeApplicationService.setFragmentationCallback(this::trackFragmentation);
        }
        
        // CID mapping callback
        if (context.cidMappingService != null) {
            writeApplicationService.setCidMappingCallback(blobId -> {
                try {
                    return context.cidMappingService.getCid(blobId).orElse(null);
                } catch (Exception e) {
                    log.debug("CID mapping lookup failed: {}", e.getMessage());
                    return null;
                }
            });
        }

        // Durability callbacks must tolerate late context wiring during startup.
        writeApplicationService.setDurabilityCallback(new WriteApplicationService.DurabilityCallback() {
            @Override
            public void onDurable(String proposalId, String durableHead) {
                forwardDurabilitySuccess(proposalId, durableHead);
            }

            @Override
            public void onFailure(String proposalId, String error) {
                forwardDurabilityFailure(proposalId, error);
            }
        });
        deleteApplicationService.setDurabilityCallback(new DeleteApplicationService.DurabilityCallback() {
            @Override
            public void onDurable(String proposalId, String durableHead) {
                forwardDurabilitySuccess(proposalId, durableHead);
            }

            @Override
            public void onFailure(String proposalId, String error) {
                forwardDurabilityFailure(proposalId, error);
            }
        });

        if (context.aeronConsensusEngine != null) {
            context.aeronConsensusEngine.setDurabilityStatusCallback(new org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine.DurabilityStatusCallback() {
                @Override
                public void onDurable(String proposalId, String durableHead) {
                    applyDurabilityStatus(proposalId, DurabilityState.ACKED, durableHead, null);
                }

                @Override
                public void onFailure(String proposalId, String error) {
                    applyDurabilityStatus(proposalId, DurabilityState.FAILED, null, error);
                }
            });
        }
    }

    public void refreshCallbacks() {
        wireServiceCallbacks();
    }

    private void forwardDurabilitySuccess(String proposalId, String durableHead) {
        if (context.aeronConsensusEngine != null) {
            context.aeronConsensusEngine.sendSegmentPersisted(proposalId, durableHead, true, null);
            return;
        }
        applyDurabilityStatus(proposalId, DurabilityState.ACKED, durableHead, null);
    }

    private void forwardDurabilityFailure(String proposalId, String error) {
        if (context.aeronConsensusEngine != null) {
            context.aeronConsensusEngine.sendSegmentPersisted(proposalId, null, false, error);
            return;
        }
        applyDurabilityStatus(proposalId, DurabilityState.FAILED, null, error);
    }

    private void applyDurabilityStatus(String proposalId, DurabilityState state, String durableHead, String error) {
        if (context.proposalQueueManager == null) {
            log.warn("⚠️  Proposal queue unavailable - cannot update durability for {} ({})", proposalId, state);
            return;
        }
        context.proposalQueueManager.updateDurability(proposalId, state, durableHead, error);
    }

    @Override
    public void close() {
        flushService.close();
    }

    /**
     * Handle POST /v1/propose-write - Signed write transaction endpoint
     * 
     * <p>Accepts signed write transactions from Sling authors. The transaction is signed
     * with the Sling author's Ethereum wallet and verified before processing.</p>
     * 
     * Parameters:
     *   - wallet: Ethereum address (e.g., 0x1234...)
     *   - signature: Signed transaction (walletAddress:timestamp:contentType:message)
     *   - message: Content to write
     *   - contentType: Type of content (default: "page")
     *   - clientId: Client identifier (from X-Client-Id header or parameter)
     *   - timestamp: Transaction timestamp
     */
    public void handleProposeWrite(HttpServletRequest request, HttpServletResponse response) throws IOException {
        writeProposalHandler.handleProposeWrite(request, response);
    }
    
    /**
     * Handle POST /v1/propose-delete - Delete proposal endpoint
     * 
     * <p>Allows Sling authors to propose deletion of content they own.
     * Ownership is verified by checking that the content path is under
     * the wallet's shard root (/oak-chain/{shard}/) and that the wallet matches the registered client.</p>
     * 
     * <p><strong>Path Structure:</strong>
     * <pre>
     * /oak-chain/{shard}/content/...  ← Wallet-owned content
     * /oak-chain/{shard}/conf/...     ← Wallet-owned config
     * /oak-chain/{shard}/apps/...     ← Wallet-owned apps (future)
     * </pre>
     * 
     * <p>Parameters:
     *   - wallet: Ethereum wallet address (must match registered client)
     *   - signature: Signed message (wallet:deleteId:contentPath)
     *   - contentPath: Path to content to delete (must be under /oak-chain/{shard}/)
     *   - clientId: Client identifier (from X-Client-Id header or parameter)</p>
     */
    public void handleDeleteProposal(HttpServletRequest request, HttpServletResponse response) throws IOException {
        deleteProposalHandler.handleDeleteProposal(request, response);
    }
    
    /**
     * Handle GET /v1/consensus/status - Return comprehensive consensus state
     * 
     * Uses Aeron Cluster for single source of truth.
     * 
     * Returns JSON with:
     * - consensusType: "leader-based" | "blockchain-poa" | "dag" | "none"
     * - currentRole: "LEADER" | "FOLLOWER" | "STANDALONE"
     * - currentLeader: URL of current leader (if follower)
     * - currentEpoch: Current epoch number
     * - leaderTermSeconds: Duration of each leader term
     * - secondsUntilRotation: Time until next epoch transition
     * - electorateSize: Number of voting validators
     * - totalValidators: Total validators (voting + non-voting)
     * - nonVotingFollowers: List of validators on probation
     * - allValidators: List of all validator URLs
     * - nextLeader: Next leader for next epoch
     */
    public void handleGetConsensusStatus(HttpServletResponse response) throws IOException {
        consensusStatusHandler.handleGetConsensusStatus(response);
    }

    /**
     * Handle GET /v1/consensus/leader - Return canonical leader-resolution data,
     * optionally from local knowledge only (see {@link ConsensusStatusHandler}).
     */
    public void handleGetConsensusLeader(boolean localOnly, HttpServletResponse response) throws IOException {
        consensusStatusHandler.handleGetConsensusLeader(localOnly, response);
    }
    
    /**
     * Get proposal status.
     * GET /v1/proposals/{proposalId}/status
     */
    public void handleGetProposalStatus(HttpServletRequest request, HttpServletResponse response) throws IOException {
        proposalQueryHandler.handleGetProposalStatus(request, response);
    }

    /**
     * Get ops.v1 operation status (adapter over proposal status).
     * GET /v1/ops/operations/{operationId}
     */
    public void handleGetOperationStatus(HttpServletRequest request, HttpServletResponse response) throws IOException {
        proposalQueryHandler.handleGetOperationStatus(request, response);
    }

    /**
     * Get basic settlement details by proposal id.
     * GET /v1/settlement/proposals/{proposalId}
     */
    public void handleGetSettlementByProposalId(HttpServletRequest request, HttpServletResponse response) throws IOException {
        proposalQueryHandler.handleGetSettlementByProposalId(request, response);
    }

    /**
     * Get basic settlement details by transaction hash.
     * GET /v1/settlement/transactions/{transactionHash}
     */
    public void handleGetSettlementByTransactionHash(HttpServletRequest request, HttpServletResponse response) throws IOException {
        proposalQueryHandler.handleGetSettlementByTransactionHash(request, response);
    }
    
    /**
     * Get pending proposals count.
     * GET /v1/proposals/pending/count
     */
    public void handleGetPendingCount(HttpServletResponse response) throws IOException {
        proposalQueryHandler.handleGetPendingCount(response);
    }

    public void handleGetQueueStats(HttpServletResponse response) throws IOException {
        proposalQueryHandler.handleGetQueueStats(response);
    }

    /**
     * Get adaptive verified-release flow.
     * GET /v1/proposals/release-flow
     */
    public void handleGetProposalReleaseFlow(HttpServletResponse response) throws IOException {
        proposalQueryHandler.handleGetProposalReleaseFlow(response);
    }

    /**
     * Get ops.v1 queue snapshot with freshness/degraded metadata.
     * GET /v1/ops/snapshots/queue
     */
    public void handleGetOpsQueueSnapshot(HttpServletResponse response) throws IOException {
        proposalQueryHandler.handleGetOpsQueueSnapshot(response);
    }
    
    /**
     * ✈️ AERON NATIVE: Apply replicated write to FileStore.
     * This is called from AeronConsensusEngine.onSessionMessage() after Aeron replicates the write.
     * 
     * <p>Delegates to {@link WriteApplicationService} for the actual write application.
     * 
     * @param ipfsCid IPFS CID from client-side upload (ADR 016), may be null
     */
    public void applyReplicatedWrite(String walletAddress, String path, String contentType,
                                     String message, String signature, String intentToken,
                                     String blobId, String mimeType, String ipfsCid,
                                     String proposalId) {
        applyReplicatedWriteWithAuditMetadata(
            walletAddress,
            path,
            contentType,
            message,
            signature,
            intentToken,
            blobId,
            mimeType,
            ipfsCid,
            MutationAuditMetadata.write(null, null, proposalId, null, null, null, null)
        );
    }

    public void applyReplicatedWriteWithAuditMetadata(String walletAddress, String path, String contentType,
                                                      String message, String signature, String intentToken,
                                                      String blobId, String mimeType, String ipfsCid,
                                                      MutationAuditMetadata auditMetadata) {
        String proposalId = auditMetadata != null ? auditMetadata.getProposalId() : null;
        if (proposalId != null && context.aeronConsensusEngine != null && context.aeronConsensusEngine.isLeader()) {
            context.aeronConsensusEngine.sendQueueSegment(proposalId);
        }
        writeApplicationService.applyWriteWithAuditMetadata(
            walletAddress, path, contentType, message, signature,
            intentToken, blobId, mimeType, ipfsCid, auditMetadata
        );
    }
    
    /**
     * ✈️ AERON NATIVE: Apply replicated delete to FileStore.
     * This is called from AeronConsensusEngine.onSessionMessage() after Aeron replicates the delete.
     * 
     * <p>Delegates to {@link DeleteApplicationService} for the actual delete application.
     * 
     * Delete in Oak = Remove node from tree (writes new segment saying "path no longer exists")
     * Old segments remain until GC/compaction runs
     */
    public void applyReplicatedDelete(String walletAddress, String path, String signature, String proposalId) {
        applyReplicatedDeleteWithAuditMetadata(
            walletAddress,
            path,
            signature,
            MutationAuditMetadata.delete(null, null, proposalId, null, null, null, null)
        );
    }

    public void applyReplicatedDeleteWithAuditMetadata(String walletAddress, String path, String signature,
                                                       MutationAuditMetadata auditMetadata) {
        String proposalId = auditMetadata != null ? auditMetadata.getProposalId() : null;
        if (proposalId != null && context.aeronConsensusEngine != null && context.aeronConsensusEngine.isLeader()) {
            context.aeronConsensusEngine.sendQueueSegment(proposalId);
        }
        deleteApplicationService.applyDeleteWithAuditMetadata(walletAddress, path, signature, auditMetadata);
    }
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DEAD CODE REMOVED - January 2026 (tech debt cleanup)
    // - discoverLeaderFromPeerClusterState() - Use LeaderDiscoveryService instead
    // - resolveUrlToIP() - Only used by discoverLeaderFromPeerClusterState
    // - getNextValidatorInRotation() - Never called
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    /**
     * Handle GET /v1/gc/estimate - GC cost estimation endpoint
     * 
     * <p>Estimates the cost of garbage collection operations in USDC.
     * 
     * <p>Query Parameters:
     *   - revision: Optional target revision (null = use HEAD)
     * 
     * <p>Response: JSON with GC cost estimate
     *   - reclaimableSegmentCount: Number of reclaimable segments
     *   - reclaimableSizeBytes: Total size of reclaimable segments
     *   - reclaimableSizeMB: Total size in MB
     *   - reclaimablePercentage: Percentage of repository reclaimable
     *   - totalSegmentCount: Total number of segments
     *   - totalSizeBytes: Total repository size
     *   - totalSizeMB: Total size in MB
     *   - estimatedCostUSDC: Estimated cost in USDC
     *   - reclaimableByTarFile: Breakdown by TAR file
     */
    public void handleGCCostEstimate(HttpServletRequest request, HttpServletResponse response) throws IOException {
        gcCostHandler.handleGCCostEstimate(request, response);
    }
    
    /**
     * Track fragmentation: Check for new TAR files and associate with wallet address.
     */
    private void trackFragmentation(String walletAddress) {
        if (context.fragmentationTracker == null || walletAddress == null || walletAddress.isEmpty()) {
            return;
        }
        
        try {
            // Get current TAR files
            java.util.List<String> currentTarFiles = new java.util.ArrayList<>();
            try (java.util.stream.Stream<java.nio.file.Path> paths = java.nio.file.Files.list(context.storeDirectory)) {
                currentTarFiles = paths
                    .filter(p -> p.toString().endsWith(".tar"))
                    .map(p -> p.getFileName().toString())
                    .sorted()
                    .collect(java.util.stream.Collectors.toList());
            }
            
            // Find new TAR files (not yet tracked for this entity)
            java.util.List<String> entityTarFiles = context.fragmentationTracker.getTarFilesForEntity(walletAddress);
            java.util.Set<String> knownTarFiles = new java.util.HashSet<>(entityTarFiles);
            
            for (String tarFile : currentTarFiles) {
                if (!knownTarFiles.contains(tarFile)) {
                    // New TAR file - get its size
                    java.nio.file.Path tarPath = context.storeDirectory.resolve(tarFile);
                    long tarFileSize = java.nio.file.Files.exists(tarPath) 
                        ? java.nio.file.Files.size(tarPath) 
                        : 0;
                    
                    // Record write (this will create or update metrics)
                    context.fragmentationTracker.recordWrite(walletAddress, tarFile, tarFileSize);
                    
                    log.debug("📊 Fragmentation tracked: entity={}, tarFile={}, size={}", 
                        walletAddress, tarFile, tarFileSize);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to track fragmentation for entity {}: {}", walletAddress, e.getMessage());
        }
    }
    
    /**
     * Query wallet statistics - GET /v1/wallets/stats
     * Returns aggregated stats for all wallets or specific wallet
     */
    public void handleWalletStats(HttpServletRequest request, HttpServletResponse response) throws IOException {
        walletQueryHandler.handleWalletStats(request, response);
    }
    
    /**
     * Query content by wallet - GET /v1/wallets/{wallet}/content
     * Returns content items for a specific wallet
     */
    public void handleWalletContent(HttpServletRequest request, HttpServletResponse response) throws IOException {
        walletQueryHandler.handleWalletContent(request, response);
    }
    
}
