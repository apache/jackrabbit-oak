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

import org.apache.jackrabbit.oak.segment.consensus.evm.EvmBridge;
import org.apache.jackrabbit.oak.segment.consensus.evm.PaymentProof;
import org.apache.jackrabbit.oak.segment.consensus.fragmentation.FragmentationTracker;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalQueuePolicy;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;
import java.util.function.Predicate;

/**
 * Manages GC proposals, voting, and execution coordination.
 * 
 * <p>Proposal, vote and execute state changes only by applying replicated log entries
 * ({@code applyReplicated*}, {@link #voteOnProposal}), using the entry's cluster timestamp and the
 * configured cluster membership, so every member decides alike. Physical cleanup is a node-local side
 * effect started at the GC_EXECUTE entry and run off the clustered service thread; payment verification
 * runs off-thread on the leader before it requests GC_EXECUTE.</p>
 */
public class GCProposalManager {
    
    private static final Logger log = LoggerFactory.getLogger(GCProposalManager.class);
    
    private final FileStore fileStore;
    private final GCCostEstimator gcCostEstimator;
    private final FragmentationTracker fragmentationTracker;
    private final EvmBridge evmBridge; // Ethereum bridge for payment verification
    private volatile IntSupplier clusterMembers; // configured cluster membership
    private volatile Predicate<String> executionRequester; // sends GC_EXECUTE through the log
    private final java.util.function.Supplier<Integer> executorIdSupplier; // Supplier for current executor ID
    private final java.util.function.Supplier<Boolean> isLeaderSupplier; // Supplier to check if this node is leader
    
    // In-memory state (for POC)
    private final Map<String, GCProposal> proposals = new ConcurrentHashMap<>();
    private final List<GCExecutionResult> gcHistory = new ArrayList<>();
    private final java.util.concurrent.ExecutorService executionExecutor; // Background executor for GC
    private final java.util.concurrent.ScheduledExecutorService scheduledExecutor; // Scheduled executor for retries
    
    public GCProposalManager(FileStore fileStore, GCCostEstimator gcCostEstimator, 
                            FragmentationTracker fragmentationTracker, int totalValidators) {
        this(fileStore, gcCostEstimator, fragmentationTracker, null, totalValidators, () -> 0, () -> true);
    }
    
    public GCProposalManager(FileStore fileStore, GCCostEstimator gcCostEstimator, 
                            FragmentationTracker fragmentationTracker, int totalValidators,
                            java.util.function.Supplier<Integer> executorIdSupplier) {
        this(fileStore, gcCostEstimator, fragmentationTracker, null, totalValidators, executorIdSupplier, () -> true);
    }
    
    public GCProposalManager(FileStore fileStore, GCCostEstimator gcCostEstimator, 
                            FragmentationTracker fragmentationTracker, int totalValidators,
                            java.util.function.Supplier<Integer> executorIdSupplier,
                            java.util.function.Supplier<Boolean> isLeaderSupplier) {
        this(fileStore, gcCostEstimator, fragmentationTracker, null, totalValidators, executorIdSupplier, isLeaderSupplier);
    }
    
    public GCProposalManager(FileStore fileStore, GCCostEstimator gcCostEstimator, 
                            FragmentationTracker fragmentationTracker, EvmBridge evmBridge, int totalValidators,
                            java.util.function.Supplier<Integer> executorIdSupplier,
                            java.util.function.Supplier<Boolean> isLeaderSupplier) {
        this.fileStore = fileStore;
        this.gcCostEstimator = gcCostEstimator;
        this.fragmentationTracker = fragmentationTracker;
        this.evmBridge = evmBridge;
        this.clusterMembers = () -> totalValidators;
        this.executorIdSupplier = executorIdSupplier != null ? executorIdSupplier : () -> 0;
        this.isLeaderSupplier = isLeaderSupplier != null ? isLeaderSupplier : () -> true;
        // Single-threaded executor for GC execution (GC should not run concurrently)
        this.executionExecutor = java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "GC-Execution-Thread");
            t.setDaemon(true);
            return t;
        });
        // Scheduled executor for payment retry checks
        this.scheduledExecutor = java.util.concurrent.Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "GC-Payment-Retry-Thread");
            t.setDaemon(true);
            return t;
        });
        log.info("GCProposalManager initialized: totalValidators={}, quorumSize={}, evmBridge={}", 
            totalValidators, quorumSize(), evmBridge != null ? "enabled" : "disabled");
    }

    /**
     * Bases vote quorum on the configured cluster membership.
     */
    public void setClusterMembership(IntSupplier clusterMembers) {
        this.clusterMembers = clusterMembers;
    }

    /**
     * Sets how an approved, paid proposal is turned into a replicated GC_EXECUTE entry.
     */
    public void setExecutionRequester(Predicate<String> executionRequester) {
        this.executionRequester = executionRequester;
    }

    private int totalValidators() {
        return clusterMembers.getAsInt();
    }

    private int quorumSize() {
        return (totalValidators() * 2 / 3) + 1; // 2/3+ majority
    }
    

    /**
     * Prepare a GC proposal with its cost estimate. It becomes state only when its GC_PROPOSAL entry is
     * applied through {@link #applyReplicatedProposal}.
     */
    public GCProposal proposeGC(String proposerWallet, String targetRevision) throws IOException {
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("🗑️  Creating GC proposal");
        log.info("   Proposer: {}", proposerWallet);
        log.info("   Target revision: {}", targetRevision != null ? targetRevision : "HEAD");
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        
        // Calculate cost estimate
        GCCostEstimate estimate = gcCostEstimator.estimateCost(targetRevision);
        
        // Calculate fragmentation overhead
        Map<String, BigInteger> fragmentationCosts = calculateFragmentationOverhead(estimate);
        long fragmentationOverheadMB = fragmentationCosts.values().stream()
            .mapToLong(bi -> bi.divide(BigInteger.valueOf(1024 * 1024)).longValue())
            .sum();
        BigDecimal fragmentationCostUSDC = BigDecimal.valueOf(fragmentationOverheadMB)
            .multiply(new BigDecimal("0.10")); // $0.10 per MB (same as base GC cost)
        
        // Create proposal
        GCProposal proposal = new GCProposal();
        proposal.proposalId = UUID.randomUUID().toString();
        proposal.proposerWallet = proposerWallet;
        proposal.targetRevision = targetRevision;
        proposal.estimatedReclaimableSizeMB = estimate.getReclaimableSizeMB();
        proposal.estimatedCostUSDC = estimate.getEstimatedCostUSDC();
        proposal.fragmentationOverheadMB = fragmentationOverheadMB;
        proposal.fragmentationCostUSDC = fragmentationCostUSDC;
        proposal.state = GCProposal.GCProposalState.PENDING;
        
        log.info("✅ GC proposal prepared: {}", proposal.proposalId);
        log.info("   Estimated reclaimable: {} MB", proposal.estimatedReclaimableSizeMB);
        log.info("   Estimated cost: {} USDC", proposal.estimatedCostUSDC);
        log.info("   Fragmentation overhead: {} MB ({} USDC)", 
            fragmentationOverheadMB, fragmentationCostUSDC);
        log.info("   Quorum required: {}/{}", quorumSize(), totalValidators());
        
        return proposal;
    }

    /**
     * Apply a GC proposal that was replicated through consensus.
     *
     * <p>Used by Aeron message callbacks so every validator materializes the same
     * proposal ID and estimate values in local manager state.</p>
     */
    public GCProposal applyReplicatedProposal(String proposalId,
                                              String proposerWallet,
                                              String targetRevision,
                                              long estimatedReclaimableSizeMB,
                                              String estimatedCostUSDC,
                                              long clusterTime) {
        if (proposalId == null || proposalId.isEmpty()) {
            throw new IllegalArgumentException("proposalId is required");
        }
        if (proposerWallet == null || proposerWallet.isEmpty()) {
            throw new IllegalArgumentException("proposerWallet is required");
        }

        GCProposal existing = proposals.get(proposalId);
        if (existing != null) {
            return existing;
        }

        GCProposal proposal = new GCProposal();
        proposal.proposalId = proposalId;
        proposal.proposerWallet = proposerWallet;
        proposal.targetRevision = "HEAD".equals(targetRevision) ? null : targetRevision;
        proposal.estimatedReclaimableSizeMB = Math.max(estimatedReclaimableSizeMB, 0L);
        try {
            proposal.estimatedCostUSDC = new BigDecimal(
                estimatedCostUSDC != null && !estimatedCostUSDC.isEmpty() ? estimatedCostUSDC : "0");
        } catch (Exception e) {
            log.warn("Invalid estimatedCostUSDC on replicated proposal {}: {}", proposalId, estimatedCostUSDC);
            proposal.estimatedCostUSDC = BigDecimal.ZERO;
        }
        proposal.fragmentationOverheadMB = 0L;
        proposal.fragmentationCostUSDC = BigDecimal.ZERO;
        proposal.state = GCProposal.GCProposalState.PENDING;
        proposal.createdAt = clusterTime;
        proposal.expiresAt = clusterTime + GCProposal.DEFAULT_TTL_MS;

        proposals.put(proposalId, proposal);
        log.info("✅ Applied replicated GC proposal: {} (wallet={}, reclaimableMB={}, costUSDC={})",
            proposalId, proposerWallet, proposal.estimatedReclaimableSizeMB, proposal.estimatedCostUSDC);
        return proposal;
    }
    
    /**
     * Apply a replicated GC vote stamped with its log entry's cluster time.
     */
    public void voteOnProposal(String proposalId, int validatorId, boolean approve, String reason, long clusterTime) {
        GCProposal proposal = proposals.get(proposalId);
        if (proposal == null) {
            log.warn("⚠️  GC proposal not found: {}", proposalId);
            return;
        }

        if (validatorId < 0 || validatorId >= totalValidators()) {
            log.warn("⚠️  GC vote from validator {} outside the cluster membership ignored: {}", validatorId, proposalId);
            return;
        }
        
        if (proposal.isExpiredAt(clusterTime)) {
            log.warn("⚠️  GC proposal expired: {}", proposalId);
            return;
        }
        
        if (proposal.state != GCProposal.GCProposalState.PENDING && proposal.state != GCProposal.GCProposalState.VOTING) {
            log.warn("⚠️  GC proposal not in votable state: {} (state: {})", proposalId, proposal.state);
            return;
        }
        
        // Check if validator already voted
        if (proposal.votes.containsKey(validatorId)) {
            log.warn("⚠️  Validator {} already voted on proposal {}", validatorId, proposalId);
            return;
        }
        
        proposal.addVote(validatorId, approve, reason, clusterTime);
        
        log.info("🗳️  Vote recorded: proposal={}, validator={}, approve={}, reason={}", 
            proposalId, validatorId, approve, reason);
        log.info("   Current votes: {}/{} (approve: {}, reject: {})", 
            proposal.getTotalVoteCount(), totalValidators(),
            proposal.getApproveVoteCount(), proposal.getRejectVoteCount());
        
        // Check for quorum
        int quorumSize = quorumSize();
        if (proposal.getApproveVoteCount() >= quorumSize) {
            proposal.state = GCProposal.GCProposalState.APPROVED;
            log.info("✅ GC proposal APPROVED: {} (quorum reached: {}/{})", 
                proposalId, proposal.getApproveVoteCount(), totalValidators());
            
            // Leader verifies payment off-thread, then requests GC_EXECUTE through the log
            scheduleGCExecution(proposalId);
        } else if (proposal.getRejectVoteCount() >= quorumSize) {
            proposal.state = GCProposal.GCProposalState.REJECTED;
            log.info("❌ GC proposal REJECTED: {} (quorum reached: {}/{})", 
                proposalId, proposal.getRejectVoteCount(), totalValidators());
        }
    }
    
    /**
     * The replicated state of every proposal, as JSON values for the Aeron snapshot. Called on the clustered
     * service thread, which is the only writer of everything captured except a finished execution's state.
     */
    public List<Map<String, Object>> snapshotProposals() {
        List<Map<String, Object>> entries = new ArrayList<>();
        for (GCProposal proposal : proposals.values()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("proposalId", proposal.proposalId);
            entry.put("proposerWallet", proposal.proposerWallet);
            entry.put("targetRevision", proposal.targetRevision);
            entry.put("estimatedReclaimableSizeMB", proposal.estimatedReclaimableSizeMB);
            entry.put("estimatedCostUSDC", proposal.estimatedCostUSDC.toPlainString());
            entry.put("createdAt", proposal.createdAt);
            entry.put("expiresAt", proposal.expiresAt);
            entry.put("state", proposal.state.name());
            List<Object> votes = new ArrayList<>();
            for (GCVote vote : proposal.votes.values()) {
                Map<String, Object> json = new LinkedHashMap<>();
                json.put("validatorId", vote.validatorId);
                json.put("approve", vote.approve);
                json.put("reason", vote.reason);
                json.put("timestamp", vote.timestamp);
                votes.add(json);
            }
            entry.put("votes", votes);
            entries.add(entry);
        }
        return entries;
    }

    /**
     * Replaces the proposals with those of {@link #snapshotProposals()}. Never schedules or starts execution.
     */
    public void restoreProposals(List<Map<String, Object>> entries) {
        proposals.clear();
        for (Map<String, Object> entry : entries) {
            GCProposal proposal = new GCProposal();
            proposal.proposalId = (String) entry.get("proposalId");
            proposal.proposerWallet = (String) entry.get("proposerWallet");
            proposal.targetRevision = (String) entry.get("targetRevision");
            proposal.estimatedReclaimableSizeMB = ((Number) entry.get("estimatedReclaimableSizeMB")).longValue();
            proposal.estimatedCostUSDC = new BigDecimal((String) entry.get("estimatedCostUSDC"));
            proposal.fragmentationCostUSDC = BigDecimal.ZERO;
            proposal.createdAt = ((Number) entry.get("createdAt")).longValue();
            proposal.expiresAt = ((Number) entry.get("expiresAt")).longValue();
            for (Object json : (List<?>) entry.get("votes")) {
                Map<?, ?> vote = (Map<?, ?>) json;
                proposal.addVote(((Number) vote.get("validatorId")).intValue(), (Boolean) vote.get("approve"),
                    (String) vote.get("reason"), ((Number) vote.get("timestamp")).longValue());
            }
            proposal.state = GCProposal.GCProposalState.valueOf((String) entry.get("state"));
            proposals.put(proposal.proposalId, proposal);
        }
        log.info("Restored {} GC proposals from the Aeron snapshot", proposals.size());
    }

    /**
     * Check if proposal has quorum.
     */
    public boolean hasQuorum(String proposalId) {
        GCProposal proposal = proposals.get(proposalId);
        if (proposal == null) {
            return false;
        }
        return proposal.getApproveVoteCount() >= quorumSize();
    }
    
    /**
     * Verify payment for GC proposal via EvmBridge.
     * 
     * @param proposalId the proposal ID
     * @return true if payment verified, false otherwise
     */
    public boolean verifyPayment(String proposalId) {
        if (evmBridge == null) {
            log.warn("⚠️  EvmBridge not configured - payment verification skipped (POC mode)");
            return true; // Allow execution in POC mode without EvmBridge
        }
        
        GCProposal proposal = proposals.get(proposalId);
        if (proposal == null) {
            log.warn("⚠️  GC proposal not found: {}", proposalId);
            return false;
        }
        int requiredConfirmations = ProposalQueuePolicy.requiredConfirmations();
        
        // If payment proof already stored, verify it's still valid
        if (proposal.paymentProof != null && !proposal.paymentProof.isEmpty()) {
            PaymentProof proof = evmBridge.verifyPayment(proposalId);
            if (proof != null && proof.isConfirmed(requiredConfirmations)) {
                log.info("✅ Payment proof verified: proposal={}, tx={}, block={}", 
                    proposalId, proof.getTransactionHash(), proof.getBlockNumber());
                return true;
            }
        }
        
        // Verify payment on-chain
        PaymentProof proof = evmBridge.verifyPayment(proposalId);
        if (proof != null && proof.isConfirmed(requiredConfirmations)) {
            // Store payment proof in proposal
            proposal.paymentProof = proof.getTransactionHash();
            log.info("✅ Payment verified on-chain: proposal={}, tx={}, block={}", 
                proposalId, proof.getTransactionHash(), proof.getBlockNumber());
            return true;
        } else {
            log.warn("⚠️  Payment not verified for GC proposal: {} (proof={})", 
                proposalId, proof != null ? "exists but not confirmed" : "not found");
            return false;
        }
    }
    
    /**
     * Apply a replicated GC_EXECUTE entry: an approved proposal moves to EXECUTING on every member, and
     * each member starts cleaning up its own store on the GC execution thread. Never blocks the caller.
     *
     * @return whether this entry started execution (false for unknown, unapproved or already executing)
     */
    public boolean applyReplicatedExecute(String proposalId, int executorId) {
        GCProposal proposal = proposals.get(proposalId);
        if (proposal == null || proposal.state != GCProposal.GCProposalState.APPROVED) {
            log.warn("⚠️  GC_EXECUTE for {} ignored (state: {})", proposalId, proposal != null ? proposal.state : "unknown");
            return false;
        }
        proposal.state = GCProposal.GCProposalState.EXECUTING;
        executionExecutor.submit(() -> {
            try {
                executeGC(proposalId, executorId);
            } catch (Exception e) {
                log.error("❌ GC cleanup failed for proposal {}", proposalId, e);
            }
        });
        return true;
    }

    /**
     * Node-local physical cleanup of an approved or executing proposal.
     */
    GCExecutionResult executeGC(String proposalId, int executorId) throws IOException {
        GCProposal proposal = proposals.get(proposalId);
        if (proposal == null) {
            throw new IllegalArgumentException("GC proposal not found: " + proposalId);
        }
        
        if (proposal.state != GCProposal.GCProposalState.APPROVED
                && proposal.state != GCProposal.GCProposalState.EXECUTING) {
            throw new IllegalStateException(
                String.format("GC proposal not approved: %s (state: %s)", proposalId, proposal.state));
        }
        
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("🗑️  Executing GC: {}", proposalId);
        log.info("   Executor: {}", executorId);
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        
        proposal.state = GCProposal.GCProposalState.EXECUTING;
        
        try {
            // Execute GC
            // Note: FileStore.cleanup() returns void, so we track files before/after
            // For POC, we'll use an empty list and estimate based on stats
            fileStore.cleanup();
            List<String> removedFiles = new ArrayList<>(); // FileStore.cleanup() doesn't return removed files
            
            // Calculate actual results
            GCExecutionResult result = new GCExecutionResult();
            result.proposalId = proposalId;
            result.executorId = executorId;
            result.filesRemoved = removedFiles;
            result.actualReclaimedSizeMB = calculateActualReclaimedSize(removedFiles);
            result.actualCostUSDC = calculateActualCost(result.actualReclaimedSizeMB);
            result.timestamp = System.currentTimeMillis();
            result.success = true;
            
            // Update fragmentation metrics after GC
            updateFragmentationMetricsAfterGC(removedFiles);
            
            // Set state to COMPLETED
            proposal.state = GCProposal.GCProposalState.COMPLETED;
            proposal.executionResult = result;
            
            // Add to history
            gcHistory.add(result);
            
            log.info("✅ GC execution completed: {}", proposalId);
            log.info("   Files removed: {}", removedFiles.size());
            log.info("   Reclaimed: {} MB", result.actualReclaimedSizeMB);
            log.info("   Cost: {} USDC", result.actualCostUSDC);
            
            return result;
        } catch (Exception e) {
            proposal.state = GCProposal.GCProposalState.FAILED;
            GCExecutionResult result = new GCExecutionResult();
            result.proposalId = proposalId;
            result.executorId = executorId;
            result.timestamp = System.currentTimeMillis();
            result.success = false;
            result.errorMessage = e.getMessage();
            proposal.executionResult = result;
            
            log.error("❌ GC execution failed: {}", proposalId, e);
            throw new IOException("GC execution failed", e);
        }
    }
    
    /**
     * Get pending proposals.
     */
    public List<GCProposal> getPendingProposals() {
        List<GCProposal> pending = new ArrayList<>();
        for (GCProposal proposal : proposals.values()) {
            if (proposal.state == GCProposal.GCProposalState.PENDING || 
                proposal.state == GCProposal.GCProposalState.VOTING ||
                proposal.state == GCProposal.GCProposalState.APPROVED) {
                pending.add(proposal);
            }
        }
        pending.sort((a, b) -> Long.compare(b.createdAt, a.createdAt)); // Newest first
        return pending;
    }
    
    /**
     * Get GC history.
     */
    public List<GCExecutionResult> getGCHistory(int limit) {
        List<GCExecutionResult> history = new ArrayList<>(gcHistory);
        history.sort((a, b) -> Long.compare(b.timestamp, a.timestamp)); // Newest first
        return history.subList(0, Math.min(limit, history.size()));
    }
    
    /**
     * On the leader, verify payment on the GC execution thread and request GC_EXECUTE through the log.
     * Nothing here changes replicated state.
     */
    private void scheduleGCExecution(String proposalId) {
        executionExecutor.submit(() -> {
            try {
                GCProposal proposal = proposals.get(proposalId);
                if (proposal == null) {
                    log.warn("⚠️  GC proposal not found for execution: {}", proposalId);
                    return;
                }
                
                // Double-check state (might have changed)
                if (proposal.state != GCProposal.GCProposalState.APPROVED) {
                    log.debug("GC proposal {} not in APPROVED state, skipping execution: {}", proposalId, proposal.state);
                    return;
                }
                
                // Only execute on leader (to avoid duplicate executions across cluster)
                // In single-node setups, isLeaderSupplier returns true
                if (!isLeaderSupplier.get()) {
                    log.debug("GC proposal {} approved, but this node is not leader - skipping execution", proposalId);
                    return;
                }
                
                // 🔒 CRITICAL: Verify payment before auto-execution (tokenomics requirement)
                if (!verifyPayment(proposalId)) {
                    log.warn("⚠️  GC proposal {} approved but payment not verified - waiting for payment", proposalId);
                    // Schedule retry in 10 seconds (payment might be pending)
                    scheduledExecutor.schedule(() -> {
                        GCProposal retryProposal = proposals.get(proposalId);
                        if (retryProposal != null && retryProposal.state == GCProposal.GCProposalState.APPROVED) {
                            scheduleGCExecution(proposalId); // Retry
                        }
                    }, 10, java.util.concurrent.TimeUnit.SECONDS);
                    return;
                }
                
                Predicate<String> requester = executionRequester;
                if (requester == null || !requester.test(proposalId)) {
                    log.warn("⚠️  GC proposal {} approved and paid, but GC_EXECUTE was not sent - retrying in 10s", proposalId);
                    scheduledExecutor.schedule(() -> scheduleGCExecution(proposalId), 10, java.util.concurrent.TimeUnit.SECONDS);
                    return;
                }
                log.info("🚀 Requested GC_EXECUTE for proposal {} (executor: {}, payment verified)",
                    proposalId, executorIdSupplier.get());
            } catch (Exception e) {
                log.error("❌ Error requesting execution of GC proposal {}", proposalId, e);
            }
        });
    }
    
    /**
     * Shutdown executors (for cleanup).
     */
    public void shutdown() {
        if (executionExecutor != null && !executionExecutor.isShutdown()) {
            executionExecutor.shutdown();
            try {
                if (!executionExecutor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)) {
                    executionExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executionExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (scheduledExecutor != null && !scheduledExecutor.isShutdown()) {
            scheduledExecutor.shutdown();
            try {
                if (!scheduledExecutor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)) {
                    scheduledExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduledExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
    
    /**
     * Get proposal by ID.
     */
    public GCProposal getProposal(String proposalId) {
        return proposals.get(proposalId);
    }
    
    /**
     * Calculate fragmentation overhead costs.
     */
    private Map<String, BigInteger> calculateFragmentationOverhead(GCCostEstimate estimate) {
        Map<String, BigInteger> costs = new HashMap<>();
        
        if (fragmentationTracker == null) {
            return costs;
        }
        
        // Get all entities with fragmentation
        Map<String, FragmentationTracker.EntityFragmentationMetrics> allMetrics = 
            fragmentationTracker.getAllMetrics();
        
        if (allMetrics.isEmpty()) {
            return costs;
        }
        
        // Calculate total fragmentation score
        long totalFragmentationScore = allMetrics.values().stream()
            .mapToLong(m -> m.fragmentationScore)
            .sum();
        
        if (totalFragmentationScore == 0) {
            return costs;
        }
        
        // Estimate fragmentation overhead as percentage of GC cost
        // Higher fragmentation = more TAR files = higher GC overhead
        BigDecimal fragmentationOverheadMultiplier = BigDecimal.valueOf(0.1); // 10% overhead
        
        // Distribute overhead proportionally to fragmentation score
        for (FragmentationTracker.EntityFragmentationMetrics metrics : allMetrics.values()) {
            if (metrics.fragmentationScore > 0) {
                double proportion = (double) metrics.fragmentationScore / totalFragmentationScore;
                BigDecimal entityOverhead = estimate.getEstimatedCostUSDC()
                    .multiply(fragmentationOverheadMultiplier)
                    .multiply(BigDecimal.valueOf(proportion));
                
                // Convert to Wei (18 decimals)
                BigInteger entityCostWei = entityOverhead
                    .multiply(BigDecimal.valueOf(10).pow(18))
                    .toBigInteger();
                
                costs.put(metrics.walletAddress, entityCostWei);
            }
        }
        
        return costs;
    }
    
    /**
     * Calculate actual reclaimed size from removed files.
     */
    private long calculateActualReclaimedSize(List<String> removedFiles) {
        // For POC, estimate based on number of files
        // In production, would calculate actual size from TAR files
        return removedFiles.size() * 256L; // Assume average 256 MB per TAR file
    }
    
    /**
     * Calculate actual cost based on reclaimed size.
     */
    private BigDecimal calculateActualCost(long reclaimedSizeMB) {
        return BigDecimal.valueOf(reclaimedSizeMB).multiply(new BigDecimal("0.10")); // $0.10 per MB
    }
    
    /**
     * Update fragmentation metrics after GC execution.
     */
    private void updateFragmentationMetricsAfterGC(List<String> removedFiles) {
        if (fragmentationTracker == null) {
            return;
        }
        
        // Find entities that created the removed TAR files
        Set<String> affectedEntities = new HashSet<>();
        for (String tarFile : removedFiles) {
            String entity = fragmentationTracker.getEntityForTarFile(tarFile);
            if (entity != null) {
                affectedEntities.add(entity);
            }
        }
        
        // Reset metrics for affected entities (TAR files compacted)
        for (String entity : affectedEntities) {
            fragmentationTracker.resetMetrics(entity);
            log.debug("📊 Reset fragmentation metrics for entity: {} (TAR files compacted)", entity);
        }
    }
}
