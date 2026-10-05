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
package org.apache.jackrabbit.oak.segment.consensus.service;

import org.agrona.concurrent.AgentTerminationException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.consensus.genesis.CanonicalGenesisContent;
import org.apache.jackrabbit.oak.segment.consensus.validation.MutationRejectedException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Service responsible for applying replicated deletes to the Oak FileStore.
 * 
 * <p>Extracted from ConsensusApiHandler to isolate delete application logic
 * and improve testability. This service is called from AeronConsensusEngine
 * after Aeron replicates the delete to all nodes.
 * 
 * <p><strong>Deterministic State Machine:</strong>
 * All nodes execute the same deletes in the same order, producing identical HEADs.
 * This is guaranteed by Aeron's Raft consensus.
 * 
 * <p><strong>Delete Semantics:</strong>
 * Delete in Oak = Remove node from tree (writes new segment saying "path no longer exists").
 * Old segments remain until GC/compaction runs.
 * 
 * @see org.apache.jackrabbit.oak.segment.consensus.validation.ContentDeleteProposal
 */
public class DeleteApplicationService {
    
    private static final Logger log = LoggerFactory.getLogger(DeleteApplicationService.class);
    private static final Pattern WALLET_NODE_NAME = Pattern.compile("0x[a-f0-9]{40}");
    
    private final FileStore fileStore;
    private final Supplier<NodeStore> nodeStoreSupplier;
    private final FileStoreFlushService flushService;
    
    // Optional callbacks for integration
    private HeadUpdateCallback headUpdateCallback;
    private SSEEventCallback sseEventCallback;
    private DurabilityCallback durabilityCallback;
    
    /**
     * Create a new DeleteApplicationService.
     * 
     * @param fileStore Oak FileStore for segment storage
     * @param nodeStore Oak NodeStore for content operations
     */
    public DeleteApplicationService(
            @NotNull FileStore fileStore,
            @NotNull NodeStore nodeStore,
            @NotNull FileStoreFlushService flushService) {
        this(fileStore, () -> nodeStore, flushService);
    }

    public DeleteApplicationService(
            @NotNull FileStore fileStore,
            @NotNull Supplier<NodeStore> nodeStoreSupplier,
            @NotNull FileStoreFlushService flushService) {
        this.fileStore = fileStore;
        this.nodeStoreSupplier = nodeStoreSupplier;
        this.flushService = flushService;
    }
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Callback Setters
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    public void setHeadUpdateCallback(HeadUpdateCallback callback) {
        this.headUpdateCallback = callback;
    }
    
    public void setSseEventCallback(SSEEventCallback callback) {
        this.sseEventCallback = callback;
    }

    public void setDurabilityCallback(DurabilityCallback callback) {
        this.durabilityCallback = callback;
    }
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Main Delete Application
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    /**
     * Apply a replicated delete to the FileStore.
     * 
     * <p>This method is called from AeronConsensusEngine.onSessionMessage() after
     * Aeron replicates the delete to all nodes via Raft consensus.
     * 
     * <p><strong>Deterministic:</strong> All nodes execute this identically,
     * producing the same HEAD.
     * 
     * <p><strong>Idempotent:</strong> Deleting a non-existent path is not an error.
     * 
     * @param walletAddress Ethereum wallet address (normalized)
     * @param path Full Oak path to delete
     * @param signature Cryptographic signature
     * @param proposalId Proposal ID for durability tracking (ADR 026)
     * @return The new HEAD after the delete, or null if path didn't exist
     * @throws RuntimeException if delete fails
     */
    @Nullable
    public String applyDelete(
            @NotNull String walletAddress,
            @NotNull String path,
            @Nullable String signature,
            @Nullable String proposalId) {
        return applyDeleteWithAuditMetadata(
            walletAddress,
            path,
            signature,
            MutationAuditMetadata.delete(null, null, proposalId, null, null, null, null)
        );
    }

    @Nullable
    public String applyDeleteWithAuditMetadata(
            @NotNull String walletAddress,
            @NotNull String path,
            @Nullable String signature,
            @Nullable MutationAuditMetadata auditMetadata) {
        String proposalId = auditMetadata != null ? auditMetadata.getProposalId() : null;
        
        try {
            CanonicalGenesisContent.requireMutable(walletAddress, path);
            log.info("🗑️  APPLYING REPLICATED DELETE: wallet={}, path={}", walletAddress, path);
            NodeStore nodeStore = requireNodeStore();
            
            // Get current HEAD for logging
            String previousHead = fileStore.getHead().getRecordId().toString();
            log.debug("📍 Previous HEAD: {}", truncate(previousHead, 20));

            if (signature == null) {
                throw new MutationRejectedException("Missing signature in replicated delete");
            }
            
            // Validate path format
            String[] pathParts = path.split("/");
            if (pathParts.length < 2) {
                log.error("❌ Invalid path format: {} (expected: /oak-chain/...)", path);
                throw new MutationRejectedException("Invalid path format: " + path);
            }
            
            // Build node structure and navigate to target
            NodeBuilder rootBuilder = nodeStore.getRoot().builder();
            NodeBuilder current = rootBuilder;
            
            // Navigate to parent of node to delete
            boolean pathExists = true;
            NodeBuilder walletNode = null;
            for (int i = 1; i < pathParts.length - 1; i++) {
                if (!pathParts[i].isEmpty()) {
                    if (!current.hasChildNode(pathParts[i])) {
                        log.warn("⚠️  Path does not exist: {} (stopping at segment: {})", path, pathParts[i]);
                        pathExists = false;
                        break;
                    }
                    current = current.getChildNode(pathParts[i]);
                    if (WALLET_NODE_NAME.matcher(pathParts[i]).matches()) {
                        walletNode = current;
                    }
                }
            }
            
            if (!pathExists) {
                log.warn("⚠️  Delete skipped - path doesn't exist: {}", path);
                // Not an error - idempotent delete (already gone)
                flushService.onChangeApplied(buildDurabilityCallback(proposalId));
                return null;
            }
            
            // Remove target node
            String targetNodeName = pathParts[pathParts.length - 1];
            if (current.hasChildNode(targetNodeName)) {
                current.getChildNode(targetNodeName).remove();
                log.info("✅ Node removed: {}", targetNodeName);
                if (walletNode != null) {
                    decrementContentCount(walletNode);
                }
            } else {
                log.warn("⚠️  Target node doesn't exist: {} (idempotent delete)", targetNodeName);
                // Not an error - already deleted
                flushService.onChangeApplied(buildDurabilityCallback(proposalId));
                return null;
            }
            
            // Commit the deletion (deterministic on all nodes)
            CommitInfo commitInfo = new CommitInfo(
                "aeron-replication-delete", 
                null, 
                Collections.singletonMap("replicated", "true")
            );
            
            if (auditMetadata != null && auditMetadata.getAppliedLogPosition() != null) {
                auditMetadata.getAppliedLogPosition().writeTo(rootBuilder);
            }
            try {
                nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, commitInfo);
            } catch (CommitFailedException e) {
                throw new RuntimeException("Failed to commit delete", e);
            }
            flushService.onChangeApplied(buildDurabilityCallback(proposalId));
            
            // Get new HEAD
            String newHead = fileStore.getHead().getRecordId().toString10();
            log.info("✅ DELETE applied, HEAD: {}...", truncate(newHead, 20));
            
            // Update HEAD cache
            if (headUpdateCallback != null) {
                headUpdateCallback.updateHead(newHead);
            }
            
            // Emit SSE delete event
            if (sseEventCallback != null) {
                String extractedOrg = extractOrganizationFromPath(path);
                sseEventCallback.emitContentDelete(path, walletAddress, extractedOrg, signature);
            }
            
            log.info("✅ Deterministic delete applied successfully - old segments remain until GC");
            return newHead;
            
        } catch (AgentTerminationException e) {
            throw e;
        } catch (Exception e) {
            if (durabilityCallback != null && proposalId != null && !proposalId.isEmpty()) {
                durabilityCallback.onFailure(proposalId, e.getMessage());
            }
            log.error("❌ Failed to apply replicated delete", e);
            if (e instanceof MutationRejectedException) {
                throw new MutationRejectedException("Failed to apply replicated delete", e);
            }
            throw new RuntimeException("Failed to apply replicated delete", e);
        }
    }

    /**
     * Mirrors {@link WriteApplicationService}, which counts a content node when it is created. {@code totalWrites}
     * stays a monotonic count of applied writes.
     */
    private static void decrementContentCount(NodeBuilder walletNode) {
        PropertyState contentCount = walletNode.getProperty("contentCount");
        if (contentCount != null) {
            walletNode.setProperty("contentCount", Math.max(0L, contentCount.getValue(Type.LONG) - 1L));
        }
    }

    private Runnable buildDurabilityCallback(String proposalId) {
        if (durabilityCallback == null || proposalId == null || proposalId.isEmpty()) {
            return null;
        }
        String appliedHead = fileStore.getHead().getRecordId().toString10();
        return () -> durabilityCallback.onDurable(proposalId, appliedHead);
    }

    @NotNull
    private NodeStore requireNodeStore() {
        NodeStore nodeStore = nodeStoreSupplier.get();
        if (nodeStore == null) {
            throw new IllegalStateException("NodeStore supplier returned null");
        }
        return nodeStore;
    }
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Helper Methods
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    /**
     * Extract organization from path (ADR 037).
     * 
     * <p>Path format: /oak-chain/XX/YY/ZZ/0xWALLET/{organization}/content/{contentId}
     */
    @Nullable
    private String extractOrganizationFromPath(@Nullable String path) {
        if (path == null || path.isEmpty()) {
            return null;
        }
        
        String[] parts = path.split("/");
        // Path: ["", "oak-chain", "XX", "YY", "ZZ", "0xWALLET", "Organization", "content", "contentId"]
        // Index:  0       1         2     3     4        5            6            7          8
        
        if (parts.length < 8) {
            return null;
        }
        
        String potentialOrg = parts[6];
        if (!"content".equals(potentialOrg) && !potentialOrg.startsWith("0x")) {
            return potentialOrg;
        }
        
        return null;
    }
    
    /**
     * Truncate string for logging.
     */
    private static String truncate(String value, int maxLength) {
        if (value == null) return "null";
        if (value.length() <= maxLength) return value;
        return value.substring(0, maxLength) + "...";
    }
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Callback Interfaces
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    /**
     * Callback for HEAD updates.
     */
    @FunctionalInterface
    public interface HeadUpdateCallback {
        void updateHead(String newHead);
    }
    
    /**
     * Callback for SSE events.
     */
    @FunctionalInterface
    public interface SSEEventCallback {
        void emitContentDelete(String path, String wallet, String org, String signature);
    }

    /**
     * Callback for durability confirmation (ADR 026).
     */
    public interface DurabilityCallback {
        void onDurable(String proposalId, String durableHead);
        void onFailure(String proposalId, String error);
    }
}
