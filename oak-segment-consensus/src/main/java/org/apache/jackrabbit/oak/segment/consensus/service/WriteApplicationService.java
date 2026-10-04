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
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.segment.consensus.config.IpfsGatewayUrls;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.server.util.JsonParser;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Service responsible for applying replicated writes to the Oak FileStore.
 * 
 * <p>Extracted from ConsensusApiHandler to isolate write application logic
 * and improve testability. This service is called from AeronConsensusEngine
 * after Aeron replicates the write to all nodes.
 * 
 * <p><strong>Deterministic State Machine:</strong>
 * All nodes execute the same writes in the same order, producing identical HEADs.
 * This is guaranteed by Aeron's Raft consensus.
 * 
 * <p><strong>Responsibilities:</strong>
 * <ul>
 *   <li>Apply write proposals to Oak NodeStore</li>
 *   <li>Handle binary content with BlobStore</li>
 *   <li>Enrich wallet nodes with metadata</li>
 *   <li>Extract organization from paths (ADR 037)</li>
 *   <li>Store IPFS CIDs (ADR 016)</li>
 *   <li>Handle intent tokens for lazy uploads (ADR 020)</li>
 * </ul>
 * 
 * @see org.apache.jackrabbit.oak.segment.consensus.validation.ContentWriteProposal
 */
public class WriteApplicationService {
    
    private static final Logger log = LoggerFactory.getLogger(WriteApplicationService.class);
    private static final String PROPOSAL_ID_PROPERTY = "oak:proposalId";
    
    private final FileStore fileStore;
    private final Supplier<NodeStore> nodeStoreSupplier;
    private final Supplier<BlobStore> blobStoreSupplier;
    private final FileStoreFlushService flushService;
    
    // Optional callbacks for integration
    private HeadUpdateCallback headUpdateCallback;
    private SSEEventCallback sseEventCallback;
    private FragmentationCallback fragmentationCallback;
    private CidMappingCallback cidMappingCallback;
    private DurabilityCallback durabilityCallback;
    
    /**
     * Create a new WriteApplicationService.
     * 
     * @param fileStore Oak FileStore for segment storage
     * @param nodeStore Oak NodeStore for content operations
     * @param blobStore BlobStore for binary content (may be null)
     */
    public WriteApplicationService(
            @NotNull FileStore fileStore,
            @NotNull NodeStore nodeStore,
            @Nullable BlobStore blobStore,
            @NotNull FileStoreFlushService flushService) {
        this(fileStore, () -> nodeStore, () -> blobStore, flushService);
    }

    public WriteApplicationService(
            @NotNull FileStore fileStore,
            @NotNull Supplier<NodeStore> nodeStoreSupplier,
            @NotNull Supplier<BlobStore> blobStoreSupplier,
            @NotNull FileStoreFlushService flushService) {
        this.fileStore = fileStore;
        this.nodeStoreSupplier = nodeStoreSupplier;
        this.blobStoreSupplier = blobStoreSupplier;
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
    
    public void setFragmentationCallback(FragmentationCallback callback) {
        this.fragmentationCallback = callback;
    }
    
    public void setCidMappingCallback(CidMappingCallback callback) {
        this.cidMappingCallback = callback;
    }

    public void setDurabilityCallback(DurabilityCallback callback) {
        this.durabilityCallback = callback;
    }
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Main Write Application
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    /**
     * Apply a replicated write to the FileStore.
     * 
     * <p>This method is called from AeronConsensusEngine.onSessionMessage() after
     * Aeron replicates the write to all nodes via Raft consensus.
     * 
     * <p><strong>Deterministic:</strong> All nodes execute this identically,
     * producing the same HEAD.
     * 
     * @param walletAddress Ethereum wallet address (normalized)
     * @param path Full Oak path for the content
     * @param contentType Content type (e.g., "page", "asset")
     * @param message Content message/body
     * @param signature Cryptographic signature (required)
     * @param intentToken Intent token for lazy uploads (ADR 020)
     * @param blobId Blob ID for binary content
     * @param mimeType MIME type for binary content
     * @param ipfsCid IPFS CID from client-side upload (ADR 016)
     * @param proposalId Proposal ID for durability tracking (ADR 026)
     * @return The new HEAD after the write
     * @throws IllegalStateException if signature is null (security violation)
     * @throws RuntimeException if write fails
     */
    @NotNull
    public String applyWrite(
            @NotNull String walletAddress,
            @NotNull String path,
            @Nullable String contentType,
            @Nullable String message,
            @NotNull String signature,
            @Nullable String intentToken,
            @Nullable String blobId,
            @Nullable String mimeType,
            @Nullable String ipfsCid,
            @Nullable String proposalId) {
        return applyWriteWithAuditMetadata(
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

    @NotNull
    public String applyWriteWithAuditMetadata(
            @NotNull String walletAddress,
            @NotNull String path,
            @Nullable String contentType,
            @Nullable String message,
            @NotNull String signature,
            @Nullable String intentToken,
            @Nullable String blobId,
            @Nullable String mimeType,
            @Nullable String ipfsCid,
            @Nullable MutationAuditMetadata auditMetadata) {
        String proposalId = auditMetadata != null ? auditMetadata.getProposalId() : null;
        Long clusterTimestamp = auditMetadata != null ? auditMetadata.getClusterTimestamp() : null;
        long appliedAt = clusterTimestamp != null ? clusterTimestamp : System.currentTimeMillis();
        
        try {
            log.debug("✈️  APPLYING REPLICATED WRITE: wallet={}, path={}, intentToken={}, blobId={}, ipfsCid={}", 
                     walletAddress, path, intentToken, blobId, ipfsCid);
            NodeStore nodeStore = requireNodeStore();
            BlobStore blobStore = blobStoreSupplier.get();
            
            // Get current HEAD for logging
            String previousHead = fileStore.getHead().getRecordId().toString();
            log.debug("📍 Previous HEAD: {}", truncate(previousHead, 20));
            
            // Validate path format
            String[] pathParts = path.split("/");
            if (pathParts.length < 4) {
                log.error("❌ Invalid path format: {} (expected: /oak-chain/{shard}/content/...)", path);
                throw new IllegalArgumentException("Invalid path format: " + path);
            }
            
            // Security check: signature must not be null
            if (signature == null) {
                throw new IllegalStateException(
                    "SECURITY VIOLATION: Signature is null in replicated write. " +
                    "This indicates Aeron message corruption or validation bypass. " +
                    "Path: " + path + ", Wallet: " + walletAddress
                );
            }
            
            // Build node structure
            NodeBuilder rootBuilder = nodeStore.getRoot().builder();
            NodeBuilder current = rootBuilder;
            
            // Navigate to parent path, tracking wallet node
            NodeBuilder walletNode = null;
            String walletNodeName = null;
            
            for (int i = 1; i < pathParts.length - 1; i++) {
                if (!pathParts[i].isEmpty()) {
                    current = current.child(pathParts[i]);
                    
                    // Detect wallet node (matches pattern: 0x[a-f0-9]{40})
                    if (pathParts[i].matches("0x[a-f0-9]{40}")) {
                        walletNode = current;
                        walletNodeName = pathParts[i];
                    }
                }
            }
            
            // Create content node
            String contentId = pathParts[pathParts.length - 1];
            boolean contentNodeExists = current.hasChildNode(contentId);
            NodeBuilder contentNode = current.child(contentId);

            if (isDuplicateProposalReplay(contentNode, contentNodeExists, proposalId)) {
                return acknowledgeDuplicateReplay(proposalId);
            }

            // Enrich wallet node with metadata
            if (walletNode != null && walletNodeName != null) {
                enrichWalletNode(walletNode, walletNodeName, walletAddress, !contentNodeExists, appliedAt);
            }
            
            // Set properties
            setContentProperties(contentNode, walletAddress, contentType, message, signature, path, proposalId, appliedAt);
            
            // ADR 059: Record binary storage mode (client vs validator)
            if (blobId != null && !blobId.isEmpty()) {
                contentNode.setProperty("oak:binaryStorageMode", "validator");
            } else if (ipfsCid != null && !ipfsCid.isEmpty()) {
                contentNode.setProperty("oak:binaryStorageMode", "client");
            }

            // Handle binary content
            if (blobId != null && !blobId.isEmpty()) {
                handleBinaryContent(contentNode, blobStore, blobId, mimeType, ipfsCid);
            } else if (ipfsCid != null && !ipfsCid.isEmpty()) {
                // Pure IPFS reference without local blob
                contentNode.setProperty("ipfsCid", ipfsCid);
                contentNode.setProperty("ipfsGateway", IpfsGatewayUrls.gatewayUrl(ipfsCid));
                log.debug("🔗 Stored pure IPFS reference (no local blob): ipfsCid={}", ipfsCid);
            }
            
            // Handle intent token for lazy uploads (ADR 020)
            if (intentToken != null && !intentToken.isEmpty()) {
                contentNode.setProperty("jcr:intentToken", intentToken);
                contentNode.setProperty("jcr:pendingBinary", true);
                log.debug("📎 Intent token stored for lazy binary upload: {}", intentToken);
            }
            
            // Commit (deterministic on all nodes)
            CommitInfo commitInfo = new CommitInfo(
                "aeron-replication", 
                null, 
                Collections.singletonMap("replicated", "true")
            );
            
            if (auditMetadata != null && auditMetadata.getAppliedLogPosition() != null) {
                auditMetadata.getAppliedLogPosition().writeTo(rootBuilder);
            }
            try {
                nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, commitInfo);
            } catch (CommitFailedException e) {
                throw new RuntimeException("Failed to commit write", e);
            }
            flushService.onChangeApplied(buildDurabilityCallback(proposalId));
            
            // Track fragmentation
            if (fragmentationCallback != null) {
                fragmentationCallback.trackFragmentation(walletAddress);
            }
            
            // Get new HEAD
            String newHead = fileStore.getHead().getRecordId().toString10();
            log.debug("✅ Write applied, HEAD: {}...", truncate(newHead, 20));
            
            // Update HEAD cache
            if (headUpdateCallback != null) {
                headUpdateCallback.updateHead(newHead);
            }
            
            // Emit SSE event
            if (sseEventCallback != null) {
                String extractedOrg = extractOrganizationFromPath(path);
                if (blobId != null && !blobId.isEmpty()) {
                    String eventCid = resolveIpfsCid(blobId, ipfsCid, path);
                    sseEventCallback.emitBinaryUpload(path, walletAddress, extractedOrg, message, eventCid, mimeType);
                } else {
                    sseEventCallback.emitContentWrite(path, walletAddress, extractedOrg, message, signature, contentType);
                }
            }
            
            log.debug("✅ Deterministic write applied successfully");
            return newHead;
            
        } catch (AgentTerminationException e) {
            throw e;
        } catch (Exception e) {
            if (durabilityCallback != null && proposalId != null && !proposalId.isEmpty()) {
                durabilityCallback.onFailure(proposalId, e.getMessage());
            }
            log.error("❌ Failed to apply replicated write", e);
            throw new RuntimeException("Failed to apply replicated write", e);
        }
    }

    private Runnable buildDurabilityCallback(String proposalId) {
        if (durabilityCallback == null || proposalId == null || proposalId.isEmpty()) {
            return null;
        }
        return () -> durabilityCallback.onDurable(
            proposalId,
            fileStore.getHead().getRecordId().toString10()
        );
    }

    private boolean isDuplicateProposalReplay(NodeBuilder contentNode,
                                              boolean contentNodeExists,
                                              @Nullable String proposalId) {
        if (!contentNodeExists || proposalId == null || proposalId.isEmpty()) {
            return false;
        }
        PropertyState existingProposalId = contentNode.getProperty(PROPOSAL_ID_PROPERTY);
        return existingProposalId != null && proposalId.equals(existingProposalId.getValue(Type.STRING));
    }

    @NotNull
    private String acknowledgeDuplicateReplay(@Nullable String proposalId) {
        String currentHead = fileStore.getHead().getRecordId().toString10();
        if (durabilityCallback != null && proposalId != null && !proposalId.isEmpty()) {
            durabilityCallback.onDurable(proposalId, currentHead);
        }
        if (headUpdateCallback != null) {
            headUpdateCallback.updateHead(currentHead);
        }
        log.info("♻️ Duplicate replicated write replay acknowledged without mutating Oak: proposalId={}", proposalId);
        return currentHead;
    }
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Helper Methods
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    /**
     * Set content properties on the node.
     */
    private void setContentProperties(
            NodeBuilder contentNode,
            String walletAddress,
            String contentType,
            String message,
            String signature,
            String path,
            @Nullable String proposalId,
            long appliedAt) {
        
        contentNode.setProperty("jcr:primaryType", "nt:unstructured");
        contentNode.setProperty("contentType", contentType != null ? contentType : "page");
        contentNode.setProperty("message", message != null ? message : "");
        contentNode.setProperty("timestamp", appliedAt);
        contentNode.setProperty("wallet", walletAddress);
        contentNode.setProperty("signature", signature);
        contentNode.setProperty("source", "aeron-replicated");
        if (proposalId != null && !proposalId.isEmpty()) {
            contentNode.setProperty(PROPOSAL_ID_PROPERTY, proposalId);
        }

        // ADR 059: Canonical JSON→JCR mapping (best-effort normalization)
        normalizeCanonicalPayload(contentNode, message);
        
        // ADR 037: Extract and store organization from path
        String extractedOrg = extractOrganizationFromPath(path);
        if (extractedOrg != null && !extractedOrg.isEmpty()) {
            contentNode.setProperty("organization", extractedOrg);
            log.debug("🏢 Stored organization property: {}", extractedOrg);
        }
    }

    private void normalizeCanonicalPayload(NodeBuilder contentNode, String message) {
        if (message == null) {
            return;
        }
        String trimmed = message.trim();
        if (!(trimmed.startsWith("{") && trimmed.endsWith("}"))) {
            return;
        }
        Map<String, Object> fields;
        try {
            fields = JsonParser.parseObject(trimmed);
        } catch (IllegalArgumentException e) {
            log.debug("Message is not a JSON object; skipping canonical mapping");
            return;
        }

        Object title = fields.get("title");
        if (title instanceof String) {
            contentNode.setProperty("oak:title", (String) title);
        }

        Object body = fields.get("body");
        if (body instanceof String) {
            contentNode.setProperty("oak:body", (String) body);
        }

        Object tags = fields.get("tags");
        if (tags instanceof List) {
            List<String> values = new ArrayList<>();
            for (Object tag : (List<?>) tags) {
                if (tag instanceof String) {
                    values.add((String) tag);
                }
            }
            if (!values.isEmpty() || ((List<?>) tags).isEmpty()) {
                contentNode.setProperty("oak:tags", values, Type.STRINGS);
            }
        }

        Object meta = fields.get("meta");
        if (meta instanceof Map) {
            contentNode.setProperty("oak:metaJson", JsonParser.toJson(meta));
        }

        Object payload = fields.get("payload");
        if (payload instanceof Map) {
            contentNode.setProperty("oak:payloadJson", JsonParser.toJson(payload));
        }
    }
    
    /**
     * Handle binary content with BlobStore.
     */
    private void handleBinaryContent(
            NodeBuilder contentNode,
            BlobStore blobStore,
            String blobId,
            String mimeType,
            String ipfsCid) {
        
        if (blobStore == null) {
            log.warn("BlobStore not available, storing blobId as string reference");
            contentNode.setProperty("jcr:data", blobId);
            if (mimeType != null && !mimeType.isEmpty()) {
                contentNode.setProperty("jcr:mimeType", mimeType);
            }
            return;
        }
        
        try {
            // Create proper Blob object from blob ID
            Blob blob = new BlobStoreBlob(blobStore, blobId);
            
            // Set as proper BINARY type property
            contentNode.setProperty("jcr:data", blob, Type.BINARY);
            
            if (mimeType != null && !mimeType.isEmpty()) {
                contentNode.setProperty("jcr:mimeType", mimeType);
            }
            
            // Store raw blob ID
            contentNode.setProperty("jcr:blobId", blobId);
            
            // Only the CID carried in the proposal (decided by the ingesting node): a lookup here would differ per node.
            if (ipfsCid != null && !ipfsCid.isEmpty()) {
                contentNode.setProperty("ipfsCid", ipfsCid);
                contentNode.setProperty("ipfsGateway", IpfsGatewayUrls.gatewayUrl(ipfsCid));
                log.info("✅ Binary stored with proposal IPFS CID: jcr:blobId={}, ipfsCid={}", blobId, ipfsCid);
            } else {
                log.info("✅ Binary stored (proposal carries no IPFS CID): jcr:blobId={}", blobId);
            }
            
        } catch (AgentTerminationException e) {
            throw e;
        } catch (Exception e) {
            log.error("❌ Failed to create Blob from blobId {}: {}", blobId, e.getMessage());
            // Fallback: store as string reference
            contentNode.setProperty("jcr:data", blobId);
            if (mimeType != null && !mimeType.isEmpty()) {
                contentNode.setProperty("jcr:mimeType", mimeType);
            }
        }
    }
    
    @NotNull
    private NodeStore requireNodeStore() {
        NodeStore nodeStore = nodeStoreSupplier.get();
        if (nodeStore == null) {
            throw new IllegalStateException("NodeStore supplier returned null");
        }
        return nodeStore;
    }
    
    /**
     * Enrich wallet node with metadata.
     */
    private void enrichWalletNode(NodeBuilder walletNode,
                                  String walletNodeName,
                                  String walletAddress,
                                  boolean newContentNode,
                                  long appliedAt) {
        try {
            boolean isNewWallet = !walletNode.hasProperty("wallet");
            
            if (isNewWallet) {
                log.info("🆕 Creating new wallet node with metadata: {}", walletNodeName);
                
                walletNode.setProperty("jcr:primaryType", "nt:unstructured");
                walletNode.setProperty("wallet", walletAddress);
                walletNode.setProperty("walletCreated", appliedAt);
                walletNode.setProperty("nodeType", "wallet-root");
                walletNode.setProperty("description", "Wallet-scoped content root for " + walletAddress);
                
                // First write creates the wallet node and its initial content entry.
                walletNode.setProperty("contentCount", 1L);
                walletNode.setProperty("totalWrites", 1L);
                walletNode.setProperty("lastWrite", appliedAt);
                
                log.debug("✅ Wallet node metadata initialized: {}", walletAddress);
            } else {
                // Update existing wallet node
                PropertyState contentCountProp = walletNode.getProperty("contentCount");
                PropertyState totalWritesProp = walletNode.getProperty("totalWrites");
                
                long contentCount = contentCountProp != null ? contentCountProp.getValue(Type.LONG) : 0L;
                long totalWrites = totalWritesProp != null ? totalWritesProp.getValue(Type.LONG) : 0L;
                long nextContentCount = contentCount + (newContentNode ? 1L : 0L);
                
                walletNode.setProperty("contentCount", nextContentCount);
                walletNode.setProperty("totalWrites", totalWrites + 1);
                walletNode.setProperty("lastWrite", appliedAt);
                
                log.debug("📊 Wallet node updated: {} (contentCount: {}, totalWrites: {})", 
                    walletAddress, nextContentCount, totalWrites + 1);
            }
        } catch (AgentTerminationException e) {
            throw e;
        } catch (Exception e) {
            log.warn("⚠️  Failed to enrich wallet node metadata for {}: {}", walletAddress, e.getMessage());
        }
    }
    
    /**
     * Extract organization from path (ADR 037).
     * 
     * <p>Path format: /oak-chain/XX/YY/ZZ/0xWALLET/{organization}/content/{contentId}
     */
    @Nullable
    public String extractOrganizationFromPath(@Nullable String path) {
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
     * Resolve IPFS CID from various sources.
     */
    @Nullable
    private String resolveIpfsCid(String blobId, String ipfsCid, String path) {
        // Prefer client-provided CID
        if (ipfsCid != null && !ipfsCid.isEmpty()) {
            return ipfsCid;
        }
        
        // Try CID mapping service
        if (cidMappingCallback != null) {
            String mappedCid = cidMappingCallback.getCid(blobId);
            if (mappedCid != null) {
                log.debug("📡 Got CID from mapping service: {}", mappedCid);
                return mappedCid;
            }
        }
        
        // Try reading from node
        try {
            NodeState current = requireNodeStore().getRoot();
            for (String part : path.substring(1).split("/")) {
                if (!part.isEmpty() && current.hasChildNode(part)) {
                    current = current.getChildNode(part);
                }
            }
            PropertyState cidProp = current.getProperty("ipfsCid");
            if (cidProp != null) {
                return cidProp.getValue(Type.STRING);
            }
        } catch (AgentTerminationException e) {
            throw e;
        } catch (Exception e) {
            log.debug("Node CID lookup failed: {}", e.getMessage());
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
    public interface SSEEventCallback {
        void emitContentWrite(String path, String wallet, String org, String message, String signature, String contentType);
        void emitBinaryUpload(String path, String wallet, String org, String message, String ipfsCid, String mimeType);
    }
    
    /**
     * Callback for fragmentation tracking.
     */
    @FunctionalInterface
    public interface FragmentationCallback {
        void trackFragmentation(String walletAddress);
    }
    
    /**
     * Callback for CID mapping lookups.
     */
    @FunctionalInterface
    public interface CidMappingCallback {
        @Nullable String getCid(String blobId);
    }

    /**
     * Callback for durability confirmation (ADR 026).
     */
    public interface DurabilityCallback {
        void onDurable(String proposalId, String durableHead);
        void onFailure(String proposalId, String error);
    }
}
