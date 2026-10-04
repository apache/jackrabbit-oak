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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalQueuePolicy;
import org.apache.jackrabbit.oak.segment.consensus.sharding.ShardWriteAuthorityEnforcer;
import org.apache.jackrabbit.oak.segment.consensus.util.WalletPathUtil;
import org.apache.jackrabbit.oak.segment.consensus.validation.ValidationResult;
import org.apache.jackrabbit.oak.segment.consensus.validation.WalletValidator;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.segment.http.server.model.ClientRegistration;
import org.apache.jackrabbit.oak.segment.http.server.util.ApiErrorUtil;
import org.apache.jackrabbit.oak.segment.http.server.util.JsonOutputUtil;
import org.apache.jackrabbit.oak.segment.http.server.util.LeaderWriteRedirectUtil;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Handler for delete proposals (`/v1/propose-delete`).
 */
public class DeleteProposalHandler {

    private static final Logger log = LoggerFactory.getLogger(DeleteProposalHandler.class);
    private static final long DEFAULT_DELETE_SIZE_MB = 1L;
    private static final long BYTES_PER_MB = 1024L * 1024L;
    private static final long ESTIMATED_BYTES_PER_NODE = 1024L;
    private static final long ESTIMATED_BYTES_PER_PROPERTY = 100L;
    private static final long BINARY_BYTES_PER_PROPERTY_UNIT = 100L;
    private static final int MAX_DELETE_ESTIMATION_NODES = 50_000;
    private static final int MAX_DELETE_ESTIMATION_DEPTH = 64;

    private final ServerContext context;

    public DeleteProposalHandler(ServerContext context) {
        this.context = context;
    }

    /**
     * Handle POST /v1/propose-delete - Delete proposal endpoint.
     */
    public void handleDeleteProposal(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // ADR 028: PRE-FLIGHT HEALTH CHECK
        // Prevent silent proposal loss by rejecting requests when cluster unhealthy
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if (context.aeronConsensusEngine == null || !context.aeronConsensusEngine.isClusterHealthy()) {
            String reason = context.aeronConsensusEngine != null
                ? context.aeronConsensusEngine.getUnhealthyReason()
                : "consensus_engine_not_configured";
            log.warn("❌ Cluster unhealthy, rejecting delete proposal: {}", reason);
            context.apiRejectedRequests.incrementAndGet();
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                "Cluster unhealthy: " + reason + ". Please retry in a few seconds.");
            return;
        }

        try {
            // Read parameters
            // CRITICAL: walletAddress is REQUIRED and must be a valid 0x Ethereum address
            String wallet = request.getParameter("walletAddress");
            if (wallet == null || wallet.isEmpty()) {
                wallet = request.getParameter("wallet"); // Fallback for backward compatibility
            }
            String signature = request.getParameter("signature");
            String contentPath = request.getParameter("contentPath");

            // Validate required parameters
            if (signature == null || signature.isEmpty()) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST, "Missing signature parameter");
                return;
            }
            if (contentPath == null || contentPath.isEmpty()) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST, "Missing contentPath parameter");
                return;
            }

            org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig blockchainConfig =
                org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.getInstance();
            if (!blockchainConfig.isMockMode()
                && !org.apache.jackrabbit.oak.segment.consensus.security.EthereumSignatureVerifier
                    .isFullVerificationAvailable()) {
                context.apiRejectedRequests.incrementAndGet();
                log.error("❌ Full Ethereum signature verification unavailable: {}",
                    org.apache.jackrabbit.oak.segment.consensus.security.EthereumSignatureVerifier
                        .getAvailabilityReason());
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                    "Full Ethereum signature verification unavailable. Validator is missing required Bouncy Castle support.");
                return;
            }

            // ✅ REFACTORED: Validate Ethereum address using WalletValidator
            ValidationResult<String> walletValidation = WalletValidator.validate(wallet);
            if (!walletValidation.isValid()) {
                ApiErrorUtil.sendJsonError(response, walletValidation.getHttpStatus(), walletValidation.getError());
                return;
            }
            String normalizedWallet = walletValidation.getNormalizedValue();

            if (!ShardWriteAuthorityEnforcer.allowLocalWrite(
                context,
                normalizedWallet,
                "/v1/propose-delete",
                response
            )) {
                return;
            }

            if (!LeaderWriteRedirectUtil.allowLeaderWrite(context, "/v1/propose-delete", response)) {
                return;
            }

            // PATH ENFORCEMENT: Look up client registration BY WALLET ADDRESS
            // This is the primary identifier - clientId is secondary
            ClientRegistration clientReg = context.findClientRegistrationByWallet(normalizedWallet);
            String clientId = clientReg != null ? clientReg.clientId : null;

            // If not found by wallet, try clientId lookup (wallet address is preferred)
            // Note: IP-based fallback has been removed - wallet address is required
            if (clientReg == null) {
                String clientIdHeader = request.getHeader("X-Client-Id");
                if (clientIdHeader == null || clientIdHeader.isEmpty()) {
                    clientIdHeader = request.getParameter("clientId");
                }
                // Only use explicit clientId header/param, not IP address
                if (clientIdHeader != null && !clientIdHeader.isEmpty()) {
                    clientReg = context.findClientRegistrationByClientId(clientIdHeader);
                    if (clientReg != null) {
                        clientId = clientIdHeader;
                    }
                }
            }

            // If still not found, reject delete
            if (clientReg == null) {
                log.warn("🚫 Delete proposal rejected: Wallet {} not registered", normalizedWallet);
                java.util.List<String> registeredWallets = new java.util.ArrayList<>();
                for (ClientRegistration reg : context.registeredClients.values()) {
                    if (reg.walletAddress != null && !reg.walletAddress.isEmpty()) {
                        registeredWallets.add(reg.walletAddress);
                    }
                }
                log.warn("   Available registered wallets: {}", registeredWallets);
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_FORBIDDEN,
                    String.format("Wallet %s not registered. Please register via /v1/register-client with walletAddress=%s before proposing deletes.",
                        normalizedWallet, normalizedWallet));
                return;
            }

            // Set clientId if not already set
            if (clientId == null) {
                clientId = clientReg.clientId;
            }

            // Verify wallet matches registered client's wallet (double-check)
            if (clientReg.walletAddress != null && !clientReg.walletAddress.isEmpty()) {
                String registeredWallet = clientReg.walletAddress.toLowerCase();
                if (!normalizedWallet.equals(registeredWallet)) {
                    log.warn("🚫 Delete proposal rejected: Wallet mismatch for client {}", clientId);
                    log.warn("   Requested wallet: {}", normalizedWallet);
                    log.warn("   Registered wallet: {}", registeredWallet);
                    ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_FORBIDDEN,
                        String.format("Wallet mismatch: Client %s registered with wallet %s, but proposal uses %s",
                                     clientId, registeredWallet, normalizedWallet));
                    return;
                }
            }

            // Verify path ownership: allow deleting either the wallet root itself
            // or any descendant beneath it.
            String shardRoot = WalletPathUtil.getShardRoot(normalizedWallet);
            String normalizedContentPath = contentPath.toLowerCase();
            String normalizedShardRoot = shardRoot.toLowerCase();
            boolean ownsPath = normalizedContentPath.equals(normalizedShardRoot)
                || normalizedContentPath.startsWith(normalizedShardRoot + "/");
            if (!ownsPath) {
                log.warn("🚫 Delete proposal rejected: Path ownership violation");
                log.warn("   Content path: {}", contentPath);
                log.warn("   Expected shard root: {}", shardRoot);
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_FORBIDDEN,
                    String.format("Path ownership violation: Content at %s does not belong to wallet %s. " +
                                 "Only content at or under %s can be deleted.",
                                 contentPath, wallet, shardRoot));
                return;
            }

            log.debug("🗑️  DELETE PROPOSAL: client={}, wallet={}, path={}", clientId, wallet, contentPath);

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // ETHEREUM PAYMENT REQUIRED: Deletes flow through same pipeline as writes
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            String ethereumTxHash = request.getParameter("ethereumTxHash");
            if (ethereumTxHash == null || ethereumTxHash.isEmpty()) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST,
                    "Missing ethereumTxHash parameter. Deletes require Ethereum payment (like writes). " +
                    "Release is adaptive after verification and valid deletes enter the durable backlog."
                );
                return;
            }

            String clientProposalId = request.getParameter("proposalId");
            String proposalId;
            if (clientProposalId != null && !clientProposalId.trim().isEmpty()) {
                proposalId = clientProposalId.trim();
                if (!isValidClientProposalId(proposalId)) {
                    ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST,
                        "Invalid proposalId format. Expected 0x-prefixed 32-byte hex.");
                    return;
                }
                if (proposalId.startsWith("0X")) {
                    proposalId = "0x" + proposalId.substring(2);
                }
            } else {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST,
                    "Missing proposalId parameter. Clients must supply a 0x-prefixed 32-byte hex proposalId from the settlement contract flow.");
                return;
            }

            if (!isChainBackedProposalId(proposalId)) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST,
                    "proposalId must be a 0x-prefixed 32-byte hex value.");
                return;
            }

            // A delete signature is an EIP-191 personal_sign over exactly the contentPath.
            if (!blockchainConfig.isMockMode()
                && !org.apache.jackrabbit.oak.segment.consensus.security.EthereumSignatureVerifier
                    .verifySignature(contentPath, signature, normalizedWallet)) {
                context.apiRejectedRequests.incrementAndGet();
                log.warn("❌ API REJECTED: Delete signature verification failed for wallet {}", normalizedWallet);
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_UNAUTHORIZED,
                    "Signature verification failed. The signature must be a personal_sign over contentPath by the claimed wallet.");
                return;
            }

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // GC DEBT TRACKING: Track debt when content is deleted (deferred cost)
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            java.math.BigDecimal gcDebtIncurred = java.math.BigDecimal.ZERO;
            java.math.BigDecimal totalDebt = java.math.BigDecimal.ZERO;
            java.math.BigDecimal pendingDebt = java.math.BigDecimal.ZERO;
            boolean writesBlocked = false;
            DeleteSizeEstimate deleteSizeEstimate = estimateDeleteSize(contentPath);

            if (context.gcAccountManager != null) {
                try {
                    // Add debt to account (pending until GC executes)
                    java.math.BigDecimal debtCost = context.gcAccountManager.addDebt(
                        normalizedWallet,
                        contentPath,
                        deleteSizeEstimate.estimatedSizeMB
                    );

                    // Get updated account state
                    org.apache.jackrabbit.oak.segment.consensus.gc.EntityGCAccount account =
                        context.gcAccountManager.getAccount(normalizedWallet);

                    gcDebtIncurred = debtCost;
                    totalDebt = account.totalDebt;
                    pendingDebt = account.getPendingDebt();
                    writesBlocked = account.writesBlocked;

                    log.info(
                        "💰 GC debt added: wallet={}, path={}, debt=${}, total=${}, pending=${}, blocked={}, estimatedMb={}, nodes={}, descendants={}, properties={}, truncated={}",
                        normalizedWallet,
                        contentPath,
                        gcDebtIncurred,
                        totalDebt,
                        pendingDebt,
                        writesBlocked,
                        deleteSizeEstimate.estimatedSizeMB,
                        deleteSizeEstimate.nodeCount,
                        deleteSizeEstimate.descendantCount(),
                        deleteSizeEstimate.propertyCount,
                        deleteSizeEstimate.truncated
                    );

                } catch (Exception e) {
                    log.warn("⚠️  Failed to track GC debt for delete: {}", e.getMessage());
                }
            }

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // QUEUE DELETE PROPOSAL: Same flow as writes, just different type
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            log.debug("📥 Queuing DELETE proposal {} (tx: {}), waiting for Ethereum confirmation",
                proposalId, ethereumTxHash);

            context.proposalQueueManager.queueDeleteProposal(
                proposalId,
                ethereumTxHash,
                normalizedWallet,
                contentPath,
                signature
            );

            // Return 202 Accepted (queued for processing)
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_ACCEPTED);
            Map<String, Object> links = new LinkedHashMap<>();
            links.put("self", "/v1/ops/operations/" + proposalId);
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("contractVersion", "ops.v1");
            payload.put("status", "accepted");
            payload.put("operationId", proposalId);
            payload.put("receivedAtMs", System.currentTimeMillis());
            payload.put("ackState", "ACCEPTED");
            payload.put("links", links);
            payload.put("proposalId", proposalId);
            payload.put("proposalIdSource", clientProposalId != null && !clientProposalId.trim().isEmpty() ? "client" : "server");
            payload.put("type", "DELETE");
            payload.put("state", "PENDING");
            payload.put("message", "Delete proposal queued, waiting for Ethereum confirmation");
            payload.put("ethereumTxHash", ethereumTxHash);
            payload.put("timeoutTimestamp", System.currentTimeMillis() + ProposalQueuePolicy.confirmationTimeoutMs());
            payload.put("wallet", wallet);
            payload.put("contentPath", contentPath);
            payload.put("estimatedDeleteSizeMb", deleteSizeEstimate.estimatedSizeMB);
            payload.put("estimatedNodeCount", deleteSizeEstimate.nodeCount);
            payload.put("estimatedDescendantCount", deleteSizeEstimate.descendantCount());
            payload.put("estimatedPropertyCount", deleteSizeEstimate.propertyCount);
            payload.put("estimationTruncated", deleteSizeEstimate.truncated);
            payload.put("gcDebtIncurred", gcDebtIncurred.toString());
            payload.put("totalDebt", totalDebt.toString());
            payload.put("pendingDebt", pendingDebt.toString());
            payload.put("writesBlocked", writesBlocked);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
            log.info("✅ DELETE proposal {} queued successfully (path: {})", proposalId, contentPath);

        } catch (Exception e) {
            log.error("❌ Delete proposal failed", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Delete proposal failed: " + e.getMessage());
        }
    }

    /**
     * Estimate delete footprint for a given subtree.
     *
     * <p>The client already tells Oak which path is being deleted. Oak should derive
     * subtree size locally from the NodeStore rather than trusting client-supplied
     * descendant counts for GC-debt accounting.</p>
     *
     * @param contentPath Path to the content node
     * @return Estimated subtree footprint, including truncation when runtime safety caps hit
     */
    private DeleteSizeEstimate estimateDeleteSize(String contentPath) {
        if (context.nodeStore == null) {
            log.debug("NodeStore not available, using default size estimate");
            return DeleteSizeEstimate.defaultEstimate();
        }

        try {
            NodeState root = context.nodeStore.getRoot();

            // Navigate to the content path
            String[] pathParts = contentPath.split("/");
            NodeState current = root;

            for (String part : pathParts) {
                if (part.isEmpty()) {
                    continue;
                }
                current = current.getChildNode(part);
                if (!current.exists()) {
                    log.debug("Path {} does not exist, using default size estimate", contentPath);
                    return DeleteSizeEstimate.defaultEstimate();
                }
            }

            Deque<TraversalFrame> stack = new ArrayDeque<>();
            stack.push(new TraversalFrame(current, 0));

            long nodeCount = 0;
            long propertyCount = 0;
            boolean truncated = false;

            while (!stack.isEmpty()) {
                TraversalFrame frame = stack.pop();
                nodeCount++;

                for (PropertyState prop : frame.node.getProperties()) {
                    propertyCount++;
                    if (prop.getType() == Type.BINARY) {
                        try {
                            Blob blob = prop.getValue(Type.BINARY);
                            propertyCount += blob.length() / BINARY_BYTES_PER_PROPERTY_UNIT;
                        } catch (Exception e) {
                            log.debug("Unable to inspect binary property size for {}: {}", contentPath, e.getMessage());
                        }
                    }
                }

                if (frame.depth >= MAX_DELETE_ESTIMATION_DEPTH) {
                    if (frame.node.getChildNodeCount(1) > 0) {
                        truncated = true;
                    }
                    continue;
                }

                for (ChildNodeEntry childEntry : frame.node.getChildNodeEntries()) {
                    if (nodeCount + stack.size() >= MAX_DELETE_ESTIMATION_NODES) {
                        truncated = true;
                        break;
                    }
                    stack.push(new TraversalFrame(childEntry.getNodeState(), frame.depth + 1));
                }
            }

            long estimatedBytes =
                (nodeCount * ESTIMATED_BYTES_PER_NODE) + (propertyCount * ESTIMATED_BYTES_PER_PROPERTY);
            long estimatedMB = Math.max(
                DEFAULT_DELETE_SIZE_MB,
                (estimatedBytes + BYTES_PER_MB - 1) / BYTES_PER_MB
            );
            DeleteSizeEstimate estimate = new DeleteSizeEstimate(estimatedMB, nodeCount, propertyCount, truncated);

            log.debug(
                "Delete size estimate for {}: nodes={}, descendants={}, properties={}, ~{} MB, truncated={}",
                contentPath,
                estimate.nodeCount,
                estimate.descendantCount(),
                estimate.propertyCount,
                estimate.estimatedSizeMB,
                estimate.truncated
            );

            return estimate;

        } catch (Exception e) {
            log.warn("Failed to estimate content size for {}: {}", contentPath, e.getMessage());
            return DeleteSizeEstimate.defaultEstimate();
        }
    }

    private static boolean isValidClientProposalId(String proposalId) {
        if (proposalId == null) {
            return false;
        }
        String value = proposalId.trim();
        return value.matches("(?i)^0x[a-f0-9]{64}$");
    }

    private static boolean isChainBackedProposalId(String proposalId) {
        return proposalId != null && proposalId.trim().matches("(?i)^0x[a-f0-9]{64}$");
    }

    private static final class TraversalFrame {
        private final NodeState node;
        private final int depth;

        private TraversalFrame(NodeState node, int depth) {
            this.node = node;
            this.depth = depth;
        }
    }

    private static final class DeleteSizeEstimate {
        private final long estimatedSizeMB;
        private final long nodeCount;
        private final long propertyCount;
        private final boolean truncated;

        private DeleteSizeEstimate(long estimatedSizeMB, long nodeCount, long propertyCount, boolean truncated) {
            this.estimatedSizeMB = estimatedSizeMB;
            this.nodeCount = nodeCount;
            this.propertyCount = propertyCount;
            this.truncated = truncated;
        }

        private long descendantCount() {
            return nodeCount > 0 ? nodeCount - 1 : 0;
        }

        private static DeleteSizeEstimate defaultEstimate() {
            return new DeleteSizeEstimate(DEFAULT_DELETE_SIZE_MB, 0, 0, false);
        }
    }

}
