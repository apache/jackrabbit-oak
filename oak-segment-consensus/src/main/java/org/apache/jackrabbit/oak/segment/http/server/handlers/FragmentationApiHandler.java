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

import org.apache.jackrabbit.oak.segment.consensus.fragmentation.FragmentationTracker;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCProposalManager;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.segment.http.server.util.ApiErrorUtil;
import org.apache.jackrabbit.oak.segment.http.server.util.FormatUtils;
import org.apache.jackrabbit.oak.segment.http.server.util.JsonOutputUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Handler for fragmentation metrics and GC/compaction consensus APIs.
 * 
 * <p>Provides endpoints for:
 * - Querying fragmentation metrics per entity
 * - GC/compaction consensus status
 * - Fragmentation tax calculations
 * - Compaction proposals and status</p>
 */
public class FragmentationApiHandler {
    
    private static final Logger log = LoggerFactory.getLogger(FragmentationApiHandler.class);
    
    private final ServerContext context;
    
    public FragmentationApiHandler(ServerContext context) {
        this.context = context;
    }
    
    /**
     * Handle GET /v1/fragmentation/metrics - Get fragmentation metrics for all entities.
     */
    public void handleGetAllMetrics(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        
        try {
            FragmentationTracker tracker = getFragmentationTracker();
            if (tracker == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Fragmentation tracker not initialized");
                return;
            }
            
            Map<String, FragmentationTracker.EntityFragmentationMetrics> allMetrics = tracker.getAllMetrics();

            List<Map<String, Object>> entities = new ArrayList<>();
            for (FragmentationTracker.EntityFragmentationMetrics metrics : allMetrics.values()) {
                entities.add(metricsToMap(metrics, tracker));
            }

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("contractVersion", "fragmentation.metrics.v1");
            payload.put("totalEntities", allMetrics.size());
            payload.put("entities", entities);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
            
        } catch (Exception e) {
            log.error("Error getting fragmentation metrics", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
    
    /**
     * Handle GET /v1/fragmentation/metrics/{walletAddress} - Get fragmentation metrics for specific entity.
     */
    public void handleGetEntityMetrics(HttpServletRequest request, HttpServletResponse response, String walletAddress) throws IOException {
        response.setContentType("application/json");
        
        try {
            FragmentationTracker tracker = getFragmentationTracker();
            if (tracker == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Fragmentation tracker not initialized");
                return;
            }
            
            FragmentationTracker.EntityFragmentationMetrics metrics = tracker.getMetrics(walletAddress);
            if (metrics == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_NOT_FOUND, "No metrics found for wallet: " + walletAddress);
                return;
            }

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("contractVersion", "fragmentation.metrics.entity.v1");
            payload.put("walletAddress", walletAddress);
            payload.put("data", metricsToMap(metrics, tracker));
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
            
        } catch (Exception e) {
            log.error("Error getting entity fragmentation metrics", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
    
    /**
     * Handle GET /v1/fragmentation/top - Get top N most fragmented entities.
     */
    public void handleGetTopFragmented(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        
        try {
            FragmentationTracker tracker = getFragmentationTracker();
            if (tracker == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Fragmentation tracker not initialized");
                return;
            }
            
            int limit = 10;
            String limitParam = request.getParameter("limit");
            if (limitParam != null) {
                try {
                    limit = Integer.parseInt(limitParam);
                } catch (NumberFormatException e) {
                    // Use default
                }
            }
            
            List<FragmentationTracker.EntityFragmentationMetrics> topEntities = tracker.getTopFragmentedEntities(limit);

            List<Map<String, Object>> entities = new ArrayList<>();
            for (FragmentationTracker.EntityFragmentationMetrics metrics : topEntities) {
                entities.add(metricsToMap(metrics, tracker));
            }

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("contractVersion", "fragmentation.top.v1");
            payload.put("limit", limit);
            payload.put("entities", entities);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
            
        } catch (Exception e) {
            log.error("Error getting top fragmented entities", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
    
    /**
     * Handle GET /v1/gc/status - Get GC/compaction consensus status.
     */
    public void handleGetGcStatus(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        
        try {
            GCProposalManager gcManager = getGCProposalManager();
            if (gcManager == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "GC Proposal Manager not initialized");
                return;
            }
            
            // Get pending proposals
            List<org.apache.jackrabbit.oak.segment.consensus.gc.GCProposal> pending = gcManager.getPendingProposals();
            
            // Get last GC execution
            List<org.apache.jackrabbit.oak.segment.consensus.gc.GCExecutionResult> history = gcManager.getGCHistory(1);
            org.apache.jackrabbit.oak.segment.consensus.gc.GCExecutionResult lastGC = history.isEmpty() ? null : history.get(0);

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("contractVersion", "gc.status.v1");
            payload.put("gcEnabled", true);
            payload.put("pendingProposals", pending.size());
            payload.put("lastGcRun", lastGC != null ? lastGC.timestamp : null);
            payload.put("lastGcReclaimedMB", lastGC != null ? lastGC.actualReclaimedSizeMB : null);
            payload.put("lastGcCostUSDC", lastGC != null && lastGC.actualCostUSDC != null ? lastGC.actualCostUSDC.toString() : null);
            payload.put("gcConsensusRequired", true);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
            
        } catch (Exception e) {
            log.error("Error getting GC status", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
    
    /**
     * Handle GET /v1/compaction/proposals - Get pending compaction proposals.
     */
    public void handleGetCompactionProposals(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        
        try {
            GCProposalManager gcManager = getGCProposalManager();
            if (gcManager == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "GC Proposal Manager not initialized");
                return;
            }
            
            List<org.apache.jackrabbit.oak.segment.consensus.gc.GCProposal> proposals = gcManager.getPendingProposals();

            List<Map<String, Object>> serialized = new ArrayList<>();
            for (org.apache.jackrabbit.oak.segment.consensus.gc.GCProposal proposal : proposals) {
                serialized.add(proposalToMap(proposal));
            }

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("contractVersion", "gc.compaction.proposals.v1");
            payload.put("proposals", serialized);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
            
        } catch (Exception e) {
            log.error("Error getting compaction proposals", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
    
    /**
     * Handle POST /v1/propose-gc - Create a new GC proposal.
     * 
     * <p>Request Body (JSON):</p>
     * <pre>
     * {
     *   "walletAddress": "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb",  // Required: Ethereum wallet address of proposer
     *   "targetRevision": "abc123..."                                    // Optional: Target revision ID (null = use HEAD)
     * }
     * </pre>
     * 
     * <p>Alternative: Query parameters (for form-encoded requests):</p>
     * <ul>
     *   <li><code>walletAddress</code> (required) - Ethereum wallet address</li>
     *   <li><code>targetRevision</code> (optional) - Target revision ID</li>
     * </ul>
     * 
     * <p>Response: JSON with GC proposal details including proposalId, estimated costs, fragmentation overhead</p>
     */
    public void handleProposeGC(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        
        try {
            GCProposalManager gcManager = getGCProposalManager();
            if (gcManager == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "GC Proposal Manager not initialized");
                return;
            }
            
            String proposerWallet = null;
            String targetRevision = null;
            
            // Try to read from JSON body first (preferred)
            String contentType = request.getContentType();
            if (contentType != null && contentType.contains("application/json")) {
                try {
                    java.io.BufferedReader reader = request.getReader();
                    StringBuilder body = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        body.append(line);
                    }
                    
                    if (body.length() > 0) {
                        String jsonBody = body.toString();
                        // Simple JSON parsing (no Gson dependency)
                        // Extract walletAddress
                        int walletStart = jsonBody.indexOf("\"walletAddress\"");
                        if (walletStart >= 0) {
                            int colonIndex = jsonBody.indexOf(":", walletStart);
                            int quoteStart = jsonBody.indexOf("\"", colonIndex);
                            if (quoteStart >= 0) {
                                int quoteEnd = jsonBody.indexOf("\"", quoteStart + 1);
                                if (quoteEnd > quoteStart) {
                                    proposerWallet = jsonBody.substring(quoteStart + 1, quoteEnd);
                                }
                            }
                        }
                        
                        // Extract targetRevision (optional)
                        int revisionStart = jsonBody.indexOf("\"targetRevision\"");
                        if (revisionStart >= 0) {
                            int colonIndex = jsonBody.indexOf(":", revisionStart);
                            // Check if null
                            int nullIndex = jsonBody.indexOf("null", colonIndex);
                            if (nullIndex >= 0 && nullIndex < colonIndex + 10) {
                                targetRevision = null; // Explicitly null
                            } else {
                                int quoteStart = jsonBody.indexOf("\"", colonIndex);
                                if (quoteStart >= 0) {
                                    int quoteEnd = jsonBody.indexOf("\"", quoteStart + 1);
                                    if (quoteEnd > quoteStart) {
                                        targetRevision = jsonBody.substring(quoteStart + 1, quoteEnd);
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    log.debug("Failed to parse JSON body, falling back to query parameters", e);
                }
            }
            
            // Fallback to query parameters (for form-encoded or query string)
            if (proposerWallet == null || proposerWallet.isEmpty()) {
                proposerWallet = request.getParameter("walletAddress");
                if (proposerWallet == null || proposerWallet.isEmpty()) {
                    proposerWallet = request.getParameter("wallet"); // Alternative parameter name
                }
            }
            
            if (targetRevision == null) {
                String paramRevision = request.getParameter("targetRevision");
                if (paramRevision != null && !paramRevision.isEmpty() && !"null".equals(paramRevision)) {
                    targetRevision = paramRevision;
                }
            }
            
            // Validate required parameter
            if (proposerWallet == null || proposerWallet.isEmpty()) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST, 
                    "walletAddress parameter required. Provide as JSON body: {\"walletAddress\":\"0x...\"} or query parameter: ?walletAddress=0x...");
                return;
            }
            
            // The proposal becomes state on every validator only when its GC_PROPOSAL log entry is applied
            org.apache.jackrabbit.oak.segment.consensus.gc.GCProposal proposal = gcManager.proposeGC(proposerWallet, targetRevision);
            boolean replicated = context.aeronConsensusEngine != null
                && context.aeronConsensusEngine.sendGCProposalThroughIngress(
                    proposal.proposalId,
                    proposal.proposerWallet,
                    proposal.targetRevision,
                    proposal.estimatedReclaimableSizeMB,
                    proposal.estimatedCostUSDC != null ? proposal.estimatedCostUSDC.toPlainString() : "0"
                );
            if (!replicated) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                    "GC proposal could not be sent to the cluster log; nothing was proposed");
                return;
            }
            log.info("✅ GC proposal {} sent through the Aeron cluster log", proposal.proposalId);
            
            Map<String, Object> payload = proposalToMap(proposal);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
            
        } catch (Exception e) {
            log.error("Error proposing GC", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
    
    /**
     * Handle POST /v1/gc/execute - Manually execute an approved GC proposal.
     * 
     * <p>Request Body (JSON):</p>
     * <pre>
     * {
     *   "proposalId": "uuid-here"  // Required: GC proposal ID to execute
     * }
     * </pre>
     * 
     * <p>Alternative: Query parameter</p>
     * <ul>
     *   <li><code>proposalId</code> (required) - GC proposal ID</li>
     * </ul>
     * 
     * <p>Note: the leader requests execution of an approved proposal once its payment is verified.
     * This endpoint requests it manually; execution starts on every validator when the GC_EXECUTE
     * log entry is applied, so the response only reports that the request entered the log.</p>
     */
    public void handleExecuteGC(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        
        try {
            GCProposalManager gcManager = getGCProposalManager();
            if (gcManager == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "GC Proposal Manager not initialized");
                return;
            }
            
            String proposalId = null;
            
            // Try to read from JSON body first
            String contentType = request.getContentType();
            if (contentType != null && contentType.contains("application/json")) {
                try {
                    java.io.BufferedReader reader = request.getReader();
                    StringBuilder body = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        body.append(line);
                    }
                    
                    if (body.length() > 0) {
                        String jsonBody = body.toString();
                        // Simple JSON parsing
                        int proposalStart = jsonBody.indexOf("\"proposalId\"");
                        if (proposalStart >= 0) {
                            int colonIndex = jsonBody.indexOf(":", proposalStart);
                            int quoteStart = jsonBody.indexOf("\"", colonIndex);
                            if (quoteStart >= 0) {
                                int quoteEnd = jsonBody.indexOf("\"", quoteStart + 1);
                                if (quoteEnd > quoteStart) {
                                    proposalId = jsonBody.substring(quoteStart + 1, quoteEnd);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    log.debug("Failed to parse JSON body, falling back to query parameters", e);
                }
            }
            
            // Fallback to query parameter
            if (proposalId == null || proposalId.isEmpty()) {
                proposalId = request.getParameter("proposalId");
            }
            
            if (proposalId == null || proposalId.isEmpty()) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST, 
                    "proposalId parameter required. Provide as JSON body: {\"proposalId\":\"uuid\"} or query parameter: ?proposalId=uuid");
                return;
            }
            
            // Get executor ID (validator/node ID)
            int executorId = 0;
            if (context.aeronConsensusEngine != null && context.aeronConsensusEngine.getCluster() != null) {
                executorId = context.aeronConsensusEngine.getCluster().memberId();
            }
            
            org.apache.jackrabbit.oak.segment.consensus.gc.GCProposal proposal = gcManager.getProposal(proposalId);
            if (proposal == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_NOT_FOUND, "GC proposal not found: " + proposalId);
                return;
            }
            if (proposal.state != org.apache.jackrabbit.oak.segment.consensus.gc.GCProposal.GCProposalState.APPROVED) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST,
                    "GC proposal not approved: " + proposalId + " (state: " + proposal.state + ")");
                return;
            }
            boolean replicated = context.aeronConsensusEngine != null
                && context.aeronConsensusEngine.sendGCExecuteThroughIngress(proposalId, executorId);
            if (!replicated) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                    "GC execute could not be sent to the cluster log");
                return;
            }

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("proposalId", proposalId);
            payload.put("executorId", executorId);
            payload.put("replicated", true);
            response.setStatus(HttpServletResponse.SC_ACCEPTED);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
            
        } catch (Exception e) {
            log.error("Error executing GC", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    /**
     * Handle POST /v1/gc/vote - Cast a vote on a GC proposal.
     *
     * <p>Request Body (JSON):</p>
     * <pre>
     * {
     *   "proposalId": "uuid-here",      // Required
     *   "validatorId": 0,               // Optional, defaults to current node ID
     *   "approve": true,                // Optional, defaults to true
     *   "reason": "approve from e2e"    // Optional
     * }
     * </pre>
     */
    public void handleVoteGC(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");

        try {
            GCProposalManager gcManager = getGCProposalManager();
            if (gcManager == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "GC Proposal Manager not initialized");
                return;
            }

            String proposalId = null;
            Integer validatorId = null;
            Boolean approve = null;
            String reason = null;

            String contentType = request.getContentType();
            if (contentType != null && contentType.contains("application/json")) {
                try {
                    java.io.BufferedReader reader = request.getReader();
                    StringBuilder body = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        body.append(line);
                    }
                    String jsonBody = body.toString();
                    if (!jsonBody.isEmpty()) {
                        proposalId = extractJsonStringField(jsonBody, "proposalId");
                        validatorId = extractJsonIntField(jsonBody, "validatorId");
                        approve = extractJsonBooleanField(jsonBody, "approve");
                        reason = extractJsonStringField(jsonBody, "reason");
                    }
                } catch (Exception e) {
                    log.debug("Failed to parse JSON vote body, falling back to query params", e);
                }
            }

            if (proposalId == null || proposalId.isEmpty()) {
                proposalId = request.getParameter("proposalId");
            }
            if (validatorId == null) {
                String validatorIdParam = request.getParameter("validatorId");
                if (validatorIdParam != null && !validatorIdParam.isEmpty()) {
                    try {
                        validatorId = Integer.parseInt(validatorIdParam);
                    } catch (NumberFormatException ignored) {
                        // Fallback below
                    }
                }
            }
            if (approve == null) {
                String approveParam = request.getParameter("approve");
                if (approveParam != null && !approveParam.isEmpty()) {
                    approve = Boolean.parseBoolean(approveParam);
                }
            }
            if (reason == null || reason.isEmpty()) {
                reason = request.getParameter("reason");
            }

            if (proposalId == null || proposalId.isEmpty()) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST,
                    "proposalId parameter required. Provide as JSON body or query parameter.");
                return;
            }

            int resolvedValidatorId = validatorId != null ? validatorId : 0;
            if (validatorId == null && context.aeronConsensusEngine != null && context.aeronConsensusEngine.getCluster() != null) {
                resolvedValidatorId = context.aeronConsensusEngine.getCluster().memberId();
            }
            boolean resolvedApprove = approve != null ? approve : true;
            String resolvedReason = reason != null ? reason : "";

            boolean replicated = false;
            boolean replicationAttempted = false;
            if (context.aeronConsensusEngine != null && context.aeronConsensusEngine.getCluster() != null) {
                replicationAttempted = true;
                replicated = context.aeronConsensusEngine.sendGCVoteThroughIngress(
                    proposalId,
                    resolvedValidatorId,
                    resolvedApprove,
                    resolvedReason
                );
            }

            // The tally changes only when the GC_VOTE log entry is applied
            if (!replicated) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                    "GC vote could not be sent to the cluster log; nothing was recorded");
                return;
            }

            org.apache.jackrabbit.oak.segment.consensus.gc.GCProposal proposal = gcManager.getProposal(proposalId);

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("proposalId", proposalId);
            payload.put("validatorId", resolvedValidatorId);
            payload.put("approve", resolvedApprove);
            payload.put("reason", resolvedReason);
            payload.put("replicated", replicated);
            payload.put("replicationAttempted", replicationAttempted);
            if (proposal != null) {
                payload.put("proposal", proposalToMap(proposal));
            }
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));

        } catch (Exception e) {
            log.error("Error voting on GC proposal", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
    
    /**
     * Get GC Proposal Manager from context.
     */
    private GCProposalManager getGCProposalManager() {
        if (context.gcProposalManager == null) {
            log.debug("GCProposalManager not initialized in ServerContext");
            return null;
        }
        return context.gcProposalManager;
    }
    
    /**
     * Format Wei to ETH (simplified - assumes 18 decimals).
     */
    private String formatWeiToEth(BigInteger wei) {
        if (wei.equals(BigInteger.ZERO)) {
            return "0";
        }
        // Simple formatting: divide by 10^18
        BigInteger eth = wei.divide(BigInteger.valueOf(10).pow(18));
        BigInteger remainder = wei.remainder(BigInteger.valueOf(10).pow(18));
        if (remainder.equals(BigInteger.ZERO)) {
            return eth.toString();
        }
        return eth.toString() + "." + remainder.toString().substring(0, Math.min(6, remainder.toString().length()));
    }

    private String extractJsonStringField(String json, String field) {
        String pattern = "\"" + field + "\"";
        int fieldStart = json.indexOf(pattern);
        if (fieldStart < 0) return null;
        int colon = json.indexOf(":", fieldStart);
        if (colon < 0) return null;
        int quoteStart = json.indexOf("\"", colon);
        if (quoteStart < 0) return null;
        int quoteEnd = json.indexOf("\"", quoteStart + 1);
        if (quoteEnd <= quoteStart) return null;
        return json.substring(quoteStart + 1, quoteEnd);
    }

    private Integer extractJsonIntField(String json, String field) {
        String pattern = "\"" + field + "\"";
        int fieldStart = json.indexOf(pattern);
        if (fieldStart < 0) return null;
        int colon = json.indexOf(":", fieldStart);
        if (colon < 0) return null;
        int idx = colon + 1;
        while (idx < json.length() && Character.isWhitespace(json.charAt(idx))) {
            idx++;
        }
        int end = idx;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
            end++;
        }
        if (end <= idx) return null;
        try {
            return Integer.parseInt(json.substring(idx, end));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Boolean extractJsonBooleanField(String json, String field) {
        String pattern = "\"" + field + "\"";
        int fieldStart = json.indexOf(pattern);
        if (fieldStart < 0) return null;
        int colon = json.indexOf(":", fieldStart);
        if (colon < 0) return null;
        int idx = colon + 1;
        while (idx < json.length() && Character.isWhitespace(json.charAt(idx))) {
            idx++;
        }
        if (json.regionMatches(idx, "true", 0, 4)) {
            return true;
        }
        if (json.regionMatches(idx, "false", 0, 5)) {
            return false;
        }
        return null;
    }
    
    /**
     * Get fragmentation tracker from context.
     */
    private FragmentationTracker getFragmentationTracker() {
        if (context.fragmentationTracker == null) {
            log.debug("FragmentationTracker not initialized in ServerContext");
            return null;
        }
        return context.fragmentationTracker;
    }
    
    /**
     * Handle GET /v1/gc/account/{walletAddress} - Get GC account status for entity.
     * 
     * @param request HTTP request
     * @param response HTTP response
     * @param walletAddress Ethereum wallet address
     */
    public void handleGetGCAccount(HttpServletRequest request, HttpServletResponse response, String walletAddress) throws IOException {
        response.setContentType("application/json");
        
        try {
            if (context.gcAccountManager == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "GC Account Manager not initialized");
                return;
            }
            
            org.apache.jackrabbit.oak.segment.consensus.gc.EntityGCAccount account = 
                context.gcAccountManager.getAccount(walletAddress);
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("walletAddress", account.walletAddress);
            payload.put("totalDebt", account.totalDebt.toString());
            payload.put("pendingDebt", account.getPendingDebt().toString());
            payload.put("executedDebt", account.executedDebt.toString());
            payload.put("debtLimit", account.debtLimit.toString());
            payload.put("writesBlocked", account.writesBlocked);
            payload.put("lastDeleteTime", account.lastDeleteTime);
            payload.put("deleteCount", account.deletes.size());
            payload.put("paymentCount", account.payments.size());
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
            
        } catch (Exception e) {
            log.error("Error getting GC account", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
    
    /**
     * Handle POST /v1/gc/account/{walletAddress}/pay - Record debt payment (manual for MVP).
     * 
     * @param request HTTP request
     * @param response HTTP response
     * @param walletAddress Ethereum wallet address
     */
    public void handlePayGCDebt(HttpServletRequest request, HttpServletResponse response, String walletAddress) throws IOException {
        response.setContentType("application/json");
        
        try {
            if (context.gcAccountManager == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "GC Account Manager not initialized");
                return;
            }
            
            // Get payment amount
            String amountStr = request.getParameter("amount");
            if (amountStr == null || amountStr.isEmpty()) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST, "amount parameter required");
                return;
            }
            
            java.math.BigDecimal amount = new java.math.BigDecimal(amountStr);
            String txHash = request.getParameter("txHash");  // Optional
            
            // Record payment
            context.gcAccountManager.recordPayment(walletAddress, amount, txHash);
            
            // Get updated account
            org.apache.jackrabbit.oak.segment.consensus.gc.EntityGCAccount account = 
                context.gcAccountManager.getAccount(walletAddress);

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("amountPaid", amount.toString());
            payload.put("remainingDebt", account.executedDebt.toString());
            payload.put("writesBlocked", account.writesBlocked);
            payload.put("message", account.writesBlocked
                ? "Payment recorded. Debt still exceeds limit - pay more to resume writes."
                : "Payment recorded. Writes resumed.");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
            
        } catch (Exception e) {
            log.error("Error recording payment", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
    
    /**
     * Handle POST /v1/gc/account/{walletAddress}/set-limit - Set debt limit (for testing).
     * 
     * @param request HTTP request
     * @param response HTTP response
     * @param walletAddress Ethereum wallet address
     */
    public void handleSetDebtLimit(HttpServletRequest request, HttpServletResponse response, String walletAddress) throws IOException {
        response.setContentType("application/json");
        
        try {
            if (context.gcAccountManager == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "GC Account Manager not initialized");
                return;
            }
            
            // Get limit
            String limitStr = request.getParameter("limit");
            if (limitStr == null || limitStr.isEmpty()) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST, "limit parameter required");
                return;
            }
            
            java.math.BigDecimal limit = new java.math.BigDecimal(limitStr);
            
            // Set limit
            context.gcAccountManager.setDebtLimit(walletAddress, limit);
            
            // Get updated account
            org.apache.jackrabbit.oak.segment.consensus.gc.EntityGCAccount account = 
                context.gcAccountManager.getAccount(walletAddress);

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("debtLimit", account.debtLimit.toString());
            payload.put("writesBlocked", account.writesBlocked);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
            
        } catch (Exception e) {
            log.error("Error setting debt limit", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
    
    /**
     * Handle POST /v1/gc/account/{walletAddress}/execute-pending - Convert pending to executed debt (simulate GC).
     * 
     * @param request HTTP request
     * @param response HTTP response
     * @param walletAddress Ethereum wallet address
     */
    public void handleExecutePendingDebt(HttpServletRequest request, HttpServletResponse response, String walletAddress) throws IOException {
        response.setContentType("application/json");
        
        try {
            if (context.gcAccountManager == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "GC Account Manager not initialized");
                return;
            }
            
            // Get account
            org.apache.jackrabbit.oak.segment.consensus.gc.EntityGCAccount account = 
                context.gcAccountManager.getAccount(walletAddress);
            
            java.math.BigDecimal pending = account.getPendingDebt();
            
            // Convert pending to executed
            account.convertPendingToExecuted(pending);

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("converted", pending.toString());
            payload.put("executedDebt", account.executedDebt.toString());
            payload.put("writesBlocked", account.writesBlocked);
            payload.put("message", account.writesBlocked
                ? "Pending debt converted to executed. Writes now BLOCKED - pay to resume."
                : "Pending debt converted to executed. Debt under limit.");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
            
        } catch (Exception e) {
            log.error("Error executing pending debt", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
    
    /**
     * Handle POST /v1/gc/trigger - Manually trigger periodic GC cycle (for testing).
     * 
     * @param request HTTP request
     * @param response HTTP response
     */
    public void handleTriggerGC(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        
        try {
            if (context.gcAccountManager == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "GC Account Manager not initialized");
                return;
            }
            
            // Call convertAllPendingToExecuted (simulates periodic GC)
            context.gcAccountManager.convertAllPendingToExecuted();
            
            // Get stats
            java.util.List<org.apache.jackrabbit.oak.segment.consensus.gc.EntityGCAccount> blocked = 
                context.gcAccountManager.getBlockedAccounts();
            java.util.List<org.apache.jackrabbit.oak.segment.consensus.gc.EntityGCAccount> withExecutedDebt = 
                context.gcAccountManager.getAccountsWithExecutedDebt();

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("message", "GC cycle executed - pending debt converted to executed");
            payload.put("entitiesWithExecutedDebt", withExecutedDebt.size());
            payload.put("entitiesBlocked", blocked.size());
            List<String> blockedWallets = new ArrayList<>();
            for (org.apache.jackrabbit.oak.segment.consensus.gc.EntityGCAccount account : blocked) {
                blockedWallets.add(account.walletAddress);
            }
            payload.put("blockedWallets", blockedWallets);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
            
            log.info("🧹 Manual GC triggered - {} entities with executed debt, {} blocked", 
                     withExecutedDebt.size(), blocked.size());
            
        } catch (Exception e) {
            log.error("Error triggering GC", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private Map<String, Object> proposalToMap(org.apache.jackrabbit.oak.segment.consensus.gc.GCProposal proposal) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("proposalId", proposal.proposalId);
        payload.put("proposerWallet", proposal.proposerWallet);
        payload.put("targetRevision", proposal.targetRevision);
        payload.put("state", proposal.state.toString());
        payload.put("estimatedReclaimableSizeMB", proposal.estimatedReclaimableSizeMB);
        payload.put("estimatedCostUSDC", proposal.estimatedCostUSDC != null ? proposal.estimatedCostUSDC.toString() : "0");
        payload.put("fragmentationOverheadMB", proposal.fragmentationOverheadMB);
        payload.put("fragmentationCostUSDC", proposal.fragmentationCostUSDC != null ? proposal.fragmentationCostUSDC.toString() : "0");
        payload.put("createdAt", proposal.createdAt);
        payload.put("expiresAt", proposal.expiresAt);

        Map<String, Object> votes = new LinkedHashMap<>();
        for (Map.Entry<Integer, org.apache.jackrabbit.oak.segment.consensus.gc.GCVote> entry : proposal.votes.entrySet()) {
            org.apache.jackrabbit.oak.segment.consensus.gc.GCVote vote = entry.getValue();
            Map<String, Object> votePayload = new LinkedHashMap<>();
            votePayload.put("vote", vote.approve ? "APPROVE" : "REJECT");
            votePayload.put("reason", vote.reason != null ? vote.reason : "");
            votePayload.put("timestamp", vote.timestamp);
            votes.put(String.valueOf(entry.getKey()), votePayload);
        }
        payload.put("votes", votes);
        payload.put("approveVotes", proposal.getApproveVoteCount());
        payload.put("rejectVotes", proposal.getRejectVoteCount());
        payload.put("totalVotes", proposal.getTotalVoteCount());
        return payload;
    }

    private Map<String, Object> metricsToMap(FragmentationTracker.EntityFragmentationMetrics metrics,
                                             FragmentationTracker tracker) {
        BigInteger tax = tracker.calculateFragmentationTax(metrics.walletAddress);
        List<String> tarFiles = tracker.getTarFilesForEntity(metrics.walletAddress);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("walletAddress", metrics.walletAddress);
        payload.put("tarFilesCreated", metrics.tarFilesCreated);
        payload.put("totalBytesWritten", metrics.totalBytesWritten);
        payload.put("totalBytesWrittenFormatted", FormatUtils.formatBytes(metrics.totalBytesWritten));
        payload.put("averageTarFileSize", metrics.averageTarFileSize);
        payload.put("averageTarFileSizeFormatted", FormatUtils.formatBytes(metrics.averageTarFileSize));
        payload.put("packingEfficiency", String.format("%.2f", metrics.packingEfficiency));
        payload.put("smallTarFileCount", metrics.smallTarFileCount);
        payload.put("fragmentationScore", metrics.fragmentationScore);
        payload.put("fragmentationTax", tax.toString());
        payload.put("fragmentationTaxFormatted", formatWeiToEth(tax) + " ETH");
        payload.put("lastWriteTimestamp", metrics.lastWriteTimestamp);
        payload.put("tarFiles", tarFiles);
        return payload;
    }
}
