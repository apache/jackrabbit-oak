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
package org.apache.jackrabbit.oak.segment.http.server;

import org.apache.jackrabbit.oak.segment.consensus.config.RuntimeConfigValueResolver;
import org.apache.jackrabbit.oak.segment.http.server.handlers.*;
import org.apache.jackrabbit.oak.segment.http.server.util.ApiErrorUtil;
import org.apache.jackrabbit.oak.segment.http.server.util.FormatUtils;
import org.apache.jackrabbit.oak.segment.http.server.util.JsonOutputUtil;
import org.apache.jackrabbit.oak.segment.http.server.sse.EventBroadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Central request router that delegates HTTP requests to appropriate handlers.
 * 
 * <p>This class replaces the large if-else chain in SegmentHttpServer with
 * a cleaner routing mechanism that delegates to specialized handler classes.</p>
 */
public class RequestRouter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RequestRouter.class);

    private final HealthHandler healthHandler;
    private final MetricsHandler metricsHandler;
    private final FileHandler fileHandler;
    private final ExplorerApiHandler explorerApiHandler;
    private final DashboardHandler dashboardHandler;
    private final ExplorerApiV1Handler explorerApiV1Handler;
    private final ConsensusApiHandler consensusApiHandler;
    private final RegistrationHandler registrationHandler;
    private final PeerDiscoveryHandler peerDiscoveryHandler;
    private final AeronApiHandler aeronApiHandler;
    private final FragmentationApiHandler fragmentationApiHandler;
    private final LeaderConsensusHandler leaderConsensusHandler;
    private final BinaryUploadHandler binaryUploadHandler;
    private final CidApiHandler cidApiHandler;
    private final EventStreamHandler eventStreamHandler;
    private final OsgiConfigApiHandler osgiConfigApiHandler;
    private final EventBroadcaster eventBroadcaster;
    private final org.apache.jackrabbit.oak.segment.http.server.binary.UploadSessionManager uploadSessionManager;
    private final AuthTokenValidator authValidator;
    private final RateLimiter rateLimiter;
    private final boolean browserUiEnabled;
    
    private final ServerContext context;

    public RequestRouter(ServerContext context) {
        this.context = context;
        this.authValidator = new AuthTokenValidator();
        this.rateLimiter = new RateLimiter();
        this.browserUiEnabled = RuntimeConfigValueResolver.readBoolean("oak.http.browser.ui.enabled", true);
        
        // Initialize all handlers
        this.healthHandler = new HealthHandler(
            context.fileStore,
            context.nodeStore,
            context.storeDirectory,
            context.aeronConsensusEngine,
            context.registeredClients,
            context.registeredValidators,
            context
        );
        this.metricsHandler = new MetricsHandler(
            context.aeronConsensusEngine,
            context.storeDirectory,
            context.registeredClients,
            context.registeredValidators,
            context
        );
        this.fileHandler = new FileHandler(
            context.fileStore,
            context.storeDirectory,
            context.connectedPeers
        );
        this.explorerApiHandler = ExplorerApiHandler.withBlobStoreSupplier(
            context.nodeStore,
            context.storeDirectory,
            () -> context.blobStore
        );
        this.dashboardHandler = new DashboardHandler(context);
        this.explorerApiV1Handler = new ExplorerApiV1Handler(context);
        this.consensusApiHandler = new ConsensusApiHandler(context);
        this.registrationHandler = new RegistrationHandler(context);
        this.peerDiscoveryHandler = new PeerDiscoveryHandler(context);
        this.aeronApiHandler = new AeronApiHandler(context);
        this.fragmentationApiHandler = new FragmentationApiHandler(context);
        this.leaderConsensusHandler = new LeaderConsensusHandler(context);
        
        // Binary upload handler (ADR 020 - lazy upload on confirmation)
        this.uploadSessionManager = new org.apache.jackrabbit.oak.segment.http.server.binary.UploadSessionManager();
        this.binaryUploadHandler = new BinaryUploadHandler(uploadSessionManager);

        // Make session manager available in context for dashboard metrics
        context.setUploadSessionManager(uploadSessionManager);
        
        // CID API handler (Oak ↔ IPFS CID mapping)
        this.cidApiHandler = new CidApiHandler(context);
        
        // SSE Event Broadcaster and Handler (ADR 036)
        this.eventBroadcaster = new EventBroadcaster();
        this.eventStreamHandler = new EventStreamHandler(context, eventBroadcaster);
        this.osgiConfigApiHandler = new OsgiConfigApiHandler();
        context.setEventBroadcaster(eventBroadcaster); // Make available to other components
    }

    /**
     * Get the ConsensusApiHandler instance.
     */
    public ConsensusApiHandler getConsensusApiHandler() {
        return consensusApiHandler;
    }
    
    /**
     * Route a request to the appropriate handler based on path and method.
     * 
     * @param request HTTP servlet request
     * @param response HTTP servlet response
     * @throws IOException if an I/O error occurs
     */
    public void route(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String path = request.getRequestURI();
        String method = request.getMethod();
        
        try {
            // Health checks (always public - needed for monitoring/load balancers)
            // Skip rate limiting for health checks
            if ("/health".equals(path) && "GET".equals(method)) {
                healthHandler.handleHealth(response);
                return;
            }

            if ("/health/local".equals(path) && "GET".equals(method)) {
                healthHandler.handleLocalHealth(response);
                return;
            }
            
            if ("/health/deep".equals(path) && "GET".equals(method)) {
                healthHandler.handleDeepHealth(response);
                return;
            }
            
            if ("/health/cluster".equals(path) && "GET".equals(method)) {
                healthHandler.handleClusterHealth(response);
                return;
            }

            if ("/v1/ops/snapshots/health".equals(path) && "GET".equals(method)) {
                healthHandler.handleGetOpsHealthSnapshot(response);
                return;
            }

            if ("/v1/ops/snapshots/runtime".equals(path) && "GET".equals(method)) {
                healthHandler.handleGetOpsRuntimeSnapshot(response);
                return;
            }

            if ("/v1/ops/snapshots/storage".equals(path) && "GET".equals(method)) {
                healthHandler.handleGetOpsStorageSnapshot(response);
                return;
            }
            
            // Internal segment-transfer endpoints are part of cluster-to-cluster read fabric,
            // not public API traffic, so they must bypass public rate limiting.
            if (!isRateLimitExempt(path, method) && !rateLimiter.allowRequest(request, response)) {
                rateLimiter.sendRateLimitResponse(response);
                return;
            }
            
            // Validate authentication for all other endpoints (if auth is enabled)
            // If auth is disabled (no token configured), this allows all requests (POC mode)
            if (!authValidator.validateRequest(request, response)) {
                return; // Response already sent by validateRequest
            }
            
            // Dashboard and UI
            if ("/".equals(path) || "/dashboard".equals(path)) {
                if ("GET".equals(method)) {
                    dashboardHandler.handleDashboard(response);
                    return;
                }
            }

            if (isBrowserUiRoute(path) && !browserUiEnabled) {
                ApiErrorUtil.sendJsonError(
                    response,
                    HttpServletResponse.SC_GONE,
                    "Browser UI routes are disabled. Use validator-native API surface (/v1/index) or an external gateway/UI. Set -Doak.http.browser.ui.enabled=true to enable."
                );
                return;
            }
            
            if ("/explorer".equals(path) && "GET".equals(method)) {
                dashboardHandler.handleExplorerUI(response);
                return;
            }
            
            if ("/api-browser".equals(path) && "GET".equals(method)) {
                dashboardHandler.handleApiBrowserUI(response);
                return;
            }

            if ("/v1/index".equals(path) && "GET".equals(method)) {
                dashboardHandler.handleApiIndex(response);
                return;
            }

            if ("/v1/config/osgi".equals(path) && "GET".equals(method)) {
                osgiConfigApiHandler.handleEffectiveConfig(response);
                return;
            }

            if ("/v1/config/osgi/schema".equals(path) && "GET".equals(method)) {
                osgiConfigApiHandler.handleConfigSchema(response);
                return;
            }

            if ("/v1/config/osgi/sources".equals(path) && "GET".equals(method)) {
                osgiConfigApiHandler.handleConfigSources(response);
                return;
            }

            if ("/v1/config/osgi/coverage".equals(path) && "GET".equals(method)) {
                osgiConfigApiHandler.handleCoverage(response);
                return;
            }

            if ("/v1/config/osgi/delta".equals(path) && "GET".equals(method)) {
                osgiConfigApiHandler.handleDelta(response);
                return;
            }
            
            // File serving
            if ("/journal.log".equals(path) && "GET".equals(method)) {
                fileHandler.handleFile(request, response, "journal.log", "text/plain");
                return;
            }
            
            if ("/manifest".equals(path)) {
                if ("HEAD".equals(method)) {
                    fileHandler.handleFileHead(response, "manifest", "text/plain");
                } else if ("GET".equals(method)) {
                    fileHandler.handleFile(request, response, "manifest", "text/plain");
                } else {
                    ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "Method not allowed");
                }
                return;
            }
            
            if ("/gc.log".equals(path) && "GET".equals(method)) {
                fileHandler.handleFile(request, response, "gc.log", "text/plain");
                return;
            }
            
            // Segments
            if (path != null && path.startsWith("/segments/")) {
                String segmentId = path.substring("/segments/".length());
                if ("HEAD".equals(method)) {
                    fileHandler.handleSegmentHead(response, segmentId);
                } else if ("GET".equals(method)) {
                    fileHandler.handleSegmentGet(request, response, segmentId);
                } else {
                    ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "Method not allowed");
                }
                return;
            }
            
            // Explorer API
            if ("/api/explore".equals(path) && "GET".equals(method)) {
                String nodePath = request.getParameter("path");
                explorerApiHandler.handleExploreNode(response, nodePath != null ? nodePath : "/");
                return;
            }
            
            if ("/api/segments/recent".equals(path) && "GET".equals(method)) {
                explorerApiHandler.handleRecentSegments(response);
                return;
            }
            
            if ("/api/segments/tars".equals(path) && "GET".equals(method)) {
                explorerApiHandler.handleTarFiles(response);
                return;
            }
            
            // Blob streaming API - serve binaries directly from Oak BlobStore
            if (path.startsWith("/api/blob/") && "GET".equals(method)) {
                String blobId = path.substring("/api/blob/".length());
                explorerApiHandler.handleBlobStream(request, response, blobId);
                return;
            }
            
            // CID API (Oak blob ID ↔ IPFS CID mapping)
            if ("/api/cid/stats".equals(path) && "GET".equals(method)) {
                cidApiHandler.handleStats(request, response);
                return;
            }
            if (path.startsWith("/api/cid/gateway/") && "GET".equals(method)) {
                cidApiHandler.handleGatewayRedirect(request, response);
                return;
            }
            if (path.startsWith("/api/cid/reverse/") && "GET".equals(method)) {
                cidApiHandler.handleReverseLookup(request, response);
                return;
            }
            if (path.startsWith("/api/cid/") && "GET".equals(method)) {
                cidApiHandler.handleGetCid(request, response);
                return;
            }
            
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // SSE Event Streaming API (ADR 036)
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if ("/v1/ops/events/stream".equals(path) && "GET".equals(method)) {
                eventStreamHandler.handleOpsEventStream(request, response);
                return;
            }

            if ("/v1/events/stream".equals(path) && "GET".equals(method)) {
                eventStreamHandler.handleEventStream(request, response);
                return;
            }
            
            if ("/v1/events/recent".equals(path) && "GET".equals(method)) {
                eventStreamHandler.handleRecentEvents(request, response);
                return;
            }
            
            if ("/v1/events/stats".equals(path) && "GET".equals(method)) {
                eventStreamHandler.handleStats(request, response);
                return;
            }
            
            // Metrics
            if ("/api/metrics".equals(path) && "GET".equals(method)) {
                metricsHandler.handleMetrics(response);
                return;
            }
            
            if ("/metrics".equals(path) && "GET".equals(method)) {
                metricsHandler.handlePrometheusMetrics(response);
                return;
            }
            
            // Consensus API
            if ("/v1/explorer/summary".equals(path) && "GET".equals(method)) {
                explorerApiV1Handler.handleSummary(response);
                return;
            }

            if ("/v1/explorer/release-flow".equals(path) && "GET".equals(method)) {
                explorerApiV1Handler.handleReleaseFlow(response);
                return;
            }

            if (path.startsWith("/v1/explorer/proposals/") && "GET".equals(method)) {
                String proposalId = path.substring("/v1/explorer/proposals/".length());
                explorerApiV1Handler.handleProposalById(response, proposalId);
                return;
            }

            if (path.startsWith("/v1/explorer/wallets/") && "GET".equals(method)) {
                String walletAddress = path.substring("/v1/explorer/wallets/".length());
                explorerApiV1Handler.handleWalletByAddress(response, walletAddress);
                return;
            }

            if ("/v1/explorer/content/nav".equals(path) && "GET".equals(method)) {
                explorerApiV1Handler.handleContentNav(response);
                return;
            }

            if (path.startsWith("/v1/explorer/content/clusters/") && "GET".equals(method)) {
                String clusterPath = path.substring("/v1/explorer/content/clusters/".length());
                int separator = clusterPath.indexOf('/');
                if (separator > 0 && separator < clusterPath.length() - 1) {
                    String clusterId = clusterPath.substring(0, separator);
                    String action = clusterPath.substring(separator + 1);
                    String requestedPath = request.getParameter("path");
                    if ("tree".equals(action)) {
                        explorerApiV1Handler.handleContentTree(response, clusterId, requestedPath);
                        return;
                    }
                    if ("node".equals(action)) {
                        explorerApiV1Handler.handleContentNode(response, clusterId, requestedPath);
                        return;
                    }
                    if ("provenance".equals(action)) {
                        explorerApiV1Handler.handleContentProvenance(response, clusterId, requestedPath);
                        return;
                    }
                }
            }

            if ("/v1/propose-write".equals(path) && "POST".equals(method)) {
                // Phase 1: Optional shard routing logging (for demonstration)
                // Phase 2: Will actually forward requests to correct shard
                if (context.shardRouter != null) {
                    String walletAddress = request.getParameter("walletAddress");
                    if (walletAddress == null || walletAddress.isEmpty()) {
                        walletAddress = request.getParameter("wallet"); // Fallback
                    }
                    if (walletAddress != null && !walletAddress.isEmpty()) {
                        try {
                            String leaderUrl = context.shardRouter.routeRequest(walletAddress);
                            if (leaderUrl != null) {
                                log.debug("🔀 Shard routing: wallet {} → leader {}", walletAddress, leaderUrl);
                                // Phase 1: Log routing decision (all requests still process locally)
                                // Phase 2: Forward to leaderUrl if different from selfUrl
                            }
                        } catch (Exception e) {
                            log.debug("Shard routing check failed: {}", e.getMessage());
                        }
                    }
                }
                consensusApiHandler.handleProposeWrite(request, response);
                return;
            }
            
            // Delete Proposal API
            if ("/v1/propose-delete".equals(path) && "POST".equals(method)) {
                consensusApiHandler.handleDeleteProposal(request, response);
                return;
            }
            
            // Binary Upload API (ADR 020 - Lazy upload on confirmation)
            if ("/v1/binary/declare-intent".equals(path) && "POST".equals(method)) {
                binaryUploadHandler.handleDeclareIntent(request, response);
                return;
            }
            
            if (path.startsWith("/v1/binary/check-intent/") && "GET".equals(method)) {
                binaryUploadHandler.handleCheckIntent(request, response);
                return;
            }
            
            if ("/v1/binary/complete-upload".equals(path) && "POST".equals(method)) {
                binaryUploadHandler.handleCompleteUpload(request, response);
                return;
            }
            
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // Mock Epoch Control API (only works in MOCK mode)
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if ("/api/mock/advance-epoch".equals(path) && "POST".equals(method)) {
                handleMockAdvanceEpoch(request, response);
                return;
            }
            
            if ("/api/mock/set-epoch-offset".equals(path) && "POST".equals(method)) {
                handleMockSetEpochOffset(request, response);
                return;
            }
            
            if ("/api/mock/epoch-status".equals(path) && "GET".equals(method)) {
                handleMockEpochStatus(request, response);
                return;
            }
            
            // HEAD endpoint - returns JSON with committedHead vs latestHead
            if ("/v1/head".equals(path) && "GET".equals(method)) {
                response.setContentType("application/json");
                response.setStatus(HttpServletResponse.SC_OK);
                
                StringBuilder json = new StringBuilder();
                json.append("{\n");
                
                // 🔄 CRITICAL: Get HEAD from AeronConsensusEngine first (tracks latest HEAD correctly)
                // Fallback to FileStore only if Aeron engine not available
                String latestHead = null;
                String committedHead = null;
                int latestEpochSeen = -1;
                int committedEpoch = -1;
                
                if (context.aeronConsensusEngine != null) {
                    // Get tracked HEAD values from AeronConsensusEngine (most accurate)
                    latestHead = context.aeronConsensusEngine.getLatestHead();
                    committedHead = context.aeronConsensusEngine.getCommittedHead();
                    latestEpochSeen = context.aeronConsensusEngine.getLatestEpochSeen();
                    committedEpoch = context.aeronConsensusEngine.getLastCommittedEpoch();
                }
                
                // Fallback to FileStore HEAD if Aeron engine not available or latestHead not set
                if (latestHead == null || latestHead.isEmpty()) {
                    latestHead = context.fileStore.getHead().getRecordId().toString10();
                }
                
                json.append("  \"latestHead\": \"").append(FormatUtils.escapeJson(latestHead)).append("\",\n");
                if (committedHead == null || committedHead.isEmpty()) {
                    json.append("  \"committedHead\": null");
                } else {
                    json.append("  \"committedHead\": \"").append(FormatUtils.escapeJson(committedHead)).append("\"");
                }
                
                if (latestEpochSeen >= 0) {
                    json.append(",\n  \"latestEpochSeen\": ").append(latestEpochSeen);
                }
                if (committedEpoch >= 0) {
                    json.append(",\n  \"committedEpoch\": ").append(committedEpoch);
                }
                
                json.append("\n}\n");
                response.getWriter().write(json.toString());
                return;
            }
            
            if ("/v1/consensus/status".equals(path) && "GET".equals(method)) {
                consensusApiHandler.handleGetConsensusStatus(response);
                return;
            }

            if ("/v1/consensus/leader".equals(path) && "GET".equals(method)) {
                consensusApiHandler.handleGetConsensusLeader(response);
                return;
            }
            
            // Query APIs
            if ("/v1/wallets/stats".equals(path) && "GET".equals(method)) {
                consensusApiHandler.handleWalletStats(request, response);
                return;
            }
            
            if ("/v1/wallets/content".equals(path) && "GET".equals(method)) {
                consensusApiHandler.handleWalletContent(request, response);
                return;
            }
            
            // Follower HEAD update endpoint (used by leader to broadcast HEAD to followers)
            if ("/v1/follower/head-update".equals(path) && "POST".equals(method)) {
                leaderConsensusHandler.handleFollowerHeadUpdate(request, response);
                return;
            }
            
            // GC Cost Estimation
            if ("/v1/gc/estimate".equals(path) && "GET".equals(method)) {
                consensusApiHandler.handleGCCostEstimate(request, response);
                return;
            }
            
            // Proposal Queue Status
            if (path.startsWith("/v1/ops/operations/") && "GET".equals(method)) {
                consensusApiHandler.handleGetOperationStatus(request, response);
                return;
            }

            if ("/v1/ops/snapshots/queue".equals(path) && "GET".equals(method)) {
                consensusApiHandler.handleGetOpsQueueSnapshot(response);
                return;
            }

            if (path.startsWith("/v1/settlement/proposals/") && "GET".equals(method)) {
                consensusApiHandler.handleGetSettlementByProposalId(request, response);
                return;
            }

            if (path.startsWith("/v1/settlement/transactions/") && "GET".equals(method)) {
                consensusApiHandler.handleGetSettlementByTransactionHash(request, response);
                return;
            }

            if ("/v1/ops/snapshots/cluster".equals(path) && "GET".equals(method)) {
                aeronApiHandler.handleGetOpsClusterSnapshot(response);
                return;
            }

            if ("/v1/ops/snapshots/replication".equals(path) && "GET".equals(method)) {
                aeronApiHandler.handleGetOpsReplicationSnapshot(response);
                return;
            }

            if (path.startsWith("/v1/proposals/") && path.endsWith("/status") && "GET".equals(method)) {
                consensusApiHandler.handleGetProposalStatus(request, response);
                return;
            }
            
            if ("/v1/proposals/pending/count".equals(path) && "GET".equals(method)) {
                consensusApiHandler.handleGetPendingCount(response);
                return;
            }

            if ("/v1/proposals/queue/stats".equals(path) && "GET".equals(method)) {
                consensusApiHandler.handleGetQueueStats(response);
                return;
            }

            if ("/v1/proposals/release-flow".equals(path) && "GET".equals(method)) {
                consensusApiHandler.handleGetProposalReleaseFlow(response);
                return;
            }
            
            // Registration
            if ("/v1/register-client".equals(path) && ("POST".equals(method) || "PUT".equals(method))) {
                registrationHandler.handleClientRegistration(request, response);
                return;
            }
            
            // Peer discovery
            if ("/v1/peers".equals(path) && "GET".equals(method)) {
                peerDiscoveryHandler.handlePeerList(response);
                return;
            }
            
            if ("/v1/ngrok-url".equals(path) && "GET".equals(method)) {
                peerDiscoveryHandler.handleNgrokUrl(response);
                return;
            }
            
            // Blockchain configuration endpoint
            if ("/v1/blockchain/config".equals(path) && "GET".equals(method)) {
                new BlockchainConfigApiHandler(context).handle(response);
                return;
            }
            
            // Aeron Cluster-specific endpoints
            if ("/v1/aeron/cluster-state".equals(path) && "GET".equals(method)) {
                aeronApiHandler.handleClusterState(response);
                return;
            }

            if ("/v1/aeron/validator-identities".equals(path) && "GET".equals(method)) {
                aeronApiHandler.handleValidatorIdentities(response);
                return;
            }
            
            if ("/v1/aeron/raft-metrics".equals(path) && "GET".equals(method)) {
                aeronApiHandler.handleRaftMetrics(response);
                return;
            }
            
            if ("/v1/aeron/node-status".equals(path) && "GET".equals(method)) {
                aeronApiHandler.handleNodeStatus(request, response);
                return;
            }
            
            if ("/v1/aeron/leadership-history".equals(path) && "GET".equals(method)) {
                aeronApiHandler.handleLeadershipHistory(request, response);
                return;
            }
            
            // ✅ ADR 025: Replication lag monitoring endpoint
            if ("/v1/aeron/replication-lag".equals(path) && "GET".equals(method)) {
                aeronApiHandler.handleReplicationLag(response);
                return;
            }
            
            // Fragmentation & GC Metrics API
            if ("/v1/fragmentation/metrics".equals(path) && "GET".equals(method)) {
                fragmentationApiHandler.handleGetAllMetrics(request, response);
                return;
            }
            
            if (path != null && path.startsWith("/v1/fragmentation/metrics/") && "GET".equals(method)) {
                String walletAddress = path.substring("/v1/fragmentation/metrics/".length());
                fragmentationApiHandler.handleGetEntityMetrics(request, response, walletAddress);
                return;
            }
            
            if ("/v1/fragmentation/top".equals(path) && "GET".equals(method)) {
                fragmentationApiHandler.handleGetTopFragmented(request, response);
                return;
            }
            
            if ("/v1/gc/status".equals(path) && "GET".equals(method)) {
                fragmentationApiHandler.handleGetGcStatus(request, response);
                return;
            }
            
            if ("/v1/compaction/proposals".equals(path) && "GET".equals(method)) {
                fragmentationApiHandler.handleGetCompactionProposals(request, response);
                return;
            }
            
            if ("/v1/propose-gc".equals(path) && "POST".equals(method)) {
                fragmentationApiHandler.handleProposeGC(request, response);
                return;
            }
            
            if ("/v1/gc/execute".equals(path) && "POST".equals(method)) {
                fragmentationApiHandler.handleExecuteGC(request, response);
                return;
            }

            if ("/v1/gc/vote".equals(path) && "POST".equals(method)) {
                fragmentationApiHandler.handleVoteGC(request, response);
                return;
            }
            
            // GC Account Management
            if (path != null && path.startsWith("/v1/gc/account/")) {
                // Extract wallet address from path
                String remaining = path.substring("/v1/gc/account/".length());
                
                // Check for sub-paths
                if (remaining.contains("/pay") && "POST".equals(method)) {
                    String walletAddress = remaining.substring(0, remaining.indexOf("/pay"));
                    fragmentationApiHandler.handlePayGCDebt(request, response, walletAddress);
                    return;
                } else if (remaining.contains("/set-limit") && "POST".equals(method)) {
                    String walletAddress = remaining.substring(0, remaining.indexOf("/set-limit"));
                    fragmentationApiHandler.handleSetDebtLimit(request, response, walletAddress);
                    return;
                } else if (remaining.contains("/execute-pending") && "POST".equals(method)) {
                    String walletAddress = remaining.substring(0, remaining.indexOf("/execute-pending"));
                    fragmentationApiHandler.handleExecutePendingDebt(request, response, walletAddress);
                    return;
                } else if ("GET".equals(method) && !remaining.contains("/")) {
                    // GET /v1/gc/account/{walletAddress}
                    fragmentationApiHandler.handleGetGCAccount(request, response, remaining);
                    return;
                }
            }
            
            // Manual GC trigger endpoint (for testing)
            if ("/v1/gc/trigger".equals(path) && "POST".equals(method)) {
                fragmentationApiHandler.handleTriggerGC(request, response);
                return;
            }
            
            // Not found - log with context
            String remoteAddr = request.getRemoteAddr();
            String userAgent = request.getHeader("User-Agent");
            
            // Filter out known invalid requests (Composum Browser, etc.) - log at debug level
            if (path != null && (path.startsWith("/bin/") || path.startsWith("/system/") || path.startsWith("/content/"))) {
                // These are Sling/AEM endpoints, not validator endpoints - suppress noise
                log.debug("⚠️  Invalid request (Sling/AEM endpoint on validator): {} {} FROM {} [UA: {}]", 
                    method, path, remoteAddr, userAgent != null ? userAgent : "unknown");
            } else {
                // Unknown endpoint - log at info level
                log.info("⚠️  404 Not Found: {} {} FROM {} [UA: {}]", 
                    method, path, remoteAddr, userAgent != null ? userAgent : "unknown");
            }
            
            if (isApiPath(path)) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_NOT_FOUND, "Not found");
            } else {
                response.sendError(HttpServletResponse.SC_NOT_FOUND);
            }
            
        } catch (Exception e) {
            log.error("Error routing request: " + path, e);
            if (isApiPath(path)) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
            } else {
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
            }
        }
    }

    private boolean isApiPath(String path) {
        if (path == null) {
            return false;
        }
        return path.startsWith("/v1/")
            || path.startsWith("/api/")
            || path.startsWith("/health")
            || path.startsWith("/metrics")
            || path.startsWith("/journal.log")
            || path.startsWith("/manifest")
            || path.startsWith("/segments/");
    }

    private boolean isBrowserUiRoute(String path) {
        if (path == null) {
            return false;
        }
        return "/explorer".equals(path)
            || "/api-browser".equals(path);
    }

    private boolean isRateLimitExempt(String path, String method) {
        if (path == null || method == null) {
            return false;
        }
        if (path.startsWith("/health")) {
            return true;
        }
        if ("/v1/ops/snapshots/health".equals(path) && "GET".equals(method)) {
            return true;
        }
        if ("/journal.log".equals(path) && "GET".equals(method)) {
            return true;
        }
        if ("/manifest".equals(path) && ("GET".equals(method) || "HEAD".equals(method))) {
            return true;
        }
        if ("/gc.log".equals(path) && "GET".equals(method)) {
            return true;
        }
        return path.startsWith("/segments/")
            && ("GET".equals(method) || "HEAD".equals(method));
    }
    
    /**
     * Get the binary upload handler (for integration with other components).
     * 
     * @return the binary upload handler
     */
    public BinaryUploadHandler getBinaryUploadHandler() {
        return binaryUploadHandler;
    }
    
    /**
     * Get the event broadcaster (for emitting SSE events from other components).
     * 
     * @return the event broadcaster
     */
    public EventBroadcaster getEventBroadcaster() {
        return eventBroadcaster;
    }

    @Override
    public void close() {
        try {
            consensusApiHandler.close();
        } catch (RuntimeException e) {
            log.warn("Failed to close consensus API handler", e);
        }
        try {
            eventBroadcaster.shutdown();
        } catch (RuntimeException e) {
            log.warn("Failed to shutdown event broadcaster", e);
        }
        try {
            uploadSessionManager.shutdown();
        } catch (RuntimeException e) {
            log.warn("Failed to shutdown upload session manager", e);
        }
        if (context.cidMappingService != null) {
            try {
                context.cidMappingService.close();
            } catch (RuntimeException e) {
                log.warn("Failed to close CID mapping service", e);
            }
        }
        try {
            rateLimiter.shutdown();
        } catch (RuntimeException e) {
            log.warn("Failed to shutdown rate limiter", e);
        }
        context.eventBroadcaster = null;
        context.uploadSessionManager = null;
        context.cidMappingService = null;
    }
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // MOCK EPOCH CONTROL HANDLERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    /**
     * Compatibility stub for removed synthetic mock epoch control.
     *
     * <p>POST /api/mock/advance-epoch?epochs=N now returns 410 Gone.</p>
     */
    private void handleMockAdvanceEpoch(jakarta.servlet.http.HttpServletRequest request, 
                                        jakarta.servlet.http.HttpServletResponse response) throws java.io.IOException {
        ApiErrorUtil.sendJsonError(
            response,
            HttpServletResponse.SC_GONE,
            "Synthetic mock epoch control was removed by ADR 080. Mock mode now uses Sepolia chain context."
        );
    }
    
    /**
     * Compatibility stub for removed synthetic mock epoch control.
     *
     * <p>POST /api/mock/set-epoch-offset?offset=N now returns 410 Gone.</p>
     */
    private void handleMockSetEpochOffset(jakarta.servlet.http.HttpServletRequest request, 
                                          jakarta.servlet.http.HttpServletResponse response) throws java.io.IOException {
        ApiErrorUtil.sendJsonError(
            response,
            HttpServletResponse.SC_GONE,
            "Synthetic mock epoch control was removed by ADR 080. Mock mode now uses Sepolia chain context."
        );
    }
    
    /**
     * Sepolia-backed epoch status view retained for compatibility in mock mode.
     */
    private void handleMockEpochStatus(jakarta.servlet.http.HttpServletRequest request, 
                                       jakarta.servlet.http.HttpServletResponse response) throws java.io.IOException {
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig config = 
            org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.getInstance();
        
        response.setContentType("application/json");
        
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"mode\":\"").append(FormatUtils.escapeJson(config.getMode().toString())).append("\",");
        
        if (context.proposalQueueManager != null) {
            org.apache.jackrabbit.oak.segment.consensus.eth.BeaconChainClient beaconClient =
                context.proposalQueueManager.getBeaconClient();
            if (beaconClient != null) {
                java.util.Map<String, Object> health = beaconClient.getHealthStatus();
                json.append("\"currentEpoch\":").append(beaconClient.getCachedCurrentEpoch()).append(",");
                json.append("\"finalizedEpoch\":").append(beaconClient.getCachedFinalizedEpoch()).append(",");
                json.append("\"fresh\":").append(beaconClient.isEpochDataFresh()).append(",");
                json.append("\"timeSinceUpdateMs\":").append(beaconClient.getMillisSinceLastUpdate());
                
                if (config.getMode() == org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.Mode.MOCK) {
                    json.append(",\"chainContext\":\"Sepolia\"");
                }
            } else {
                json.append("\"error\":\"BeaconChainClient not available\"");
            }
        } else {
            json.append("\"error\":\"ProposalQueueManager or EpochQueue not available\"");
        }
        
        json.append("}");
        response.getWriter().write(json.toString());
    }
}
