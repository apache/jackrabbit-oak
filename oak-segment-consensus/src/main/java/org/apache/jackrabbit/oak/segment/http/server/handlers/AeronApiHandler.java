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

import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.segment.http.server.util.ApiErrorUtil;
import org.apache.jackrabbit.oak.segment.http.server.util.JsonParser;
import org.apache.jackrabbit.oak.segment.http.server.util.JsonOutputUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handler for Aeron Cluster-specific API endpoints.
 * 
 * <p>This handler provides Aeron Cluster-specific information that isn't available
 * through the generic consensus APIs. It exposes Raft metrics, cluster membership,
 * and leadership history.</p>
 */
public class AeronApiHandler {

    private static final Logger log = LoggerFactory.getLogger(AeronApiHandler.class);
    private static final long OPS_SNAPSHOT_TTL_MS = 1000L;

    private final ServerContext context;
    private final Object clusterSnapshotLock = new Object();
    private final Object replicationSnapshotLock = new Object();
    private volatile Map<String, Object> cachedClusterSnapshotData;
    private volatile long cachedClusterSnapshotSourceTimestampMs;
    private volatile Map<String, Object> cachedReplicationSnapshotData;
    private volatile long cachedReplicationSnapshotSourceTimestampMs;

    public AeronApiHandler(ServerContext context) {
        this.context = context;
    }

    /**
     * ✈️ AERON NATIVE: Get Aeron Cluster state using native Aeron APIs.
     * 
     * This uses Aeron's internal cluster state directly - no HTTP API calls or custom discovery.
     * Returns complete Aeron Cluster state including:
     * - role: Current role (LEADER/FOLLOWER) from cluster.role()
     * - memberId: This node's member ID from cluster.memberId()
     * - leadershipTermId: Current Raft term from cluster.leadershipTermId()
     * - clusterMemberCount: Total members from cluster.clusterMemberCount()
     * - clusterTime: Cluster time from cluster.time()
     * - logPosition: Log position from cluster.logPosition()
     * - members: List of all cluster members from cluster.clusterMembers()
     * 
     * @return Cluster state map, or null if Aeron Cluster not configured
     */
    public Map<String, Object> getClusterStateData() {
        if (context.aeronConsensusEngine == null) {
            return null;
        }

        // ✈️ AERON NATIVE: Use Aeron's native cluster state API
        Map<String, Object> nativeState = context.aeronConsensusEngine.getNativeClusterState();
        if (nativeState == null) {
            return null;
        }
        
        // Add our cluster identifier and enrich with additional info
        Map<String, Object> state = new HashMap<>(nativeState);
        state.put("clusterId", "oak-consensus-cluster");
        Object memberIdValue = state.get("memberId");
        if (memberIdValue instanceof Number && ((Number) memberIdValue).intValue() >= 0) {
            state.put("nodeId", ((Number) memberIdValue).intValue());
        } else {
            state.put("nodeId", getNodeIdFromUrl(context.selfUrl));
        }
        
        // Add validator identity (wallet address and public key)
        Map<String, Object> validatorIdentity = new HashMap<>();
        if (context.aeronConsensusEngine != null) {
            String walletAddress = context.aeronConsensusEngine.getWalletAddress();
            String publicKey = context.aeronConsensusEngine.getPublicKeyHex();
            if (walletAddress != null) {
                validatorIdentity.put("walletAddress", walletAddress);
            }
            if (publicKey != null) {
                validatorIdentity.put("publicKey", publicKey);
            }
        }
        if (!validatorIdentity.isEmpty()) {
            state.put("validatorIdentity", validatorIdentity);
        }
        
        // Enrich members list with wallet addresses (only self - fast, no network calls)
        // Peer wallet addresses are optional and can be fetched via separate endpoint if needed
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> members = (List<Map<String, Object>>) state.get("members");
        if (members != null) {
            enrichSelfWalletAddress(members);
        }
        
        // Add MediaDriver health status
        Map<String, Object> mediaDriver = new HashMap<>();
        if (context.aeronClusterLauncher != null) {
            org.apache.jackrabbit.oak.segment.consensus.aeron.CrashHandler crashHandler = 
                context.aeronClusterLauncher.getCrashHandler();
            if (crashHandler != null) {
                mediaDriver.put("status", crashHandler.hasCrashed() ? "UNHEALTHY" : "HEALTHY");
                mediaDriver.put("crashCount", crashHandler.getCrashCount());
                mediaDriver.put("hasCrashed", crashHandler.hasCrashed());
                mediaDriver.put("forceBootstrap", crashHandler.shouldForceBootstrap());
            } else {
                mediaDriver.put("status", "UNKNOWN");
            }
        } else {
            mediaDriver.put("status", "NOT_CONFIGURED");
        }
        state.put("mediaDriver", mediaDriver);
        
        // Add quorum information
        Integer memberCount = (Integer) state.get("memberCount");
        if (memberCount == null) {
            Object clusterMemberCount = state.get("clusterMemberCount");
            if (clusterMemberCount instanceof Number) {
                memberCount = ((Number) clusterMemberCount).intValue();
            }
        }
        if (memberCount != null) {
            Map<String, Object> quorum = new HashMap<>();
            int quorumSize = (memberCount / 2) + 1; // Majority
            quorum.put("required", quorumSize);
            quorum.put("current", context.aeronConsensusEngine.getReachableValidatorCount());
            quorum.put("totalMembers", memberCount);
            quorum.put("hasQuorum", context.aeronConsensusEngine.getReachableValidatorCount() >= quorumSize);
            state.put("quorum", quorum);
        }
        
        // Add reachable count (if available from native state, otherwise use fallback)
        if (!state.containsKey("reachableCount")) {
            state.put("reachableCount", context.aeronConsensusEngine.getReachableValidatorCount());
        }
        
        // Ensure consensus metrics are present
        Map<String, Object> consensus = new HashMap<>();
        consensus.put("reachableValidators", 
            state.containsKey("reachableCount") ? state.get("reachableCount") : 
            context.aeronConsensusEngine.getReachableValidatorCount());
        consensus.put("totalMembers", state.get("clusterMemberCount"));
        consensus.put("lastHeartbeat", context.aeronConsensusEngine.getLastHeartbeatTime());
        state.put("consensus", consensus);
        
        // Add Aeron metrics summary (if available)
        if (context.aeronPrometheusMetrics != null) {
            Map<String, Object> aeronMetrics = new HashMap<>();
            aeronMetrics.put("available", true);
            aeronMetrics.put("note", "Detailed metrics available at /metrics endpoint");
            state.put("aeronMetrics", aeronMetrics);
        }
        
        // Add cluster health summary
        Map<String, Object> health = new HashMap<>();
        boolean hasQuorum = false;
        if (memberCount != null) {
            int quorumSize = (memberCount / 2) + 1;
            hasQuorum = context.aeronConsensusEngine.getReachableValidatorCount() >= quorumSize;
        }
        health.put("status", hasQuorum ? "HEALTHY" : "DEGRADED");
        health.put("hasQuorum", hasQuorum);
        health.put("mediaDriverHealthy", mediaDriver.get("status").equals("HEALTHY"));
        state.put("health", health);
        state.put("internalIngressClient", context.aeronConsensusEngine.getInternalIngressClientDiagnostics());
        
        return state;
    }
    
    /**
     * Handle GET /v1/aeron/cluster-state - Returns complete Aeron Cluster state
     * 
     * Response includes:
     * - clusterId: Cluster identifier
     * - nodeId: This node's ID
     * - role: Current role (LEADER/FOLLOWER)
     * - term: Current Raft term
     * - members: List of all cluster members with their status
     * - consensus: Raft consensus metrics
     */
    public void handleClusterState(HttpServletResponse response) throws IOException {
        Map<String, Object> state = getClusterStateData();
        if (state == null) {
            sendError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, 
                "Aeron Cluster consensus not configured");
            return;
        }

        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        
        // Write JSON response
        writeJsonResponse(response, state);
    }

    /**
     * Handle GET /v1/aeron/validator-identities - Returns validator identity map across cluster members.
     *
     * Response includes per-validator:
     * - memberId, nodeId, url, role, status
     * - walletAddress, publicKey (when known)
     */
    public void handleValidatorIdentities(HttpServletResponse response) throws IOException {
        Map<String, Object> data = getValidatorIdentitiesData();
        if (data == null) {
            sendError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                "Aeron Cluster consensus not configured");
            return;
        }

        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        writeJsonResponse(response, data);
    }
    
    /**
     * Resolve hostname-based URL to IP-based URL for reliable networking.
     */
    private String resolveUrlToIP(String url) {
        try {
            java.net.URL parsedUrl = new java.net.URL(url);
            String hostname = parsedUrl.getHost();
            int port = parsedUrl.getPort();
            String protocol = parsedUrl.getProtocol();
            String path = parsedUrl.getPath();
            
            // If already an IP address, return as-is
            if (hostname.matches("^\\d+\\.\\d+\\.\\d+\\.\\d+$")) {
                return url;
            }
            
            // Resolve hostname to IP
            try {
                String ip = java.net.InetAddress.getByName(hostname).getHostAddress();
                return String.format("%s://%s%s%s",
                    protocol,
                    ip,
                    port != -1 ? ":" + port : "",
                    path != null ? path : "");
            } catch (java.net.UnknownHostException e) {
                // If resolution fails, return original URL (may be ngrok/Ethos URL)
                return url;
            }
        } catch (Exception e) {
            return url;
        }
    }

    /**
     * Handle GET /v1/aeron/raft-metrics - Returns Raft-specific metrics
     * 
     * Response includes:
     * - electionMetrics: Leader election statistics
     * - replicationMetrics: Log replication statistics
     * - commitMetrics: Commit statistics
     */
    public void handleRaftMetrics(HttpServletResponse response) throws IOException {
        if (context.aeronConsensusEngine == null) {
            sendError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, 
                "Aeron Cluster consensus not configured");
            return;
        }

        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);

        Map<String, Object> metrics = new HashMap<>();
        
        // Election metrics
        Map<String, Object> electionMetrics = new HashMap<>();
        electionMetrics.put("currentTerm", context.aeronConsensusEngine.getCurrentTerm());
        electionMetrics.put("isLeader", context.aeronConsensusEngine.isLeader());
        electionMetrics.put("currentLeader", context.aeronConsensusEngine.getCurrentLeader());
        metrics.put("electionMetrics", electionMetrics);
        
        // Replication metrics
        Map<String, Object> replicationMetrics = new HashMap<>();
        replicationMetrics.put("reachableValidators", context.aeronConsensusEngine.getReachableValidatorCount());
        replicationMetrics.put("totalFollowers", context.aeronConsensusEngine.getAllFollowers().size());
        metrics.put("replicationMetrics", replicationMetrics);
        
        // Commit metrics
        Map<String, Object> commitMetrics = new HashMap<>();
        commitMetrics.put("currentEpoch", context.aeronConsensusEngine.getCurrentEpoch());
        commitMetrics.put("ethereumEpoch", context.aeronConsensusEngine.getCurrentEthereumEpoch());
        metrics.put("commitMetrics", commitMetrics);
        
        // Write JSON response
        writeJsonResponse(response, metrics);
    }

    /**
     * Handle GET /v1/aeron/node-status - Returns status of specific cluster node
     * 
     * Query params:
     * - nodeId: Node ID (optional, defaults to self)
     * - url: Node URL (optional, alternative to nodeId)
     */
    public void handleNodeStatus(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (context.aeronConsensusEngine == null) {
            sendError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, 
                "Aeron Cluster consensus not configured");
            return;
        }

        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);

        String nodeIdParam = request.getParameter("nodeId");
        String urlParam = request.getParameter("url");
        
        String targetUrl = context.selfUrl; // Default to self
        if (urlParam != null && !urlParam.isEmpty()) {
            targetUrl = urlParam;
        } else if (nodeIdParam != null && !nodeIdParam.isEmpty()) {
            // Find URL by node ID
            int targetNodeId = Integer.parseInt(nodeIdParam);
            String currentLeader = context.aeronConsensusEngine.getCurrentLeader();
            if (currentLeader != null && getNodeIdFromUrl(currentLeader) == targetNodeId) {
                targetUrl = currentLeader;
            } else {
                for (String followerUrl : context.aeronConsensusEngine.getAllFollowers()) {
                    if (getNodeIdFromUrl(followerUrl) == targetNodeId) {
                        targetUrl = followerUrl;
                        break;
                    }
                }
            }
        }
        
        Map<String, Object> nodeStatus = new HashMap<>();
        nodeStatus.put("nodeId", getNodeIdFromUrl(targetUrl));
        nodeStatus.put("url", targetUrl);
        nodeStatus.put("role", targetUrl.equals(context.aeronConsensusEngine.getCurrentLeader()) ? "LEADER" : "FOLLOWER");
        nodeStatus.put("status", "ACTIVE");
        nodeStatus.put("lastHeartbeat", context.aeronConsensusEngine.getLastHeartbeatTime());
        nodeStatus.put("isSelf", targetUrl.equals(context.selfUrl));
        
        // Basic metrics
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("reachableValidators", context.aeronConsensusEngine.getReachableValidatorCount());
        nodeStatus.put("metrics", metrics);
        
        // Write JSON response
        writeJsonResponse(response, nodeStatus);
    }

    /**
     * ✈️ AERON NATIVE: Handle GET /v1/aeron/leadership-history - Returns recent leadership rotations
     * 
     * This uses Aeron's onRoleChange() callback history to show when leaders rotated.
     * 
     * Query params:
     * - limit: Number of entries to return (default: 10)
     */
    public void handleLeadershipHistory(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (context.aeronConsensusEngine == null) {
            sendError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, 
                "Aeron Cluster consensus not configured");
            return;
        }

        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);

        // Parse limit parameter
        int limit = 10; // Default
        String limitParam = request.getParameter("limit");
        if (limitParam != null && !limitParam.isEmpty()) {
            try {
                limit = Integer.parseInt(limitParam);
                if (limit < 1) limit = 10;
                if (limit > 100) limit = 100; // Cap at 100
            } catch (NumberFormatException e) {
                // Use default
            }
        }
        
        // ✈️ AERON NATIVE: Get leadership history from onRoleChange() callbacks
        java.util.List<org.apache.jackrabbit.oak.segment.consensus.aeron.LeadershipChange> changes =
            context.aeronConsensusEngine.getLeadershipHistory(limit);
        
        Map<String, Object> history = new HashMap<>();
        history.put("contractVersion", "aeron.leadership-history.v2");
        List<Map<String, Object>> entries = new ArrayList<>();
        
        for (org.apache.jackrabbit.oak.segment.consensus.aeron.LeadershipChange change : changes) {
            Map<String, Object> entry = new HashMap<>();
            entry.put("timestamp", change.timestamp);
            entry.put("observedAtMs", change.timestamp);
            entry.put("clusterTime", change.clusterTime);
            entry.put("term", change.term);
            entry.put("memberId", change.memberId);
            entry.put("memberUrl", change.memberUrl);
            entry.put("previousRole", change.previousRole != null ? change.previousRole.name() : "UNKNOWN");
            entry.put("newRole", change.newRole.name());
            entry.put("isLeaderRotation", change.newRole == io.aeron.cluster.service.Cluster.Role.LEADER && 
                change.previousRole != io.aeron.cluster.service.Cluster.Role.LEADER);
            entries.add(entry);
        }
        
        history.put("history", entries);
        history.put("totalEntries", entries.size());
        history.put("limit", limit);
        
        // Write JSON response
        writeJsonResponse(response, history);
    }

    /**
     * Enrich self member with wallet address (fast, no network calls).
     * Peer wallet addresses are not included to keep API response fast (<100ms).
     * Use /v1/aeron/node-status?url=<peer-url> to get peer wallet addresses if needed.
     */
    private void enrichSelfWalletAddress(List<Map<String, Object>> members) {
        // Only enrich self wallet address (fast, no network calls)
        for (Map<String, Object> member : members) {
            String memberUrl = (String) member.get("url");
            if (memberUrl != null && memberUrl.equals(context.selfUrl)) {
                // This is us - add our wallet info
                if (context.aeronConsensusEngine != null) {
                    String walletAddress = context.aeronConsensusEngine.getWalletAddress();
                    String publicKey = context.aeronConsensusEngine.getPublicKeyHex();
                    if (walletAddress != null) {
                        member.put("walletAddress", walletAddress);
                    }
                    if (publicKey != null) {
                        member.put("publicKey", publicKey);
                    }
                }
                break; // Found self, no need to continue
            }
        }
    }

    /**
     * Build authoritative validator identity list by querying each validator's local cluster-state.
     */
    public Map<String, Object> getValidatorIdentitiesData() {
        if (context.aeronConsensusEngine == null) {
            return null;
        }

        Set<String> validatorUrls = new LinkedHashSet<>();
        if (context.selfUrl != null && !context.selfUrl.isEmpty()) {
            validatorUrls.add(context.selfUrl);
        }
        List<String> followers = context.aeronConsensusEngine.getAllFollowers();
        if (followers != null) {
            for (String follower : followers) {
                if (follower != null && !follower.isEmpty()) {
                    validatorUrls.add(follower);
                }
            }
        }

        List<Map<String, Object>> validators = new ArrayList<>();
        for (String validatorUrl : validatorUrls) {
            validators.add(fetchValidatorIdentity(validatorUrl));
        }

        Map<String, Object> payload = new HashMap<>();
        payload.put("validators", validators);
        payload.put("totalValidators", validators.size());
        payload.put("knownWallets", validators.stream()
            .filter(v -> v.get("walletAddress") != null && !String.valueOf(v.get("walletAddress")).isEmpty())
            .count());
        payload.put("timestamp", System.currentTimeMillis());
        return payload;
    }

    private Map<String, Object> fetchValidatorIdentity(String validatorUrl) {
        Map<String, Object> identity = new HashMap<>();
        identity.put("url", validatorUrl);
        identity.put("nodeId", getNodeIdFromUrl(validatorUrl));
        identity.put("memberId", -1);
        identity.put("role", "UNKNOWN");
        identity.put("status", "UNKNOWN");
        identity.put("walletAddress", null);
        identity.put("publicKey", null);

        if (validatorUrl == null || validatorUrl.isEmpty()) {
            return identity;
        }

        // Self identity is available locally and avoids an HTTP round-trip.
        if (isSameUrlByPort(validatorUrl, context.selfUrl)) {
            int memberId = context.aeronConsensusEngine.getNativeClusterState() != null
                ? parseIntSafely(context.aeronConsensusEngine.getNativeClusterState().get("memberId"), -1)
                : -1;
            identity.put("memberId", memberId);
            identity.put("role", context.aeronConsensusEngine.isLeader() ? "LEADER" : "FOLLOWER");
            identity.put("status", "ACTIVE");
            identity.put("walletAddress", context.aeronConsensusEngine.getWalletAddress());
            identity.put("publicKey", context.aeronConsensusEngine.getPublicKeyHex());
            if (memberId >= 0) {
                identity.put("nodeId", memberId);
            }
            return identity;
        }

        try {
            String queryUrl = resolveUrlToIP(validatorUrl);
            java.net.URL apiUrl = new java.net.URL(queryUrl + "/v1/aeron/cluster-state");
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) apiUrl.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(1500);
            conn.setReadTimeout(2000);

            int code = conn.getResponseCode();
            if (code == 200) {
                java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(conn.getInputStream())
                );
                String response = reader.lines().collect(java.util.stream.Collectors.joining());
                reader.close();

                int memberId = parseIntSafely(JsonParser.extractField(response, "memberId"), -1);
                if (memberId >= 0) {
                    identity.put("memberId", memberId);
                    identity.put("nodeId", memberId);
                }

                String role = JsonParser.extractField(response, "role");
                if (role != null && !role.isEmpty()) {
                    identity.put("role", role.toUpperCase());
                }

                String status = JsonParser.extractField(response, "status");
                if (status != null && !status.isEmpty()) {
                    identity.put("status", status.toUpperCase());
                } else {
                    identity.put("status", "ACTIVE");
                }

                String validatorIdentity = JsonParser.extractObject(response, "validatorIdentity");
                String walletAddress = null;
                String publicKey = null;
                if (validatorIdentity != null) {
                    walletAddress = JsonParser.extractField(validatorIdentity, "walletAddress");
                    publicKey = JsonParser.extractField(validatorIdentity, "publicKey");
                }
                if (walletAddress == null || walletAddress.isEmpty()) {
                    walletAddress = JsonParser.extractField(response, "walletAddress");
                }
                if (publicKey == null || publicKey.isEmpty()) {
                    publicKey = JsonParser.extractField(response, "publicKey");
                }
                identity.put("walletAddress", walletAddress);
                identity.put("publicKey", publicKey);
            }
        } catch (Exception e) {
            log.debug("Failed to fetch validator identity from {}: {}", validatorUrl, e.getMessage());
        }

        return identity;
    }

    private int parseIntSafely(Object value, int fallback) {
        if (value == null) return fallback;
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (Exception e) {
            return fallback;
        }
    }

    private boolean isSameUrlByPort(String url1, String url2) {
        if (url1 == null || url2 == null) {
            return false;
        }
        try {
            int port1 = extractPort(url1);
            int port2 = extractPort(url2);
            return port1 == port2 && port1 != -1;
        } catch (Exception e) {
            return url1.equals(url2);
        }
    }

    private int extractPort(String url) {
        try {
            int colonIndex = url.lastIndexOf(':');
            if (colonIndex > 0) {
                String portStr = url.substring(colonIndex + 1);
                if (portStr.endsWith("/")) {
                    portStr = portStr.substring(0, portStr.length() - 1);
                }
                return Integer.parseInt(portStr);
            }
        } catch (Exception e) {
            // Ignore parsing errors
        }
        return -1;
    }
    
    /**
     * Extract node ID from validator URL.
     * Assumes format: http://validator-N:port
     */
    private int getNodeIdFromUrl(String url) {
        if (url == null) return -1;
        try {
            // Extract hostname from URL
            java.net.URL urlObj = new java.net.URL(url);
            String host = urlObj.getHost();
            if (host.startsWith("validator-")) {
                String nodeIdStr = host.substring("validator-".length());
                return Integer.parseInt(nodeIdStr);
            }
            // Fallback: hash URL to get consistent node ID
            return Math.abs(url.hashCode() % 100);
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Write JSON response from Map.
     */
    private void writeJsonResponse(HttpServletResponse response, Map<String, Object> data) throws IOException {
        response.getWriter().write(JsonOutputUtil.toJson(data));
    }

    /**
     * ✅ ADR 025: Handle GET /v1/aeron/replication-lag - Returns replication lag status
     * 
     * <p>Shows how far behind this follower is from the leader's log position.
     * Useful for monitoring cluster health and detecting slow followers.
     * 
     * <p>Response includes:
     * - role: Current role (LEADER/FOLLOWER)
     * - myLogPosition: This node's log position
     * - leaderLogPosition: Leader's log position
     * - replicationLag: Number of messages behind leader
     * - lagThreshold: Alert threshold (1000 messages)
     * - measurementAvailable: Whether the leader position is known locally
     * - measurementAgeMs: Age of the observed leader position
     * - healthStatus: HEALTHY, LAGGING, or UNKNOWN
     * - healthy: Whether measured lag is acceptable, or null when unavailable
     */
    public void handleReplicationLag(HttpServletResponse response) throws IOException {
        if (context.aeronConsensusEngine == null) {
            sendError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, 
                "Aeron Cluster consensus not configured");
            return;
        }
        
        Map<String, Object> lagStatus = context.aeronConsensusEngine.getReplicationLagStatus();
        
        if (lagStatus == null) {
            sendError(response, HttpServletResponse.SC_NOT_FOUND, 
                "Replication lag not applicable (cluster not initialized)");
            return;
        }
        
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        writeJsonResponse(response, lagStatus);
    }

    /**
     * Get Aeron replication lag status data map.
     */
    public Map<String, Object> getReplicationLagData() {
        if (context.aeronConsensusEngine == null) {
            return null;
        }
        return context.aeronConsensusEngine.getReplicationLagStatus();
    }

    /**
     * Get ops.v1 cluster snapshot with freshness/degraded metadata.
     * GET /v1/ops/snapshots/cluster
     */
    public void handleGetOpsClusterSnapshot(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        long servedAtMs = System.currentTimeMillis();

        try {
            Map<String, Object> data;
            long sourceTimestampMs;
            boolean fromCache = false;

            synchronized (clusterSnapshotLock) {
                long now = System.currentTimeMillis();
                boolean cacheValid = cachedClusterSnapshotData != null
                    && cachedClusterSnapshotSourceTimestampMs > 0
                    && (now - cachedClusterSnapshotSourceTimestampMs) <= OPS_SNAPSHOT_TTL_MS;

                if (cacheValid) {
                    data = cachedClusterSnapshotData;
                    sourceTimestampMs = cachedClusterSnapshotSourceTimestampMs;
                    fromCache = true;
                } else {
                    data = getClusterStateData();
                    if (data == null) {
                        throw new IllegalStateException("Aeron Cluster consensus not configured");
                    }
                    sourceTimestampMs = now;
                    cachedClusterSnapshotData = data;
                    cachedClusterSnapshotSourceTimestampMs = sourceTimestampMs;
                }
            }

                long stalenessMs = Math.max(0L, servedAtMs - sourceTimestampMs);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(buildOpsSnapshotEnvelope(
                data, sourceTimestampMs, servedAtMs, stalenessMs, false, null, fromCache));
        } catch (Exception e) {
            log.warn("Error building ops cluster snapshot, attempting stale fallback: {}", e.getMessage());
            if (cachedClusterSnapshotData != null && cachedClusterSnapshotSourceTimestampMs > 0) {
                long stalenessMs = Math.max(0L, servedAtMs - cachedClusterSnapshotSourceTimestampMs);
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(buildOpsSnapshotEnvelope(
                    cachedClusterSnapshotData,
                    cachedClusterSnapshotSourceTimestampMs,
                    servedAtMs,
                    stalenessMs,
                    true,
                    "STALE_CACHE_FALLBACK",
                    true));
                return;
            }
            sendError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, e.getMessage());
        }
    }

    /**
     * Get ops.v1 replication snapshot with freshness/degraded metadata.
     * GET /v1/ops/snapshots/replication
     */
    public void handleGetOpsReplicationSnapshot(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        long servedAtMs = System.currentTimeMillis();

        try {
            Map<String, Object> data;
            long sourceTimestampMs;
            boolean fromCache = false;

            synchronized (replicationSnapshotLock) {
                long now = System.currentTimeMillis();
                boolean cacheValid = cachedReplicationSnapshotData != null
                    && cachedReplicationSnapshotSourceTimestampMs > 0
                    && (now - cachedReplicationSnapshotSourceTimestampMs) <= OPS_SNAPSHOT_TTL_MS;

                if (cacheValid) {
                    data = cachedReplicationSnapshotData;
                    sourceTimestampMs = cachedReplicationSnapshotSourceTimestampMs;
                    fromCache = true;
                } else {
                    data = getReplicationLagData();
                    if (data == null) {
                        throw new IllegalStateException("Replication lag not applicable (cluster not initialized)");
                    }
                    sourceTimestampMs = now;
                    cachedReplicationSnapshotData = data;
                    cachedReplicationSnapshotSourceTimestampMs = sourceTimestampMs;
                }
            }

            long stalenessMs = Math.max(0L, servedAtMs - sourceTimestampMs);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(buildOpsSnapshotEnvelope(
                data, sourceTimestampMs, servedAtMs, stalenessMs, false, null, fromCache));
        } catch (Exception e) {
            log.warn("Error building ops replication snapshot, attempting stale fallback: {}", e.getMessage());
            if (cachedReplicationSnapshotData != null && cachedReplicationSnapshotSourceTimestampMs > 0) {
                long stalenessMs = Math.max(0L, servedAtMs - cachedReplicationSnapshotSourceTimestampMs);
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(buildOpsSnapshotEnvelope(
                    cachedReplicationSnapshotData,
                    cachedReplicationSnapshotSourceTimestampMs,
                    servedAtMs,
                    stalenessMs,
                    true,
                    "STALE_CACHE_FALLBACK",
                    true));
                return;
            }
            sendError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, e.getMessage());
        }
    }
    
    /**
     * Send standardized error response.
     */
    private void sendError(HttpServletResponse response, int statusCode, String message) throws IOException {
        ApiErrorUtil.sendJsonError(response, statusCode, message);
    }

    private String buildOpsSnapshotEnvelope(Object data,
                                            long sourceTimestampMs,
                                            long servedAtMs,
                                            long stalenessMs,
                                            boolean degraded,
                                            String degradedReason,
                                            boolean cacheHit) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("contractVersion", "ops.v1");
        payload.put("sourceTimestampMs", sourceTimestampMs);
        payload.put("servedAtMs", servedAtMs);
        payload.put("stalenessMs", stalenessMs);
        payload.put("degraded", degraded);
        payload.put("degradedReason", degradedReason);
        Map<String, Object> cache = new HashMap<>();
        cache.put("hit", cacheHit);
        cache.put("ttlMs", OPS_SNAPSHOT_TTL_MS);
        payload.put("cache", cache);
        payload.put("data", data);
        return JsonOutputUtil.toJson(payload);
    }
}
