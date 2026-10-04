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
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import io.aeron.cluster.service.Cluster;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Service responsible for discovering the current cluster leader.
 * 
 * <p>Extracted from AeronConsensusEngine to isolate leader discovery logic
 * and improve testability. Handles mapping Aeron member IDs to validator URLs
 * and caching leader information.
 * 
 * <p><strong>OSGi Component:</strong> Stateful service with lifecycle management.
 * 
 * <p><strong>Responsibilities:</strong>
 * <ul>
 *   <li>Discover leader URL from Aeron cluster state</li>
 *   <li>Map Aeron member IDs to validator URLs</li>
 *   <li>Cache leader information to reduce lookups</li>
 *   <li>Resolve URLs to IP addresses for matching</li>
 * </ul>
 * 
 * <p><strong>Leader Discovery Strategy:</strong>
 * <ol>
 *   <li>Check cache (TTL 10s)</li>
 *   <li>This node's own role, then the leader learned from the log
 *       ({@code onNewLeadershipTermEvent}, mapped member ID to validator URL)</li>
 *   <li>Fallback to peer polling only while no leader is known</li>
 * </ol>
 */
@Component(
    service = LeaderDiscoveryService.class,
    immediate = true,
    configurationPolicy = ConfigurationPolicy.OPTIONAL,
    property = {
        "service.description=Leader Discovery Service",
        "service.vendor=Apache Software Foundation"
    }
)
public class LeaderDiscoveryService {
    
    private static final Logger log = LoggerFactory.getLogger(LeaderDiscoveryService.class);
    
    /** Leader cache TTL (ms) */
    private static final long LEADER_CACHE_TTL_MS = 10000; // 10 seconds
    
    /** HTTP connection timeout for peer polling (ms) */
    private static final int HTTP_CONNECT_TIMEOUT_MS = 2000;
    
    /** HTTP read timeout for peer polling (ms) */
    private static final int HTTP_READ_TIMEOUT_MS = 3000;
    
    private final Map<Integer, String> nodeIdToUrl;
    private final List<String> peerUrls;
    
    private volatile String cachedLeaderUrl = null;
    private volatile long cachedLeaderTimestamp = 0;
    
    // Track known leader from role change callbacks
    private volatile String knownLeaderUrl = null;
    private volatile int knownLeaderMemberId = -1;
    
    // Self URL for comparison
    private volatile String selfUrl = null;
    
    /**
     * Create a new leader discovery service (default constructor for OSGi).
     */
    public LeaderDiscoveryService() {
        this.nodeIdToUrl = new java.util.concurrent.ConcurrentHashMap<>();
        this.peerUrls = new java.util.ArrayList<>();
    }
    
    /**
     * Create a new leader discovery service with mappings (for programmatic use).
     * 
     * @param nodeIdToUrl mapping of Aeron member IDs to validator URLs
     * @param peerUrls list of peer validator URLs
     */
    public LeaderDiscoveryService(Map<Integer, String> nodeIdToUrl, List<String> peerUrls) {
        this.nodeIdToUrl = nodeIdToUrl;
        this.peerUrls = peerUrls;
    }
    
    /**
     * OSGi lifecycle: Activate component.
     */
    @Activate
    protected void activate() {
        log.info("✅ LeaderDiscoveryService activated: peers={}", peerUrls.size());
    }
    
    /**
     * OSGi lifecycle: Deactivate component.
     */
    @Deactivate
    protected void deactivate() {
        log.info("✅ LeaderDiscoveryService deactivated");
        invalidateCache();
    }
    
    /**
     * Set node ID mappings (for OSGi injection).
     */
    public void setNodeIdMapping(Map<Integer, String> nodeIdToUrl) {
        this.nodeIdToUrl.clear();
        this.nodeIdToUrl.putAll(nodeIdToUrl);
        log.debug("Updated node ID mapping: {}", nodeIdToUrl);
    }
    
    /**
     * Set peer URLs (for OSGi injection).
     */
    public void setPeerUrls(List<String> peerUrls) {
        this.peerUrls.clear();
        this.peerUrls.addAll(peerUrls);
        log.debug("Updated peer URLs: {}", peerUrls);
    }
    
    /**
     * Set self URL for this validator.
     */
    public void setSelfUrl(String selfUrl) {
        this.selfUrl = selfUrl;
        log.debug("Set self URL: {}", selfUrl);
    }
    
    /**
     * Notify that this node became leader.
     * Called from AeronConsensusEngine.onRoleChange() when role becomes LEADER.
     * 
     * @param memberId the Aeron member ID of this node
     */
    public void notifyBecameLeader(int memberId) {
        this.knownLeaderUrl = selfUrl;
        this.knownLeaderMemberId = memberId;
        this.cachedLeaderUrl = selfUrl;
        this.cachedLeaderTimestamp = System.currentTimeMillis();
        log.info("🎯 This node is now leader (memberId: {}, url: {})", memberId, selfUrl);
    }
    
    /**
     * Notify that this node is no longer leader.
     * Called from AeronConsensusEngine.onRoleChange() when role changes from LEADER.
     *
     * <p>Only clears a leader that is this node. Role changes and leadership term events are
     * separate callbacks with no guaranteed order, so a newer leader already learned from the
     * log must survive a late step-down notification.
     */
    public void notifyLostLeadership() {
        invalidateCache();
        String known = knownLeaderUrl;
        if (known != null && !known.equals(selfUrl)) {
            log.info("🔄 This node lost leadership - keeping newer leader from the log: {}", known);
            return;
        }
        this.knownLeaderUrl = null;
        this.knownLeaderMemberId = -1;
        log.info("🔄 This node lost leadership - will discover new leader");
    }
    
    /**
     * Set the known leader (called when we discover leader from another source).
     * 
     * @param leaderUrl URL of the leader
     * @param memberId Aeron member ID of the leader (-1 if unknown)
     */
    public void setKnownLeader(String leaderUrl, int memberId) {
        this.knownLeaderUrl = leaderUrl;
        this.knownLeaderMemberId = memberId;
        this.cachedLeaderUrl = leaderUrl;
        this.cachedLeaderTimestamp = System.currentTimeMillis();
        log.debug("Set known leader: {} (memberId: {})", leaderUrl, memberId);
    }

    /**
     * Return the best locally known leader hint without performing network I/O.
     */
    public String getKnownLeaderHint() {
        if (knownLeaderUrl != null && !knownLeaderUrl.isEmpty()) {
            return knownLeaderUrl;
        }
        if (cachedLeaderUrl != null && !cachedLeaderUrl.isEmpty()) {
            return cachedLeaderUrl;
        }
        return null;
    }
    
    /**
     * Discover the current leader URL.
     * 
     * @param cluster Aeron cluster instance
     * @return leader URL, or null if not found
     */
    public String discoverLeader(Cluster cluster) {
        // Check cache
        if (cachedLeaderUrl != null && 
            (System.currentTimeMillis() - cachedLeaderTimestamp) < LEADER_CACHE_TTL_MS) {
            return cachedLeaderUrl;
        }
        
        // Discover from Aeron cluster
        String leaderUrl = discoverFromAeronCluster(cluster);
        
        if (leaderUrl != null) {
            // Update cache
            cachedLeaderUrl = leaderUrl;
            cachedLeaderTimestamp = System.currentTimeMillis();
            return leaderUrl;
        }
        
        // Fallback: poll peers
        log.debug("Leader discovery from Aeron failed, polling peers...");
        leaderUrl = discoverFromPeers();
        
        if (leaderUrl != null) {
            cachedLeaderUrl = leaderUrl;
            cachedLeaderTimestamp = System.currentTimeMillis();
        }
        
        return leaderUrl;
    }
    
    /**
     * Discover leader from Aeron cluster state.
     * 
     * <p><strong>Implementation Strategy:</strong>
     * The {@code Cluster} interface has no leader accessor; followers learn the leader member ID from
     * {@code ClusteredService.onNewLeadershipTermEvent}, which sets {@link #setKnownLeader}.
     * <ol>
     *   <li>Check if current node is leader via cluster.role()</li>
     *   <li>Use the known leader (from the log, or from this node's own election)</li>
     * </ol>
     */
    private String discoverFromAeronCluster(Cluster cluster) {
        if (cluster == null) {
            return null;
        }
        
        try {
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // STRATEGY 1: Check if WE are the leader
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if (cluster.role() == Cluster.Role.LEADER) {
                log.debug("This node is leader (role check)");
                return selfUrl;
            }
            
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // STRATEGY 2: Use known leader (leadership term event or own election)
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if (knownLeaderUrl != null) {
                log.debug("Using tracked leader: {}", knownLeaderUrl);
                return knownLeaderUrl;
            }
            
            log.debug("Could not determine leader from Aeron cluster state");
            
        } catch (Exception e) {
            log.error("Failed to discover leader from Aeron cluster", e);
        }
        
        return null;
    }
    
    /**
     * Discover leader by polling peers.
     * 
     * <p>This is a fallback when Aeron cluster state is not available.
     * It polls each peer's /v1/aeron/cluster-state endpoint to find who reports as leader.
     */
    private String discoverFromPeers() {
        log.debug("Polling {} peers for leader discovery", peerUrls.size());
        
        for (String peerUrl : peerUrls) {
            try {
                String leaderUrl = pollPeerForLeader(peerUrl);
                if (leaderUrl != null) {
                    log.info("Discovered leader via peer polling: {}", leaderUrl);
                    return leaderUrl;
                }
            } catch (Exception e) {
                log.debug("Failed to poll peer {}: {}", peerUrl, e.getMessage());
            }
        }

        return null;
    }
    
    /**
     * Poll a single peer for leader information.
     * 
     * @param peerUrl URL of the peer to poll
     * @return leader URL if this peer knows the leader, null otherwise
     */
    private String pollPeerForLeader(String peerUrl) {
        try {
            java.net.URL apiUrl = new java.net.URL(peerUrl + "/v1/consensus/leader");
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) apiUrl.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(HTTP_READ_TIMEOUT_MS);
            
            // Handle ngrok URLs
            if (peerUrl.contains("ngrok")) {
                conn.setRequestProperty("ngrok-skip-browser-warning", "true");
            }
            
            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                String json = readResponseBody(conn);
                String currentLeader = extractJsonField(json, "currentLeader");
                if (currentLeader != null && !currentLeader.isEmpty() && !"null".equals(currentLeader)) {
                    log.debug("Peer {} reports leader as: {}", peerUrl, currentLeader);
                    return currentLeader;
                }

                String isLeader = extractJsonField(json, "isLeader");
                if ("true".equalsIgnoreCase(isLeader)) {
                    log.debug("Peer {} reports as LEADER", peerUrl);
                    return peerUrl;
                }
            } else if (responseCode == java.net.HttpURLConnection.HTTP_NOT_FOUND) {
                return pollPeerForLeaderLegacy(peerUrl);
            }
            
        } catch (java.net.SocketTimeoutException e) {
            log.debug("Timeout polling peer {}", peerUrl);
        } catch (java.io.IOException e) {
            log.debug("IO error polling peer {}: {}", peerUrl, e.getMessage());
        } catch (Exception e) {
            log.debug("Error polling peer {}: {}", peerUrl, e.getMessage());
        }
        
        return null;
    }

    private String pollPeerForLeaderLegacy(String peerUrl) {
        try {
            java.net.URL apiUrl = new java.net.URL(peerUrl + "/v1/aeron/cluster-state");
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) apiUrl.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(HTTP_READ_TIMEOUT_MS);

            if (peerUrl.contains("ngrok")) {
                conn.setRequestProperty("ngrok-skip-browser-warning", "true");
            }

            if (conn.getResponseCode() != 200) {
                return null;
            }

            String json = readResponseBody(conn);
            String currentLeader = extractJsonField(json, "currentLeader");
            if (currentLeader != null && !currentLeader.isEmpty() && !"null".equals(currentLeader)) {
                return currentLeader;
            }
            String role = extractJsonField(json, "role");
            if ("LEADER".equalsIgnoreCase(role)) {
                return peerUrl;
            }

            String leaderUrl = extractJsonField(json, "leaderUrl");
            if (leaderUrl != null && !leaderUrl.isEmpty() && !"null".equals(leaderUrl)) {
                return leaderUrl;
            }
        } catch (Exception e) {
            log.debug("Legacy leader polling failed for {}: {}", peerUrl, e.getMessage());
        }
        return null;
    }

    private String readResponseBody(java.net.HttpURLConnection conn) throws java.io.IOException {
        java.io.BufferedReader reader = new java.io.BufferedReader(
            new java.io.InputStreamReader(conn.getInputStream())
        );
        try {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            return response.toString();
        } finally {
            reader.close();
        }
    }
    
    /**
     * Extract a field from JSON string (simple parser).
     */
    private String extractJsonField(String json, String field) {
        String pattern = "\"" + field + "\":\"";
        int startIdx = json.indexOf(pattern);
        if (startIdx < 0) {
            // Try without quotes (for non-string values)
            pattern = "\"" + field + "\":";
            startIdx = json.indexOf(pattern);
            if (startIdx < 0) {
                return null;
            }
            startIdx += pattern.length();
            // Skip whitespace
            while (startIdx < json.length() && Character.isWhitespace(json.charAt(startIdx))) {
                startIdx++;
            }
            // Handle quoted or unquoted value
            if (startIdx < json.length() && json.charAt(startIdx) == '"') {
                startIdx++;
                int endIdx = json.indexOf("\"", startIdx);
                return endIdx > startIdx ? json.substring(startIdx, endIdx) : null;
            }
            // Unquoted value (number, boolean, null)
            int endIdx = startIdx;
            while (endIdx < json.length() && !Character.isWhitespace(json.charAt(endIdx)) 
                   && json.charAt(endIdx) != ',' && json.charAt(endIdx) != '}') {
                endIdx++;
            }
            return endIdx > startIdx ? json.substring(startIdx, endIdx) : null;
        }
        startIdx += pattern.length();
        int endIdx = json.indexOf("\"", startIdx);
        return endIdx > startIdx ? json.substring(startIdx, endIdx) : null;
    }
    
    /**
     * Invalidate leader cache.
     * 
     * <p>Call this when leader changes to force re-discovery.
     */
    public void invalidateCache() {
        cachedLeaderUrl = null;
        cachedLeaderTimestamp = 0;
        log.debug("Leader cache invalidated");
    }
    
    /**
     * Check if two URLs point to the same validator (by port).
     * 
     * <p>This handles cases where URLs might differ in protocol or hostname
     * but actually refer to the same validator instance.
     */
    public boolean isSameUrl(String url1, String url2) {
        if (url1 == null || url2 == null) {
            return false;
        }
        
        if (url1.equals(url2)) {
            return true;
        }
        
        try {
            URI uri1 = new URI(url1.startsWith("http") ? url1 : "http://" + url1);
            URI uri2 = new URI(url2.startsWith("http") ? url2 : "http://" + url2);
            
            // Same host and port = same validator
            if (uri1.getPort() == uri2.getPort()) {
                String host1 = resolveUrlToIP(uri1.getHost());
                String host2 = resolveUrlToIP(uri2.getHost());
                
                return host1 != null && host1.equals(host2);
            }
            
        } catch (Exception e) {
            log.debug("Failed to compare URLs: {} vs {}", url1, url2, e);
        }
        
        return false;
    }
    
    /**
     * Resolve URL hostname to IP address.
     */
    private String resolveUrlToIP(String hostname) {
        try {
            // Handle localhost specially
            if ("localhost".equalsIgnoreCase(hostname)) {
                return "127.0.0.1";
            }
            
            // Already an IP?
            if (hostname.matches("\\d+\\.\\d+\\.\\d+\\.\\d+")) {
                return hostname;
            }
            
            // Resolve DNS
            InetAddress addr = InetAddress.getByName(hostname);
            return addr.getHostAddress();
            
        } catch (Exception e) {
            log.debug("Failed to resolve hostname: {}", hostname, e);
            return null;
        }
    }
    
    /**
     * Get cached leader URL (for testing/monitoring).
     */
    public String getCachedLeaderUrl() {
        return cachedLeaderUrl;
    }
    
    /**
     * Get known leader URL (from role change tracking).
     */
    public String getKnownLeaderUrl() {
        return knownLeaderUrl;
    }
    
    /**
     * Get known leader member ID.
     */
    public int getKnownLeaderMemberId() {
        return knownLeaderMemberId;
    }
    
    /**
     * Check if leader is known (either cached or tracked).
     */
    public boolean isLeaderKnown() {
        return knownLeaderUrl != null || cachedLeaderUrl != null;
    }
    
    /**
     * Get the best known leader URL (tracked > cached).
     */
    public String getBestKnownLeaderUrl() {
        if (knownLeaderUrl != null) {
            return knownLeaderUrl;
        }
        return cachedLeaderUrl;
    }
}
