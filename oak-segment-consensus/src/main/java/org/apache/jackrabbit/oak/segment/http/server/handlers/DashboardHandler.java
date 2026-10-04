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

import org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig;
import org.apache.jackrabbit.oak.segment.consensus.config.RuntimeConfigValueResolver;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.segment.http.server.util.DashboardDataService;
import org.apache.jackrabbit.oak.segment.http.server.util.FormatUtils;
import org.apache.jackrabbit.oak.segment.http.server.util.JsonOutputUtil;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Handler for UI rendering endpoints (dashboard, explorer, API browser).
 * 
 * <p>Extracted from SegmentHttpServer for better separation of concerns.</p>
 */
public class DashboardHandler {
    
    private final ServerContext context;
    private final DashboardDataService dataService;
    
    public DashboardHandler(ServerContext context) {
        this.context = context;
        this.dataService = new DashboardDataService(context);
    }
    
    /**
     * Handle root landing page for operators.
     *
     * <p>This endpoint intentionally avoids rendering the legacy dashboard UI.
     * It serves a compact API-first entry page that points operators to
     * the API browser, health endpoints, and external dashboard.</p>
     */
    public void handleDashboard(HttpServletResponse response) throws IOException {
        final String version = DashboardHandler.class.getPackage() != null
                && DashboardHandler.class.getPackage().getImplementationVersion() != null
                ? DashboardHandler.class.getPackage().getImplementationVersion()
                : "dev";
        final String externalDashboardUrl = RuntimeConfigValueResolver.readString("oak.dashboard.external.url", "");
        final String uptime = formatUptime(java.lang.management.ManagementFactory.getRuntimeMXBean().getUptime());
        final String now = formatTimestamp(System.currentTimeMillis());

        Map<String, Object> clusterState = Collections.emptyMap();
        try {
            clusterState = dataService.getAeronClusterState();
        } catch (Exception ignored) {
            // Keep landing page available even if cluster probing fails.
        }
        if (clusterState == null) {
            clusterState = Collections.emptyMap();
        }

        final String role = safeString(clusterState.get("role"), "UNKNOWN").toUpperCase();
        final int nodeIdInt = asInt(clusterState.get("memberId"), -1);
        final String nodeId = nodeIdInt >= 0 ? String.valueOf(nodeIdInt) : "UNKNOWN";
        final int leaderNodeInt = resolveLeaderNodeId(clusterState, role, nodeIdInt);
        final String leaderNode = leaderNodeInt >= 0 ? String.valueOf(leaderNodeInt) : "UNKNOWN";
        final long termValue = asLong(clusterState.get("term"),
            asLong(clusterState.get("leadershipTerm"),
                asLong(clusterState.get("leadershipTermId"), 0L)));
        final String term = String.valueOf(termValue);
        final int membersValue = asInt(clusterState.get("memberCount"),
            asInt(clusterState.get("clusterMemberCount"), 0));
        final String members = String.valueOf(membersValue);
        final Map<String, Object> quorumState = asMap(clusterState.get("quorum"));
        final Map<String, Object> consensusState = asMap(clusterState.get("consensus"));
        final int reachableValue = asInt(clusterState.get("reachableCount"),
            asInt(consensusState.get("reachableValidators"), -1));
        final String reachable = reachableValue >= 0 && membersValue > 0
            ? reachableValue + "/" + membersValue
            : reachableValue >= 0 ? String.valueOf(reachableValue) : "UNKNOWN";
        final int quorumRequired = asInt(quorumState.get("required"), membersValue > 0 ? (membersValue / 2) + 1 : -1);
        final boolean quorumKnown = quorumRequired > 0 && reachableValue >= 0;
        final boolean hasQuorum = quorumKnown
            ? reachableValue >= quorumRequired
            : Boolean.TRUE.equals(quorumState.get("hasQuorum"));
        final String quorum = quorumKnown
            ? (hasQuorum ? "YES" : "NO") + " (" + quorumRequired + ")"
            : "UNKNOWN";
        final String[] posture = resolvePosture(quorumKnown, hasQuorum, reachableValue, membersValue);
        final String[] modeTokens = resolveModeTemplateTokens();
        final String modeClass = modeTokens[0];
        final String modeLabel = modeTokens[1];
        Map<String, String> tokens = buildSharedTemplateTokens("dashboard", modeClass, modeLabel);
        tokens.put("{{POSTURE_TONE}}", posture[0]);
        tokens.put("{{POSTURE_LABEL}}", posture[1]);
        tokens.put("{{MEMBER_LIST}}", buildMemberList(clusterState.get("members"), nodeIdInt));
        tokens.put("{{VERSION}}", FormatUtils.escapeHtml(version));
        tokens.put("{{ROLE}}", FormatUtils.escapeHtml(role));
        tokens.put("{{NODE_ID}}", FormatUtils.escapeHtml(nodeId));
        tokens.put("{{LEADER_NODE}}", FormatUtils.escapeHtml(leaderNode));
        tokens.put("{{TERM}}", FormatUtils.escapeHtml(term));
        tokens.put("{{MEMBERS}}", FormatUtils.escapeHtml(members));
        tokens.put("{{REACHABLE}}", FormatUtils.escapeHtml(reachable));
        tokens.put("{{QUORUM}}", FormatUtils.escapeHtml(quorum));
        tokens.put("{{UPTIME}}", FormatUtils.escapeHtml(uptime));
        tokens.put("{{UPDATED_AT}}", FormatUtils.escapeHtml(now));
        tokens.put("{{EXTERNAL_DASHBOARD_LINK}}", buildExternalDashboardLink(externalDashboardUrl));
        writeResolvedTemplate(response, "/dashboard-template.html", tokens);
    }

    /**
     * API discovery index for tooling and API Browser dynamic catalog rendering.
     */
    public void handleApiIndex(HttpServletResponse response) throws IOException {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json; charset=UTF-8");

        List<Map<String, Object>> endpoints = new ArrayList<>();
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/v1/index", "Live validator surface manifest", "Discovery", null);
        addSourceIndexEntry(endpoints, "GET", "/v1/config/osgi", "Effective OSGi config values", "Configuration", "config.osgi.v1", "/ops/v1/config/osgi");
        addSourceIndexEntry(endpoints, "GET", "/v1/config/osgi/schema", "OSGi config metadata schema", "Configuration", "config.osgi.schema.v1", "/ops/v1/config/osgi/schema");
        addSourceIndexEntry(endpoints, "GET", "/v1/config/osgi/sources", "OSGi config source map", "Configuration", "config.osgi.sources.v1", "/ops/v1/config/osgi/sources");
        addSourceIndexEntry(endpoints, "GET", "/v1/config/osgi/coverage", "OSGi config coverage and missing keys", "Configuration", "config.osgi.coverage.v1", "/ops/v1/config/osgi/coverage");
        addSourceIndexEntry(endpoints, "GET", "/v1/config/osgi/delta", "OSGi config values drift from defaults", "Configuration", "config.osgi.delta.v1", "/ops/v1/config/osgi/delta");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/health", "Shallow health", "Health", "/ops/v1/health");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/health/local", "Local-only liveness", "Health", null);
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/health/deep", "Deep dependency health", "Health", "/v1/ops/snapshots/runtime");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/health/cluster", "Cluster-only health", "Health", "/v1/ops/snapshots/health");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/api/metrics", "Consensus and replication metrics", "Health", "/v1/ops/snapshots/runtime");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/metrics", "Prometheus metrics", "Health", null);

        addSourceIndexEntry(endpoints, "GET", "/v1/consensus/leader", "Canonical leader resolution", "Consensus", "consensus.leader.v1", "/ops/v1/cluster");
        addSourceIndexEntry(endpoints, "GET", "/v1/consensus/status", "Consensus status", "Consensus", "consensus.status.v1", "/ops/v1/cluster");
        addInternalIndexEntry(endpoints, "POST", "/v1/propose-write", "Propose signed write", "Consensus", null);
        addInternalIndexEntry(endpoints, "POST", "/v1/propose-delete", "Propose signed delete", "Consensus", null);
        addInternalIndexEntry(endpoints, "GET", "/v1/proposals/pending/count", "Pending proposal count", "Consensus", "/ops/v1/proposals");
        addSourceIndexEntry(endpoints, "GET", "/v1/proposals/queue/stats", "Queue and finality counters", "Consensus", "ops.v1", "/ops/v1/proposals/queue/stats");
        addSourceIndexEntry(endpoints, "GET", "/v1/proposals/release-flow", "Adaptive proposal release flow", "Consensus", "release-flow.v1", "/ops/v1/proposals/release-flow");
        addInternalIndexEntry(endpoints, "GET", "/v1/proposals/{id}/status", "Proposal status by id", "Consensus", null);
        addSourceIndexEntry(endpoints, "GET", "/v1/settlement/proposals/{proposalId}", "Basic settlement details by proposal id", "Settlement", "settlement.v1", "/ops/v1/settlement/proposals/{proposalId}");
        addSourceIndexEntry(endpoints, "GET", "/v1/settlement/transactions/{transactionHash}", "Basic settlement details by transaction hash", "Settlement", "settlement.v1", "/ops/v1/settlement/transactions/{transactionHash}");
        addInternalIndexEntry(endpoints, "GET", "/v1/head", "Head status", "Consensus", null);

        addSourceIndexEntry(endpoints, "GET", "/v1/explorer/summary", "Explorer summary contract", "CRX/OC", "explorer.v1", "/ops/v1/explorer/summary");
        addSourceIndexEntry(endpoints, "GET", "/v1/explorer/release-flow", "Explorer adaptive release flow", "CRX/OC", "explorer.v1", "/ops/v1/explorer/release-flow");
        addSourceIndexEntry(endpoints, "GET", "/v1/explorer/proposals/{proposalId}", "Explorer proposal detail", "CRX/OC", "explorer.v1", "/ops/v1/explorer/proposals/{proposalId}");
        addSourceIndexEntry(endpoints, "GET", "/v1/explorer/wallets/{walletAddress}", "Explorer wallet detail", "CRX/OC", "explorer.v1", "/ops/v1/explorer/wallets/{walletAddress}");
        addSourceIndexEntry(endpoints, "GET", "/v1/explorer/content/nav", "CRX/OC cluster-aware content navigation", "CRX/OC", "explorer.content.v1", "/ops/v1/explorer/content/nav");
        addSourceIndexEntry(endpoints, "GET", "/v1/explorer/content/clusters/{clusterId}/tree", "CRX/OC cluster-scoped content tree browse", "CRX/OC", "explorer.content.v1", "/ops/v1/explorer/content/clusters/{clusterId}/tree");
        addSourceIndexEntry(endpoints, "GET", "/v1/explorer/content/clusters/{clusterId}/node", "CRX/OC cluster-scoped node detail", "CRX/OC", "explorer.content.v1", "/ops/v1/explorer/content/clusters/{clusterId}/node");
        addSourceIndexEntry(endpoints, "GET", "/v1/explorer/content/clusters/{clusterId}/provenance", "CRX/OC cluster-scoped provenance and authority facts", "CRX/OC", "explorer.content.v1", "/ops/v1/explorer/content/clusters/{clusterId}/provenance");
        addLocalUiIndexEntry(endpoints, "GET", "/explorer", "Validator-local CRX/OC read-only content explorer", "CRX/OC");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/api/explore?path=/", "Legacy CRX/OC local node tree browse API", "CRX/OC", "/ops/v1/explorer/content/*");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/api/segments/recent", "Recent segments", "Storage", "/v1/ops/snapshots/storage");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/api/segments/tars", "TAR file listing", "Storage", "/v1/ops/snapshots/storage");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/api/blob/{blobId}", "Blob stream by blob id", "Storage", null);
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/api/cid/{oakBlobId}", "CID mapping by Oak blob id", "Storage", null);
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/api/cid/stats", "CID mapping stats", "Storage", null);
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/api/cid/reverse/{cid}", "Reverse CID lookup", "Storage", null);

        addInternalIndexEntry(endpoints, "GET", "/v1/wallets/stats", "Wallet usage and counts", "Wallets", null);
        addInternalIndexEntry(endpoints, "GET", "/v1/wallets/content?wallet=0x...", "Wallet content query", "Wallets", null);
        addInternalIndexEntry(endpoints, "POST|PUT", "/v1/register-client", "Register client", "Registration", null);
        addInternalIndexEntry(endpoints, "GET", "/v1/peers", "Peer list", "Registration", null);
        addInternalIndexEntry(endpoints, "GET", "/v1/ngrok-url", "Current ngrok URL", "Registration", null);
        addSourceIndexEntry(endpoints, "GET", "/v1/blockchain/config", "Blockchain mode config", "Configuration", "blockchain.config.v1", "/ops/v1/blockchain/config");

        addLocalDiagnosticIndexEntry(endpoints, "GET", "/v1/aeron/cluster-state", "Aeron cluster state", "Aeron", "/v1/ops/snapshots/cluster");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/v1/aeron/validator-identities", "Validator identity map", "Aeron", "/v1/ops/snapshots/runtime");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/v1/aeron/raft-metrics", "Raft metrics", "Aeron", "/v1/ops/snapshots/runtime");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/v1/aeron/node-status?nodeId=0", "Per-node status", "Aeron", "/v1/ops/snapshots/runtime");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/v1/aeron/leadership-history?limit=10", "Leadership history", "Aeron", "/ops/v1/events/recent");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/v1/aeron/replication-lag", "Replication lag", "Aeron", "/v1/ops/snapshots/replication");
        addInternalIndexEntry(endpoints, "POST", "/v1/follower/head-update", "Follower head update (internal)", "Aeron", null);

        addSourceIndexEntry(endpoints, "GET", "/v1/ops/snapshots/health", "Ops health snapshot", "Ops Snapshots", "ops.v1", "/ops/v1/health");
        addSourceIndexEntry(endpoints, "GET", "/v1/ops/snapshots/runtime", "Ops runtime snapshot", "Ops Snapshots", "ops.runtime.v1", "/ops/v1/runtime/*");
        addSourceIndexEntry(endpoints, "GET", "/v1/ops/snapshots/storage", "Ops storage snapshot", "Ops Snapshots", "ops.storage.v1", "/ops/v1/runtime/storage");
        addSourceIndexEntry(endpoints, "GET", "/v1/ops/snapshots/cluster", "Ops cluster snapshot", "Ops Snapshots", "ops.v1", "/ops/v1/cluster");
        addSourceIndexEntry(endpoints, "GET", "/v1/ops/snapshots/replication", "Ops replication snapshot", "Ops Snapshots", "ops.v1", "/ops/v1/replication");
        addSourceIndexEntry(endpoints, "GET", "/v1/ops/snapshots/queue", "Ops queue snapshot", "Ops Snapshots", "ops.v1", "/ops/v1/queue");
        addInternalIndexEntry(endpoints, "GET", "/v1/ops/operations/{operationId}", "Ops operation status", "Ops Snapshots", null);

        addSourceIndexEntry(endpoints, "GET", "/v1/events/recent?limit=50", "Recent events", "Events", "events.recent.v1", "/ops/v1/events/recent");
        addSourceIndexEntry(endpoints, "GET", "/v1/events/stats", "Event stats", "Events", "events.stats.v1", "/ops/v1/events/stats");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/v1/events/stream", "Event stream (SSE)", "Events", "/ops/v1/events/*");
        addLocalDiagnosticIndexEntry(endpoints, "GET", "/v1/ops/events/stream", "Ops event stream (SSE)", "Events", "/ops/v1/events/*");

        addSourceIndexEntry(endpoints, "GET", "/v1/gc/estimate", "GC estimate", "GC", "gc.estimate.v1", "/ops/v1/gc/estimate");
        addSourceIndexEntry(endpoints, "GET", "/v1/gc/status", "GC status", "GC", "gc.status.v1", "/ops/v1/gc/status");
        addInternalIndexEntry(endpoints, "POST", "/v1/propose-gc", "Propose GC operation", "GC", null);
        addInternalIndexEntry(endpoints, "POST", "/v1/gc/trigger", "Trigger GC check", "GC", null);
        addInternalIndexEntry(endpoints, "POST", "/v1/gc/execute", "Execute approved GC", "GC", null);
        addSourceIndexEntry(endpoints, "GET", "/v1/compaction/proposals", "Compaction proposals", "GC", "gc.compaction.proposals.v1", "/ops/v1/compaction/proposals");
        addInternalIndexEntry(endpoints, "GET", "/v1/gc/account/{walletAddress}", "GC account status", "GC Accounts", "/ops/v1/gc/account/{walletAddress}");
        addInternalIndexEntry(endpoints, "POST", "/v1/gc/account/{walletAddress}/pay?amount=X", "GC debt payment", "GC Accounts", null);
        addInternalIndexEntry(endpoints, "POST", "/v1/gc/account/{walletAddress}/set-limit?limit=X", "Set debt limit", "GC Accounts", null);
        addInternalIndexEntry(endpoints, "POST", "/v1/gc/account/{walletAddress}/execute-pending", "Execute pending debt", "GC Accounts", null);

        addSourceIndexEntry(endpoints, "GET", "/v1/fragmentation/metrics", "All fragmentation metrics", "Fragmentation", "fragmentation.metrics.v1", "/ops/v1/fragmentation/metrics");
        addSourceIndexEntry(endpoints, "GET", "/v1/fragmentation/metrics/{walletAddress}", "Fragmentation by wallet", "Fragmentation", "fragmentation.metrics.entity.v1", "/ops/v1/fragmentation/metrics/{walletAddress}");
        addSourceIndexEntry(endpoints, "GET", "/v1/fragmentation/top?limit=20", "Top fragmented wallets", "Fragmentation", "fragmentation.top.v1", "/ops/v1/fragmentation/top");

        addInternalIndexEntry(endpoints, "POST", "/v1/binary/declare-intent", "Declare binary upload intent", "Binary", null);
        addInternalIndexEntry(endpoints, "GET", "/v1/binary/check-intent/{token}", "Check binary intent", "Binary", null);
        addInternalIndexEntry(endpoints, "POST", "/v1/binary/complete-upload", "Complete binary upload", "Binary", null);

        addLocalUiIndexEntry(endpoints, "GET", "/api-browser", "Validator-local diagnostic API browser", "UI");
        addLocalUiIndexEntry(endpoints, "GET", "/dashboard", "Control-plane landing page", "UI");
        addLocalUiIndexEntry(endpoints, "GET", "/", "Control-plane landing page", "UI");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("contractVersion", "index.v1");
        payload.put("surfaceRole", "validator-native");
        payload.put("surfaceAuthority", "runtime-and-source");
        payload.put("preferredBrowserContract", "/ops/v1/* via edge/gateway");
        payload.put("browserContractNotes", "Local HTML routes remain diagnostic-only; upstream UX should consume governed /ops/v1/* surfaces.");
        payload.put("upstreamAuthority", "/ops/v1/*");
        payload.put("surfaceClasses", Arrays.asList("source", "local-ui", "local-diagnostic", "internal"));
        payload.put("intendedConsumers", Arrays.asList("operators", "edge-adapters", "automation", "cli"));
        payload.put("generatedAtMs", System.currentTimeMillis());
        payload.put("count", endpoints.size());
        payload.put("endpoints", endpoints);

        response.getWriter().write(JsonOutputUtil.toJson(payload));
    }

    // ========== Helper methods shared by dashboard handlers (explorer, api-browser, etc.) ==========

    private void addSourceIndexEntry(List<Map<String, Object>> endpoints,
                                     String method,
                                     String path,
                                     String description,
                                     String category,
                                     String contractVersion,
                                     String replacement) {
        addIndexEntry(endpoints, method, path, description, category, "source", true, contractVersion, replacement);
    }

    private void addLocalUiIndexEntry(List<Map<String, Object>> endpoints,
                                      String method,
                                      String path,
                                      String description,
                                      String category) {
        addIndexEntry(endpoints, method, path, description, category, "local-ui", false, null, "/ops/v1/* via edge/gateway");
    }

    private void addLocalDiagnosticIndexEntry(List<Map<String, Object>> endpoints,
                                              String method,
                                              String path,
                                              String description,
                                              String category,
                                              String replacement) {
        addIndexEntry(endpoints, method, path, description, category, "local-diagnostic", false, null, replacement);
    }

    private void addInternalIndexEntry(List<Map<String, Object>> endpoints,
                                       String method,
                                       String path,
                                       String description,
                                       String category,
                                       String replacement) {
        addIndexEntry(endpoints, method, path, description, category, "internal", false, null, replacement);
    }

    private void addIndexEntry(List<Map<String, Object>> endpoints,
                               String method,
                               String path,
                               String description,
                               String category,
                               String surfaceClass,
                               boolean upstreamAllowed,
                               String contractVersion,
                               String replacement) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("method", method);
        item.put("path", path);
        item.put("description", description);
        item.put("category", category);
        item.put("surfaceClass", surfaceClass);
        item.put("upstreamAllowed", upstreamAllowed);
        if (contractVersion != null) {
            item.put("contractVersion", contractVersion);
        }
        item.put("replacement", replacement);
        endpoints.add(item);
    }
    
    private String formatTimestamp(long epochMillis) {
        if (epochMillis <= 0) {
            return "-";
        }
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneId.systemDefault())
                .format(Instant.ofEpochMilli(epochMillis));
    }

    private String formatUptime(long uptimeMs) {
        if (uptimeMs <= 0) {
            return "0s";
        }
        long totalSeconds = uptimeMs / 1000;
        long days = totalSeconds / 86400;
        long hours = (totalSeconds % 86400) / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        if (days > 0) {
            return String.format("%dd %dh %dm", days, hours, minutes);
        }
        if (hours > 0) {
            return String.format("%dh %dm %ds", hours, minutes, seconds);
        }
        if (minutes > 0) {
            return String.format("%dm %ds", minutes, seconds);
        }
        return String.format("%ds", seconds);
    }

    private long asLong(Object value, long fallback) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return fallback;
    }

    private int asInt(Object value, int fallback) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return fallback;
    }

    private String safeString(Object value, String fallback) {
        return value != null ? value.toString() : fallback;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Object value) {
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return Collections.emptyMap();
    }

    private int resolveLeaderNodeId(Map<String, Object> clusterState, String role, int nodeId) {
        int leaderNode = asInt(clusterState.get("leaderNodeId"),
            asInt(clusterState.get("leaderMemberId"), -1));
        if (leaderNode >= 0) {
            return leaderNode;
        }
        if ("LEADER".equalsIgnoreCase(role) && nodeId >= 0) {
            return nodeId;
        }
        String leaderUrl = safeString(clusterState.get("currentLeader"), null);
        if (leaderUrl != null) {
            int fromUrl = nodeIdFromValidatorUrl(leaderUrl);
            if (fromUrl >= 0) {
                return fromUrl;
            }
        }
        return -1;
    }

    private int nodeIdFromValidatorUrl(String url) {
        if (url == null || url.isEmpty()) {
            return -1;
        }
        try {
            URL parsed = new URL(url);
            int port = parsed.getPort();
            if (port >= 8090 && (port - 8090) % 2 == 0) {
                return (port - 8090) / 2;
            }
        } catch (Exception ignored) {
            // fall through
        }
        return -1;
    }

    /**
     * Handle blockchain explorer UI - Rich Etherscan-like interface.
     * Enhanced with IPFS links, property type indicators, and better visualization.
     * Now uses external template for consistent Blockchain AEM styling.
     */
    public void handleExplorerUI(HttpServletResponse response) throws IOException {
        writeModeAwareTemplate(response, "/explorer-template.html", "explorer");
    }
    
    /**
     * Handle interactive API Browser UI (HAL-style explorer).
     * Now uses external template for consistent Blockchain AEM styling.
     */
    public void handleApiBrowserUI(HttpServletResponse response) throws IOException {
        writeModeAwareTemplate(response, "/api-browser-template.html", "api-browser");
    }

    private void writeModeAwareTemplate(HttpServletResponse response, String resourcePath, String activeNav) throws IOException {
        String[] modeTokens = resolveModeTemplateTokens();
        Map<String, String> tokens = buildSharedTemplateTokens(activeNav, modeTokens[0], modeTokens[1]);
        writeResolvedTemplate(response, resourcePath, tokens);
    }

    private void writeResolvedTemplate(HttpServletResponse response, String resourcePath, Map<String, String> tokens) throws IOException {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("text/html; charset=UTF-8");

        String template = loadTemplate(resourcePath);
        String html = applyTemplateTokens(template, tokens);
        response.getWriter().write(html);
    }

    private Map<String, String> buildSharedTemplateTokens(String activeNav, String modeClass, String modeLabel) throws IOException {
        Map<String, String> tokens = new LinkedHashMap<>();
        tokens.put("{{MODE_CLASS}}", modeClass);
        tokens.put("{{MODE_LABEL}}", modeLabel);
        tokens.put("{{SHARED_HEADER_STYLES}}", loadTemplate("/shared-header.css"));
        tokens.put("{{SHARED_HEADER}}", buildSharedHeader(activeNav, modeClass, modeLabel));
        return tokens;
    }

    private String buildSharedHeader(String activeNav, String modeClass, String modeLabel) throws IOException {
        boolean dashboardActive = "dashboard".equals(activeNav);
        boolean explorerActive = "explorer".equals(activeNav);
        boolean apiBrowserActive = "api-browser".equals(activeNav);

        return loadTemplate("/shared-header.html")
            .replace("{{NAV_DASHBOARD_CLASS}}", dashboardActive ? "nav-link active" : "nav-link")
            .replace("{{NAV_DASHBOARD_CURRENT}}", dashboardActive ? "aria-current=\"page\"" : "")
            .replace("{{NAV_EXPLORER_CLASS}}", explorerActive ? "nav-link active" : "nav-link")
            .replace("{{NAV_EXPLORER_CURRENT}}", explorerActive ? "aria-current=\"page\"" : "")
            .replace("{{NAV_API_BROWSER_CLASS}}", apiBrowserActive ? "nav-link active" : "nav-link")
            .replace("{{NAV_API_BROWSER_CURRENT}}", apiBrowserActive ? "aria-current=\"page\"" : "")
            .replace("{{MODE_CLASS}}", modeClass)
            .replace("{{MODE_LABEL}}", modeLabel);
    }

    private String[] resolvePosture(boolean quorumKnown, boolean hasQuorum, int reachable, int members) {
        if (!quorumKnown) {
            return new String[] { "unknown", "Posture unknown" };
        }
        if (!hasQuorum) {
            return new String[] { "bad", "No quorum" };
        }
        if (members > 0 && reachable < members) {
            return new String[] { "warn", "Quorum held, " + (members - reachable) + " unreachable" };
        }
        return new String[] { "ok", "Quorum healthy" };
    }

    /**
     * Cluster members as list items, ordered by member id. Roles come from the
     * cluster-state view; reachability is reported in aggregate, not per member.
     */
    private String buildMemberList(Object membersValue, int selfMemberId) {
        if (!(membersValue instanceof List)) {
            return "";
        }
        List<Map<String, Object>> members = new ArrayList<>();
        for (Object member : (List<?>) membersValue) {
            Map<String, Object> entry = asMap(member);
            if (!entry.isEmpty()) {
                members.add(entry);
            }
        }
        members.sort((a, b) -> Integer.compare(asInt(a.get("memberId"), Integer.MAX_VALUE), asInt(b.get("memberId"), Integer.MAX_VALUE)));
        StringBuilder html = new StringBuilder();
        for (Map<String, Object> member : members) {
            int memberId = asInt(member.get("memberId"), -1);
            String role = safeString(member.get("role"), "UNKNOWN").toUpperCase();
            String url = safeString(member.get("url"), "");
            boolean leader = "LEADER".equals(role);
            boolean self = memberId >= 0 && memberId == selfMemberId;
            String label = "Node " + (memberId >= 0 ? memberId : "?");
            html.append("<li class=\"member").append(leader ? " is-leader" : "").append(self ? " is-self" : "").append("\">");
            if (url.startsWith("http://") || url.startsWith("https://")) {
                html.append("<a class=\"member-name\" href=\"").append(FormatUtils.escapeHtml(url.endsWith("/") ? url : url + "/")).append("\" title=\"")
                    .append(FormatUtils.escapeHtml(url)).append("\">").append(label).append("</a>");
            } else {
                html.append("<span class=\"member-name\">").append(label).append("</span>");
            }
            html.append("<span class=\"member-role\">").append(FormatUtils.escapeHtml(role)).append("</span>");
            if (self) {
                html.append("<span class=\"member-self\">this node</span>");
            }
            html.append("</li>");
        }
        return html.toString();
    }

    private String buildExternalDashboardLink(String externalDashboardUrl) {
        if (externalDashboardUrl == null || externalDashboardUrl.trim().isEmpty()) {
            return "";
        }
        return "<a class=\"link\" href=\"" + FormatUtils.escapeHtml(externalDashboardUrl)
            + "\"><strong>External Ops Dashboard</strong><div class=\"muted\">Optional upstream operator surface configured via <code>-Doak.dashboard.external.url</code>.</div></a>";
    }

    private String applyTemplateTokens(String template, Map<String, String> tokens) {
        String resolved = template;
        for (Map.Entry<String, String> entry : tokens.entrySet()) {
            resolved = resolved.replace(entry.getKey(), entry.getValue());
        }
        return resolved;
    }

    private String[] resolveModeTemplateTokens() {
        BlockchainConfig config = BlockchainConfig.getInstance();
        String modeClass, modeLabel;
        switch (config.getMode()) {
            case MOCK:
                modeClass = "mode-mock";
                modeLabel = "MOCK MODE";
                break;
            case SEPOLIA:
                modeClass = "mode-sepolia";
                modeLabel = "SEPOLIA";
                break;
            case MAINNET:
                modeClass = "mode-mainnet";
                modeLabel = "MAINNET";
                break;
            default:
                modeClass = "mode-mock";
                modeLabel = "UNKNOWN";
        }
        return new String[] { modeClass, modeLabel };
    }
    
    /**
     * Load HTML template from resources.
     */
    private String loadTemplate(String resourcePath) throws IOException {
        try (InputStream is = getClass().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IOException("Template not found: " + resourcePath);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
    
}
