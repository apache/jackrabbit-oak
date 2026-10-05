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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalStatus;
import org.apache.jackrabbit.oak.segment.consensus.sharding.ShardingRuntimeConfig;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.segment.http.server.model.WriteMetadata;
import org.apache.jackrabbit.oak.segment.http.server.util.ApiErrorUtil;
import org.apache.jackrabbit.oak.segment.http.server.util.JsonOutputUtil;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Explorer-focused API surface for external blockscan/etherscan-style UIs.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>GET /v1/explorer/summary</li>
 *   <li>GET /v1/explorer/proposals/{proposalId}</li>
 *   <li>GET /v1/explorer/wallets/{walletAddress}</li>
 *   <li>GET /v1/explorer/release-flow</li>
 *   <li>GET /v1/explorer/content/nav</li>
 *   <li>GET /v1/explorer/content/clusters/{clusterId}/tree?path=/oak-chain</li>
 *   <li>GET /v1/explorer/content/clusters/{clusterId}/node?path=/oak-chain</li>
 *   <li>GET /v1/explorer/content/clusters/{clusterId}/provenance?path=/oak-chain</li>
 * </ul>
 */
public class ExplorerApiV1Handler {

    private static final Logger log = LoggerFactory.getLogger(ExplorerApiV1Handler.class);
    private static final String CONTENT_CONTRACT_VERSION = "explorer.content.v1";
    public static final int DEFAULT_TREE_PAGE_SIZE = 200;
    public static final int MAX_TREE_PAGE_SIZE = 500;
    private static final String CONTENT_ROOT_PATH = "/oak-chain";
    private final ServerContext context;

    public ExplorerApiV1Handler(ServerContext context) {
        this.context = context;
    }

    public void handleSummary(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        try {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("contractVersion", "explorer.v1");
            payload.put("generatedAtMs", System.currentTimeMillis());

            Map<String, Object> cluster = new LinkedHashMap<>();
            if (context.aeronConsensusEngine != null) {
                int nodeCount = Math.max(0, context.aeronConsensusEngine.getClusterSize());
                int quorum = nodeCount > 0 ? (nodeCount / 2) + 1 : 0;
                int reachable = Math.max(0, context.aeronConsensusEngine.getReachableValidatorCount());
                cluster.put("consensusType", "aeron-cluster");
                cluster.put("role", context.aeronConsensusEngine.getCurrentRole().name());
                cluster.put("isLeader", context.aeronConsensusEngine.isLeader());
                cluster.put("currentLeader", context.aeronConsensusEngine.getCurrentLeader());
                cluster.put("currentTerm", context.aeronConsensusEngine.getCurrentTerm());
                cluster.put("currentEpoch", context.aeronConsensusEngine.getCurrentEpoch());
                cluster.put("ethereumEpoch", context.aeronConsensusEngine.getCurrentEthereumEpoch());
                cluster.put("nodeCount", nodeCount);
                cluster.put("quorum", quorum);
                cluster.put("reachableValidators", reachable);
                cluster.put("clusterState", reachable >= quorum ? "HEALTHY" : "DEGRADED");
            } else {
                cluster.put("consensusType", "none");
                cluster.put("role", "STANDALONE");
                cluster.put("isLeader", true);
                cluster.put("nodeCount", 1);
                cluster.put("quorum", 1);
                cluster.put("reachableValidators", 1);
                cluster.put("clusterState", "HEALTHY");
            }
            payload.put("cluster", cluster);

            if (context.proposalQueueManager != null) {
                Map<String, Object> queue = context.proposalQueueManager.getQueueStats();
                Map<String, Object> compact = new LinkedHashMap<>();
                long verified = asLong(queue.get("verifiedCount"));
                long finalized = asLong(queue.get("totalFinalizedCount"));
                long queuePending = Math.max(asLong(queue.get("batchQueueSize")), asLong(queue.get("pendingCount")));
                long backpressurePending = asLong(queue.get("backpressurePendingCount"));
                long backpressureRaw = asLong(queue.get("backpressurePendingRawCount"));
                long backpressureMax = asLong(queue.get("backpressureMaxPending"));
                long sentCurrent = asLong(queue.get("totalProposalsSent"));
                long routingDebt = Math.max(0L, sentCurrent - finalized);
                compact.put("queuePending", queuePending);
                compact.put("pendingCount", asLong(queue.get("pendingCount")));
                compact.put("batchQueueSize", asLong(queue.get("batchQueueSize")));
                compact.put("mempoolPendingCount", asLong(queue.get("mempoolPendingCount")));
                compact.put("verified", verified);
                compact.put("finalized", finalized);
                compact.put("gap", Math.max(0L, verified - finalized));
                compact.put("rejected", asLong(queue.get("rejectedCount")));
                compact.put("backpressurePending", backpressurePending);
                compact.put("backpressurePendingRaw", backpressureRaw);
                compact.put("backpressureMax", backpressureMax);
                compact.put("backpressureActive", asBoolean(queue.get("backpressureActive")));
                compact.put("routingDebt", routingDebt);
                compact.put("releaseMode", queue.get("releaseMode"));
                compact.put("requiredConfirmations", queue.get("requiredConfirmations"));
                compact.put("verifiedResidentProposalCount", asLong(queue.get("verifiedResidentProposalCount")));
                compact.put("releaseReadyProposalCount", asLong(queue.get("releaseReadyProposalCount")));
                compact.put("backpressureOverflowProposalCount", asLong(queue.get("backpressureOverflowProposalCount")));
                if (queue.get("adaptiveReleaseGovernorState") != null) {
                    compact.put("adaptiveReleaseGovernorState", String.valueOf(queue.get("adaptiveReleaseGovernorState")));
                }
                if (queue.get("adaptiveReleaseAction") != null) {
                    compact.put("adaptiveReleaseAction", String.valueOf(queue.get("adaptiveReleaseAction")));
                }
                compact.put("currentEpoch", asLong(queue.get("currentEpoch")));
                compact.put("finalizedEpoch", asLong(queue.get("finalizedEpoch")));
                compact.put("epochsUntilFinality", asLong(queue.get("epochsUntilFinality")));

                Map<String, Object> queuePayload = new LinkedHashMap<>();
                queuePayload.put("compact", compact);
                queuePayload.put("raw", queue);
                payload.put("queue", queuePayload);
            } else {
                payload.put("queue", new LinkedHashMap<>());
            }

            Map<String, Object> identities = new LinkedHashMap<>();
            identities.put("validatorWalletAddress", context.validatorWalletAddress);
            identities.put("clusterWalletAddress", context.clusterWalletAddress);
            identities.put("registeredClients", context.registeredClients.size());
            identities.put("registeredValidators", context.registeredValidators.size());
            if (context.aeronConsensusEngine != null) {
                identities.put("reachableValidators", Math.max(0, context.aeronConsensusEngine.getReachableValidatorCount()));
                identities.put("clusterNodeCount", Math.max(0, context.aeronConsensusEngine.getClusterSize()));
            }
            payload.put("identities", identities);

            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
        } catch (Exception e) {
            log.error("Failed explorer summary", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Failed explorer summary: " + e.getMessage());
        }
    }

    public void handleProposalById(HttpServletResponse response, String proposalId) throws IOException {
        response.setContentType("application/json");
        if (proposalId == null || proposalId.isEmpty()) {
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST, "proposalId is required");
            return;
        }
        if (context.proposalQueueManager == null) {
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Proposal queue not available");
            return;
        }
        try {
            ProposalStatus status = context.proposalQueueManager.getProposalStatus(proposalId);
            if (status == null) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_NOT_FOUND, "Proposal not found");
                return;
            }

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("contractVersion", "explorer.v1");
            payload.put("generatedAtMs", System.currentTimeMillis());
            payload.put("proposalId", status.getProposalId());
            payload.put("state", status.getState().name());
            payload.put("ethereumTxHash", status.getEthereumTxHash());
            payload.put("timeoutTimestamp", status.getTimeoutTimestamp());
            payload.put("confirmedBlock", status.getConfirmedBlock());
            payload.put("rejectionReason", status.getRejectionReason());
            payload.put("durabilityState", status.getDurabilityState() != null ? status.getDurabilityState().name() : "UNKNOWN");
            payload.put("durabilityTimestamp", status.getDurabilityTimestamp());
            payload.put("durabilityError", status.getDurabilityError());
            payload.put("durableHead", status.getDurableHead());

            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
        } catch (Exception e) {
            log.error("Failed explorer proposal lookup {}", proposalId, e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Failed explorer proposal lookup: " + e.getMessage());
        }
    }

    public void handleWalletByAddress(HttpServletResponse response, String walletAddress) throws IOException {
        response.setContentType("application/json");
        if (walletAddress == null || walletAddress.isEmpty()) {
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST, "walletAddress is required");
            return;
        }

        try {
            String[] levels = org.apache.jackrabbit.oak.segment.consensus.util.WalletPathUtil.getShardLevels(walletAddress);
            NodeState root = context.nodeStore.getRoot();
            NodeState walletNode = root.getChildNode("oak-chain")
                .getChildNode(levels[0])
                .getChildNode(levels[1])
                .getChildNode(levels[2])
                .getChildNode(walletAddress);

            if (!walletNode.exists()) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_NOT_FOUND, "Wallet not found");
                return;
            }

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("contractVersion", "explorer.v1");
            payload.put("generatedAtMs", System.currentTimeMillis());
            payload.put("wallet", walletAddress);
            payload.put("walletPath", String.format("/oak-chain/%s/%s/%s/%s", levels[0], levels[1], levels[2], walletAddress));
            payload.put("authority", buildWalletAuthority(walletAddress));

            Map<String, Object> walletMeta = new LinkedHashMap<>();
            if (walletNode.hasProperty("contentCount")) walletMeta.put("contentCount", walletNode.getProperty("contentCount").getValue(Type.LONG));
            if (walletNode.hasProperty("totalWrites")) walletMeta.put("totalWrites", walletNode.getProperty("totalWrites").getValue(Type.LONG));
            if (walletNode.hasProperty("walletCreated")) walletMeta.put("walletCreated", walletNode.getProperty("walletCreated").getValue(Type.LONG));
            if (walletNode.hasProperty("lastWrite")) walletMeta.put("lastWrite", walletNode.getProperty("lastWrite").getValue(Type.LONG));
            if (walletNode.hasProperty("nodeType")) walletMeta.put("nodeType", walletNode.getProperty("nodeType").getValue(Type.STRING));
            payload.put("meta", walletMeta);

            NodeState contentNode = walletNode.getChildNode("content");
            List<Map<String, Object>> recentContent = new ArrayList<>();
            if (contentNode.exists()) {
                int i = 0;
                for (ChildNodeEntry entry : contentNode.getChildNodeEntries()) {
                    if (i >= 50) break;
                    NodeState item = entry.getNodeState();
                    Map<String, Object> content = new LinkedHashMap<>();
                    content.put("name", entry.getName());
                    if (item.hasProperty("contentType")) content.put("contentType", item.getProperty("contentType").getValue(Type.STRING));
                    if (item.hasProperty("timestamp")) content.put("timestamp", item.getProperty("timestamp").getValue(Type.LONG));
                    if (item.hasProperty("message")) content.put("message", item.getProperty("message").getValue(Type.STRING));
                    recentContent.add(content);
                    i++;
                }
            }
            payload.put("recentContent", recentContent);

            if (context.gcAccountManager != null) {
                try {
                    org.apache.jackrabbit.oak.segment.consensus.gc.EntityGCAccount account = context.gcAccountManager.getAccount(walletAddress.toLowerCase());
                    if (account != null) {
                        Map<String, Object> gc = new LinkedHashMap<>();
                        gc.put("totalDebt", account.totalDebt != null ? account.totalDebt.toString() : "0");
                        gc.put("pendingDebt", account.getPendingDebt() != null ? account.getPendingDebt().toString() : "0");
                        gc.put("writesBlocked", account.writesBlocked);
                        payload.put("gcAccount", gc);
                    }
                } catch (Exception ignored) {
                    // Keep explorer endpoint resilient even if GC account introspection fails.
                }
            }

            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
        } catch (Exception e) {
            log.error("Failed explorer wallet lookup {}", walletAddress, e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Failed explorer wallet lookup: " + e.getMessage());
        }
    }

    public void handleContentNav(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        try {
            List<ClusterDescriptor> clusters = buildClusterDescriptors();
            ClusterDescriptor localCluster = clusters.isEmpty() ? buildLocalClusterDescriptor() : clusters.get(0);
            List<Map<String, Object>> remoteClusters = new ArrayList<>();
            for (int i = 1; i < clusters.size(); i++) {
                remoteClusters.add(toClusterSurface(clusters.get(i), true));
            }

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("contractVersion", CONTENT_CONTRACT_VERSION);
            payload.put("generatedAtMs", System.currentTimeMillis());
            payload.put("topologyModel", "Aeron fiefdoms + lazy read fabric");
            payload.put("networkStatus", remoteClusters.isEmpty() ? "local-only" : "observable");
            payload.put("localCluster", toClusterSurface(localCluster, true));
            payload.put("mountedNeighbors", remoteClusters);
            payload.put("outerNetwork", buildOuterNetwork(remoteClusters.size()));
            payload.put("cacheHints", buildCacheHints());
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
        } catch (Exception e) {
            log.error("Failed explorer content nav", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Failed explorer content nav: " + e.getMessage());
        }
    }

    public void handleContentTree(HttpServletResponse response, String clusterId, String requestedPath) throws IOException {
        handleContentTree(response, clusterId, requestedPath, 0, DEFAULT_TREE_PAGE_SIZE);
    }

    /**
     * Content tree with offset paging over visible children. Pages follow the
     * node's child iteration order at read time, so writes between page
     * requests can shift entries.
     */
    public void handleContentTree(HttpServletResponse response, String clusterId, String requestedPath,
                                  int offset, int limit) throws IOException {
        response.setContentType("application/json");
        try {
            ClusterDescriptor cluster = requireCluster(clusterId, response);
            if (cluster == null) {
                return;
            }

            String path = normalizeContentPath(requestedPath, cluster.browseRootPath);
            if (!isPathAllowed(cluster, path)) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST, "Path is outside the requested cluster scope");
                return;
            }

            NodeState node = getNode(path);
            if (!node.exists()) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_NOT_FOUND, "Node not found");
                return;
            }

            Map<String, Object> payload = buildContentEnvelope(cluster, path, node);
            payload.put("node", buildNodeSummary(path, node));
            int pageOffset = Math.max(0, offset);
            int pageLimit = Math.min(MAX_TREE_PAGE_SIZE, Math.max(1, limit));
            List<Map<String, Object>> children = buildVisibleChildren(cluster, path, node, pageOffset, pageLimit + 1);
            boolean hasMore = children.size() > pageLimit;
            if (hasMore) {
                children = children.subList(0, pageLimit);
            }
            Map<String, Object> page = new LinkedHashMap<>();
            page.put("offset", pageOffset);
            page.put("limit", pageLimit);
            page.put("returned", children.size());
            page.put("nextOffset", hasMore ? pageOffset + pageLimit : null);
            payload.put("children", children);
            payload.put("childrenPage", page);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
        } catch (Exception e) {
            log.error("Failed explorer content tree for cluster {}", clusterId, e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Failed explorer content tree: " + e.getMessage());
        }
    }

    public void handleContentNode(HttpServletResponse response, String clusterId, String requestedPath) throws IOException {
        response.setContentType("application/json");
        try {
            ClusterDescriptor cluster = requireCluster(clusterId, response);
            if (cluster == null) {
                return;
            }

            String path = normalizeContentPath(requestedPath, cluster.browseRootPath);
            if (!isPathAllowed(cluster, path)) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST, "Path is outside the requested cluster scope");
                return;
            }

            NodeState node = getNode(path);
            if (!node.exists()) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_NOT_FOUND, "Node not found");
                return;
            }

            Map<String, Object> payload = buildContentEnvelope(cluster, path, node);
            payload.put("node", buildNodeSummary(path, node));
            payload.put("properties", buildProperties(node));
            payload.put("childrenPreview", buildVisibleChildren(cluster, path, node, 0, 24));
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
        } catch (Exception e) {
            log.error("Failed explorer content node for cluster {}", clusterId, e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Failed explorer content node: " + e.getMessage());
        }
    }

    public void handleContentProvenance(HttpServletResponse response, String clusterId, String requestedPath) throws IOException {
        response.setContentType("application/json");
        try {
            ClusterDescriptor cluster = requireCluster(clusterId, response);
            if (cluster == null) {
                return;
            }

            String path = normalizeContentPath(requestedPath, cluster.browseRootPath);
            if (!isPathAllowed(cluster, path)) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST, "Path is outside the requested cluster scope");
                return;
            }

            NodeState node = getNode(path);
            if (!node.exists()) {
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_NOT_FOUND, "Node not found");
                return;
            }

            Map<String, Object> payload = buildContentEnvelope(cluster, path, node);
            payload.put("node", buildNodeSummary(path, node));
            payload.put("writeMetadata", findNearestWriteMetadata(path));
            payload.put("walletAuthority", buildWalletAuthority(extractWalletAddress(path)));

            Map<String, Object> facts = new LinkedHashMap<>();
            facts.put("readOnly", cluster.readOnly);
            facts.put("authoritative", cluster.authoritative);
            facts.put("propertyCount", countProperties(node));
            facts.put("childCount", countChildren(node));
            payload.put("contentFacts", facts);

            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
        } catch (Exception e) {
            log.error("Failed explorer content provenance for cluster {}", clusterId, e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Failed explorer content provenance: " + e.getMessage());
        }
    }

    public void handleReleaseFlow(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        if (context.proposalQueueManager == null) {
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Proposal queue not available");
            return;
        }
        try {
            Map<String, Object> flow = context.proposalQueueManager.getProposalReleaseFlowStats();
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("contractVersion", "explorer.v1");
            payload.put("generatedAtMs", System.currentTimeMillis());
            payload.put("releaseFlow", flow);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
        } catch (Exception e) {
            log.error("Failed explorer release flow", e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Failed explorer release flow: " + e.getMessage());
        }
    }

    private ClusterDescriptor requireCluster(String clusterId, HttpServletResponse response) throws IOException {
        if (clusterId == null || clusterId.trim().isEmpty()) {
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST, "clusterId is required");
            return null;
        }

        for (ClusterDescriptor descriptor : buildClusterDescriptors()) {
            if (descriptor.clusterId.equals(clusterId)) {
                return descriptor;
            }
        }

        ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_NOT_FOUND, "Cluster not found");
        return null;
    }

    private List<ClusterDescriptor> buildClusterDescriptors() {
        List<ClusterDescriptor> descriptors = new ArrayList<>();
        descriptors.add(buildLocalClusterDescriptor());

        ShardingRuntimeConfig runtimeConfig = context != null ? context.shardingRuntimeConfig : null;
        if (runtimeConfig == null || !runtimeConfig.isEnabled()) {
            return descriptors;
        }

        Map<String, List<ShardingRuntimeConfig.ReadOnlyMount>> mountsByEndpoint = new LinkedHashMap<>();
        for (ShardingRuntimeConfig.ReadOnlyMount mount : runtimeConfig.expandRemoteReadOnlyMounts()) {
            List<ShardingRuntimeConfig.ReadOnlyMount> mounts = mountsByEndpoint.get(mount.getEndpoint());
            if (mounts == null) {
                mounts = new ArrayList<>();
                mountsByEndpoint.put(mount.getEndpoint(), mounts);
            }
            mounts.add(mount);
        }

        int ordinal = 1;
        for (Map.Entry<String, List<ShardingRuntimeConfig.ReadOnlyMount>> entry : mountsByEndpoint.entrySet()) {
            List<String> allowedRoots = new ArrayList<>();
            List<String> prefixes = new ArrayList<>();
            for (ShardingRuntimeConfig.ReadOnlyMount mount : entry.getValue()) {
                allowedRoots.add(mount.getMountPath());
                prefixes.add(mount.getL1Prefix());
            }
            descriptors.add(new ClusterDescriptor(
                clusterIdForEndpoint(entry.getKey(), "remote"),
                displayNameForEndpoint(entry.getKey(), ordinal),
                "remote",
                true,
                false,
                CONTENT_ROOT_PATH,
                allowedRoots,
                summarizePrefixes(prefixes),
                entry.getKey(),
                "Lazy read-only remote cluster",
                "HTTP segment transfer",
                "visible",
                "Remote cluster remains outside local consensus and is visible through read-only mounts."
            ));
            ordinal++;
        }

        return descriptors;
    }

    private ClusterDescriptor buildLocalClusterDescriptor() {
        ShardingRuntimeConfig runtimeConfig = context != null ? context.shardingRuntimeConfig : null;
        List<String> allowedRoots = new ArrayList<>();
        String ownedPrefixes = "all";

        if (runtimeConfig != null && runtimeConfig.isEnabled()) {
            List<String> localPrefixes = runtimeConfig.expandLocalPrefixes();
            for (String prefix : localPrefixes) {
                allowedRoots.add(CONTENT_ROOT_PATH + "/" + prefix);
            }
            ownedPrefixes = runtimeConfig.describeLocalRanges();
        }

        if (allowedRoots.isEmpty()) {
            allowedRoots.add(CONTENT_ROOT_PATH);
        }

        return new ClusterDescriptor(
            clusterIdForEndpoint(context != null ? context.selfUrl : "local", "local"),
            displayNameForEndpoint(context != null ? context.selfUrl : "local", 0),
            "local",
            false,
            true,
            CONTENT_ROOT_PATH,
            allowedRoots,
            ownedPrefixes,
            context != null ? context.selfUrl : null,
            "Authoritative local write scope",
            "Aeron consensus",
            resolveLocalClusterStatus(),
            "Local wallets write here; foreign wallets redirect before queueing."
        );
    }

    private Map<String, Object> buildContentEnvelope(ClusterDescriptor cluster, String path, NodeState node) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("contractVersion", CONTENT_CONTRACT_VERSION);
        payload.put("generatedAtMs", System.currentTimeMillis());
        payload.put("cluster", toClusterSurface(cluster, false));
        payload.put("authority", buildAuthority(cluster, path));
        payload.put("breadcrumbs", buildBreadcrumbs(path));
        payload.put("path", path);
        payload.put("namespace", inferNamespace(cluster, path));
        payload.put("exists", node.exists());
        return payload;
    }

    private Map<String, Object> buildAuthority(ClusterDescriptor cluster, String path) {
        Map<String, Object> authority = new LinkedHashMap<>();
        authority.put("clusterId", cluster.clusterId);
        authority.put("scope", cluster.scope);
        authority.put("readOnly", cluster.readOnly);
        authority.put("authoritative", cluster.authoritative);
        authority.put("ownedPrefixes", cluster.ownedPrefixes);
        authority.put("browseRoot", cluster.browseRootPath);
        authority.put("namespace", inferNamespace(cluster, path));
        if (cluster.endpoint != null) {
            authority.put("endpoint", cluster.endpoint);
        }
        return authority;
    }

    private Map<String, Object> toClusterSurface(ClusterDescriptor cluster, boolean includeRoots) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("clusterId", cluster.clusterId);
        payload.put("displayName", cluster.displayName);
        payload.put("scope", cluster.scope);
        payload.put("readOnly", cluster.readOnly);
        payload.put("authoritative", cluster.authoritative);
        payload.put("roleLabel", cluster.roleLabel);
        payload.put("ownedPrefixes", cluster.ownedPrefixes);
        payload.put("status", cluster.status);
        payload.put("transport", cluster.transport);
        payload.put("note", cluster.note);
        payload.put("browseRoot", cluster.browseRootPath);
        if (cluster.endpoint != null) {
            payload.put("endpoint", cluster.endpoint);
        }
        if (includeRoots) {
            payload.put("roots", buildBrowseRoots(cluster));
        }
        if ("local".equals(cluster.scope)) {
            payload.put("nodeCount", resolveLocalNodeCount());
            payload.put("leaderLabel", resolveLeaderLabel());
            payload.put("authority", "This Aeron cluster is the local writable authority plane.");
            payload.put("consensusPlane", "Aeron consensus");
            payload.put("writeRule", cluster.note);
        } else {
            payload.put("relation", cluster.roleLabel);
        }
        return payload;
    }

    private List<Map<String, Object>> buildBrowseRoots(ClusterDescriptor cluster) {
        List<Map<String, Object>> roots = new ArrayList<>();
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("label", cluster.displayName);
        root.put("path", cluster.browseRootPath);
        root.put("namespace", inferNamespace(cluster, cluster.browseRootPath));
        root.put("readOnly", cluster.readOnly);
        root.put("ownedPrefixes", cluster.ownedPrefixes);
        roots.add(root);
        return roots;
    }

    private Map<String, Object> buildOuterNetwork(int remoteClusterCount) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("label", "Oak Chain beyond the local mount horizon");
        payload.put("status", remoteClusterCount > 0 ? "observable" : "local-only");
        payload.put("summary", "Independent Aeron fiefdoms can be read across a lazy fabric without collapsing into one consensus domain.");
        payload.put("discoveryPlane", "Separate control plane");
        payload.put("readFabric", remoteClusterCount > 0 ? "Lazy read-only mounts over HTTP segment transfer" : "No mounted neighbors observed yet");
        payload.put("writeAuthority", "Each cluster writes only its owned prefixes.");
        payload.put("observedClusterCount", 1 + remoteClusterCount);
        payload.put("mountedClusterCount", remoteClusterCount);
        List<String> principles = new ArrayList<>();
        principles.add("Aeron governs the local writable repository only.");
        principles.add("Cross-cluster reads are lazy and read-only.");
        principles.add("Discovery stays separate from consensus.");
        payload.put("principles", principles);
        return payload;
    }

    private Map<String, Object> buildCacheHints() {
        Map<String, Object> hints = new LinkedHashMap<>();
        Map<String, Object> local = new LinkedHashMap<>();
        local.put("strategy", "event-invalidated");
        local.put("fallbackTtlMs", 10000L);
        hints.put("local", local);

        Map<String, Object> remote = new LinkedHashMap<>();
        remote.put("strategy", "ttl");
        remote.put("ttlMs", 24L * 60L * 60L * 1000L);
        hints.put("remote", remote);
        return hints;
    }

    private List<Map<String, Object>> buildVisibleChildren(ClusterDescriptor cluster, String path, NodeState node,
                                                           int offset, int limit) {
        List<Map<String, Object>> children = new ArrayList<>();
        int skipped = 0;
        for (ChildNodeEntry entry : node.getChildNodeEntries()) {
            String childPath = joinPath(path, entry.getName());
            if (!isPathVisible(cluster, childPath)) {
                continue;
            }
            if (skipped < offset) {
                skipped++;
                continue;
            }
            children.add(buildNodeSummary(childPath, entry.getNodeState()));
            if (children.size() >= limit) {
                break;
            }
        }
        return children;
    }

    private Map<String, Object> buildNodeSummary(String path, NodeState node) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("name", nodeName(path));
        summary.put("path", path);
        summary.put("primaryType", readStringProperty(node, "jcr:primaryType", "nt:unstructured"));
        summary.put("childCount", countChildren(node));
        summary.put("propertyCount", countProperties(node));
        summary.put("hasChildren", countChildren(node) > 0);
        return summary;
    }

    private List<Map<String, Object>> buildProperties(NodeState node) {
        List<Map<String, Object>> properties = new ArrayList<>();
        for (PropertyState property : node.getProperties()) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("name", property.getName());
            payload.put("type", property.getType().toString());
            payload.put("multiValued", property.isArray());
            if (property.isArray()) {
                List<String> values = new ArrayList<>();
                for (int i = 0; i < property.count(); i++) {
                    values.add(stringifyPropertyValue(property, i));
                }
                payload.put("values", values);
            } else {
                payload.put("value", stringifyPropertyValue(property));
            }
            properties.add(payload);
        }
        return properties;
    }

    private String stringifyPropertyValue(PropertyState property) {
        if (property.getType() == Type.BINARY) {
            return "[Binary: " + property.size() + " bytes]";
        }
        if (property.getType() == Type.BOOLEAN) {
            return String.valueOf(property.getValue(Type.BOOLEAN));
        }
        if (property.getType() == Type.LONG) {
            return String.valueOf(property.getValue(Type.LONG));
        }
        if (property.getType() == Type.DOUBLE) {
            return String.valueOf(property.getValue(Type.DOUBLE));
        }
        if (property.getType() == Type.DATE) {
            return String.valueOf(property.getValue(Type.DATE));
        }
        return String.valueOf(property.getValue(Type.STRING));
    }

    private String stringifyPropertyValue(PropertyState property, int index) {
        try {
            return String.valueOf(property.getValue(Type.STRING, index));
        } catch (Exception ignored) {
            return String.valueOf(property.getValue(Type.STRINGS).iterator().next());
        }
    }

    private Map<String, Object> findNearestWriteMetadata(String path) {
        if (context == null || context.recentWriteMetadata == null || context.recentWriteMetadata.isEmpty()) {
            return null;
        }

        String current = path;
        while (current != null && !current.isEmpty()) {
            WriteMetadata metadata = context.recentWriteMetadata.get(current);
            if (metadata != null) {
                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("matchPath", current);
                payload.put("exact", current.equals(path));
                payload.put("recordId", metadata.recordId);
                payload.put("source", metadata.source);
                payload.put("validator", metadata.validator);
                payload.put("timestamp", metadata.timestamp);
                payload.put("message", metadata.message);
                return payload;
            }
            current = parentPath(current);
        }
        return null;
    }

    private Map<String, Object> buildWalletAuthority(String walletAddress) {
        if (walletAddress == null || walletAddress.trim().isEmpty()) {
            return null;
        }

        ShardingRuntimeConfig runtimeConfig = context != null ? context.shardingRuntimeConfig : null;
        if (runtimeConfig == null) {
            return null;
        }

        ShardingRuntimeConfig.ResolvedWallet resolvedWallet = runtimeConfig.resolveWallet(walletAddress);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("wallet", walletAddress);
        payload.put("ownership", resolvedWallet.getOwnership().name().toLowerCase(Locale.ROOT));
        payload.put("l1Prefix", resolvedWallet.getL1Prefix());
        payload.put("scope", resolvedWallet.getOwnership() == ShardingRuntimeConfig.Ownership.REMOTE ? "remote" : "local");
        payload.put("readOnly", resolvedWallet.getOwnership() == ShardingRuntimeConfig.Ownership.REMOTE);
        payload.put("redirectUrl", resolvedWallet.buildRedirectUrl("/v1/explorer/wallets/" + walletAddress));
        return payload;
    }

    private String extractWalletAddress(String path) {
        if (path == null || !path.startsWith(CONTENT_ROOT_PATH + "/")) {
            return null;
        }
        String[] segments = path.split("/");
        if (segments.length < 6) {
            return null;
        }
        String wallet = segments[5];
        return wallet.startsWith("0x") ? wallet : null;
    }

    private List<Map<String, Object>> buildBreadcrumbs(String path) {
        List<Map<String, Object>> breadcrumbs = new ArrayList<>();
        if (path == null || path.trim().isEmpty() || "/".equals(path)) {
            return breadcrumbs;
        }

        String[] segments = path.split("/");
        StringBuilder current = new StringBuilder();
        for (String segment : segments) {
            if (segment == null || segment.isEmpty()) {
                continue;
            }
            current.append("/").append(segment);
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("label", segment);
            item.put("path", current.toString());
            breadcrumbs.add(item);
        }
        return breadcrumbs;
    }

    private NodeState getNode(String path) {
        NodeState current = context.nodeStore.getRoot();
        if (path == null || "/".equals(path)) {
            return current;
        }
        String[] segments = path.split("/");
        for (String segment : segments) {
            if (segment == null || segment.isEmpty()) {
                continue;
            }
            current = current.getChildNode(segment);
        }
        return current;
    }

    private String normalizeContentPath(String requestedPath, String fallback) {
        String candidate = requestedPath == null || requestedPath.trim().isEmpty() ? fallback : requestedPath.trim();
        if (!candidate.startsWith("/")) {
            candidate = "/" + candidate;
        }
        if (candidate.length() > 1 && candidate.endsWith("/")) {
            candidate = candidate.substring(0, candidate.length() - 1);
        }
        return candidate;
    }

    private boolean isPathAllowed(ClusterDescriptor cluster, String path) {
        if (cluster == null || path == null || !path.startsWith(CONTENT_ROOT_PATH)) {
            return false;
        }
        if (CONTENT_ROOT_PATH.equals(path)) {
            return CONTENT_ROOT_PATH.equals(cluster.browseRootPath);
        }
        return isPathVisible(cluster, path);
    }

    private boolean isPathVisible(ClusterDescriptor cluster, String path) {
        if (cluster.allowedRootPaths.isEmpty()) {
            return path.startsWith(cluster.browseRootPath);
        }
        for (String root : cluster.allowedRootPaths) {
            if (path.equals(root) || path.startsWith(root + "/")) {
                return true;
            }
        }
        return false;
    }

    private String inferNamespace(ClusterDescriptor cluster, String path) {
        if ("remote".equals(cluster.scope)) {
            for (String root : cluster.allowedRootPaths) {
                if (path.equals(root) || path.startsWith(root + "/")) {
                    return root;
                }
            }
            return cluster.browseRootPath;
        }

        for (String root : cluster.allowedRootPaths) {
            if (path.equals(root) || path.startsWith(root + "/")) {
                return root;
            }
        }
        return cluster.browseRootPath;
    }

    private String resolveLeaderLabel() {
        if (context != null && context.aeronConsensusEngine != null) {
            String currentLeader = context.aeronConsensusEngine.getCurrentLeader();
            if (currentLeader != null && !currentLeader.trim().isEmpty()) {
                return currentLeader;
            }
        }
        return "Leader unresolved";
    }

    private int resolveLocalNodeCount() {
        if (context != null && context.aeronConsensusEngine != null) {
            return Math.max(0, context.aeronConsensusEngine.getClusterSize());
        }
        return 1;
    }

    private String resolveLocalClusterStatus() {
        if (context != null && context.aeronConsensusEngine != null) {
            return context.aeronConsensusEngine.getCurrentRole().name();
        }
        return "STANDALONE";
    }

    private String clusterIdForEndpoint(String endpoint, String prefix) {
        String source = endpoint == null || endpoint.trim().isEmpty() ? prefix : endpoint;
        try {
            URI uri = new URI(source);
            String host = uri.getHost() != null ? uri.getHost() : prefix;
            int port = uri.getPort();
            String candidate = host.replaceAll("[^a-zA-Z0-9]+", "-").toLowerCase(Locale.ROOT);
            if (port > 0) {
                candidate = candidate + "-" + port;
            }
            return prefix + "-" + candidate;
        } catch (Exception ignored) {
            return prefix + "-" + source.replaceAll("[^a-zA-Z0-9]+", "-").toLowerCase(Locale.ROOT);
        }
    }

    private String displayNameForEndpoint(String endpoint, int ordinal) {
        if (endpoint == null || endpoint.trim().isEmpty()) {
            return ordinal == 0 ? "Local Aeron fiefdom" : "Remote cluster " + ordinal;
        }
        try {
            URI uri = new URI(endpoint);
            String host = uri.getHost();
            int port = uri.getPort();
            if (host != null && !host.isEmpty()) {
                return ordinal == 0
                    ? "Local " + host + (port > 0 ? ":" + port : "")
                    : "Remote " + host + (port > 0 ? ":" + port : "");
            }
        } catch (Exception ignored) {
            // Fall back below.
        }
        return ordinal == 0 ? "Local Aeron fiefdom" : "Remote cluster " + ordinal;
    }

    private String summarizePrefixes(List<String> prefixes) {
        if (prefixes == null || prefixes.isEmpty()) {
            return "none";
        }
        Set<String> ordered = new LinkedHashSet<>(prefixes);
        List<String> compact = new ArrayList<>(ordered);
        Collections.sort(compact);
        if (compact.size() <= 8) {
            return String.join(", ", compact);
        }
        return compact.get(0) + " ... " + compact.get(compact.size() - 1) + " (" + compact.size() + " prefixes)";
    }

    private String readStringProperty(NodeState node, String propertyName, String fallback) {
        if (node == null || !node.hasProperty(propertyName)) {
            return fallback;
        }
        try {
            return node.getProperty(propertyName).getValue(Type.STRING);
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private int countChildren(NodeState node) {
        int count = 0;
        for (ChildNodeEntry ignored : node.getChildNodeEntries()) {
            count++;
        }
        return count;
    }

    private int countProperties(NodeState node) {
        int count = 0;
        for (PropertyState ignored : node.getProperties()) {
            count++;
        }
        return count;
    }

    private String joinPath(String parent, String child) {
        if ("/".equals(parent)) {
            return "/" + child;
        }
        return parent + "/" + child;
    }

    private String nodeName(String path) {
        if (path == null || "/".equals(path) || path.isEmpty()) {
            return "/";
        }
        int index = path.lastIndexOf('/');
        return index >= 0 ? path.substring(index + 1) : path;
    }

    private String parentPath(String path) {
        if (path == null || path.isEmpty() || "/".equals(path)) {
            return null;
        }
        int index = path.lastIndexOf('/');
        if (index <= 0) {
            return "/";
        }
        return path.substring(0, index);
    }

    private long asLong(Object value) {
        if (value == null) return 0L;
        if (value instanceof Number) return ((Number) value).longValue();
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (Exception e) {
            return 0L;
        }
    }

    private boolean asBoolean(Object value) {
        if (value instanceof Boolean) return (Boolean) value;
        if (value == null) return false;
        return Boolean.parseBoolean(String.valueOf(value));
    }

    private static final class ClusterDescriptor {
        private final String clusterId;
        private final String displayName;
        private final String scope;
        private final boolean readOnly;
        private final boolean authoritative;
        private final String browseRootPath;
        private final List<String> allowedRootPaths;
        private final String ownedPrefixes;
        private final String endpoint;
        private final String roleLabel;
        private final String transport;
        private final String status;
        private final String note;

        private ClusterDescriptor(String clusterId,
                                  String displayName,
                                  String scope,
                                  boolean readOnly,
                                  boolean authoritative,
                                  String browseRootPath,
                                  List<String> allowedRootPaths,
                                  String ownedPrefixes,
                                  String endpoint,
                                  String roleLabel,
                                  String transport,
                                  String status,
                                  String note) {
            this.clusterId = clusterId;
            this.displayName = displayName;
            this.scope = scope;
            this.readOnly = readOnly;
            this.authoritative = authoritative;
            this.browseRootPath = browseRootPath;
            this.allowedRootPaths = allowedRootPaths != null ? allowedRootPaths : Collections.<String>emptyList();
            this.ownedPrefixes = ownedPrefixes;
            this.endpoint = endpoint;
            this.roleLabel = roleLabel;
            this.transport = transport;
            this.status = status;
            this.note = note;
        }
    }
}
