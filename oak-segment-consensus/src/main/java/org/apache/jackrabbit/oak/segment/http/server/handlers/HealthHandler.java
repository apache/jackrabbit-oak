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

import org.apache.jackrabbit.oak.segment.consensus.config.IpfsGatewayUrls;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterLauncher;
import org.apache.jackrabbit.oak.segment.consensus.aeron.CrashHandler;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.segment.http.server.util.FormatUtils;
import org.apache.jackrabbit.oak.segment.http.server.util.JsonOutputUtil;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Handler for health check endpoints.
 * 
 * <p>Extracted from SegmentHttpServer to separate health check concerns.</p>
 */
public class HealthHandler {
    private static final Logger log = LoggerFactory.getLogger(HealthHandler.class);
    private static final long OPS_HEALTH_SNAPSHOT_TTL_MS = 1000L;
    private static final long OPS_RUNTIME_SNAPSHOT_TTL_MS = 1000L;
    private static final long OPS_STORAGE_SNAPSHOT_TTL_MS = 5000L;
    
    private final FileStore fileStore;
    private final NodeStore nodeStore;
    private final Path storeDirectory;
    private final ServerContext context;
    private final Map<String, ?> registeredClients;
    private final Map<String, ?> registeredValidators;
    private final Object opsHealthSnapshotLock = new Object();
    private final Object opsRuntimeSnapshotLock = new Object();
    private final Object opsStorageSnapshotLock = new Object();
    private volatile Map<String, Object> cachedOpsHealthSnapshotData;
    private volatile long cachedOpsHealthSnapshotSourceTimestampMs;
    private volatile Map<String, Object> cachedOpsRuntimeSnapshotData;
    private volatile long cachedOpsRuntimeSnapshotSourceTimestampMs;
    private volatile Map<String, Object> cachedOpsStorageSnapshotData;
    private volatile long cachedOpsStorageSnapshotSourceTimestampMs;
    
    public HealthHandler(
            FileStore fileStore,
            NodeStore nodeStore,
            Path storeDirectory,
            AeronConsensusEngine aeronConsensusEngine,
            Map<String, ?> registeredClients,
            Map<String, ?> registeredValidators,
            ServerContext context) {
        this.fileStore = fileStore;
        this.nodeStore = nodeStore;
        this.storeDirectory = storeDirectory;
        this.context = context;
        this.registeredClients = registeredClients;
        this.registeredValidators = registeredValidators;
    }
    
    /**
     * Handle simple health check endpoint.
     * 
     * <p>🔄 FINALITY-AWARE: Includes committedHead vs latestHead for clients to know what is safe.</p>
     * 
     * <p>ADR 028: Returns 503 if cluster is unhealthy (no leader, session timeout, etc.)</p>
     */
    public void handleHealth(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        
        // ADR 028: Check cluster health for status code
        boolean isClusterHealthy = true;
        String unhealthyReason = null;
        
        if (context != null && context.aeronConsensusEngine != null) {
            isClusterHealthy = context.aeronConsensusEngine.isClusterHealthy();
            if (!isClusterHealthy) {
                unhealthyReason = context.aeronConsensusEngine.getUnhealthyReason();
            }
        }
        
        // Return 503 if cluster unhealthy, 200 otherwise
        response.setStatus(isClusterHealthy ? HttpServletResponse.SC_OK : HttpServletResponse.SC_SERVICE_UNAVAILABLE);

        Map<String, Object> payload = new HashMap<>();
        payload.put("success", isClusterHealthy);
        payload.put("status", isClusterHealthy ? "UP" : "UNHEALTHY");
        payload.put("timestamp", System.currentTimeMillis());
        if (!isClusterHealthy && unhealthyReason != null) {
            payload.put("unhealthyReason", unhealthyReason);
        }
        payload.put("store", String.valueOf(storeDirectory));

        if (context != null && context.blobStoreType != null) {
            payload.put("blobStoreType", context.blobStoreType);
            payload.put("blobStoreActive", context.blobStore != null);
        }

        if (context != null && context.aeronConsensusEngine != null) {
            payload.put("clusterHealthy", isClusterHealthy);
            payload.put("reachableCount", context.aeronConsensusEngine.getReachableValidatorCount());
            payload.put("totalMembers", context.aeronConsensusEngine.getTotalMemberCount());
            payload.put("quorumSize", context.aeronConsensusEngine.getQuorumSize());
            payload.put("currentRole", context.aeronConsensusEngine.getCurrentRole().name());
            payload.put("internalIngressClient", context.aeronConsensusEngine.getInternalIngressClientDiagnostics());

            String committedHead = context.aeronConsensusEngine.getCommittedHead();
            String latestHead = context.aeronConsensusEngine.getLatestHead();
            int latestEpochSeen = context.aeronConsensusEngine.getLatestEpochSeen();
            int committedEpoch = context.aeronConsensusEngine.getLastCommittedEpoch();

            if (committedHead != null && !committedHead.isEmpty()) {
                payload.put("committedHead", committedHead);
            }
            if (latestHead != null && !latestHead.isEmpty()) {
                payload.put("latestHead", latestHead);
            }
            if (latestEpochSeen >= 0) {
                payload.put("latestEpochSeen", latestEpochSeen);
            }
            if (committedEpoch >= 0) {
                payload.put("committedEpoch", committedEpoch);
            }
        }

        payload.put("sharding", buildShardingPayload());

        response.getWriter().write(JsonOutputUtil.toJson(payload));
    }

    /**
     * Handle lightweight local liveness endpoint.
     *
     * <p>This endpoint intentionally avoids quorum and peer-probe checks so
     * validators can probe each other's HTTP responsiveness without causing
     * recursive health fan-out.</p>
     */
    public void handleLocalHealth(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");

        boolean localReady = fileStore != null && nodeStore != null && storeDirectory != null;
        response.setStatus(localReady ? HttpServletResponse.SC_OK : HttpServletResponse.SC_SERVICE_UNAVAILABLE);

        Map<String, Object> payload = new HashMap<>();
        payload.put("success", localReady);
        payload.put("status", localReady ? "UP" : "DOWN");
        payload.put("scope", "local");
        payload.put("timestamp", System.currentTimeMillis());
        payload.put("store", String.valueOf(storeDirectory));

        if (context != null && context.blobStoreType != null) {
            payload.put("blobStoreType", context.blobStoreType);
            payload.put("blobStoreActive", context.blobStore != null);
        }

        if (context != null && context.aeronConsensusEngine != null) {
            payload.put("currentRole", context.aeronConsensusEngine.getCurrentRole().name());
        }

        response.getWriter().write(JsonOutputUtil.toJson(payload));
    }
    
    /**
     * Handle comprehensive health check - validates all system components.
     */
    public void handleDeepHealth(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        boolean allHealthy = true;
        Map<String, Object> payload = new HashMap<>();

        Map<String, Object> fileStoreHealth = new HashMap<>();
        try {
            if (fileStore != null) {
                String headId = fileStore.getHead().getRecordId().toString10();
                fileStoreHealth.put("status", "UP");
                fileStoreHealth.put("head", headId.substring(0, Math.min(16, headId.length())) + "...");
                AeronConsensusEngine aeronEngine = (context != null) ? context.aeronConsensusEngine : null;
                if (aeronEngine != null) {
                    if (aeronEngine.getCommittedHead() != null && !aeronEngine.getCommittedHead().isEmpty()) {
                        fileStoreHealth.put("committedHead", aeronEngine.getCommittedHead());
                    }
                    if (aeronEngine.getLatestHead() != null && !aeronEngine.getLatestHead().isEmpty()) {
                        fileStoreHealth.put("latestHead", aeronEngine.getLatestHead());
                    }
                    if (aeronEngine.getLatestEpochSeen() >= 0) {
                        fileStoreHealth.put("latestEpochSeen", aeronEngine.getLatestEpochSeen());
                    }
                    if (aeronEngine.getLastCommittedEpoch() >= 0) {
                        fileStoreHealth.put("committedEpoch", aeronEngine.getLastCommittedEpoch());
                    }
                }
            } else {
                fileStoreHealth.put("status", "DOWN");
                fileStoreHealth.put("error", "FileStore not initialized");
                allHealthy = false;
            }
        } catch (Exception e) {
            fileStoreHealth.put("status", "DOWN");
            fileStoreHealth.put("error", e.getMessage());
            allHealthy = false;
        }
        payload.put("fileStore", fileStoreHealth);

        Map<String, Object> cluster = new HashMap<>();
        try {
            AeronConsensusEngine aeronEngine = (context != null) ? context.aeronConsensusEngine : null;
            if (aeronEngine != null) {
                boolean clusterHealthy = aeronEngine.isClusterHealthy();
                cluster.put("status", clusterHealthy ? "UP" : "UNHEALTHY");
                cluster.put("reachableCount", aeronEngine.getReachableValidatorCount());
                cluster.put("totalMembers", aeronEngine.getTotalMemberCount());
                cluster.put("quorumSize", aeronEngine.getQuorumSize());
                cluster.put("currentRole", aeronEngine.getCurrentRole().name());
                cluster.put("heartbeatAgeMs", aeronEngine.getHeartbeatAgeMs());
                cluster.put("internalIngressClient", aeronEngine.getInternalIngressClientDiagnostics());
                if (!clusterHealthy && aeronEngine.getUnhealthyReason() != null) {
                    cluster.put("unhealthyReason", aeronEngine.getUnhealthyReason());
                }
                if (!clusterHealthy) {
                    allHealthy = false;
                }
            } else {
                cluster.put("status", "UNKNOWN");
                cluster.put("error", "Aeron consensus engine not initialized");
                allHealthy = false;
            }
        } catch (Exception e) {
            cluster.put("status", "DOWN");
            cluster.put("error", e.getMessage());
            allHealthy = false;
        }
        payload.put("cluster", cluster);

        Map<String, Object> nodeStoreHealth = new HashMap<>();
        try {
            if (nodeStore != null) {
                nodeStoreHealth.put("status", "UP");
                nodeStoreHealth.put("rootExists", nodeStore.getRoot() != null);
            } else {
                nodeStoreHealth.put("status", "DOWN");
                nodeStoreHealth.put("error", "NodeStore not initialized");
                allHealthy = false;
            }
        } catch (Exception e) {
            nodeStoreHealth.put("status", "DOWN");
            nodeStoreHealth.put("error", e.getMessage());
            allHealthy = false;
        }
        payload.put("nodeStore", nodeStoreHealth);

        Map<String, Object> diskSpace = new HashMap<>();
        try {
            java.nio.file.FileStore fs = Files.getFileStore(storeDirectory);
            long totalSpace = fs.getTotalSpace();
            long usableSpace = fs.getUsableSpace();
            double usagePercent = ((totalSpace - usableSpace) * 100.0) / totalSpace;
            boolean diskHealthy = usagePercent < 90.0;
            diskSpace.put("status", diskHealthy ? "UP" : "WARN");
            diskSpace.put("totalGb", String.format("%.2f", totalSpace / (1024.0 * 1024.0 * 1024.0)));
            diskSpace.put("usableGb", String.format("%.2f", usableSpace / (1024.0 * 1024.0 * 1024.0)));
            diskSpace.put("usagePercent", String.format("%.1f", usagePercent));
            if (!diskHealthy) {
                allHealthy = false;
            }
        } catch (Exception e) {
            diskSpace.put("status", "DOWN");
            diskSpace.put("error", e.getMessage());
            allHealthy = false;
        }
        payload.put("diskSpace", diskSpace);

        AeronClusterLauncher aeronLauncher = (context != null) ? context.aeronClusterLauncher : null;
        if (aeronLauncher != null) {
            Map<String, Object> mediaDriver = new HashMap<>();
            try {
                CrashHandler crashHandler = aeronLauncher.getCrashHandler();
                org.apache.jackrabbit.oak.segment.consensus.aeron.MediaDriverHealthMonitor healthMonitor = aeronLauncher.getHealthMonitor();
                boolean mediaDriverHealthy = true;
                if (crashHandler != null) {
                    mediaDriver.put("crashState", crashHandler.getState());
                    mediaDriver.put("hasCrashed", crashHandler.hasCrashed());
                    mediaDriver.put("forceBootstrap", crashHandler.shouldForceBootstrap());
                    if (crashHandler.hasCrashed()) {
                        mediaDriverHealthy = false;
                    }
                } else {
                    mediaDriver.put("crashHandler", "not_initialized");
                }
                if (healthMonitor != null) {
                    mediaDriver.put("status", healthMonitor.isHealthy() ? "UP" : "DEGRADED");
                    mediaDriver.put("healthStatus", healthMonitor.getHealthStatus());
                    mediaDriver.put("errorCount", healthMonitor.getErrorCount());
                    mediaDriver.put("timeoutCount", healthMonitor.getTimeoutCount());
                    mediaDriver.put("backpressureCount", healthMonitor.getBackpressureCount());
                    mediaDriver.put("freeSpaceMB", healthMonitor.getFreeSpaceMB());
                    if (!healthMonitor.isHealthy()) {
                        mediaDriverHealthy = false;
                    }
                } else {
                    mediaDriver.put("status", "UP");
                    mediaDriver.put("healthMonitor", "not_initialized");
                }
                if (!mediaDriverHealthy) {
                    allHealthy = false;
                }
            } catch (Exception e) {
                mediaDriver.put("status", "DOWN");
                mediaDriver.put("error", e.getMessage());
                allHealthy = false;
            }
            payload.put("mediaDriver", mediaDriver);
        }

        AeronConsensusEngine aeronEngine = (context != null) ? context.aeronConsensusEngine : null;
        if (aeronEngine != null) {
            Map<String, Object> consensus = new HashMap<>();
            try {
                consensus.put("status", "UP");
                consensus.put("mode", "aeron-cluster");
                consensus.put("role", aeronEngine.getCurrentRole().toString());
                consensus.put("isLeader", aeronEngine.isLeader());
                consensus.put("epoch", aeronEngine.getCurrentEpoch());
                consensus.put("term", aeronEngine.getCurrentTerm());
                consensus.put("reachableValidators", aeronEngine.getReachableValidatorCount());
                String currentLeader = aeronEngine.getCurrentLeaderHint();
                consensus.put("currentLeader", currentLeader != null ? currentLeader : "none");
            } catch (Exception e) {
                consensus.put("status", "DOWN");
                consensus.put("error", e.getMessage());
                allHealthy = false;
            }
            payload.put("consensus", consensus);
        }

        Map<String, Object> clients = new HashMap<>();
        clients.put("status", "UP");
        clients.put("registeredClients", registeredClients.size());
        clients.put("registeredValidators", registeredValidators.size());
        payload.put("clients", clients);

        Map<String, Object> blobStore = new HashMap<>();
        if (context != null && context.blobStoreType != null) {
            String blobStoreType = context.blobStoreType;
            blobStore.put("type", blobStoreType);
            if (context.blobStore != null) {
                blobStore.put("status", "UP");
                if ("ipfs".equalsIgnoreCase(blobStoreType)) {
                    blobStore.put("cidMappingAvailable", context.cidMappingService != null);
                    blobStore.put("ipfsGateway", IpfsGatewayUrls.gatewayBase());
                    blobStore.put("ipfsLocalGateway", IpfsGatewayUrls.localGatewayBase());
                } else {
                    blobStore.put("note", blobStoreType + " storage configured");
                }
            } else {
                blobStore.put("status", "DEGRADED");
                blobStore.put("error", "BlobStore not initialized");
            }
        } else {
            blobStore.put("type", "default");
            blobStore.put("status", "UP");
            blobStore.put("note", "FileDataStore (embedded)");
        }
        payload.put("blobStore", blobStore);
        payload.put("sharding", buildShardingPayload());

        Map<String, Object> overall = new HashMap<>();
        overall.put("status", allHealthy ? "UP" : "DEGRADED");
        overall.put("timestamp", new java.util.Date().toString());
        payload.put("overall", overall);
        payload.put("timestamp", System.currentTimeMillis());
        payload.put("success", allHealthy);

        response.setStatus(allHealthy ? HttpServletResponse.SC_OK : HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        response.getWriter().write(JsonOutputUtil.toJson(payload));
    }
    
    /**
     * Handle cluster-only health check endpoint.
     * 
     * <p>ADR 028: Exposes quorum + heartbeat + leader status for pre-flight checks.</p>
     */
    public void handleClusterHealth(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        
        if (context == null || context.aeronConsensusEngine == null) {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            Map<String, Object> payload = new HashMap<>();
            payload.put("success", false);
            payload.put("status", "UNAVAILABLE");
            payload.put("reason", "cluster_not_initialized");
            payload.put("timestamp", System.currentTimeMillis());
            response.getWriter().write(JsonOutputUtil.toJson(payload));
            return;
        }
        
        boolean healthy = context.aeronConsensusEngine.isClusterHealthy();
        String reason = healthy ? null : context.aeronConsensusEngine.getUnhealthyReason();
        
        response.setStatus(healthy ? HttpServletResponse.SC_OK : HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        
        Map<String, Object> payload = new HashMap<>();
        payload.put("success", healthy);
        payload.put("status", healthy ? "UP" : "UNHEALTHY");
        payload.put("timestamp", System.currentTimeMillis());
        if (!healthy && reason != null) {
            payload.put("unhealthyReason", reason);
        }
        payload.put("reachableCount", context.aeronConsensusEngine.getReachableValidatorCount());
        payload.put("totalMembers", context.aeronConsensusEngine.getTotalMemberCount());
        payload.put("quorumSize", context.aeronConsensusEngine.getQuorumSize());
        payload.put("hasQuorum", context.aeronConsensusEngine.hasQuorum());
        payload.put("lastHeartbeatTime", context.aeronConsensusEngine.getLastHeartbeatTime());
        payload.put("heartbeatAgeMs", context.aeronConsensusEngine.getHeartbeatAgeMs());
        payload.put("leaderUrl", context.aeronConsensusEngine.getCurrentLeaderHint());
        response.getWriter().write(JsonOutputUtil.toJson(payload));
    }

    /**
     * Handle lightweight ops.v1 health snapshot endpoint.
     * GET /v1/ops/snapshots/health
     */
    public void handleGetOpsHealthSnapshot(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        long servedAtMs = System.currentTimeMillis();

        try {
            Map<String, Object> data;
            long sourceTimestampMs;
            boolean fromCache = false;

            synchronized (opsHealthSnapshotLock) {
                long now = System.currentTimeMillis();
                boolean cacheValid = cachedOpsHealthSnapshotData != null
                    && cachedOpsHealthSnapshotSourceTimestampMs > 0
                    && (now - cachedOpsHealthSnapshotSourceTimestampMs) <= OPS_HEALTH_SNAPSHOT_TTL_MS;

                if (cacheValid) {
                    data = cachedOpsHealthSnapshotData;
                    sourceTimestampMs = cachedOpsHealthSnapshotSourceTimestampMs;
                    fromCache = true;
                } else {
                    data = buildOpsHealthData();
                    sourceTimestampMs = now;
                    cachedOpsHealthSnapshotData = data;
                    cachedOpsHealthSnapshotSourceTimestampMs = sourceTimestampMs;
                }
            }

            long stalenessMs = Math.max(0L, servedAtMs - sourceTimestampMs);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(buildOpsEnvelope(
                "ops.v1", OPS_HEALTH_SNAPSHOT_TTL_MS, data, sourceTimestampMs, servedAtMs, stalenessMs, false, null, fromCache));
        } catch (Exception e) {
            log.warn("Error building ops health snapshot, attempting stale fallback: {}", e.getMessage());
            if (cachedOpsHealthSnapshotData != null && cachedOpsHealthSnapshotSourceTimestampMs > 0) {
                long stalenessMs = Math.max(0L, servedAtMs - cachedOpsHealthSnapshotSourceTimestampMs);
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(buildOpsEnvelope(
                    "ops.v1",
                    OPS_HEALTH_SNAPSHOT_TTL_MS,
                    cachedOpsHealthSnapshotData,
                    cachedOpsHealthSnapshotSourceTimestampMs,
                    servedAtMs,
                    stalenessMs,
                    true,
                    "STALE_CACHE_FALLBACK",
                    true));
                return;
            }

            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            response.getWriter().write(buildUnavailableOpsPayload("ops.v1"));
        }
    }

    /**
     * Handle governed runtime/operator snapshot endpoint.
     * GET /v1/ops/snapshots/runtime
     */
    public void handleGetOpsRuntimeSnapshot(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        long servedAtMs = System.currentTimeMillis();

        try {
            Map<String, Object> data;
            long sourceTimestampMs;
            boolean fromCache = false;

            synchronized (opsRuntimeSnapshotLock) {
                long now = System.currentTimeMillis();
                boolean cacheValid = cachedOpsRuntimeSnapshotData != null
                    && cachedOpsRuntimeSnapshotSourceTimestampMs > 0
                    && (now - cachedOpsRuntimeSnapshotSourceTimestampMs) <= OPS_RUNTIME_SNAPSHOT_TTL_MS;

                if (cacheValid) {
                    data = cachedOpsRuntimeSnapshotData;
                    sourceTimestampMs = cachedOpsRuntimeSnapshotSourceTimestampMs;
                    fromCache = true;
                } else {
                    data = buildOpsRuntimeData();
                    sourceTimestampMs = now;
                    cachedOpsRuntimeSnapshotData = data;
                    cachedOpsRuntimeSnapshotSourceTimestampMs = sourceTimestampMs;
                }
            }

            long stalenessMs = Math.max(0L, servedAtMs - sourceTimestampMs);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(buildOpsEnvelope(
                "ops.runtime.v1",
                OPS_RUNTIME_SNAPSHOT_TTL_MS,
                data,
                sourceTimestampMs,
                servedAtMs,
                stalenessMs,
                false,
                null,
                fromCache));
        } catch (Exception e) {
            log.warn("Error building ops runtime snapshot, attempting stale fallback: {}", e.getMessage());
            if (cachedOpsRuntimeSnapshotData != null && cachedOpsRuntimeSnapshotSourceTimestampMs > 0) {
                long stalenessMs = Math.max(0L, servedAtMs - cachedOpsRuntimeSnapshotSourceTimestampMs);
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(buildOpsEnvelope(
                    "ops.runtime.v1",
                    OPS_RUNTIME_SNAPSHOT_TTL_MS,
                    cachedOpsRuntimeSnapshotData,
                    cachedOpsRuntimeSnapshotSourceTimestampMs,
                    servedAtMs,
                    stalenessMs,
                    true,
                    "STALE_CACHE_FALLBACK",
                    true));
                return;
            }

            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            response.getWriter().write(buildUnavailableOpsPayload("ops.runtime.v1"));
        }
    }

    /**
     * Handle governed storage/operator snapshot endpoint.
     * GET /v1/ops/snapshots/storage
     */
    public void handleGetOpsStorageSnapshot(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        long servedAtMs = System.currentTimeMillis();

        try {
            Map<String, Object> data;
            long sourceTimestampMs;
            boolean fromCache = false;

            synchronized (opsStorageSnapshotLock) {
                long now = System.currentTimeMillis();
                boolean cacheValid = cachedOpsStorageSnapshotData != null
                    && cachedOpsStorageSnapshotSourceTimestampMs > 0
                    && (now - cachedOpsStorageSnapshotSourceTimestampMs) <= OPS_STORAGE_SNAPSHOT_TTL_MS;

                if (cacheValid) {
                    data = cachedOpsStorageSnapshotData;
                    sourceTimestampMs = cachedOpsStorageSnapshotSourceTimestampMs;
                    fromCache = true;
                } else {
                    data = buildOpsStorageData();
                    sourceTimestampMs = now;
                    cachedOpsStorageSnapshotData = data;
                    cachedOpsStorageSnapshotSourceTimestampMs = sourceTimestampMs;
                }
            }

            long stalenessMs = Math.max(0L, servedAtMs - sourceTimestampMs);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(buildOpsEnvelope(
                "ops.storage.v1",
                OPS_STORAGE_SNAPSHOT_TTL_MS,
                data,
                sourceTimestampMs,
                servedAtMs,
                stalenessMs,
                false,
                null,
                fromCache));
        } catch (Exception e) {
            log.warn("Error building ops storage snapshot, attempting stale fallback: {}", e.getMessage());
            if (cachedOpsStorageSnapshotData != null && cachedOpsStorageSnapshotSourceTimestampMs > 0) {
                long stalenessMs = Math.max(0L, servedAtMs - cachedOpsStorageSnapshotSourceTimestampMs);
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(buildOpsEnvelope(
                    "ops.storage.v1",
                    OPS_STORAGE_SNAPSHOT_TTL_MS,
                    cachedOpsStorageSnapshotData,
                    cachedOpsStorageSnapshotSourceTimestampMs,
                    servedAtMs,
                    stalenessMs,
                    true,
                    "STALE_CACHE_FALLBACK",
                    true));
                return;
            }

            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            response.getWriter().write(buildUnavailableOpsPayload("ops.storage.v1"));
        }
    }

    private Map<String, Object> buildOpsHealthData() {
        boolean clusterHealthy = true;
        String unhealthyReason = null;
        int reachableCount = -1;
        int totalMembers = -1;
        int quorumSize = -1;
        String currentRole = "UNKNOWN";
        String leaderUrl = null;

        if (context != null && context.aeronConsensusEngine != null) {
            clusterHealthy = context.aeronConsensusEngine.isClusterHealthy();
            if (!clusterHealthy) {
                unhealthyReason = context.aeronConsensusEngine.getUnhealthyReason();
            }
            reachableCount = context.aeronConsensusEngine.getReachableValidatorCount();
            totalMembers = context.aeronConsensusEngine.getTotalMemberCount();
            quorumSize = context.aeronConsensusEngine.getQuorumSize();
            currentRole = context.aeronConsensusEngine.getCurrentRole().name();
            leaderUrl = context.aeronConsensusEngine.getCurrentLeaderHint();
        }

        boolean blobStoreActive = context != null && context.blobStore != null;
        String blobStoreType = context != null ? context.blobStoreType : null;

        Map<String, Object> payload = new HashMap<>();
        payload.put("status", clusterHealthy ? "UP" : "UNHEALTHY");
        payload.put("clusterHealthy", clusterHealthy);
        payload.put("unhealthyReason", unhealthyReason);
        payload.put("blobStoreType", blobStoreType);
        payload.put("blobStoreActive", blobStoreActive);
        payload.put("reachableCount", reachableCount);
        payload.put("totalMembers", totalMembers);
        payload.put("quorumSize", quorumSize);
        payload.put("currentRole", currentRole);
        payload.put("leaderUrl", leaderUrl);
        payload.put("registeredClients", registeredClients != null ? registeredClients.size() : 0);
        payload.put("registeredValidators", registeredValidators != null ? registeredValidators.size() : 0);
        payload.put("sharding", buildShardingPayload());
        return payload;
    }

    private Map<String, Object> buildOpsRuntimeData() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("validator", buildValidatorRuntimeData());
        payload.put("aeron", buildAeronRuntimeData());
        payload.put("mediaDriver", buildMediaDriverPayload());
        payload.put("metrics", buildMetricsPayload());
        payload.put("sharding", buildShardingPayload());
        return payload;
    }

    private Map<String, Object> buildOpsStorageData() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("storePath", storeDirectory != null ? storeDirectory.toString() : null);
        payload.put("fileStore", buildFileStorePayload());
        payload.put("nodeStore", buildNodeStorePayload());
        payload.put("diskSpace", buildDiskSpacePayload());
        payload.put("blobStore", buildBlobStorePayload());
        List<Map<String, Object>> tarFiles = buildTarEntries();
        payload.put("tarFiles", tarFiles);
        payload.put("tarFileCount", tarFiles.size());
        payload.put("totalTarSizeBytes", totalTarSizeBytes(tarFiles));
        payload.put("totalTarSizeFormatted", FormatUtils.formatBytes(totalTarSizeBytes(tarFiles)));
        payload.put("sharding", buildShardingPayload());
        return payload;
    }

    private Map<String, Object> buildValidatorRuntimeData() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("storePath", storeDirectory != null ? storeDirectory.toString() : null);
        payload.put("registeredClients", registeredClients != null ? registeredClients.size() : 0);
        payload.put("registeredValidators", registeredValidators != null ? registeredValidators.size() : 0);
        if (context != null && context.selfUrl != null) {
            payload.put("selfUrl", context.selfUrl);
        }
        return payload;
    }

    private Map<String, Object> buildAeronRuntimeData() {
        Map<String, Object> payload = new LinkedHashMap<>();
        if (context == null || context.aeronConsensusEngine == null) {
            payload.put("status", "UNAVAILABLE");
            payload.put("consensusType", "none");
            return payload;
        }

        AeronConsensusEngine aeronEngine = context.aeronConsensusEngine;
        payload.put("status", aeronEngine.isClusterHealthy() ? "UP" : "UNHEALTHY");
        payload.put("consensusType", "aeron-cluster");
        payload.put("currentRole", aeronEngine.getCurrentRole().name());
        payload.put("isLeader", aeronEngine.isLeader());
        payload.put("currentLeader", aeronEngine.getCurrentLeader());
        payload.put("leaderHint", aeronEngine.getCurrentLeaderHint());
        payload.put("currentEpoch", aeronEngine.getCurrentEpoch());
        payload.put("currentTerm", aeronEngine.getCurrentTerm());
        payload.put("ethereumEpoch", aeronEngine.getCurrentEthereumEpoch());
        payload.put("reachableValidators", aeronEngine.getReachableValidatorCount());
        payload.put("totalMembers", aeronEngine.getTotalMemberCount());
        payload.put("quorumSize", aeronEngine.getQuorumSize());
        payload.put("lastHeartbeatTime", aeronEngine.getLastHeartbeatTime());
        payload.put("heartbeatAgeMs", aeronEngine.getHeartbeatAgeMs());
        payload.put("unhealthyReason", aeronEngine.getUnhealthyReason());

        Map<String, Object> nativeClusterState = aeronEngine.getNativeClusterState();
        if (nativeClusterState != null && !nativeClusterState.isEmpty()) {
            payload.put("nativeClusterState", nativeClusterState);
        }

        Map<String, Object> validatorIdentities = new AeronApiHandler(context).getValidatorIdentitiesData();
        if (validatorIdentities != null) {
            payload.put("validatorIdentities", validatorIdentities);
        }

        Map<String, Object> raft = new LinkedHashMap<>();
        raft.put("currentTerm", aeronEngine.getCurrentTerm());
        raft.put("isLeader", aeronEngine.isLeader());
        raft.put("currentLeader", aeronEngine.getCurrentLeader());
        raft.put("reachableValidators", aeronEngine.getReachableValidatorCount());
        raft.put("totalFollowers", aeronEngine.getAllFollowers() != null ? aeronEngine.getAllFollowers().size() : 0);
        raft.put("currentEpoch", aeronEngine.getCurrentEpoch());
        raft.put("ethereumEpoch", aeronEngine.getCurrentEthereumEpoch());
        payload.put("raft", raft);

        return payload;
    }

    private Map<String, Object> buildMediaDriverPayload() {
        Map<String, Object> payload = new LinkedHashMap<>();
        AeronClusterLauncher aeronLauncher = (context != null) ? context.aeronClusterLauncher : null;
        if (aeronLauncher == null) {
            payload.put("status", "NOT_CONFIGURED");
            return payload;
        }

        CrashHandler crashHandler = aeronLauncher.getCrashHandler();
        org.apache.jackrabbit.oak.segment.consensus.aeron.MediaDriverHealthMonitor healthMonitor =
            aeronLauncher.getHealthMonitor();

        if (crashHandler != null) {
            payload.put("crashState", crashHandler.getState());
            payload.put("crashCount", crashHandler.getCrashCount());
            payload.put("hasCrashed", crashHandler.hasCrashed());
            payload.put("forceBootstrap", crashHandler.shouldForceBootstrap());
        }

        if (healthMonitor != null) {
            payload.put("status", healthMonitor.isHealthy() ? "UP" : "DEGRADED");
            payload.put("healthStatus", healthMonitor.getHealthStatus());
            payload.put("errorCount", healthMonitor.getErrorCount());
            payload.put("timeoutCount", healthMonitor.getTimeoutCount());
            payload.put("backpressureCount", healthMonitor.getBackpressureCount());
            payload.put("freeSpaceMB", healthMonitor.getFreeSpaceMB());
        } else {
            payload.put("status", "UP");
            payload.put("healthMonitor", "not_initialized");
        }
        return payload;
    }

    private Map<String, Object> buildMetricsPayload() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("consensus", buildConsensusMetricsPayload());
        payload.put("replication", buildReplicationMetricsPayload());

        Map<String, Object> validator = new LinkedHashMap<>();
        validator.put("registeredClients", registeredClients != null ? registeredClients.size() : 0);
        validator.put("registeredValidators", registeredValidators != null ? registeredValidators.size() : 0);
        validator.put("storePath", storeDirectory != null ? storeDirectory.toString() : "");
        payload.put("validator", validator);
        payload.put("ipfsPolicy", buildIpfsPolicyPayload());
        return payload;
    }

    private Map<String, Object> buildConsensusMetricsPayload() {
        if (context == null || context.aeronConsensusEngine == null) {
            return null;
        }

        AeronConsensusEngine aeronEngine = context.aeronConsensusEngine;
        Map<String, Object> consensus = new LinkedHashMap<>();
        consensus.put("role", aeronEngine.getCurrentRole().name());
        consensus.put("isLeader", aeronEngine.isLeader());
        consensus.put("currentEpoch", aeronEngine.getCurrentEpoch());
        consensus.put("currentTerm", aeronEngine.getCurrentTerm());
        consensus.put("reachableValidators", aeronEngine.getReachableValidatorCount());
        consensus.put("totalMembers", aeronEngine.getTotalMemberCount());
        consensus.put("quorumSize", aeronEngine.getQuorumSize());
        consensus.put("heartbeatAgeMs", aeronEngine.getHeartbeatAgeMs());
        consensus.put("healthy", aeronEngine.isClusterHealthy());
        if (aeronEngine.getUnhealthyReason() != null) {
            consensus.put("unhealthyReason", aeronEngine.getUnhealthyReason());
        }
        return consensus;
    }

    private Map<String, Object> buildReplicationMetricsPayload() {
        if (context == null || context.aeronConsensusEngine == null) {
            return null;
        }

        Map<String, Object> status = context.aeronConsensusEngine.getReplicationLagStatus();
        if (status == null) {
            return null;
        }

        Map<String, Object> replication = new LinkedHashMap<>();
        replication.put("role", status.get("role"));
        replication.put("myLogPosition", status.get("myLogPosition"));
        replication.put("leaderLogPosition", status.get("leaderLogPosition"));
        replication.put("replicationLag", status.get("replicationLag"));
        replication.put("lagThreshold", status.get("lagThreshold"));
        replication.put("measurementAvailable", status.get("measurementAvailable"));
        replication.put("measurementAgeMs", status.get("measurementAgeMs"));
        replication.put("healthStatus", status.get("healthStatus"));
        replication.put("healthy", status.get("healthy"));
        if (status.get("reason") != null) {
            replication.put("reason", status.get("reason"));
        }
        return replication;
    }

    private Map<String, Object> buildIpfsPolicyPayload() {
        if (context == null) {
            return null;
        }

        Map<String, Object> policy = new LinkedHashMap<>();
        policy.put("rejectedAmbiguousSource", context.apiIpfsPolicyRejectAmbiguousSource.get());
        policy.put("rejectedNonEnterpriseCid", context.apiIpfsPolicyRejectNonEnterpriseCid.get());
        policy.put("rejectedUnknownCid", context.apiIpfsPolicyRejectUnknownCid.get());
        policy.put("rejectedCidServiceUnavailable", context.apiIpfsPolicyRejectCidServiceUnavailable.get());
        policy.put("acceptedEnterpriseCid", context.apiIpfsPolicyAcceptedEnterpriseCid.get());
        return policy;
    }

    private Map<String, Object> buildFileStorePayload() {
        Map<String, Object> payload = new LinkedHashMap<>();
        try {
            if (fileStore == null) {
                payload.put("status", "DOWN");
                payload.put("error", "FileStore not initialized");
                return payload;
            }

            String headId = fileStore.getHead().getRecordId().toString10();
            payload.put("status", "UP");
            payload.put("head", headId);
            if (context != null && context.aeronConsensusEngine != null) {
                payload.put("committedHead", context.aeronConsensusEngine.getCommittedHead());
                payload.put("latestHead", context.aeronConsensusEngine.getLatestHead());
                payload.put("latestEpochSeen", context.aeronConsensusEngine.getLatestEpochSeen());
                payload.put("committedEpoch", context.aeronConsensusEngine.getLastCommittedEpoch());
            }
        } catch (Exception e) {
            payload.put("status", "DOWN");
            payload.put("error", e.getMessage());
        }
        return payload;
    }

    private Map<String, Object> buildNodeStorePayload() {
        Map<String, Object> payload = new LinkedHashMap<>();
        try {
            if (nodeStore == null) {
                payload.put("status", "DOWN");
                payload.put("error", "NodeStore not initialized");
                return payload;
            }
            payload.put("status", "UP");
            payload.put("rootExists", nodeStore.getRoot() != null);
        } catch (Exception e) {
            payload.put("status", "DOWN");
            payload.put("error", e.getMessage());
        }
        return payload;
    }

    private Map<String, Object> buildDiskSpacePayload() {
        Map<String, Object> diskSpace = new LinkedHashMap<>();
        try {
            java.nio.file.FileStore fs = Files.getFileStore(storeDirectory);
            long totalSpace = fs.getTotalSpace();
            long usableSpace = fs.getUsableSpace();
            double usagePercent = totalSpace > 0 ? ((totalSpace - usableSpace) * 100.0) / totalSpace : 0.0;
            boolean diskHealthy = usagePercent < 90.0;
            diskSpace.put("status", diskHealthy ? "UP" : "WARN");
            diskSpace.put("totalBytes", totalSpace);
            diskSpace.put("usableBytes", usableSpace);
            diskSpace.put("totalGb", String.format("%.2f", totalSpace / (1024.0 * 1024.0 * 1024.0)));
            diskSpace.put("usableGb", String.format("%.2f", usableSpace / (1024.0 * 1024.0 * 1024.0)));
            diskSpace.put("usagePercent", String.format("%.1f", usagePercent));
        } catch (Exception e) {
            diskSpace.put("status", "DOWN");
            diskSpace.put("error", e.getMessage());
        }
        return diskSpace;
    }

    private Map<String, Object> buildBlobStorePayload() {
        Map<String, Object> blobStore = new LinkedHashMap<>();
        if (context != null && context.blobStoreType != null) {
            String blobStoreType = context.blobStoreType;
            blobStore.put("type", blobStoreType);
            if (context.blobStore != null) {
                blobStore.put("status", "UP");
                if ("ipfs".equalsIgnoreCase(blobStoreType)) {
                    blobStore.put("cidMappingAvailable", context.cidMappingService != null);
                    blobStore.put("ipfsGateway", IpfsGatewayUrls.gatewayBase());
                    blobStore.put("ipfsLocalGateway", IpfsGatewayUrls.localGatewayBase());
                } else {
                    blobStore.put("note", blobStoreType + " storage configured");
                }
            } else {
                blobStore.put("status", "DEGRADED");
                blobStore.put("error", "BlobStore not initialized");
            }
        } else {
            blobStore.put("type", "default");
            blobStore.put("status", "UP");
            blobStore.put("note", "FileDataStore (embedded)");
        }
        return blobStore;
    }

    private List<Map<String, Object>> buildTarEntries() {
        List<Map<String, Object>> tarEntries = new ArrayList<>();
        if (storeDirectory == null || !Files.exists(storeDirectory)) {
            return tarEntries;
        }

        try {
            int totalSegments = 0;
            Path journalPath = storeDirectory.resolve("journal.log");
            if (Files.exists(journalPath)) {
                totalSegments = Files.readAllLines(journalPath).size();
            }

            List<Path> tarFiles = new ArrayList<>();
            long totalSize = 0L;
            try (java.util.stream.Stream<Path> paths = Files.list(storeDirectory)) {
                tarFiles = paths
                    .filter(p -> p.toString().endsWith(".tar"))
                    .sorted(java.util.Comparator.comparing(Path::toString))
                    .collect(java.util.stream.Collectors.toList());
                for (Path tarFile : tarFiles) {
                    totalSize += Files.size(tarFile);
                }
            }

            for (Path tarFile : tarFiles) {
                long fileSize = Files.size(tarFile);
                BasicFileAttributes attrs = Files.readAttributes(tarFile, BasicFileAttributes.class);
                int estimatedSegments = totalSize > 0 ? (int) ((fileSize * totalSegments) / totalSize) : 0;

                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("name", tarFile.getFileName().toString());
                entry.put("size", fileSize);
                entry.put("sizeFormatted", FormatUtils.formatBytes(fileSize));
                entry.put("segmentCount", estimatedSegments);
                entry.put("estimatedCount", true);
                entry.put("created", attrs.creationTime().toString());
                entry.put("modified", attrs.lastModifiedTime().toString());
                tarEntries.add(entry);
            }
        } catch (Exception e) {
            log.warn("Error building tar inventory snapshot: {}", e.getMessage());
        }

        return tarEntries;
    }

    private long totalTarSizeBytes(List<Map<String, Object>> tarFiles) {
        long total = 0L;
        for (Map<String, Object> tarFile : tarFiles) {
            Object size = tarFile.get("size");
            if (size instanceof Number) {
                total += ((Number) size).longValue();
            }
        }
        return total;
    }

    private Map<String, Object> buildShardingPayload() {
        Map<String, Object> sharding = new HashMap<>();
        if (context == null || context.shardingRuntimeConfig == null) {
            sharding.put("enabled", false);
            sharding.put("localPrefixes", "none");
            sharding.put("remoteMountCount", 0);
            sharding.put("authoritativeStoreSeparated", false);
            return sharding;
        }

        sharding.put("enabled", context.shardingRuntimeConfig.isEnabled());
        sharding.put("localPrefixes", context.shardingRuntimeConfig.describeLocalRanges());
        sharding.put("remoteMountCount", context.shardingRuntimeConfig.expandRemoteReadOnlyMounts().size());
        sharding.put(
            "authoritativeStoreSeparated",
            context.authoritativeNodeStore != null && context.authoritativeNodeStore != context.nodeStore
        );
        return sharding;
    }

    private String buildOpsEnvelope(String contractVersion,
                                    long ttlMs,
                                    Object data,
                                    long sourceTimestampMs,
                                    long servedAtMs,
                                    long stalenessMs,
                                    boolean degraded,
                                    String degradedReason,
                                    boolean cacheHit) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("contractVersion", contractVersion);
        payload.put("sourceTimestampMs", sourceTimestampMs);
        payload.put("servedAtMs", servedAtMs);
        payload.put("stalenessMs", stalenessMs);
        payload.put("degraded", degraded);
        payload.put("degradedReason", degradedReason);
        Map<String, Object> cache = new HashMap<>();
        cache.put("hit", cacheHit);
        cache.put("ttlMs", ttlMs);
        payload.put("cache", cache);
        payload.put("data", data);
        return JsonOutputUtil.toJson(payload);
    }

    private String buildUnavailableOpsPayload(String contractVersion) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("contractVersion", contractVersion);
        payload.put("degraded", true);
        payload.put("degradedReason", "UPSTREAM_UNAVAILABLE");
        return JsonOutputUtil.toJson(payload);
    }

}
