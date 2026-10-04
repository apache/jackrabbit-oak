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

import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.exceptions.ActiveDriverException;
import io.aeron.exceptions.DriverTimeoutException;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Launcher for Aeron Cluster consensus engine.
 * 
 * <p>This class handles the cluster launch pattern, configuring and starting
 * the Aeron Cluster components (MediaDriver, Archive, ConsensusModule, ClusteredServiceContainer).
 * 
 * <p>Based on proven Oak + Aeron Cluster patterns from reference implementations.
 */
public class AeronClusterLauncher {
    
    private static final Logger log = LoggerFactory.getLogger(AeronClusterLauncher.class);
    
    public static final int CLIENT_FACING_PORT_OFFSET = AeronClusterTopology.CLIENT_FACING_PORT_OFFSET;
    private static final int REPLICATION_PORT_OFFSET = 7;
    private static final int DEFAULT_CLUSTER_TERM_LENGTH_BYTES = 128 * 1024 * 1024; // 128MB
    private static final int DEFAULT_PUBLICATION_TERM_BUFFER_LENGTH_BYTES = 64 * 1024 * 1024; // 64MB
    private static final int DEFAULT_DRIVER_TIMEOUT_MS = 60000;
    private static final int DEFAULT_SOCKET_BUFFER_BYTES = 16 * 1024;
    private static final String SOCKET_SNDBUF_PROPERTY = "aeron.socket.so_sndbuf";
    private static final String SOCKET_RCVBUF_PROPERTY = "aeron.socket.so_rcvbuf";
    private static final String MEDIA_DRIVER_TIMEOUT_MS_PROPERTY = "oak.cluster.media.driver.timeout.ms";
    private static final String PUBLICATION_TERM_BUFFER_LENGTH_PROPERTY = "oak.cluster.publication.term.buffer.length.bytes";
    private static final String CLUSTER_TERM_LENGTH_PROPERTY = "oak.cluster.term.length.bytes";
    private static final int DEFAULT_SESSION_TIMEOUT_MINUTES = 20;
    private static final int DEV_SESSION_TIMEOUT_MINUTES = 2;
    private static final int STAGING_SESSION_TIMEOUT_MINUTES = 5;
    private static final String SESSION_TIMEOUT_MINUTES_PROPERTY = "oak.cluster.session.timeout.minutes";
    private static final String CLUSTER_ENVIRONMENT_PROPERTY = "oak.cluster.environment";
    private static final String CLUSTER_ENVIRONMENT_ENV = "OAK_CLUSTER_ENV";
    
    private final int nodeId;
    private final List<String> hostnames;
    private final File baseDir;
    private final ClusteredService clusteredService;
    private final AeronClusterAddressResolver addressResolver;
    private final AeronClusterErrorPolicy errorPolicy;
    private final LaunchInvoker launchInvoker;
    private final ContainerLaunchInvoker containerLaunchInvoker;
    
    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;
    private ShutdownSignalBarrier barrier;
    private String aeronDirectoryName;
    private CrashHandler crashHandler;
    private ExecutorService shutdownExecutor;
    private AtomicBoolean shutdownScheduled = new AtomicBoolean(false);
    private volatile Runnable shutdownCallback;
    private MediaDriverHealthMonitor healthMonitor;
    private AeronClusterFailureCoordinator failureCoordinator;
    
    public AeronClusterLauncher(int nodeId, List<String> hostnames, File baseDir, ClusteredService clusteredService) {
        this(
            nodeId,
            hostnames,
            baseDir,
            clusteredService,
            AeronClusterAddressResolver.system(nodeId, hostnames),
            new AeronClusterErrorPolicy(),
            LaunchInvoker.DEFAULT,
            ContainerLaunchInvoker.DEFAULT
        );
    }

    AeronClusterLauncher(int nodeId,
                         List<String> hostnames,
                         File baseDir,
                         ClusteredService clusteredService,
                         AeronClusterAddressResolver addressResolver,
                         AeronClusterErrorPolicy errorPolicy) {
        this(
            nodeId,
            hostnames,
            baseDir,
            clusteredService,
            addressResolver,
            errorPolicy,
            LaunchInvoker.DEFAULT,
            ContainerLaunchInvoker.DEFAULT
        );
    }

    AeronClusterLauncher(int nodeId,
                         List<String> hostnames,
                         File baseDir,
                         ClusteredService clusteredService,
                         AeronClusterAddressResolver addressResolver,
                         AeronClusterErrorPolicy errorPolicy,
                         LaunchInvoker launchInvoker) {
        this(
            nodeId,
            hostnames,
            baseDir,
            clusteredService,
            addressResolver,
            errorPolicy,
            launchInvoker,
            ContainerLaunchInvoker.DEFAULT
        );
    }

    AeronClusterLauncher(int nodeId,
                         List<String> hostnames,
                         File baseDir,
                         ClusteredService clusteredService,
                         AeronClusterAddressResolver addressResolver,
                         AeronClusterErrorPolicy errorPolicy,
                         LaunchInvoker launchInvoker,
                         ContainerLaunchInvoker containerLaunchInvoker) {
        this.nodeId = nodeId;
        this.hostnames = hostnames;
        this.baseDir = baseDir;
        this.clusteredService = clusteredService;
        this.addressResolver = addressResolver;
        this.errorPolicy = errorPolicy;
        this.launchInvoker = launchInvoker;
        this.containerLaunchInvoker = containerLaunchInvoker;
    }
    
    /**
     * Launch the Aeron Cluster.
     * 
     * <p>🌐 P2P-ORGANIC APPROACH: Nodes can start in any order.
     * 
     * <p>Aeron Cluster handles bootstrap automatically:
     * - If cluster directory is empty → starts fresh (can be any node, not just "genesis")
     * - If cluster directory has recordings → replays log and joins existing cluster
     * - Empty nodes will sync via Raft log replication from other cluster members
     * - Peers that aren't ready yet will be handled gracefully (DNS retry, connection retry)
     * 
     * <p>Key P2P features:
     * - No manual ordering required - any node can start first
     * - DNS resolution is resilient (aggressive retry with exponential backoff)
     * - Unavailable peers don't block startup - Aeron will connect when they're ready
     * - Quorum forms organically as nodes come online
     * 
     * @throws Exception if cluster launch fails
     */
    public void launch() throws Exception {
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("🚀 Launching Aeron Cluster");
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("   Node ID: {}", nodeId);
        log.info("   Hostname: {}", getHostname());
        log.info("   Base Dir: {}", baseDir.getAbsolutePath());
        log.info("   Cluster Members: {}", hostnames.size());
        
        // Check if this is a fresh start (no cluster state)
        File clusterDir = new File(baseDir, "cluster");
        boolean isFreshStart = !clusterDir.exists() || (clusterDir.exists() && clusterDir.listFiles() == null || clusterDir.listFiles().length == 0);
        
        if (isFreshStart) {
            log.info("   Bootstrap: 🌐 DYNAMIC CLUSTER MODE - Fresh start (no existing cluster state)");
            log.info("   → Starting with {} member(s) - quorum = {}", hostnames.size(), hostnames.size() == 1 ? "1 (self)" : "majority");
            if (hostnames.size() == 1) {
                log.info("   → Single node = quorum of 1 → will become leader immediately");
                log.info("   → Peers can join dynamically as they come online");
                log.info("   🛡️ RESILIENCE: Single node can operate independently (worst-case scenario)");
            } else {
                log.info("   → Multiple nodes configured - will form quorum organically as peers join");
            }
        } else {
            log.info("   Bootstrap: Existing cluster state found");
            log.info("   → Will replay log and join existing cluster");
            log.info("   → Cluster size: {} members", hostnames.size());
            log.info("   🛡️ RESILIENCE: If partition occurs, fragment can reform with available nodes");
        }
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        
        // 🌐 P2P-ORGANIC: Get our IP address FIRST (before resolving peers)
        // This ensures we use the correct validator-network IP, not client-network IP
        String myIPAddress = addressResolver.resolveLocalNodeAddress(getHostname());
        
        // Now resolve hostnames to IP addresses with resilient retry logic
        // Peers that aren't ready yet will use placeholder IPs - Aeron will retry DNS resolution
        // Using IPs avoids DNS caching issues when containers/nodes restart
        List<String> ipAddresses = addressResolver.resolveClusterMemberAddresses(myIPAddress);
        log.info("   Using IP addresses for Aeron Cluster channels (P2P-organic mode)");
        log.info("   My IP: {} (hostname: {})", myIPAddress, getHostname());
        log.info("   Note: Unavailable peers use placeholder IPs - Aeron will retry DNS when peers come online");
        
        this.aeronDirectoryName = CommonContext.getAeronDirectoryName() + "-" + nodeId + "-driver";
        String aeronDirName = this.aeronDirectoryName;
        barrier = new ShutdownSignalBarrier();
        
        // Initialize crash handler for error tracking and recovery
        crashHandler = new CrashHandler(baseDir, nodeId);
        shutdownExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "aeron-shutdown-handler");
            t.setDaemon(true);
            return t;
        });
        failureCoordinator = AeronClusterFailureCoordinator.system(
            crashHandler,
            shutdownExecutor,
            shutdownScheduled,
            this::performShutdown,
            () -> shutdownCallback
        );
        new AeronClusterStartupPreflight(nodeId, crashHandler).run(aeronDirName);
        
        // Media Driver Context
        // ✈️ AERON RESILIENCE: Enhanced configuration for stability and performance
        // Based on Aeron best practices for production systems:
        // - Larger term buffers reduce backpressure and improve throughput
        // - Sparse files reduce disk I/O for better performance
        // - Shared threading mode balances latency and resource usage
        // - Error handler provides graceful shutdown on FATAL errors
        // Socket buffer sizes (configurable via system properties)
        // Default: 16KB for Mac/Darwin (Aeron best practices)
        // Can be overridden: -Daeron.socket.so_sndbuf=32768 -Daeron.socket.so_rcvbuf=32768
        int socketSndbufLength = getPositiveIntProperty(SOCKET_SNDBUF_PROPERTY, DEFAULT_SOCKET_BUFFER_BYTES);
        int socketRcvbufLength = getPositiveIntProperty(SOCKET_RCVBUF_PROPERTY, DEFAULT_SOCKET_BUFFER_BYTES);
        int publicationTermBufferLength = getPositiveIntProperty(
            PUBLICATION_TERM_BUFFER_LENGTH_PROPERTY,
            DEFAULT_PUBLICATION_TERM_BUFFER_LENGTH_BYTES
        );
        int driverTimeoutMs = getPositiveIntProperty(
            MEDIA_DRIVER_TIMEOUT_MS_PROPERTY,
            DEFAULT_DRIVER_TIMEOUT_MS
        );
        int clusterTermLengthBytes = resolveClusterTermLengthBytes();
        log.info("📡 Socket buffer configuration: SO_SNDBUF={}KB, SO_RCVBUF={}KB", 
            socketSndbufLength / 1024, socketRcvbufLength / 1024);
        log.info(
            "✈️  Aeron transport config: publicationTermBuffer={}MB, clusterTermLength={}MB, driverTimeout={}ms",
            publicationTermBufferLength / (1024 * 1024),
            clusterTermLengthBytes / (1024 * 1024),
            driverTimeoutMs
        );
        
        // Consensus Module Context (use IP addresses for cluster members)
        // Note: Aeron Cluster automatically manages snapshot intervals based on log size
        // Snapshots are taken periodically by the leader to enable faster recovery
        // Default behavior: snapshot after significant log growth (typically ~1024 entries)
        log.info("📸 Aeron snapshot management: automatic (leader-controlled)");
        SessionTimeoutConfig sessionTimeoutConfig = resolveSessionTimeoutConfig();
        log.info(
            "⏱️  Aeron session timeout: {} minute(s) [source={}, env={}]",
            sessionTimeoutConfig.timeoutMinutes,
            sessionTimeoutConfig.source,
            sessionTimeoutConfig.environment
        );

        AeronClusterContextFactory.LaunchContexts contexts = AeronClusterContextFactory.create(
            nodeId,
            baseDir,
            clusteredService,
            aeronDirName,
            myIPAddress,
            ipAddresses,
            barrier,
            socketSndbufLength,
            socketRcvbufLength,
            publicationTermBufferLength,
            driverTimeoutMs,
            clusterTermLengthBytes,
            sessionTimeoutConfig,
            failureCoordinator.decorate(errorHandler("Media Driver")),
            failureCoordinator.decorate(errorHandler("Consensus Module")),
            failureCoordinator.decorate(errorHandler("Clustered Service"))
        );
        
        // Launch cluster
        clusteredMediaDriver = launchMediaDriver(contexts, aeronDirName);
        
        container = launchClusteredServiceContainer(contexts);
        
        AeronClusterRuntimeBridge.RuntimeBridgeResult runtimeBridgeResult =
            new AeronClusterRuntimeBridge(clusteredService).activate(container, aeronDirName, failureCoordinator);
        healthMonitor = runtimeBridgeResult.healthMonitor;
        
        log.info("✅ Aeron Cluster launched successfully");
        log.info("   Node {} started on {}", nodeId, getHostname());
    }
    
    /**
     * Set callback to be invoked on graceful shutdown.
     * This allows the parent process to handle shutdown (e.g., exit JVM, restart container).
     */
    public void setShutdownCallback(Runnable callback) {
        this.shutdownCallback = callback;
    }
    
    /**
     * Get Aeron instance from ClusteredServiceContainer.
     * Used for accessing CountersReader for metrics.
     */
    public io.aeron.Aeron getAeron() {
        if (container != null) {
            return container.context().aeron();
        }
        return null;
    }
    
    /**
     * Shutdown the Aeron Cluster gracefully.
     */
    public void shutdown() {
        boolean shutdownAccepted = failureCoordinator != null
            ? failureCoordinator.requestShutdown()
            : shutdownScheduled.compareAndSet(false, true);
        if (shutdownAccepted) {
            performShutdown();
        }
    }
    
    /**
     * Wait for shutdown signal (blocks until shutdown).
     */
    public void awaitShutdown() {
        if (barrier != null) {
            barrier.await();
        }
    }
    
    /**
     * Get the Aeron directory name used by this cluster node.
     * This is needed for creating Aeron clients that connect to the cluster.
     */
    public String getAeronDirectoryName() {
        return aeronDirectoryName;
    }
    
    /**
     * Get the cluster base port.
     */
    public static int getPortBase() {
        return AeronClusterTopology.getPortBase();
    }

    public int getClusterBasePort() {
        return getPortBase();
    }
    
    private String getHostname() {
        return hostnames.get(nodeId);
    }
    
    public static int calculatePort(int nodeId, int offset) {
        return AeronClusterTopology.calculatePort(nodeId, offset);
    }

    public static int calculatePort(int clusterBasePort, int nodeId, int offset) {
        return AeronClusterTopology.calculatePort(clusterBasePort, nodeId, offset);
    }
    
    /**
     * Error handler for Aeron Cluster components.
     * 
     * <p>🌐 P2P-ORGANIC: Filters out expected DNS resolution errors for unavailable peers.
     * These are normal in P2P startup and will resolve when peers come online.
     * 
     * <p>✈️ AERON CLUSTER: Filters out ClusterEvent warnings (heartbeat timeouts, etc.)
     * These are informational events from Aeron Cluster, not actual errors.
     */
    private ErrorHandler errorHandler(String context) {
        return errorPolicy.createHandler(context, log);
    }

    ClusteredMediaDriver launchMediaDriver(AeronClusterContextFactory.LaunchContexts contexts, String aeronDirName) {
        for (int attempt = 1; attempt <= 4; attempt++) {
            try {
                return launchInvoker.launch(contexts.freshCopy());
            } catch (DriverTimeoutException ex) {
                if (attempt == 4 || !deleteCncFileIfUninitialised(ex, aeronDirName)) {
                    throw ex;
                }
            } catch (ActiveDriverException ex) {
                if (attempt == 4 || !deleteDriverDirectoryIfActiveDriverDetected(ex, aeronDirName)) {
                    throw ex;
                }
            } catch (IllegalStateException ex) {
                if (attempt == 4 || !deleteMarkFileIfActiveMarkDetected(ex)) {
                    throw ex;
                }
            }
        }
        throw new IllegalStateException("Aeron ClusteredMediaDriver launch did not complete");
    }

    ClusteredServiceContainer launchClusteredServiceContainer(AeronClusterContextFactory.LaunchContexts contexts) {
        for (int attempt = 1; attempt <= 4; attempt++) {
            try {
                return containerLaunchInvoker.launch(contexts.freshCopy().clusteredServiceContext);
            } catch (IllegalStateException ex) {
                if (attempt == 4 || !deleteMarkFileIfActiveMarkDetected(ex)) {
                    throw ex;
                }
            }
        }
        throw new IllegalStateException("Aeron ClusteredServiceContainer launch did not complete");
    }
    
    /**
     * Get crash handler for external access (e.g., health checks).
     */
    public CrashHandler getCrashHandler() {
        return crashHandler;
    }
    
    /**
     * Get MediaDriver health monitor for external access (e.g., health checks, metrics).
     * 
     * @return Health monitor instance, or null if not initialized
     */
    public MediaDriverHealthMonitor getHealthMonitor() {
        return healthMonitor;
    }

    static SessionTimeoutConfig resolveSessionTimeoutConfig() {
        String explicit = System.getProperty(SESSION_TIMEOUT_MINUTES_PROPERTY);
        Integer explicitMinutes = parsePositiveInt(explicit);
        if (explicitMinutes != null) {
            return new SessionTimeoutConfig(
                explicitMinutes,
                TimeUnit.MINUTES.toNanos(explicitMinutes),
                "system-property",
                "override"
            );
        }

        String environment = firstNonBlank(
            System.getProperty(CLUSTER_ENVIRONMENT_PROPERTY),
            System.getenv(CLUSTER_ENVIRONMENT_ENV)
        );
        String normalized = environment == null ? "prod" : environment.trim().toLowerCase(Locale.ROOT);
        int minutes;
        if ("dev".equals(normalized) || "development".equals(normalized) || "local".equals(normalized) || "test".equals(normalized)) {
            minutes = DEV_SESSION_TIMEOUT_MINUTES;
        } else if ("staging".equals(normalized) || "stage".equals(normalized) || "preprod".equals(normalized)) {
            minutes = STAGING_SESSION_TIMEOUT_MINUTES;
        } else {
            minutes = DEFAULT_SESSION_TIMEOUT_MINUTES;
        }
        return new SessionTimeoutConfig(
            minutes,
            TimeUnit.MINUTES.toNanos(minutes),
            "environment-profile",
            normalized
        );
    }

    private static Integer parsePositiveInt(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed > 0 ? parsed : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int getPositiveIntProperty(String key, int defaultValue) {
        Integer configured = parsePositiveInt(System.getProperty(key));
        return configured != null ? configured : defaultValue;
    }

    private static int resolveClusterTermLengthBytes() {
        return getPositiveIntProperty(CLUSTER_TERM_LENGTH_PROPERTY, DEFAULT_CLUSTER_TERM_LENGTH_BYTES);
    }

    private static String firstNonBlank(String first, String second) {
        if (first != null && !first.trim().isEmpty()) {
            return first;
        }
        if (second != null && !second.trim().isEmpty()) {
            return second;
        }
        return null;
    }

    private void performShutdown() {
        new AeronClusterShutdownCoordinator(nodeId)
            .shutdown(healthMonitor, container, clusteredMediaDriver, barrier, shutdownExecutor);
    }

    static boolean deleteCncFileIfUninitialised(DriverTimeoutException ex, String aeronDirName) {
        String message = ex.getMessage();
        if (message == null || !message.contains("CnC file is created but not initialised")) {
            return false;
        }
        Path cncFile = Path.of(aeronDirName, "cnc.dat");
        log.warn(
            "Detected uninitialised Aeron CnC file at {}. Deleting and retrying MediaDriver launch...",
            cncFile
        );
        try {
            Files.deleteIfExists(cncFile);
        } catch (IOException ioEx) {
            log.warn(
                "Failed to delete Aeron CnC file at {}. Retrying MediaDriver launch anyway...",
                cncFile,
                ioEx
            );
        }
        return true;
    }

    static boolean deleteDriverDirectoryIfActiveDriverDetected(ActiveDriverException ex, String aeronDirName) {
        String message = ex.getMessage();
        if (message == null || !message.contains("ERROR - active driver detected")) {
            return false;
        }
        File aeronDir = new File(aeronDirName);
        log.warn(
            "Detected stale Aeron MediaDriver directory at {}. Deleting and retrying MediaDriver launch...",
            aeronDir.getAbsolutePath()
        );
        try {
            if (aeronDir.exists()) {
                IoUtil.delete(aeronDir, true);
            }
        } catch (Exception cleanupError) {
            log.warn(
                "Failed to delete Aeron MediaDriver directory at {}. Retrying MediaDriver launch anyway...",
                aeronDir.getAbsolutePath(),
                cleanupError
            );
        }
        return true;
    }

    static boolean deleteMarkFileIfActiveMarkDetected(IllegalStateException ex) {
        String message = ex.getMessage();
        String prefix = "active mark file detected:";
        if (message == null || !message.contains(prefix)) {
            return false;
        }

        String pathText = message.substring(message.indexOf(prefix) + prefix.length()).trim();
        if (pathText.isEmpty()) {
            return false;
        }

        Path markFile = Path.of(pathText);
        log.warn(
            "Detected stale Aeron mark file at {}. Deleting and retrying MediaDriver launch...",
            markFile
        );
        try {
            Files.deleteIfExists(markFile);
        } catch (IOException ioEx) {
            log.warn(
                "Failed to delete Aeron mark file at {}. Retrying MediaDriver launch anyway...",
                markFile,
                ioEx
            );
        }
        return true;
    }

    static final class SessionTimeoutConfig {
        final int timeoutMinutes;
        final long timeoutNs;
        final String source;
        final String environment;

        SessionTimeoutConfig(int timeoutMinutes, long timeoutNs, String source, String environment) {
            this.timeoutMinutes = timeoutMinutes;
            this.timeoutNs = timeoutNs;
            this.source = source;
            this.environment = environment;
        }
    }

    interface LaunchInvoker {
        LaunchInvoker DEFAULT = contexts -> ClusteredMediaDriver.launch(
            contexts.mediaDriverContext,
            contexts.archiveContext,
            contexts.consensusModuleContext
        );

        ClusteredMediaDriver launch(AeronClusterContextFactory.LaunchContexts contexts);
    }

    interface ContainerLaunchInvoker {
        ContainerLaunchInvoker DEFAULT = ClusteredServiceContainer::launch;

        ClusteredServiceContainer launch(ClusteredServiceContainer.Context context);
    }
}
