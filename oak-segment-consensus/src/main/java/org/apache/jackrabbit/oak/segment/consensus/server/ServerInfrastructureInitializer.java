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
package org.apache.jackrabbit.oak.segment.consensus.server;

import java.io.File;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.function.Supplier;

import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterConfig;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.consensus.config.RuntimeConfigValueResolver;
import org.apache.jackrabbit.oak.segment.consensus.config.StorageBackendConfig;
import org.apache.jackrabbit.oak.segment.consensus.fragmentation.FragmentationTracker;
import org.apache.jackrabbit.oak.segment.consensus.fragmentation.WalletStorageMetrics;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCAccountManager;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCCostEstimator;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCProposalManager;
import org.apache.jackrabbit.oak.segment.consensus.gc.PeriodicGCJob;
import org.apache.jackrabbit.oak.segment.consensus.sharding.ShardingRuntimeConfig;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.http.server.SegmentHttpServer;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.segment.http.server.binary.CidMappingService;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ServerInfrastructureInitializer {

    private static final Logger log = LoggerFactory.getLogger(ServerInfrastructureInitializer.class);
    private final BlobStoreStartupCoordinator blobStoreStartupCoordinator = new BlobStoreStartupCoordinator();

    InitializationResult initialize(File storeDir,
                                    int port,
                                    AeronClusterConfig aeronConfig,
                                    GlobalStoreServerComponentFactory componentFactory)
            throws IOException, InvalidFileStoreVersionException {
        StorageBackendConfig storageConfig = StorageBackendConfig.load();
        log.info("Storage: segment={}, blob={}", storageConfig.getSegmentBackend(), storageConfig.getBlobBackend());

        BlobStoreStartupCoordinator.StartupResult blobStoreStartup =
            blobStoreStartupCoordinator.initialize(storeDir, storageConfig, componentFactory);
        BlobStore blobStore = blobStoreStartup.getBlobStore();
        String blobStoreType = blobStoreStartup.getBlobStoreType();

        ServerStorageRuntime storageRuntime = StorageBackendFactory.createStorageRuntime(storeDir, blobStore, storageConfig);
        FileStore fileStore = storageRuntime.getFileStore();
        NodeStore authoritativeNodeStore = storageRuntime.getAuthoritativeNodeStore();
        NodeStore readViewNodeStore = storageRuntime.getReadViewNodeStore();
        Closeable readViewResources = storageRuntime.getReadViewResources();

        log.info("✅ Oak FileStore initialized");
        log.info("   - Store version: {}", fileStore.getHead().getRecordId());
        log.info("   - Segments: {}", storeDir.getAbsolutePath());

        GCCostEstimator gcCostEstimator = initializeGCCostEstimator(fileStore, componentFactory);
        SegmentHttpServer httpServer = initializeHttpServer(storeDir, port, aeronConfig, componentFactory,
            blobStore, blobStoreType, fileStore, readViewNodeStore, authoritativeNodeStore, gcCostEstimator);
        FragmentationTracker fragmentationTracker = initializeFragmentationTracker(httpServer, componentFactory);
        initializeWalletStorageMetrics(httpServer, fileStore, componentFactory);
        initializeGcConsensusSupport(httpServer, aeronConfig, componentFactory, fileStore, gcCostEstimator,
            fragmentationTracker);

        log.info("✅ HTTP server initialized (not yet started)");
        return new InitializationResult(blobStoreType, blobStore, fileStore, authoritativeNodeStore, readViewNodeStore,
            readViewResources, httpServer, gcCostEstimator);
    }

    private GCCostEstimator initializeGCCostEstimator(FileStore fileStore,
                                                      GlobalStoreServerComponentFactory componentFactory) {
        log.info("Initializing GC Cost Estimator...");
        try {
            BigDecimal usdcPerMB = new BigDecimal(RuntimeConfigValueResolver.readString("gc.usdc.per.mb", "0.10"));
            GCCostEstimator gcCostEstimator = componentFactory.createGCCostEstimator(
                fileStore,
                componentFactory.extractTarFiles(fileStore),
                usdcPerMB
            );

            log.info("✅ GC Cost Estimator initialized");
            log.info("   - USDC rate: ${} per MB", usdcPerMB);
            return gcCostEstimator;
        } catch (Exception e) {
            log.warn("⚠️  Failed to initialize GC Cost Estimator: {}", e.getMessage());
            log.warn("   GC cost estimation will not be available");
            return null;
        }
    }

    private SegmentHttpServer initializeHttpServer(File storeDir,
                                                   int port,
                                                   AeronClusterConfig aeronConfig,
                                                   GlobalStoreServerComponentFactory componentFactory,
                                                   BlobStore blobStore,
                                                   String blobStoreType,
                                                   FileStore fileStore,
                                                   NodeStore readViewNodeStore,
                                                   NodeStore authoritativeNodeStore,
                                                   GCCostEstimator gcCostEstimator) {
        log.info("Initializing HTTP server on port {}...", port);
        SegmentHttpServer httpServer = componentFactory.createHttpServer(storeDir, port, fileStore, readViewNodeStore);
        ServerContext context = httpServer.getContext();
        String selfUrl = GlobalStoreRuntimeConfigUtil.resolveSelfUrl(port, aeronConfig);

        if (GlobalStoreRuntimeConfigUtil.isConfiguredSelfUrl(aeronConfig)) {
            log.info("   Using configured self URL: {}", selfUrl);
        } else {
            log.info("   Resolved self URL to IP: {}", selfUrl);
        }

        httpServer.setSelfUrl(selfUrl);
        context.setAuthoritativeNodeStore(authoritativeNodeStore);
        context.setShardingRuntimeConfig(ShardingRuntimeConfig.load());
        if (gcCostEstimator != null) {
            context.setGCCostEstimator(gcCostEstimator);
        }

        context.blobStoreType = blobStoreType;
        context.blobStore = blobStore;

        if ("ipfs".equalsIgnoreCase(blobStoreType)) {
            initializeCidMappingService(storeDir, componentFactory, context);
        }

        return httpServer;
    }

    private void initializeCidMappingService(File storeDir,
                                             GlobalStoreServerComponentFactory componentFactory,
                                             ServerContext context) {
        log.info("Initializing CID Mapping Service...");
        try {
            CidMappingService cidMappingService = componentFactory.createCidMappingService(storeDir.toPath());
            context.cidMappingService = cidMappingService;
            log.info("✅ CID Mapping Service initialized");
            log.info("   - Maps Oak blob IDs ↔ IPFS CIDs");
            log.info("   - Persistence: {}/cid-mappings.properties", storeDir.getAbsolutePath());
            log.info("   - API: /api/cid/{oakBlobId} -> IPFS CID lookup");
        } catch (Exception e) {
            log.warn("⚠️  Failed to initialize CID Mapping Service: {}", e.getMessage());
        }
    }

    private FragmentationTracker initializeFragmentationTracker(SegmentHttpServer httpServer,
                                                                GlobalStoreServerComponentFactory componentFactory) {
        log.info("Initializing Fragmentation Tracker...");
        try {
            FragmentationTracker fragmentationTracker = componentFactory.createFragmentationTracker();
            httpServer.getContext().setFragmentationTracker(fragmentationTracker);
            log.info("✅ Fragmentation Tracker initialized");
            log.info("   - Tracks TAR file creation per entity");
            log.info("   - Calculates fragmentation scores and taxes");
            return fragmentationTracker;
        } catch (Exception e) {
            log.warn("⚠️  Failed to initialize Fragmentation Tracker: {}", e.getMessage());
            log.warn("   Fragmentation tracking will not be available");
            return null;
        }
    }

    private void initializeWalletStorageMetrics(SegmentHttpServer httpServer,
                                                FileStore fileStore,
                                                GlobalStoreServerComponentFactory componentFactory) {
        log.info("Initializing Wallet Storage Metrics...");
        try {
            WalletStorageMetrics walletStorageMetrics = componentFactory.createWalletStorageMetrics(fileStore);
            httpServer.getContext().setWalletStorageMetrics(walletStorageMetrics);
            log.info("✅ Wallet Storage Metrics initialized");
            log.info("   - Tracks per-wallet storage ownership %");
            log.info("   - Calculates storage tax and delete tax");
            log.info("   - Monitors capacity (2 TB upper bound)");
        } catch (Exception e) {
            log.warn("⚠️  Failed to initialize Wallet Storage Metrics: {}", e.getMessage());
            log.warn("   Storage metrics will not be available");
        }
    }

    private void initializeGcConsensusSupport(SegmentHttpServer httpServer,
                                              AeronClusterConfig aeronConfig,
                                              GlobalStoreServerComponentFactory componentFactory,
                                              FileStore fileStore,
                                              GCCostEstimator gcCostEstimator,
                                              FragmentationTracker fragmentationTracker) {
        log.info("Initializing GC Proposal Manager...");
        try {
            ServerContext context = httpServer.getContext();
            List<String> configuredPeers = GlobalStoreRuntimeConfigUtil.resolvePeerUrls(aeronConfig);
            int totalValidators = configuredPeers.isEmpty() ? 1 : configuredPeers.size() + 1;
            Supplier<Integer> executorIdSupplier = createExecutorIdSupplier(context);
            Supplier<Boolean> isLeaderSupplier = createLeaderSupplier(context);

            GCProposalManager gcProposalManager = componentFactory.createGCProposalManager(
                fileStore,
                gcCostEstimator,
                fragmentationTracker,
                context.evmBridge,
                totalValidators,
                executorIdSupplier,
                isLeaderSupplier
            );

            gcProposalManager.setClusterMembership(() -> {
                AeronConsensusEngine aeronEngine = context.aeronConsensusEngine;
                return aeronEngine != null ? aeronEngine.getTotalMemberCount() : totalValidators;
            });
            gcProposalManager.setExecutionRequester(proposalId -> {
                AeronConsensusEngine aeronEngine = context.aeronConsensusEngine;
                return aeronEngine != null && aeronEngine.sendGCExecuteThroughIngress(proposalId, executorIdSupplier.get());
            });
            context.setGCProposalManager(gcProposalManager);

            log.info("✅ GC Proposal Manager initialized");
            log.info("   - Total validators: {}", totalValidators);
            log.info("   - Quorum required: {}/{}", ((totalValidators * 2 / 3) + 1), totalValidators);
            log.info("   - Tracks GC proposals, voting, and execution");

            GCAccountManager gcAccountManager = componentFactory.createGCAccountManager();
            context.gcAccountManager = gcAccountManager;

            log.info("✅ GC Account Manager initialized");
            log.info("   - Tracks GC debt per entity (wallet address)");
            log.info("   - Default debt limit: $100.00");
            log.info("   - Enforces write blocking when debt exceeds limit");

            PeriodicGCJob periodicGCJob = componentFactory.createPeriodicGCJob(gcAccountManager);
            periodicGCJob.start();
            context.periodicGCJob = periodicGCJob;

            log.info("✅ Periodic GC Job started");
            log.info("   - Interval: {}s", periodicGCJob.getIntervalSeconds());
            log.info("   - Initial delay: {}s", periodicGCJob.getInitialDelaySeconds());
            log.info("   - Action: Converts pending debt -> executed debt");
            log.info("   - Blocks writes when executed debt > limit");
        } catch (Exception e) {
            log.warn("⚠️  Failed to initialize GC Proposal Manager: {}", e.getMessage());
            log.warn("   GC consensus will not be available");
        }
    }

    private Supplier<Integer> createExecutorIdSupplier(ServerContext context) {
        return () -> {
            AeronConsensusEngine aeronEngine = context.aeronConsensusEngine;
            if (aeronEngine != null && aeronEngine.getCluster() != null) {
                try {
                    return aeronEngine.getCluster().memberId();
                } catch (Exception e) {
                    // Fall back to the standalone default below.
                }
            }
            return 0;
        };
    }

    private Supplier<Boolean> createLeaderSupplier(ServerContext context) {
        return () -> {
            AeronConsensusEngine aeronEngine = context.aeronConsensusEngine;
            if (aeronEngine != null) {
                return aeronEngine.isLeader();
            }
            return true;
        };
    }

    static final class InitializationResult {
        private final String blobStoreType;
        private final BlobStore blobStore;
        private final FileStore fileStore;
        private final NodeStore nodeStore;
        private final NodeStore readViewNodeStore;
        private final Closeable readViewResources;
        private final SegmentHttpServer httpServer;
        private final GCCostEstimator gcCostEstimator;

        InitializationResult(String blobStoreType,
                             BlobStore blobStore,
                             FileStore fileStore,
                             NodeStore nodeStore,
                             NodeStore readViewNodeStore,
                             Closeable readViewResources,
                             SegmentHttpServer httpServer,
                             GCCostEstimator gcCostEstimator) {
            this.blobStoreType = blobStoreType;
            this.blobStore = blobStore;
            this.fileStore = fileStore;
            this.nodeStore = nodeStore;
            this.readViewNodeStore = readViewNodeStore;
            this.readViewResources = readViewResources;
            this.httpServer = httpServer;
            this.gcCostEstimator = gcCostEstimator;
        }

        String getBlobStoreType() {
            return blobStoreType;
        }

        BlobStore getBlobStore() {
            return blobStore;
        }

        FileStore getFileStore() {
            return fileStore;
        }

        NodeStore getNodeStore() {
            return nodeStore;
        }

        NodeStore getReadViewNodeStore() {
            return readViewNodeStore;
        }

        Closeable getReadViewResources() {
            return readViewResources;
        }

        SegmentHttpServer getHttpServer() {
            return httpServer;
        }

        GCCostEstimator getGcCostEstimator() {
            return gcCostEstimator;
        }
    }
}
