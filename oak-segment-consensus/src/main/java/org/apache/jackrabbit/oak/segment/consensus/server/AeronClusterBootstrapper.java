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
import java.io.IOException;
import java.util.List;

import org.apache.jackrabbit.oak.segment.consensus.config.RuntimeConfigValueResolver;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterLauncher;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.consensus.security.EthereumWallet;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.server.SegmentHttpServer;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AeronClusterBootstrapper {

    private static final Logger log = LoggerFactory.getLogger(AeronClusterBootstrapper.class);

    private final FileStore fileStore;
    private final NodeStore nodeStore;
    private final SegmentHttpServer httpServer;
    private final EthereumWallet wallet;
    private final String storeDirectory;
    private final BlobStore blobStore;

    public AeronClusterBootstrapper(FileStore fileStore,
                                    NodeStore nodeStore,
                                    SegmentHttpServer httpServer,
                                    EthereumWallet wallet,
                                    String storeDirectory,
                                    BlobStore blobStore) {
        this.fileStore = fileStore;
        this.nodeStore = nodeStore;
        this.httpServer = httpServer;
        this.wallet = wallet;
        this.storeDirectory = storeDirectory;
        this.blobStore = blobStore;
    }

    public AeronClusterStartupResult startCluster(String selfUrl,
                                                  List<String> peerUrls,
                                                  boolean observeElections,
                                                  boolean logClusterStateDetails) throws IOException {
        // Get node ID from system property (default: 0)
        int nodeId = Integer.parseInt(System.getProperty("aeron.cluster.nodeId", "0"));

        // 🌐 DYNAMIC CLUSTER SIZE: Start with just self, discover peers organically
        String hostnamesConfig = RuntimeConfigValueResolver.readString("aeron.cluster.hostnames", "");
        AeronClusterBootstrapPlan bootstrapPlan = AeronClusterBootstrapPlan.create(
            nodeId,
            selfUrl,
            peerUrls,
            storeDirectory,
            hostnamesConfig
        );
        List<String> hostnamesList = bootstrapPlan.hostnames;
        boolean hasExistingCluster = bootstrapPlan.hasExistingCluster;

        if (logClusterStateDetails) {
            log.info("🔍 DEBUG: Cluster state check:");
            log.info("   - Cluster dir exists: {}", bootstrapPlan.clusterDirExists);
            log.info("   - Cluster dir path: {}", bootstrapPlan.clusterDir.getAbsolutePath());
            if (bootstrapPlan.clusterDirExists) {
                log.info("   - Cluster dir files: {}", bootstrapPlan.clusterDirFileCount);
            }
            log.info("   - hasExistingCluster: {}", hasExistingCluster);
        }

        if (bootstrapPlan.startupMode == AeronClusterBootstrapPlan.StartupMode.EXISTING_CLUSTER) {
            log.info("🌐 Existing cluster detected - will join with {} members", hostnamesList.size());
        } else if (bootstrapPlan.startupMode == AeronClusterBootstrapPlan.StartupMode.FRESH_CONFIGURED) {
            log.info("🌐 Fresh cluster start - using configured hostnames ({} members)", hostnamesList.size());
            if (logClusterStateDetails) {
                log.info("   -> Starting with self only (quorum = 1), peers will join dynamically");
            }
        } else {
            log.info("🌐 Fresh cluster start - starting with self only (quorum = 1)");
            if (logClusterStateDetails) {
                log.info("   -> Peers can join dynamically as they come online");
            }
        }

        AeronConsensusEngine aeronEngine =
            new AeronConsensusEngine(fileStore, nodeStore, selfUrl, peerUrls, wallet, storeDirectory, blobStore);

        // Initialize Ethereum integration if configured
        String beaconApiUrl = System.getProperty("ethereum.beacon.api.url", "https://beaconcha.in/api");
        aeronEngine.initializeEthereumIntegration(beaconApiUrl);

        // Create cluster base directory
        File clusterBaseDir = bootstrapPlan.clusterBaseDir;
        clusterBaseDir.mkdirs();

        // Build node ID to URL mapping for leader lookup
        aeronEngine.setNodeIdMapping(bootstrapPlan.nodeIdToUrl);

        new AeronClusterCallbackBinder().bind(aeronEngine, httpServer);

        // Wire Aeron engine to HTTP server context
        httpServer.setAeronConsensusEngine(aeronEngine);

        AeronClusterLauncher aeronClusterLauncher =
            new AeronClusterLaunchCoordinator().launch(nodeId, hostnamesList, clusterBaseDir, aeronEngine);

        if (observeElections && !hasExistingCluster && hostnamesList.size() >= 3) {
            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            log.info("🔄 STARTUP ELECTION OBSERVATION");
            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            log.info("Purpose: Observe elections for 15s to verify all {} nodes can participate",
                hostnamesList.size());
            log.info("         before performing critical genesis writes");

            try {
                new AeronClusterElectionObserver().observe(aeronEngine, 15000);
                log.info("✅ Election observation complete - cluster verified healthy");
                log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            } catch (Exception e) {
                log.warn("⚠️  Election observation failed: {}", e.getMessage());
                log.warn("   Proceeding with genesis, but cluster health uncertain");
            }
        }

        new AeronClusterRuntimeAttacher().attach(httpServer, aeronClusterLauncher);

        try {
            new ShardRouterInitializer().initialize(httpServer, selfUrl, peerUrls, logClusterStateDetails);
        } catch (Exception e) {
            log.warn("⚠️  WARNING: Failed to initialize Shard Router: {}", e.getMessage(), e);
            log.warn("   -> Shard routing disabled, requests will route directly");
        }

        return new AeronClusterStartupResult(aeronEngine, aeronClusterLauncher, hostnamesList, nodeId);
    }
}
