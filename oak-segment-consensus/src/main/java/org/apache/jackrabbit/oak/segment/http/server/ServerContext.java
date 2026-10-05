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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.consensus.util.WalletPathUtil;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.http.server.sse.EventBroadcaster;
import org.apache.jackrabbit.oak.segment.consensus.security.ProofVerifier;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCCostEstimator;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCProposalManager;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalQueueManagerOptimized;
import org.apache.jackrabbit.oak.segment.consensus.sharding.ShardingRuntimeConfig;
import org.apache.jackrabbit.oak.segment.consensus.fragmentation.FragmentationTracker;
import org.apache.jackrabbit.oak.segment.consensus.sharding.ShardRouter;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.segment.http.server.model.ClientRegistration;
import org.apache.jackrabbit.oak.segment.http.server.model.ValidatorRegistration;
import org.apache.jackrabbit.oak.segment.http.server.model.WriteMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared context for HTTP server handlers.
 * 
 * <p>Holds all shared dependencies and state that handlers need access to.
 * This avoids passing many individual parameters to each handler.</p>
 */
public class ServerContext {
    private static final Logger log = LoggerFactory.getLogger(ServerContext.class);
    
    public final FileStore fileStore;
    public final NodeStore nodeStore; // Composite/read-view store
    public volatile NodeStore authoritativeNodeStore; // Local writable store
    public final Path storeDirectory;
    public volatile AeronConsensusEngine aeronConsensusEngine;
    public volatile org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterLauncher aeronClusterLauncher;
    public volatile org.apache.jackrabbit.oak.segment.consensus.aeron.AeronPrometheusMetrics aeronPrometheusMetrics;
    public volatile ProofVerifier proofVerifier;
    public volatile String selfUrl;
    public volatile GCCostEstimator gcCostEstimator;
    public volatile GCProposalManager gcProposalManager;
    public volatile org.apache.jackrabbit.oak.segment.consensus.gc.GCAccountManager gcAccountManager;
    public volatile org.apache.jackrabbit.oak.segment.consensus.gc.PeriodicGCJob periodicGCJob;
    public volatile ProposalQueueManagerOptimized proposalQueueManager;
    public volatile FragmentationTracker fragmentationTracker;
    public volatile org.apache.jackrabbit.oak.segment.consensus.fragmentation.WalletStorageMetrics walletStorageMetrics;
    public volatile org.apache.jackrabbit.oak.segment.consensus.evm.EvmBridge evmBridge;
    public volatile ShardRouter shardRouter; // Optional - for sharded routing
    public volatile org.apache.jackrabbit.oak.segment.consensus.economics.ValidatorEarningsTracker validatorEarningsTracker;
    public volatile org.apache.jackrabbit.oak.segment.http.server.binary.UploadSessionManager uploadSessionManager; // ADR 020 lazy binary upload
    public volatile String blobStoreType = "default"; // file, ipfs, s3, azure
    public volatile org.apache.jackrabbit.oak.spi.blob.BlobStore blobStore; // For eager binary uploads
    public volatile org.apache.jackrabbit.oak.segment.http.server.binary.CidMappingService cidMappingService; // Oak ↔ IPFS CID mapping
    public volatile String validatorWalletAddress = "0x0000000000000000000000000000000000000000"; // This node's Ethereum address
    public volatile String clusterWalletAddress = "0x0000000000000000000000000000000000000000"; // Cluster payment wallet (ADR 046)
    public volatile EventBroadcaster eventBroadcaster; // SSE event broadcasting (ADR 036)
    public volatile ShardingRuntimeConfig shardingRuntimeConfig = ShardingRuntimeConfig.disabled();
    
    // API-level metrics (rejections before reaching queue)
    public final java.util.concurrent.atomic.AtomicLong apiRejectedRequests = new java.util.concurrent.atomic.AtomicLong(0);
    public final java.util.concurrent.atomic.AtomicLong apiAcceptedRequests = new java.util.concurrent.atomic.AtomicLong(0);
    public final java.util.concurrent.atomic.AtomicLong apiIpfsPolicyRejectAmbiguousSource = new java.util.concurrent.atomic.AtomicLong(0);
    public final java.util.concurrent.atomic.AtomicLong apiIpfsPolicyRejectNonEnterpriseCid = new java.util.concurrent.atomic.AtomicLong(0);
    public final java.util.concurrent.atomic.AtomicLong apiIpfsPolicyRejectUnknownCid = new java.util.concurrent.atomic.AtomicLong(0);
    public final java.util.concurrent.atomic.AtomicLong apiIpfsPolicyRejectCidServiceUnavailable = new java.util.concurrent.atomic.AtomicLong(0);
    public final java.util.concurrent.atomic.AtomicLong apiIpfsPolicyAcceptedEnterpriseCid = new java.util.concurrent.atomic.AtomicLong(0);
    
    // Shared state
    public final Map<String, ClientRegistration> registeredClients;
    private final DurableClientRegistrationStore durableClientRegistrationStore;
    public final Map<String, ValidatorRegistration> registeredValidators;
    public final Set<String> connectedPeers;
    public final Map<String, WriteMetadata> recentWriteMetadata;
    
    // Validator identity for rejoin
    public volatile String myValidatorId;
    public volatile String myValidatorUrl;
    public volatile java.util.List<String> myPeerUrls;
    
    // Genesis tracking for proof verification
    public volatile String genesisSegmentId;
    public volatile String genesisHash;
    public volatile long genesisTimestamp;
    
    public ServerContext(
            FileStore fileStore,
            NodeStore nodeStore,
            Path storeDirectory,
            String selfUrl) {
        this.fileStore = fileStore;
        this.nodeStore = nodeStore;
        this.authoritativeNodeStore = nodeStore;
        this.storeDirectory = storeDirectory;
        this.selfUrl = selfUrl;
        
        // Initialize shared state
        this.registeredClients = new ConcurrentHashMap<>();
        this.durableClientRegistrationStore = new DurableClientRegistrationStore(storeDirectory);
        this.registeredValidators = new ConcurrentHashMap<>();
        this.connectedPeers = java.util.concurrent.ConcurrentHashMap.newKeySet();
        this.recentWriteMetadata = new ConcurrentHashMap<>();
        loadDurableClientRegistrations();
    }
    
    // Setters for consensus engines (can be set after construction)
    public void setProofVerifier(ProofVerifier proofVerifier) {
        this.proofVerifier = proofVerifier;
    }
    
    public void setSelfUrl(String selfUrl) {
        this.selfUrl = selfUrl;
    }
    
    public void setAeronConsensusEngine(AeronConsensusEngine aeronConsensusEngine) {
        this.aeronConsensusEngine = aeronConsensusEngine;
    }
    
    public void setAeronClusterLauncher(org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterLauncher aeronClusterLauncher) {
        this.aeronClusterLauncher = aeronClusterLauncher;
        log.info("✅ ServerContext.aeronClusterLauncher field set");
        
        // Initialize Aeron Prometheus metrics if Aeron is available
        if (aeronClusterLauncher != null) {
            try {
                io.aeron.Aeron aeron = aeronClusterLauncher.getAeron();
                if (aeron != null) {
                    this.aeronPrometheusMetrics = new org.apache.jackrabbit.oak.segment.consensus.aeron.AeronPrometheusMetrics(aeron);
                    log.info("✅ Aeron Prometheus metrics initialized");
                } else {
                    log.debug("Aeron instance not yet available - metrics will be initialized later");
                }
            } catch (Exception e) {
                log.warn("Failed to initialize Aeron Prometheus metrics", e);
            }
        }
    }
    
    public void setAeronPrometheusMetrics(org.apache.jackrabbit.oak.segment.consensus.aeron.AeronPrometheusMetrics aeronPrometheusMetrics) {
        this.aeronPrometheusMetrics = aeronPrometheusMetrics;
    }
    
    public void setGCCostEstimator(GCCostEstimator gcCostEstimator) {
        this.gcCostEstimator = gcCostEstimator;
        log.info("✅ GC Cost Estimator initialized");
    }
    
    public void setProposalQueueManager(ProposalQueueManagerOptimized proposalQueueManager) {
        this.proposalQueueManager = proposalQueueManager;
        log.info("✅ Proposal Queue Manager initialized");
    }

    public void setAuthoritativeNodeStore(NodeStore authoritativeNodeStore) {
        this.authoritativeNodeStore = authoritativeNodeStore;
        log.info("✅ Authoritative NodeStore initialized");
    }

    public ClientRegistration findClientRegistrationByWallet(String walletAddress) {
        if (walletAddress == null || walletAddress.isBlank()) {
            return null;
        }

        String normalizedWallet = walletAddress.trim().toLowerCase();
        ClientRegistration existing = registeredClients.get(normalizedWallet);
        if (existing != null) {
            return existing;
        }

        for (ClientRegistration registration : registeredClients.values()) {
            if (registration != null
                    && registration.walletAddress != null
                    && normalizedWallet.equals(registration.walletAddress.toLowerCase())) {
                indexClientRegistration(registration);
                return registration;
            }
        }

        return recoverClientRegistrationFromWalletContent(normalizedWallet);
    }

    public ClientRegistration findClientRegistrationByClientId(String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return null;
        }
        return registeredClients.get(clientId);
    }

    public synchronized ClientRegistration registerClient(
            String clientId,
            String clientUrl,
            String walletAddress,
            String clientType) {
        String normalizedWallet = walletAddress.trim().toLowerCase();
        String resolvedClientId = clientId == null || clientId.isBlank() ? normalizedWallet : clientId;
        String resolvedClientUrl = clientUrl == null || clientUrl.isBlank() ? "wallet://" + normalizedWallet : clientUrl;

        ClientRegistration walletRegistration = registeredClients.get(normalizedWallet);
        ClientRegistration clientIdRegistration = resolvedClientId.equals(normalizedWallet)
            ? walletRegistration
            : registeredClients.get(resolvedClientId);
        ClientRegistration existing = walletRegistration != null ? walletRegistration : clientIdRegistration;

        if (clientIdRegistration != null
                && clientIdRegistration.walletAddress != null
                && !normalizedWallet.equals(clientIdRegistration.walletAddress.toLowerCase())) {
            throw new IllegalStateException(String.format(
                "Client %s already registered with wallet %s",
                resolvedClientId,
                clientIdRegistration.walletAddress
            ));
        }

        long registeredAt = existing != null ? existing.registeredAt : System.currentTimeMillis();
        ClientRegistration updated = ClientRegistration.restore(
            resolvedClientId,
            resolvedClientUrl,
            normalizedWallet,
            clientType,
            registeredAt,
            System.currentTimeMillis()
        );

        removeClientRegistrationAliases(existing);
        indexClientRegistration(updated);
        persistRegisteredClients();
        return updated;
    }

    public synchronized void touchClientRegistration(ClientRegistration registration) {
        if (registration == null) {
            return;
        }
        registration.updateLastSeen();
        indexClientRegistration(registration);
        persistRegisteredClients();
    }
    
    public void setFragmentationTracker(FragmentationTracker fragmentationTracker) {
        this.fragmentationTracker = fragmentationTracker;
        log.info("✅ Fragmentation Tracker initialized");
    }
    
    public void setWalletStorageMetrics(org.apache.jackrabbit.oak.segment.consensus.fragmentation.WalletStorageMetrics walletStorageMetrics) {
        this.walletStorageMetrics = walletStorageMetrics;
        log.info("✅ Wallet Storage Metrics initialized");
    }
    
    public void setGCProposalManager(GCProposalManager gcProposalManager) {
        this.gcProposalManager = gcProposalManager;
        log.info("✅ GC Proposal Manager initialized");
    }
    
    public void setShardRouter(ShardRouter shardRouter) {
        this.shardRouter = shardRouter;
        log.info("✅ Shard Router initialized");
    }
    
    public void setValidatorEarningsTracker(org.apache.jackrabbit.oak.segment.consensus.economics.ValidatorEarningsTracker validatorEarningsTracker) {
        this.validatorEarningsTracker = validatorEarningsTracker;
        log.info("✅ Validator Earnings Tracker initialized");
    }
    
    public void setUploadSessionManager(org.apache.jackrabbit.oak.segment.http.server.binary.UploadSessionManager uploadSessionManager) {
        this.uploadSessionManager = uploadSessionManager;
        log.info("✅ Upload Session Manager initialized (ADR 020 lazy binary upload)");
    }
    
    public void setEventBroadcaster(EventBroadcaster eventBroadcaster) {
        this.eventBroadcaster = eventBroadcaster;
        log.info("📡 Event Broadcaster initialized (ADR 036 SSE streaming)");
    }

    public void setShardingRuntimeConfig(ShardingRuntimeConfig shardingRuntimeConfig) {
        this.shardingRuntimeConfig = shardingRuntimeConfig != null ? shardingRuntimeConfig : ShardingRuntimeConfig.disabled();
        if (this.shardingRuntimeConfig.isEnabled()) {
            log.info("✅ Sharding runtime config initialized");
            log.info("   - Local prefixes: {}", this.shardingRuntimeConfig.describeLocalRanges());
            log.info("   - Remote routes: {}", this.shardingRuntimeConfig.describeRemoteRoutes());
        } else {
            log.info("ℹ️  Sharding runtime disabled");
        }
    }

    private void loadDurableClientRegistrations() {
        int loaded = 0;
        for (ClientRegistration registration : durableClientRegistrationStore.load()) {
            indexClientRegistration(registration);
            loaded++;
        }
        if (loaded > 0) {
            log.info("✅ Restored {} durable client registrations", loaded);
        }
    }

    private synchronized ClientRegistration recoverClientRegistrationFromWalletContent(String normalizedWallet) {
        ClientRegistration existing = registeredClients.get(normalizedWallet);
        if (existing != null) {
            return existing;
        }

        NodeStore lookupStore = authoritativeNodeStore != null ? authoritativeNodeStore : nodeStore;
        if (lookupStore == null) {
            return null;
        }

        NodeState walletRoot = findWalletRoot(lookupStore, normalizedWallet);
        if (walletRoot == null || !walletRoot.exists()) {
            return null;
        }

        long registeredAt = longProperty(walletRoot, "walletCreated", System.currentTimeMillis());
        long lastSeen = longProperty(walletRoot, "lastWrite", registeredAt);
        ClientRegistration recovered = ClientRegistration.restore(
            normalizedWallet,
            "wallet://" + normalizedWallet,
            normalizedWallet,
            ClientRegistration.CLIENT_TYPE_SUPPLY_CHAIN,
            registeredAt,
            lastSeen
        );
        indexClientRegistration(recovered);
        persistRegisteredClients();
        log.info("♻️  Recovered durable client registration from wallet content: {}", normalizedWallet);
        return recovered;
    }

    private static NodeState findWalletRoot(NodeStore nodeStore, String normalizedWallet) {
        try {
            String shardRoot = WalletPathUtil.getShardRoot(normalizedWallet);
            NodeState current = nodeStore.getRoot();
            if (current == null) {
                return null;
            }
            for (String part : shardRoot.substring(1).split("/")) {
                if (!current.hasChildNode(part)) {
                    return null;
                }
                current = current.getChildNode(part);
            }
            return current;
        } catch (Exception e) {
            return null;
        }
    }

    private static long longProperty(NodeState nodeState, String propertyName, long defaultValue) {
        PropertyState propertyState = nodeState.getProperty(propertyName);
        if (propertyState == null) {
            return defaultValue;
        }
        try {
            return propertyState.getValue(Type.LONG);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private synchronized void persistRegisteredClients() {
        Map<String, ClientRegistration> primaryRegistrations = new LinkedHashMap<>();
        for (ClientRegistration registration : registeredClients.values()) {
            if (registration == null || registration.walletAddress == null || registration.walletAddress.isBlank()) {
                continue;
            }
            primaryRegistrations.putIfAbsent(registration.walletAddress.toLowerCase(), registration);
        }
        durableClientRegistrationStore.save(primaryRegistrations.values());
    }

    private void indexClientRegistration(ClientRegistration registration) {
        if (registration == null || registration.walletAddress == null || registration.walletAddress.isBlank()) {
            return;
        }
        String normalizedWallet = registration.walletAddress.toLowerCase();
        registeredClients.put(normalizedWallet, registration);
        if (registration.clientId != null && !registration.clientId.isBlank() && !normalizedWallet.equals(registration.clientId)) {
            registeredClients.put(registration.clientId, registration);
        }
    }

    private void removeClientRegistrationAliases(ClientRegistration registration) {
        if (registration == null || registration.walletAddress == null || registration.walletAddress.isBlank()) {
            return;
        }
        String normalizedWallet = registration.walletAddress.toLowerCase();
        registeredClients.remove(normalizedWallet, registration);
        if (registration.clientId != null && !registration.clientId.isBlank() && !normalizedWallet.equals(registration.clientId)) {
            registeredClients.remove(registration.clientId, registration);
        }
    }
}
