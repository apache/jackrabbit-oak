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
package org.apache.jackrabbit.oak.segment.consensus.eth;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig;
import org.apache.jackrabbit.oak.segment.consensus.config.RuntimeConfigValueResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caches and polls Ethereum Beacon Chain epoch data.
 *
 * <p>Epoch fetching is delegated to a {@link FallbackBeaconChainProvider}.
 * Mock mode uses a local clock-backed provider. Chain-backed modes try
 * providers in priority order (local beacon node → beaconcha.in) with
 * per-provider circuit breakers (ADR 081 Track 6a).
 *
 * <p>The public API and the package-private test constructor are preserved from
 * the single-provider implementation.
 */
public class BeaconChainClient {

    private static final Logger log = LoggerFactory.getLogger(BeaconChainClient.class);

    private static final long EPOCH_DURATION_MS = 384_000L; // 32 slots × 12 s
    private static final long POLL_INTERVAL_MS  = 60_000L;  // 1 minute

    private final BlockchainConfig.Mode networkMode;
    private final FallbackBeaconChainProvider provider;

    // Cached epoch state
    private volatile long cachedFinalizedEpoch = -1;
    private volatile long cachedCurrentEpoch   = -1;
    private volatile long lastUpdateTime       = 0L;
    private volatile String lastError          = null;

    private volatile ScheduledExecutorService pollingExecutor;

    // ── constructors ─────────────────────────────────────────────────────────

    /**
     * Production constructor. Reads {@code BEACON_LOCAL_NODE_URL} /
     * {@code beacon.local.node.url} for an optional higher-priority local
     * beacon node in chain-backed modes. Mock mode never performs Beacon HTTP.
     *
     * @param beaconApiUrl ignored — mode-specific URL is derived internally;
     *                     parameter kept for binary compatibility with
     *                     {@code ConsensusServicesInitializer.BeaconChainClientFactory}
     */
    public BeaconChainClient(String beaconApiUrl) {
        this(BlockchainConfig.getInstance().getMode());
    }

    private BeaconChainClient(BlockchainConfig.Mode mode) {
        this(mode, buildDefaultProvider(mode));
    }

    /** Package-private: used by tests to inject a mock {@link HttpFetcher}. */
    BeaconChainClient(BlockchainConfig.Mode mode, HttpFetcher fetcher) {
        this(mode, mode == BlockchainConfig.Mode.MOCK
            ? buildMockProvider()
            : new FallbackBeaconChainProvider(
                new CircuitBreakingBeaconProvider(
                    new BeaconchainDotInProvider(modeToApiUrl(mode), fetcher),
                    Integer.MAX_VALUE, Long.MAX_VALUE))); // never open in tests
    }

    BeaconChainClient(BlockchainConfig.Mode mode, FallbackBeaconChainProvider provider) {
        this.networkMode = mode;
        this.provider    = provider;
        log.info("Beacon Chain Client: mode={}, providers={}", mode, provider.getProviderNames());
        updateCachedEpochs();
    }

    // ── polling lifecycle ─────────────────────────────────────────────────────

    public void startBackgroundPolling() {
        if (pollingExecutor != null) {
            log.warn("Background polling already started");
            return;
        }
        pollingExecutor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "beacon-chain-epoch-poller");
            t.setDaemon(true);
            return t;
        });
        pollingExecutor.scheduleAtFixedRate(() -> {
            try {
                updateCachedEpochs();
            } catch (Exception e) {
                log.error("Epoch update failed: {}", e.getMessage());
            }
        }, POLL_INTERVAL_MS, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);

        log.info("Epoch refresh scheduled (interval={}s, providers={}, externalNetworkPolling={})",
            POLL_INTERVAL_MS / 1000, provider.getProviderNames(), networkMode != BlockchainConfig.Mode.MOCK);
    }

    public void stopBackgroundPolling() {
        if (pollingExecutor != null) {
            pollingExecutor.shutdown();
            pollingExecutor = null;
            log.info("Beacon Chain polling stopped");
        }
    }

    // ── epoch refresh ─────────────────────────────────────────────────────────

    private void updateCachedEpochs() {
        try {
            long finalizedEpoch = provider.fetchFinalizedEpoch();
            if (finalizedEpoch < 0) {
                throw new RuntimeException("Provider returned -1");
            }

            if (cachedFinalizedEpoch > 0 && finalizedEpoch < cachedFinalizedEpoch) {
                log.warn("Epoch went backwards: {} -> {} (API glitch?)",
                    cachedFinalizedEpoch, finalizedEpoch);
            }
            if (finalizedEpoch != cachedFinalizedEpoch) {
                log.info("Epoch advanced: {} -> {} (current: {})",
                    cachedFinalizedEpoch, finalizedEpoch, finalizedEpoch + 2);
            }

            cachedFinalizedEpoch = finalizedEpoch;
            cachedCurrentEpoch   = finalizedEpoch + 2;
            lastUpdateTime       = System.currentTimeMillis();
            lastError            = null;
        } catch (Exception e) {
            String msg = e.getMessage();
            if (!String.valueOf(msg).equals(String.valueOf(lastError))) {
                log.error("Epoch fetch failed: {}", msg);
            }
            lastError = msg;
            if (cachedFinalizedEpoch < 0) {
                fallbackCalculateEpochs();
            }
        }
    }

    private void fallbackCalculateEpochs() {
        long genesisMs = (networkMode == BlockchainConfig.Mode.MAINNET)
            ? 1_606_824_023_000L   // mainnet genesis: Dec 1 2020
            : 1_655_733_600_000L;  // sepolia genesis: Jun 20 2022
        long current   = (System.currentTimeMillis() - genesisMs) / EPOCH_DURATION_MS;
        cachedFinalizedEpoch = Math.max(0L, current - 2);
        cachedCurrentEpoch   = current;
        lastUpdateTime       = System.currentTimeMillis();
        log.warn("Using calculated epoch fallback: finalized={}, current={}",
            cachedFinalizedEpoch, cachedCurrentEpoch);
    }

    // ── public accessors ──────────────────────────────────────────────────────

    public long getCachedFinalizedEpoch() { return cachedFinalizedEpoch; }
    public long getCachedCurrentEpoch()   { return cachedCurrentEpoch;   }
    public BlockchainConfig.Mode getNetworkMode() { return networkMode;  }
    public long getMillisSinceLastUpdate() { return System.currentTimeMillis() - lastUpdateTime; }
    public long getLastUpdateTime()        { return lastUpdateTime; }

    public boolean isEpochDataFresh() {
        return (System.currentTimeMillis() - lastUpdateTime) < 300_000L;
    }

    public void checkEpochFreshness() {
        if (!isEpochDataFresh()) {
            throw new IllegalStateException("Epoch data is stale!");
        }
    }

    public Map<String, Object> getHealthStatus() {
        Map<String, Object> h = new HashMap<>();
        h.put("mode",              networkMode.toString());
        h.put("providers",         provider.getProviderNames());
        h.put("fresh",             isEpochDataFresh());
        h.put("currentEpoch",      cachedCurrentEpoch);
        h.put("finalizedEpoch",    cachedFinalizedEpoch);
        h.put("lastUpdateTime",    lastUpdateTime);
        h.put("timeSinceUpdateMs", System.currentTimeMillis() - lastUpdateTime);
        h.put("lastError",         lastError);
        h.put("chainContext",      networkMode == BlockchainConfig.Mode.MAINNET ? "mainnet" : "sepolia");
        h.put("epochSource",       networkMode == BlockchainConfig.Mode.MOCK ? "local-clock" : "beacon-provider");
        h.put("externalNetworkPolling", networkMode != BlockchainConfig.Mode.MOCK);
        return h;
    }

    // ── legacy EpochData API ──────────────────────────────────────────────────

    @Deprecated
    public EpochData getLatestFinalizedEpoch() throws Exception {
        return getEpochDetails(cachedFinalizedEpoch);
    }

    public EpochData getEpochDetails(long epochNumber) {
        EpochData d = new EpochData();
        d.epochNumber         = epochNumber;
        d.timestamp           = System.currentTimeMillis();
        d.finalized           = (epochNumber <= cachedFinalizedEpoch);
        d.epochsBehindCurrent = (int) (cachedCurrentEpoch - epochNumber);
        d.blocksProposed      = 32;
        d.blocksSkipped       = 0;
        d.attestations        = 150;
        d.totalValidators     = 2_127_176L;
        d.activeValidators    = 2_126_153L;
        d.slashings           = 0;
        d.deposits            = 0;
        d.voluntaryExits      = 0;
        return d;
    }

    // ── stub compat (ADR 080 removed synthetic mock epoch control) ────────────

    public boolean setMockEpochOffset(long offset) {
        log.warn("Synthetic mock epoch control removed by ADR 080");
        return false;
    }

    public boolean advanceMockEpoch(int epochs) {
        log.warn("Synthetic mock epoch control removed by ADR 080");
        return false;
    }

    public long getMockEpochOffset() { return 0L; }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static String modeToApiUrl(BlockchainConfig.Mode mode) {
        return mode == BlockchainConfig.Mode.MAINNET
            ? BeaconchainDotInProvider.MAINNET_BASE
            : BeaconchainDotInProvider.SEPOLIA_BASE;
    }

    private static FallbackBeaconChainProvider buildDefaultProvider(BlockchainConfig.Mode mode) {
        if (mode == BlockchainConfig.Mode.MOCK) {
            return buildMockProvider();
        }
        return FallbackBeaconChainProvider.buildDefault(
            modeToApiUrl(mode),
            RuntimeConfigValueResolver.readString(
                "beacon.local.node.url", "BEACON_LOCAL_NODE_URL", ""));
    }

    private static FallbackBeaconChainProvider buildMockProvider() {
        return new FallbackBeaconChainProvider(new ClockBeaconChainProvider());
    }
}
