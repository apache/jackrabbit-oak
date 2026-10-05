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
package org.apache.jackrabbit.oak.segment.consensus.queue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Verified packing buffer for adaptive release.
 *
 * <p>Unlike the epoch queue, this buffer is time- and capacity-governed rather
 * than epoch-governed. It preserves wallet/path-aware packing, but releases
 * based on measured pressure rather than fixed Ethereum cadence.
 */
final class AdaptivePackingBuffer {

    private static final int OPTIMAL_BATCH_SIZE = 25;

    private final ConcurrentHashMap<String, List<QueuedProposal>> pendingWalletWrites = new ConcurrentHashMap<String, List<QueuedProposal>>();
    private final AtomicLong totalProposalsQueued = new AtomicLong();
    private final AtomicLong totalProposalsDrained = new AtomicLong();
    private final AtomicLong totalBatchesCreated = new AtomicLong();

    void addProposal(QueuedProposal proposal, long verifiedAtMs) {
        if (proposal == null) {
            return;
        }
        proposal.setVerifiedTimestampMs(verifiedAtMs);
        // Add inside compute so it cannot interleave with removeIfEmpty: a list fetched first and added to
        // after the drainer removed it from the map would hold the proposal where nothing drains it.
        pendingWalletWrites.compute(proposal.getWalletAddress(), (wallet, existing) -> {
            List<QueuedProposal> walletProposals = existing != null
                ? existing
                : Collections.synchronizedList(new ArrayList<QueuedProposal>());
            walletProposals.add(proposal);
            return walletProposals;
        });
        totalProposalsQueued.incrementAndGet();
    }

    /** Lock order is map entry, then list: never call this while holding a wallet list's monitor. */
    private void removeIfEmpty(String walletAddress) {
        pendingWalletWrites.computeIfPresent(walletAddress, (wallet, proposals) -> proposals.isEmpty() ? null : proposals);
    }

    List<List<QueuedProposal>> drainReadyBatches(long nowMs, AdaptiveReleaseGovernor.Decision decision) {
        DrainSettings settings = DrainSettings.forDecision(decision);
        List<WalletCandidate> candidates = new ArrayList<WalletCandidate>();

        for (Map.Entry<String, List<QueuedProposal>> entry : pendingWalletWrites.entrySet()) {
            String walletAddress = entry.getKey();
            List<QueuedProposal> proposals = entry.getValue();
            if (proposals == null) {
                continue;
            }

            long oldestVerifiedMs = Long.MAX_VALUE;
            synchronized (proposals) {
                for (QueuedProposal proposal : proposals) {
                    oldestVerifiedMs = Math.min(oldestVerifiedMs, proposal.getVerifiedTimestampMs());
                }
            }
            if (oldestVerifiedMs == Long.MAX_VALUE) {
                removeIfEmpty(walletAddress);
                continue;
            }

            long residencyMs = oldestVerifiedMs == Long.MAX_VALUE ? 0L : Math.max(0L, nowMs - oldestVerifiedMs);
            boolean forced = residencyMs >= settings.maxResidencyMs;
            boolean ready = forced || residencyMs >= settings.holdWindowMs;
            if (ready) {
                candidates.add(new WalletCandidate(walletAddress, proposals, oldestVerifiedMs, forced));
            }
        }

        candidates.sort(Comparator
            .comparing(WalletCandidate::isForced).reversed()
            .thenComparingLong(WalletCandidate::getOldestVerifiedMs)
            .thenComparing(WalletCandidate::getWalletAddress));

        List<List<QueuedProposal>> batches = new ArrayList<List<QueuedProposal>>();
        int walletsProcessed = 0;
        for (WalletCandidate candidate : candidates) {
            if (walletsProcessed >= settings.maxWalletsPerCycle) {
                break;
            }
            walletsProcessed++;
            drainWallet(candidate, settings.maxBatchesPerWallet, batches);
        }

        return batches;
    }

    Map<String, Object> getStatsMap() {
        Map<String, Object> stats = new HashMap<String, Object>();
        int pendingProposals = 0;
        for (List<QueuedProposal> proposals : pendingWalletWrites.values()) {
            synchronized (proposals) {
                pendingProposals += proposals.size();
            }
        }

        stats.put("walletCount", pendingWalletWrites.size());
        stats.put("pendingProposals", pendingProposals);
        stats.put("totalProposalsQueued", totalProposalsQueued.get());
        stats.put("totalProposalsDrained", totalProposalsDrained.get());
        stats.put("totalBatchesCreated", totalBatchesCreated.get());
        return stats;
    }

    String getStats() {
        Map<String, Object> stats = getStatsMap();
        return String.format(
            "Wallets: %d, Pending Proposals: %d, Total Queued: %d, Total Drained: %d, Batches Created: %d",
            stats.get("walletCount"),
            stats.get("pendingProposals"),
            stats.get("totalProposalsQueued"),
            stats.get("totalProposalsDrained"),
            stats.get("totalBatchesCreated")
        );
    }

    void clear() {
        pendingWalletWrites.clear();
        totalProposalsQueued.set(0L);
        totalProposalsDrained.set(0L);
        totalBatchesCreated.set(0L);
    }

    private void drainWallet(WalletCandidate candidate, int maxBatchesPerWallet, List<List<QueuedProposal>> batches) {
        List<QueuedProposal> proposals = candidate.getProposals();
        synchronized (proposals) {
            List<QueuedProposal> sorted = new ArrayList<QueuedProposal>(proposals);
            sorted.sort(Comparator
                .comparing(QueuedProposal::getPath)
                .thenComparingLong(QueuedProposal::getTimestamp));

            int releaseCount = Math.min(sorted.size(), maxBatchesPerWallet * OPTIMAL_BATCH_SIZE);
            List<QueuedProposal> toRelease = new ArrayList<QueuedProposal>(sorted.subList(0, releaseCount));
            proposals.removeAll(toRelease);

            for (int i = 0; i < toRelease.size(); i += OPTIMAL_BATCH_SIZE) {
                int endIndex = Math.min(i + OPTIMAL_BATCH_SIZE, toRelease.size());
                batches.add(new ArrayList<QueuedProposal>(toRelease.subList(i, endIndex)));
                totalBatchesCreated.incrementAndGet();
            }
            totalProposalsDrained.addAndGet(toRelease.size());
        }
        removeIfEmpty(candidate.getWalletAddress());
    }

    private static final class WalletCandidate {
        private final String walletAddress;
        private final List<QueuedProposal> proposals;
        private final long oldestVerifiedMs;
        private final boolean forced;

        private WalletCandidate(String walletAddress, List<QueuedProposal> proposals, long oldestVerifiedMs, boolean forced) {
            this.walletAddress = walletAddress;
            this.proposals = proposals;
            this.oldestVerifiedMs = oldestVerifiedMs;
            this.forced = forced;
        }

        String getWalletAddress() {
            return walletAddress;
        }

        List<QueuedProposal> getProposals() {
            return proposals;
        }

        long getOldestVerifiedMs() {
            return oldestVerifiedMs;
        }

        boolean isForced() {
            return forced;
        }
    }

    private static final class DrainSettings {
        private final long holdWindowMs;
        private final long maxResidencyMs;
        private final int maxWalletsPerCycle;
        private final int maxBatchesPerWallet;

        private DrainSettings(long holdWindowMs, long maxResidencyMs, int maxWalletsPerCycle, int maxBatchesPerWallet) {
            this.holdWindowMs = holdWindowMs;
            this.maxResidencyMs = maxResidencyMs;
            this.maxWalletsPerCycle = maxWalletsPerCycle;
            this.maxBatchesPerWallet = maxBatchesPerWallet;
        }

        static DrainSettings forDecision(AdaptiveReleaseGovernor.Decision decision) {
            AdaptiveReleaseGovernor.GovernorState state = decision != null
                ? decision.getState()
                : AdaptiveReleaseGovernor.GovernorState.HEALTHY;
            switch (state) {
                case OVERLOADED:
                    return new DrainSettings(500L, 2_000L, 2, 1);
                case PRESSURED:
                    return new DrainSettings(150L, 1_500L, 4, 1);
                case HEALTHY:
                default:
                    return new DrainSettings(25L, 750L, 8, 2);
            }
        }
    }
}
