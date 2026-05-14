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
package org.apache.jackrabbit.oak.benchmark;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import javax.jcr.Repository;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.segment.SegmentCache.SegmentCachePolicy;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * Benchmark measuring actual wall-clock elapsed time per segment cache policy using
 * real TAR file I/O.  Unlike {@link SegmentCacheMemoizationBenchmark}, which uses mock
 * segments (free TAR reads), cache misses here trigger actual disk reads — so a policy
 * with a higher miss rate is measurably slower.
 *
 * <h3>Fixture note</h3>
 * <p>The {@code RepositoryFixture} parameter only controls the JCR repository created by
 * {@code AbstractTest} infrastructure.  This benchmark creates its own {@link FileStore} in
 * {@link #beforeSuite()} and always reads real TAR files, regardless of which fixture is
 * passed.  Use {@code Oak-MemoryNS} to avoid wasting disk space on an unused second store.</p>
 *
 * <h3>Access path</h3>
 * <p>Every access calls {@link SegmentId#getSegment()}, which follows the full production
 * chain: L1 memoization → on L1 miss: store → L2 cache → on L2 miss: loader (disk read).
 * Stats decompose accesses into L1-hit%, L2-hit%, and TAR-read% (loader invocations).</p>
 *
 * <h3>Scenarios (all in {@code afterSuite})</h3>
 * <ul>
 *   <li><b>Scenario 1 (Zipfian steady-state)</b> — live run driven by the AbstractTest
 *       timing loop; isolated per-policy elapsed time with full tier breakdown.</li>
 *   <li><b>Scenario 2 (drifting active set)</b> — sliding Zipfian window; Caffeine's
 *       W-TinyLFU admission gate rejects new-window entries (freq=0) against incumbents,
 *       triggering perpetual TAR-read loops.  Caffeine is typically slower than Guava here.</li>
 *   <li><b>Scenario 3 (post-compaction cold-start)</b> — cache warmed on old-gen segments;
 *       traffic switches to new-gen (freq=0, LRU-cold).  Per-epoch TAR% tracks warm-up speed.</li>
 * </ul>
 */
public class SegmentCacheTarBenchmark extends AbstractTest {

    // ----- content generation -----
    private static final int N_NODES       = 4_000;
    private static final int N_BATCH       = 1_000;
    private static final int PROPERTY_KB   = 10;
    private static final int PROPERTY_BYTES = PROPERTY_KB * 1024;

    // ----- cache config: ~10 MB ≈ 40 data segments at 256 KB each -----
    private static final int CACHE_SIZE_MB = 10;

    // ----- Scenario 1: Zipfian steady-state -----
    private static final int    BATCH_SIZE   = Integer.getInteger("segment.batch.size", 500);
    private static final int    WARMUP_OPS   =  5_000;
    private static final int    MEASURE_OPS  = 50_000;
    private static final double ZIPF_EXP     = 1.0;

    // ----- Scenario 2: drifting active set -----
    private static final int    WIDTH_2      = 100;  // active window > cache capacity
    private static final int    DRIFT_2      = 5;    // advance cursor every N ops
    private static final double ZIPF_2_EXP  = 0.5;  // flatter → more entries compete for cache
    private static final int    WARMUP_2    = 20_000;
    private static final int    MEASURE_2   = 100_000;
    private static final int    EPOCH_OPS_2 = 10_000;

    // ----- Scenario 3: post-compaction cold-start -----
    private static final int WARMUP_3    = 20_000;  // warm on old-gen
    private static final int MEASURE_3   = 100_000;
    private static final int EPOCH_OPS_3 = 10_000;

    private static final SegmentCachePolicy[] POLICIES    = {
        SegmentCachePolicy.CAFFEINE,
        SegmentCachePolicy.CAFFEINE_WITH_EXPIRY,
        SegmentCachePolicy.LIRS,
        SegmentCachePolicy.GUAVA
    };
    private static final String[] POLICY_NAMES = {"CAFFEINE", "CAFFEINE_WITH_EXPIRY", "LIRS", "GUAVA"};
    private static final int      NUM_POLICIES  = POLICIES.length;

    // ----- live-run state -----
    private File           storeDir;
    private int            poolSize;
    private double[]       zipfCdf;
    private ReadOnlyFileStore[] liveStores;
    private SegmentId[][]  liveIds;        // liveIds[policy][segIdx]
    private long[]         liveTotalOps;   // per-policy access counter for statsValues()

    @Override
    public String toString() {
        return "SegmentCacheTarBenchmark";
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        return fixture.setUpCluster(1);
    }

    /**
     * Generates real TAR content, discovers the data-segment pool, and opens the
     * per-policy live stores for the AbstractTest timing loop.
     */
    @Override
    protected void beforeSuite() throws Exception {
        storeDir = Files.createTempDirectory("SegmentCacheTarBenchmark-").toFile();
        generateContent();
        poolSize = discoverPoolSize();
        zipfCdf  = buildZipfCdf(poolSize, ZIPF_EXP);
        liveTotalOps = new long[NUM_POLICIES];
        openLiveStores();
        System.out.printf(
                "%nSegmentCacheTarBenchmark setup complete:"
                        + " pool=%d data-segments  cache=%dMB  dir=%s%n"
                        + "  (fixture controls only the JCR repo; TAR reads always hit real disk)%n",
                poolSize, CACHE_SIZE_MB, storeDir);
    }

    /**
     * Writes {@value N_NODES} nodes with unique {@value PROPERTY_KB}KB string properties to
     * force creation of many on-heap data segments in the TAR store.
     */
    private void generateContent()
            throws IOException, InvalidFileStoreVersionException, CommitFailedException {
        char[] pad = new char[PROPERTY_BYTES - 20];
        Arrays.fill(pad, 'x');
        String padStr = new String(pad);
        try (FileStore fs = FileStoreBuilder.fileStoreBuilder(storeDir)
                .withSegmentCacheSize(CACHE_SIZE_MB).withMemoryMapping(false).build()) {
            var ns = SegmentNodeStoreBuilders.builder(fs).build();
            for (int start = 0; start < N_NODES; start += N_BATCH) {
                int end = Math.min(start + N_BATCH, N_NODES);
                NodeBuilder root = ns.getRoot().builder();
                for (int i = start; i < end; i++) {
                    root.child("n" + i).setProperty("v", padStr + String.format("%020d", i));
                }
                ns.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                fs.flush();
            }
        }
    }

    /** Counts data segments in the store to set the Zipfian pool size. */
    private int discoverPoolSize() throws IOException, InvalidFileStoreVersionException {
        try (ReadOnlyFileStore store = FileStoreBuilder.fileStoreBuilder(storeDir)
                .withSegmentCacheSize(1).withMemoryMapping(false).buildReadOnly()) {
            int count = 0;
            for (SegmentId id : store.getSegmentIds()) {
                if (id.isDataSegmentId()) count++;
            }
            return count;
        }
    }

    /** Opens one {@link ReadOnlyFileStore} per policy for the live timing loop. */
    private void openLiveStores() throws IOException, InvalidFileStoreVersionException {
        liveStores = new ReadOnlyFileStore[NUM_POLICIES];
        liveIds    = new SegmentId[NUM_POLICIES][];
        for (int p = 0; p < NUM_POLICIES; p++) {
            ReadOnlyFileStore store = openReadOnly(POLICIES[p]);
            liveStores[p] = store;
            liveIds[p]    = collectDataIds(store);
        }
    }

    /** Opens a fresh on-heap {@link ReadOnlyFileStore} with the given policy. */
    private ReadOnlyFileStore openReadOnly(SegmentCachePolicy policy)
            throws IOException, InvalidFileStoreVersionException {
        return FileStoreBuilder.fileStoreBuilder(storeDir)
                .withSegmentCacheSize(CACHE_SIZE_MB)
                .withSegmentCachePolicy(policy)
                .withMemoryMapping(false)
                .buildReadOnly();
    }

    /** Returns all data-segment IDs from {@code store} as an array. */
    private static SegmentId[] collectDataIds(ReadOnlyFileStore store) {
        List<SegmentId> ids = new ArrayList<>();
        for (SegmentId id : store.getSegmentIds()) {
            if (id.isDataSegmentId()) ids.add(id);
        }
        return ids.toArray(new SegmentId[0]);
    }

    // -----------------------------------------------------------------------
    // AbstractTest hook overrides
    // -----------------------------------------------------------------------

    /** Runs one Zipfian batch across all policies to drive the AbstractTest throughput counter. */
    @Override
    protected void runTest() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int n = liveIds[0].length;
        for (int i = 0; i < BATCH_SIZE; i++) {
            int idx = zipfSample(zipfCdf, rng.nextDouble()) % n;
            for (int p = 0; p < NUM_POLICIES; p++) {
                liveIds[p][idx].getSegment();
                liveTotalOps[p]++;
            }
        }
    }

    @Override
    protected String[] statsNames() {
        return new String[]{"  Caff_tar%", "  CaffEx_tar%", "  LIRS_tar%", "  Guav_tar%"};
    }

    @Override
    protected String[] statsFormats() {
        return new String[]{"  %10.1f", "  %10.1f", "  %10.1f", "  %10.1f"};
    }

    /** TAR-read% per policy (loader invocations / total accesses × 100). */
    @Override
    protected Object[] statsValues() {
        Object[] vals = new Object[NUM_POLICIES];
        for (int p = 0; p < NUM_POLICIES; p++) {
            long tar   = liveStores[p].getSegmentCacheStats().getMissCount();
            long total = liveTotalOps[p];
            vals[p] = total == 0 ? 0.0 : 100.0 * tar / total;
        }
        return vals;
    }

    /**
     * Prints live-run tier breakdown, then runs Scenarios 1–3 in isolation and prints
     * per-epoch TAR% tables plus total timing for Scenario 1.
     */
    @Override
    protected void afterSuite() throws Exception {
        System.out.printf(
                "%n--- SegmentCacheTarBenchmark: live run summary"
                        + " (all policies share I/O bandwidth) ---%n"
                        + "  pool=%d data-segs  cache=%dMB%n",
                poolSize, CACHE_SIZE_MB);
        for (int p = 0; p < NUM_POLICIES; p++) {
            CacheStatsMBean s = liveStores[p].getSegmentCacheStats();
            long total    = liveTotalOps[p];
            long l1Hits   = s.getHitCount();
            long tarReads = s.getMissCount();
            long l2Hits   = Math.max(0, total - l1Hits - tarReads);
            printResult(POLICY_NAMES[p], total, l1Hits, l2Hits, tarReads, -1);
        }
        for (ReadOnlyFileStore s : liveStores) {
            s.close();
        }

        runScenario1();
        runScenario2();
        runScenario3();

        FileUtils.deleteDirectory(storeDir);
    }

    // -----------------------------------------------------------------------
    // Scenario runners
    // -----------------------------------------------------------------------

    /**
     * Scenario 1: Zipfian steady-state — isolated per-policy elapsed time.
     * Caffeine is expected to have the lowest TAR-read% (W-TinyLFU vs LRU).
     */
    private void runScenario1() throws IOException, InvalidFileStoreVersionException {
        System.out.printf(
                "%n--- Scenario 1: Zipfian steady-state"
                        + " (warmup=%,d  measure=%,d  zipf=%.1f  cache=%dMB) ---%n"
                        + "  Caffeine W-TinyLFU should have fewest TAR reads; note later"
                        + " policies see warmer OS page cache.%n",
                WARMUP_OPS, MEASURE_OPS, ZIPF_EXP, CACHE_SIZE_MB);
        double[] cdf = buildZipfCdf(poolSize, ZIPF_EXP);
        for (int p = 0; p < NUM_POLICIES; p++) {
            try (ReadOnlyFileStore store = openReadOnly(POLICIES[p])) {
                SegmentId[] ids = collectDataIds(store);
                int n = ids.length;
                ThreadLocalRandom rng = ThreadLocalRandom.current();
                for (int i = 0; i < WARMUP_OPS; i++) {
                    ids[zipfSample(cdf, rng.nextDouble()) % n].getSegment();
                }
                long h0 = store.getSegmentCacheStats().getHitCount();
                long m0 = store.getSegmentCacheStats().getMissCount();
                long t0 = System.currentTimeMillis();
                for (int i = 0; i < MEASURE_OPS; i++) {
                    ids[zipfSample(cdf, rng.nextDouble()) % n].getSegment();
                }
                long elapsed  = System.currentTimeMillis() - t0;
                long l1Hits   = store.getSegmentCacheStats().getHitCount()  - h0;
                long tarReads = store.getSegmentCacheStats().getMissCount()  - m0;
                long l2Hits   = Math.max(0, MEASURE_OPS - l1Hits - tarReads);
                printResult(POLICY_NAMES[p], MEASURE_OPS, l1Hits, l2Hits, tarReads, elapsed);
            }
        }
    }

    /**
     * Scenario 2: drifting active set.  A sliding Zipfian window forces continuous cache
     * churn.  Caffeine's admission gate rejects new-window entries (freq=0) against
     * incumbents, triggering L1-clear loops and more TAR reads than Guava.
     */
    private void runScenario2() throws IOException, InvalidFileStoreVersionException {
        int width = Math.min(WIDTH_2, poolSize - 1);
        System.out.printf(
                "%n--- Scenario 2: drifting active set"
                        + " (pool=%d  width=%d  drift=%d  zipf=%.1f"
                        + "  warmup=%,d  measure=%,d  epoch=%,d) ---%n"
                        + "  Caffeine admission gate rejects new-window entries (freq=0)"
                        + " → L1-clear loop → more TAR reads than Guava.%n",
                poolSize, width, DRIFT_2, ZIPF_2_EXP, WARMUP_2, MEASURE_2, EPOCH_OPS_2);
        int numEpochs = MEASURE_2 / EPOCH_OPS_2;
        long[][][] epochs = new long[NUM_POLICIES][numEpochs][];
        long[][]   totals = new long[NUM_POLICIES][];
        for (int p = 0; p < NUM_POLICIES; p++) {
            try (ReadOnlyFileStore store = openReadOnly(POLICIES[p])) {
                SegmentId[] ids = collectDataIds(store);
                epochs[p] = new long[numEpochs][];
                totals[p] = runDriftingEpochs(store, ids, width, epochs[p]);
            }
        }
        printEpochTable(epochs, EPOCH_OPS_2, "tar%");
        for (int p = 0; p < NUM_POLICIES; p++) {
            printResult(POLICY_NAMES[p], totals[p][0], totals[p][1], totals[p][2], totals[p][3], -1);
        }
    }

    /**
     * Scenario 3: post-compaction cold-start.  Cache is warmed on old-gen segments;
     * all traffic then switches to new-gen (freq=0 / LRU-cold).  Per-epoch TAR%
     * shows how quickly each policy warms up.
     */
    private void runScenario3() throws IOException, InvalidFileStoreVersionException {
        int oldGen = poolSize / 2;
        int newGen = poolSize - oldGen;
        System.out.printf(
                "%n--- Scenario 3: post-compaction cold-start"
                        + " (old-gen=%d  new-gen=%d  warmup=%,d  measure=%,d  epoch=%,d) ---%n"
                        + "  new-gen has freq=0 / LRU-cold; Caffeine may reject entries initially.%n",
                oldGen, newGen, WARMUP_3, MEASURE_3, EPOCH_OPS_3);
        int numEpochs = MEASURE_3 / EPOCH_OPS_3;
        long[][][] epochs = new long[NUM_POLICIES][numEpochs][];
        long[][]   totals = new long[NUM_POLICIES][];
        for (int p = 0; p < NUM_POLICIES; p++) {
            try (ReadOnlyFileStore store = openReadOnly(POLICIES[p])) {
                SegmentId[] ids = collectDataIds(store);
                epochs[p] = new long[numEpochs][];
                totals[p] = runCompactionEpochs(store, ids, oldGen, epochs[p]);
            }
        }
        printEpochTable(epochs, EPOCH_OPS_3, "tar%");
        for (int p = 0; p < NUM_POLICIES; p++) {
            printResult(POLICY_NAMES[p], totals[p][0], totals[p][1], totals[p][2], totals[p][3], -1);
        }
    }

    // -----------------------------------------------------------------------
    // Epoch-based runners (one policy at a time)
    // -----------------------------------------------------------------------

    /**
     * Runs the drifting-window scenario for one policy.
     * Warms the cache, then measures {@value EPOCH_OPS_2} ops per epoch.
     *
     * @param store      freshly opened store for this policy
     * @param pool       all data-segment IDs from the store
     * @param width      sliding window width
     * @param epochStats receives per-epoch [total, l1Hits, l2Hits, tarReads] arrays
     * @return aggregate [total, l1Hits, l2Hits, tarReads] across all epochs
     */
    private static long[] runDriftingEpochs(ReadOnlyFileStore store, SegmentId[] pool,
                                             int width, long[][] epochStats) {
        double[] cdf = buildZipfCdf(width, ZIPF_2_EXP);
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int n = pool.length;
        int cursor = 0;
        int opCount = 0;

        for (int i = 0; i < WARMUP_2; i++) {
            if (opCount % DRIFT_2 == 0) cursor = (cursor + 1) % n;
            pool[(cursor + zipfSample(cdf, rng.nextDouble())) % n].getSegment();
            opCount++;
        }

        long h0 = store.getSegmentCacheStats().getHitCount();
        long m0 = store.getSegmentCacheStats().getMissCount();
        long totTotal = 0, totL1 = 0, totL2 = 0, totTar = 0;
        for (int epoch = 0; epoch < epochStats.length; epoch++) {
            for (int i = 0; i < EPOCH_OPS_2; i++) {
                if (opCount % DRIFT_2 == 0) cursor = (cursor + 1) % n;
                pool[(cursor + zipfSample(cdf, rng.nextDouble())) % n].getSegment();
                opCount++;
            }
            long l1  = store.getSegmentCacheStats().getHitCount()  - h0;
            long tar = store.getSegmentCacheStats().getMissCount()  - m0;
            long l2  = Math.max(0, EPOCH_OPS_2 - l1 - tar);
            epochStats[epoch] = new long[]{EPOCH_OPS_2, l1, l2, tar};
            totTotal += EPOCH_OPS_2; totL1 += l1; totL2 += l2; totTar += tar;
            h0 = store.getSegmentCacheStats().getHitCount();
            m0 = store.getSegmentCacheStats().getMissCount();
        }
        return new long[]{totTotal, totL1, totL2, totTar};
    }

    /**
     * Runs the post-compaction cold-start scenario for one policy.
     * Warms on old-gen then measures access to new-gen only, epoch by epoch.
     *
     * @param store      freshly opened store for this policy
     * @param pool       all data-segment IDs; first {@code oldGen} = old-gen
     * @param oldGen     split index: [0, oldGen) = old-gen, [oldGen, pool.length) = new-gen
     * @param epochStats receives per-epoch [total, l1Hits, l2Hits, tarReads] arrays
     * @return aggregate [total, l1Hits, l2Hits, tarReads] across all epochs
     */
    private static long[] runCompactionEpochs(ReadOnlyFileStore store, SegmentId[] pool,
                                               int oldGen, long[][] epochStats) {
        int newGen = pool.length - oldGen;
        double[] oldCdf = buildZipfCdf(oldGen, ZIPF_EXP);
        double[] newCdf = buildZipfCdf(newGen, ZIPF_EXP);
        ThreadLocalRandom rng = ThreadLocalRandom.current();

        for (int i = 0; i < WARMUP_3; i++) {
            pool[zipfSample(oldCdf, rng.nextDouble())].getSegment();
        }

        long h0 = store.getSegmentCacheStats().getHitCount();
        long m0 = store.getSegmentCacheStats().getMissCount();
        long totTotal = 0, totL1 = 0, totL2 = 0, totTar = 0;
        for (int epoch = 0; epoch < epochStats.length; epoch++) {
            for (int i = 0; i < EPOCH_OPS_3; i++) {
                pool[oldGen + zipfSample(newCdf, rng.nextDouble()) % newGen].getSegment();
            }
            long l1  = store.getSegmentCacheStats().getHitCount()  - h0;
            long tar = store.getSegmentCacheStats().getMissCount()  - m0;
            long l2  = Math.max(0, EPOCH_OPS_3 - l1 - tar);
            epochStats[epoch] = new long[]{EPOCH_OPS_3, l1, l2, tar};
            totTotal += EPOCH_OPS_3; totL1 += l1; totL2 += l2; totTar += tar;
            h0 = store.getSegmentCacheStats().getHitCount();
            m0 = store.getSegmentCacheStats().getMissCount();
        }
        return new long[]{totTotal, totL1, totL2, totTar};
    }

    // -----------------------------------------------------------------------
    // Reporting helpers
    // -----------------------------------------------------------------------

    /**
     * Prints a per-epoch table with one column per policy.
     *
     * @param policyEpochs  [policy][epoch] = [total, l1Hits, l2Hits, tarReads]
     * @param epochOps      ops per epoch (denominator for percentages)
     * @param metric        column header suffix, e.g. "tar%"
     */
    private static void printEpochTable(long[][][] policyEpochs, int epochOps, String metric) {
        System.out.printf("  %8s", "ops");
        for (String name : POLICY_NAMES) {
            System.out.printf("  %22s", name + "_" + metric);
        }
        System.out.println();
        int numEpochs = policyEpochs[0].length;
        for (int e = 0; e < numEpochs; e++) {
            System.out.printf("  %8d", (long)(e + 1) * epochOps);
            for (int p = 0; p < NUM_POLICIES; p++) {
                long[] ep = policyEpochs[p][e];
                long tar = ep[3];
                System.out.printf("  %22.1f", pct(tar, ep[0]));
            }
            System.out.println();
        }
    }

    /**
     * Prints one result row: policy name, L1/L2/TAR tier breakdown, optional elapsed time.
     *
     * @param label     policy display name
     * @param total     total accesses in the window
     * @param l1Hits    served from SegmentId memoization field — no L2 call made
     * @param l2Hits    found in L2 — no loader/disk read (mainly LIRS HIR hits)
     * @param tarReads  loader invocations — actual disk-read equivalents
     * @param elapsedMs wall-clock ms, or -1 to omit timing columns
     */
    private static void printResult(String label, long total, long l1Hits,
                                    long l2Hits, long tarReads, long elapsedMs) {
        double l1Pct  = pct(l1Hits,  total);
        double l2Pct  = pct(l2Hits,  total);
        double tarPct = pct(tarReads, total);
        if (elapsedMs < 0) {
            System.out.printf(
                    "  %-22s  l1%%=%5.1f  l2%%=%5.1f  tar%%=%5.1f"
                            + "  (total=%,d  l1=%,d  l2=%,d  tar=%,d)%n",
                    label, l1Pct, l2Pct, tarPct, total, l1Hits, l2Hits, tarReads);
        } else {
            double opsPerSec = elapsedMs == 0 ? Double.MAX_VALUE : 1000.0 * total / elapsedMs;
            System.out.printf(
                    "  %-22s  elapsed=%,6d ms  ops/sec=%,9.0f"
                            + "  l1%%=%5.1f  l2%%=%5.1f  tar%%=%5.1f  (tar=%,d)%n",
                    label, elapsedMs, opsPerSec, l1Pct, l2Pct, tarPct, tarReads);
        }
    }

    private static double pct(long num, long denom) {
        return denom == 0 ? 0.0 : 100.0 * num / denom;
    }

    // -----------------------------------------------------------------------
    // Zipfian helpers
    // -----------------------------------------------------------------------

    private static double[] buildZipfCdf(int n, double exponent) {
        double[] cdf = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            sum += 1.0 / Math.pow(i + 1, exponent);
            cdf[i] = sum;
        }
        for (int i = 0; i < n; i++) {
            cdf[i] /= sum;
        }
        return cdf;
    }

    private static int zipfSample(double[] cdf, double u) {
        int lo = 0, hi = cdf.length - 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (cdf[mid] < u) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }
}
