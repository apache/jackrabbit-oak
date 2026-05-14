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

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import javax.jcr.Repository;

import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.SegmentCache.SegmentCachePolicy;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;

/**
 * Benchmark measuring TAR-read counts under each cache policy using the full
 * production access path: {@link SegmentId#getSegment()} (L1) → {@link SegmentStore#readSegment}
 * → {@link SegmentCache#getSegment} (L2) → loader (TAR read).
 *
 * <h3>Why this differs from {@link SegmentCachePolicyBenchmark}</h3>
 *
 * <p>{@link SegmentCachePolicyBenchmark} calls {@code SegmentCache.getSegment()} directly on
 * every access, so Caffeine updates its frequency sketch and Guava refreshes its LRU position
 * on every call — including calls that in production would be L1 hits served from
 * {@link SegmentId#segment} without touching L2.  This makes both caches appear better
 * than they actually are.</p>
 *
 * <p>In production, hot segments are served from the {@link SegmentId} memoization field
 * (L1) without entering L2.  Sketch frequencies and LRU positions only advance on L2 misses
 * (real TAR reads).  Over time these counts go stale; when an entry is evicted and re-loaded,
 * Caffeine's admission gate may reject it (stale count ≤ victim count), firing the eviction
 * listener and clearing L1 again — creating a perpetual TAR-read loop invisible to benchmarks
 * that bypass L1.</p>
 *
 * <h3>Scenarios</h3>
 * <ul>
 *   <li><b>Scenario 1 (live run)</b>: Zipfian steady-state; reported per-iteration during
 *       the AbstractTest timing loop via {@code statsValues()}.</li>
 *   <li><b>Scenario 2</b>: post-compaction cold-start.  Cache warmed with old-gen; all
 *       traffic switches to new-gen (freq=0/LRU-cold). Per-epoch TAR-read% tracks warm-up.</li>
 *   <li><b>Scenario 3</b>: drifting active set.  Sliding Zipfian window reveals how long
 *       the L1-staleness loop sustains itself as the working set continuously shifts.</li>
 * </ul>
 */
public class SegmentCacheMemoizationBenchmark extends AbstractTest {

    // ----- cache sizing: avg ~130 KB/segment; 130 MB ≈ 1 000 entries -----
    private static final int CACHE_SIZE_MB = 130;
    private static final int MIN_SEG_KB = 4;
    private static final int MAX_SEG_KB = 256;
    private static final long RANDOM_SEED = 42L;
    private static final double ZIPF_EXPONENT = 1.0;

    // ----- Scenario 1 (live run): Zipfian steady-state -----
    private static final int POOL_1 = 10_000;
    private static final int BATCH_SIZE = Integer.getInteger("segment.batch.size", 1_000);

    // ----- Scenario 2: post-compaction cold-start -----
    private static final int OLD_GEN_2 = 5_000;
    private static final int NEW_GEN_2 = 5_000;
    private static final int WARMUP_2 = 10_000;
    private static final int MEASURE_2 = 200_000;
    private static final int EPOCH_OPS_2 = 10_000;

    // ----- Scenario 3: drifting active set -----
    private static final int POOL_3 = 20_000;
    private static final int WIDTH_3 = 1_500;
    private static final int DRIFT_3 = 5;
    private static final double ZIPF_3_EXP = 0.5;
    private static final int WARMUP_3 = 50_000;
    private static final int MEASURE_3 = 200_000;
    private static final int EPOCH_OPS_3 = 10_000;

    private static final long DATA_SEG_LSB_MASK = 0xa000000000000000L;

    private static final SegmentCachePolicy[] POLICIES = {
        SegmentCachePolicy.CAFFEINE,
        SegmentCachePolicy.CAFFEINE_WITH_EXPIRY,
        SegmentCachePolicy.LIRS,
        SegmentCachePolicy.GUAVA
    };
    private static final String[] POLICY_NAMES = {"CAFFEINE", "CAFFEINE_WITH_EXPIRY", "LIRS", "GUAVA"};
    private static final int NUM_POLICIES = POLICIES.length;

    // ----- live Scenario 1 state (used by runTest / statsValues) -----
    private double[] liveCdf;
    private Random liveRng;
    private CacheSetup[] liveSetups;

    @Override
    public String toString() {
        return "SegmentCacheMemoizationBenchmark";
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        return fixture.setUpCluster(1);
    }

    /**
     * Initialises the live Scenario 1 caches used by {@link #runTest()}.
     */
    @Override
    protected void beforeSuite() {
        liveCdf = buildZipfCdf(POOL_1, ZIPF_EXPONENT);
        liveRng = new Random(RANDOM_SEED);
        liveSetups = new CacheSetup[NUM_POLICIES];
        for (int p = 0; p < NUM_POLICIES; p++) {
            liveSetups[p] = freshSetup(POLICIES[p], POOL_1);
        }
    }

    /**
     * Runs one Zipfian batch against all policies; paces the AbstractTest timing loop
     * and feeds the live TAR-read% columns reported by {@link #statsValues()}.
     */
    @Override
    protected void runTest() {
        for (int i = 0; i < BATCH_SIZE; i++) {
            int segIdx = zipfSample(liveCdf, liveRng.nextDouble());
            for (int p = 0; p < NUM_POLICIES; p++) {
                liveSetups[p].access(segIdx);
            }
        }
    }

    /** Column headers for the AbstractTest output row. */
    @Override
    protected String[] statsNames() {
        return new String[]{"  Caff_tar%", "  CaffEx_tar%", "  LIRS_tar%", "  Guav_tar%"};
    }

    /** Format strings for the per-policy TAR-read% columns. */
    @Override
    protected String[] statsFormats() {
        return new String[]{"  %10.1f", "  %10.1f", "  %10.1f", "  %10.1f"};
    }

    /** Current running TAR-read% for each policy from the live Scenario 1 run. */
    @Override
    protected Object[] statsValues() {
        Object[] vals = new Object[NUM_POLICIES];
        for (int p = 0; p < NUM_POLICIES; p++) {
            long tar = liveSetups[p].store.tarReads.get();
            long total = liveSetups[p].store.totalAccesses.get();
            vals[p] = total == 0 ? 0.0 : 100.0 * tar / total;
        }
        return vals;
    }

    /**
     * Runs Scenarios 2 and 3 after the timed loop and prints a detailed report.
     */
    @Override
    protected void afterSuite() {
        int avgWeight = 32 + (MIN_SEG_KB + MAX_SEG_KB) / 2 * 1024;
        int cacheCapacity = (int) ((long) CACHE_SIZE_MB * 1024 * 1024 / avgWeight);
        System.out.printf(
                "%nSegmentCacheMemoizationBenchmark  cacheCapacity~=%d%n"
                        + "  TAR reads = loader invocations (actual disk equivalents);"
                        + " L1 hits bypass L2 entirely.%n%n",
                cacheCapacity);

        System.out.println("--- Scenario 1: Zipfian steady-state (live run — see timed output above) ---");
        for (int p = 0; p < NUM_POLICIES; p++) {
            long[] snap = liveSetups[p].snapshotAndReset();
            printResult(POLICY_NAMES[p], snap[0], snap[1], snap[2], snap[3]);
        }

        runScenario2();
        runScenario3();
    }

    // -----------------------------------------------------------------------
    // Scenario runners
    // -----------------------------------------------------------------------

    private void runScenario2() {
        System.out.printf(
                "%n--- Scenario 2: post-compaction cold-start"
                        + " (old-gen=%,d  new-gen=%,d  warmup=%,d  measure=%,d  epoch=%,d ops) ---%n",
                OLD_GEN_2, NEW_GEN_2, WARMUP_2, MEASURE_2, EPOCH_OPS_2);
        System.out.println(
                "  Caffeine rejection fires id.unloaded() → L1 cold → next access is also a TAR read.");
        long[][][] epochs = new long[NUM_POLICIES][][];
        long[][] totals = new long[NUM_POLICIES][];
        for (int p = 0; p < NUM_POLICIES; p++) {
            List<long[]> epochList = new ArrayList<>();
            CacheSetup setup = freshSetup(POLICIES[p], OLD_GEN_2 + NEW_GEN_2);
            totals[p] = runCompactionColdStart(setup, epochList);
            epochs[p] = epochList.toArray(new long[0][]);
        }
        printEpochTable(epochs, epochs[0].length, EPOCH_OPS_2);
        for (int p = 0; p < NUM_POLICIES; p++) {
            printResult(POLICY_NAMES[p], totals[p][0], totals[p][1], totals[p][2], totals[p][3]);
        }
    }

    private void runScenario3() {
        System.out.printf(
                "%n--- Scenario 3: drifting active set"
                        + " (pool=%,d  width=%,d  drift=%d  warmup=%,d"
                        + "  measure=%,d  epoch=%,d  zipf=%.1f) ---%n",
                POOL_3, WIDTH_3, DRIFT_3, WARMUP_3, MEASURE_3, EPOCH_OPS_3, ZIPF_3_EXP);
        System.out.println(
                "  stale sketch/LRU from L1 hits → eviction → rejection loop under working-set churn.");
        long[][][] epochs = new long[NUM_POLICIES][][];
        long[][] totals = new long[NUM_POLICIES][];
        for (int p = 0; p < NUM_POLICIES; p++) {
            List<long[]> epochList = new ArrayList<>();
            CacheSetup setup = freshSetup(POLICIES[p], POOL_3);
            totals[p] = runDriftingWindow(setup, epochList);
            epochs[p] = epochList.toArray(new long[0][]);
        }
        printEpochTable(epochs, epochs[0].length, EPOCH_OPS_3);
        for (int p = 0; p < NUM_POLICIES; p++) {
            printResult(POLICY_NAMES[p], totals[p][0], totals[p][1], totals[p][2], totals[p][3]);
        }
    }

    // -----------------------------------------------------------------------
    // CacheSetup — production-faithful L1 → store → L2 → loader path
    // -----------------------------------------------------------------------

    /**
     * Holds a cache and SegmentIds wired through {@link InstrumentedStore} so that
     * every {@link #access} call follows the production path:
     * {@code id.getSegment()} → L1 check → on miss: {@code store.readSegment()} →
     * {@link SegmentCache#getSegment} → on L2 miss: loader (TAR read).
     */
    private static final class CacheSetup {
        final SegmentCache cache;
        final SegmentId[] ids;
        final InstrumentedStore store;
        private long evictionBaseline = 0;

        CacheSetup(SegmentCache cache, SegmentId[] ids, InstrumentedStore store) {
            this.cache = cache;
            this.ids = ids;
            this.store = store;
        }

        /** One production-faithful access: L1 check → store → L2 → loader. */
        void access(int idx) {
            store.totalAccesses.incrementAndGet();
            ids[idx].getSegment();
        }

        /**
         * Returns [total, l1Hits, tarReads, evictionsDelta] for the window since the
         * last call, then resets the counters.  Evictions are computed as a delta so
         * repeated calls give per-epoch (not cumulative) values.
         */
        long[] snapshotAndReset() {
            long total    = store.totalAccesses.getAndSet(0);
            long l1Hits   = store.l1Hits.getAndSet(0);
            long tarReads = store.tarReads.getAndSet(0);
            cache.cleanUp();
            long currentEvictions = cache.getCacheStats().getEvictionCount();
            long evictionsDelta   = currentEvictions - evictionBaseline;
            evictionBaseline      = currentEvictions;
            return new long[]{total, l1Hits, tarReads, evictionsDelta};
        }
    }

    /**
     * Minimal {@link SegmentStore} that bridges L1 misses to the {@link SegmentCache},
     * counting TAR reads (loader invocations), L1 hits (via {@code onAccess}), and
     * total accesses.
     */
    private static final class InstrumentedStore implements SegmentStore {

        private final SegmentCache cache;
        private final Map<SegmentId, Segment> segMap;

        final AtomicLong totalAccesses = new AtomicLong();
        final AtomicLong l1Hits        = new AtomicLong();
        final AtomicLong tarReads      = new AtomicLong();

        InstrumentedStore(SegmentCache cache, Map<SegmentId, Segment> segMap) {
            this.cache  = cache;
            this.segMap = segMap;
        }

        /**
         * Called by {@link SegmentId#getSegment()} on L1 miss; bridges to the L2
         * cache.  The loader increments {@link #tarReads} only on an L2 miss.
         */
        @Override
        @NotNull
        public Segment readSegment(@NotNull SegmentId id) {
            Segment mock = segMap.get(id);
            if (mock == null) {
                throw new IllegalStateException("Unknown segment: " + id);
            }
            try {
                return cache.getSegment(id, () -> {
                    tarReads.incrementAndGet();
                    return mock;
                });
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean containsSegment(@NotNull SegmentId id) {
            return segMap.containsKey(id);
        }

        @Override
        public void writeSegment(@NotNull SegmentId id, byte[] bytes, int offset, int length)
        throws IOException {
            throw new UnsupportedOperationException("benchmark store is read-only");
        }
    }

    /**
     * Builds a fresh {@link CacheSetup} with {@code n} mock segments.  Each
     * {@link SegmentId} is wired to the {@link InstrumentedStore} so that
     * {@code id.getSegment()} exercises the full L1 → store → L2 → loader chain.
     * The {@code onAccess} callback counts L1 hits.
     *
     * @param policy the eviction policy to use
     * @param n      number of distinct segments in the pool
     */
    private static CacheSetup freshSetup(SegmentCachePolicy policy, int n) {
        SegmentCache cache = SegmentCache.newSegmentCache(CACHE_SIZE_MB, policy);
        SegmentId[] ids = new SegmentId[n];
        Segment[]   segs = new Segment[n];
        Map<SegmentId, Segment> segMap = new IdentityHashMap<>(n * 2);
        Random r = new Random(RANDOM_SEED);

        // Create mock segments (sizes only; id refs set after SegmentId creation)
        for (int i = 0; i < n; i++) {
            int memUsage = MIN_SEG_KB * 1024 + r.nextInt((MAX_SEG_KB - MIN_SEG_KB) * 1024);
            segs[i] = Mockito.mock(Segment.class);
            Mockito.when(segs[i].estimateMemoryUsage()).thenReturn(memUsage);
        }

        InstrumentedStore store = new InstrumentedStore(cache, segMap);

        for (int i = 0; i < n; i++) {
            UUID uuid = UUID.randomUUID();
            long msb = uuid.getMostSignificantBits();
            long lsb  = (uuid.getLeastSignificantBits() & 0x0fffffffffffffffL) | DATA_SEG_LSB_MASK;
            // onAccess fires on L1 hit — increment the L1-hit counter
            ids[i] = new SegmentId(store, msb, lsb, store.l1Hits::incrementAndGet);
            Mockito.when(segs[i].getSegmentId()).thenReturn(ids[i]);
            segMap.put(ids[i], segs[i]);
        }

        return new CacheSetup(cache, ids, store);
    }

    // -----------------------------------------------------------------------
    // Scenario implementations
    // -----------------------------------------------------------------------

    /**
     * Scenario 2: post-compaction cold-start.  Warms the cache with old-gen segments
     * then measures access to new-gen segments only, per epoch.
     *
     * @param epochStats receives per-epoch [total, l1Hits, tarReads, evictions]
     * @return aggregate [total, l1Hits, tarReads, evictions] across all epochs
     */
    private static long[] runCompactionColdStart(CacheSetup setup, List<long[]> epochStats) {
        double[] oldCdf = buildZipfCdf(OLD_GEN_2, ZIPF_EXPONENT);
        double[] newCdf = buildZipfCdf(NEW_GEN_2, ZIPF_EXPONENT);
        Random r = new Random(RANDOM_SEED);

        for (int i = 0; i < WARMUP_2; i++) {
            setup.access(zipfSample(oldCdf, r.nextDouble()));
        }
        setup.snapshotAndReset(); // discard warmup counts + reset eviction baseline

        long totTotal = 0, totL1 = 0, totTar = 0, totEvict = 0;
        for (int epoch = 0; epoch < MEASURE_2 / EPOCH_OPS_2; epoch++) {
            for (int i = 0; i < EPOCH_OPS_2; i++) {
                setup.access(OLD_GEN_2 + zipfSample(newCdf, r.nextDouble()));
            }
            long[] snap = setup.snapshotAndReset();
            epochStats.add(snap);
            totTotal += snap[0]; totL1 += snap[1]; totTar += snap[2]; totEvict += snap[3];
        }
        return new long[]{totTotal, totL1, totTar, totEvict};
    }

    /**
     * Scenario 3: drifting active set.  Slides a Zipfian window through the pool;
     * per-epoch TAR-read% reveals whether L1-staleness compounds under churn.
     *
     * @param epochStats receives per-epoch [total, l1Hits, tarReads, evictions]
     * @return aggregate [total, l1Hits, tarReads, evictions] across all epochs
     */
    private static long[] runDriftingWindow(CacheSetup setup, List<long[]> epochStats) {
        double[] cdf = buildZipfCdf(WIDTH_3, ZIPF_3_EXP);
        Random r = new Random(RANDOM_SEED);
        int cursor = 0;
        int opCount = 0;

        for (int i = 0; i < WARMUP_3; i++) {
            if (opCount % DRIFT_3 == 0) cursor = (cursor + 1) % POOL_3;
            setup.access((cursor + zipfSample(cdf, r.nextDouble())) % POOL_3);
            opCount++;
        }
        setup.snapshotAndReset(); // discard warmup counts + reset eviction baseline

        long totTotal = 0, totL1 = 0, totTar = 0, totEvict = 0;
        for (int epoch = 0; epoch < MEASURE_3 / EPOCH_OPS_3; epoch++) {
            for (int i = 0; i < EPOCH_OPS_3; i++) {
                if (opCount % DRIFT_3 == 0) cursor = (cursor + 1) % POOL_3;
                setup.access((cursor + zipfSample(cdf, r.nextDouble())) % POOL_3);
                opCount++;
            }
            long[] snap = setup.snapshotAndReset();
            epochStats.add(snap);
            totTotal += snap[0]; totL1 += snap[1]; totTar += snap[2]; totEvict += snap[3];
        }
        return new long[]{totTotal, totL1, totTar, totEvict};
    }

    // -----------------------------------------------------------------------
    // Reporting helpers
    // -----------------------------------------------------------------------

    private static void printEpochTable(long[][][] epochs, int numEpochs, int epochOps) {
        System.out.printf("  %8s", "epoch");
        for (int p = 0; p < NUM_POLICIES; p++) {
            System.out.printf("  %22s", POLICY_NAMES[p] + "_tar%");
        }
        System.out.println();
        for (int e = 0; e < numEpochs; e++) {
            System.out.printf("  %8d", (long) (e + 1) * epochOps);
            for (int p = 0; p < NUM_POLICIES; p++) {
                long[] ep = epochs[p][e];
                long total = ep[0];
                System.out.printf("  %22.1f", total == 0 ? 0.0 : 100.0 * ep[2] / total);
            }
            System.out.println();
        }
    }

    /**
     * Prints one result row: policy name, L1-hit%, TAR-read%, totals, and evictions.
     *
     * @param label     policy name
     * @param total     total accesses in the measurement window
     * @param l1Hits    accesses served from L1 — no L2 call
     * @param tarReads  loader invocations — disk-read equivalents
     * @param evictions eviction count delta for the measurement window
     */
    private static void printResult(String label, long total, long l1Hits,
                                    long tarReads, long evictions) {
        double l1Pct   = total == 0 ? 0.0 : 100.0 * l1Hits   / total;
        double tarPct  = total == 0 ? 0.0 : 100.0 * tarReads  / total;
        double evPct   = total == 0 ? 0.0 : 100.0 * evictions / total;
        System.out.printf(
                "  %-22s  l1%%=%5.1f  tar%%=%5.1f"
                        + "  total=%,10d  l1Hits=%,9d  tarReads=%,9d"
                        + "  evictions=%,9d  evict%%=%6.1f%n",
                label, l1Pct, tarPct, total, l1Hits, tarReads, evictions, evPct);
    }

    // -----------------------------------------------------------------------
    // Zipfian distribution helpers
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
