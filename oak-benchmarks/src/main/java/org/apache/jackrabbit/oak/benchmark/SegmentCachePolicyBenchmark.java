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

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.LongAdder;

import javax.jcr.Repository;

import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.SegmentCache.SegmentCachePolicy;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.mockito.Mockito;

/**
 * Benchmark comparing CAFFEINE, LIRS, and GUAVA eviction policies inside
 * {@link SegmentCache} under three realistic AEM segment access scenarios.
 *
 * <p>All three policies go through the same {@code SegmentCache.NonEmptyCache}
 * code path; only the backing store differs.  This exercises the real
 * production code: load callbacks, weight tracking, eviction callbacks, and
 * L1/L2 memoisation.</p>
 *
 * <h3>Scenario A — Zipfian steady-state (timed run)</h3>
 * A small number of segments are extremely popular (templates, nav components)
 * and access probability decreases with rank.  Cache sized at ~10% of pool.
 * Favours frequency-aware policies (Caffeine W-TinyLFU).
 *
 * <h3>Scenario B — scan pollution (afterSuite)</h3>
 * A large sequential scan (GC traversal, index rebuild) precedes a Zipfian
 * workload.  The scan fills the TinyLFU frequency sketch with equal weights,
 * slowing post-scan re-admission of the true working set.
 *
 * <h3>Scenario C — cold-start regression (afterSuite)</h3>
 * A short scan below one TinyLFU decay period leaves scan entries at freq=1.
 * Working-set entries start at freq=0 and must beat the scan baseline to enter
 * main space.  Demonstrates the admission penalty in W-TinyLFU vs LRU.
 *
 * <p>Configurable via system properties:
 * <ul>
 *   <li>{@code -Dsegment.batch.size=1000} — accesses per {@code runTest()} call</li>
 *   <li>{@code -Dsegment.zipf.exponent=1.0} — Zipf exponent</li>
 *   <li>{@code -Dsegment.random.seed=42} — RNG seed for reproducibility</li>
 * </ul>
 */
public class SegmentCachePolicyBenchmark extends AbstractTest {

    // ----- cache sizing: 1 MB with MOCK_MEM_USAGE=1016 gives ~1000 entries -----
    private static final int CACHE_SIZE_MB = 1;
    private static final int MOCK_MEM_USAGE = 1016;   // weight = 32 + 1016 = 1048 bytes

    // ----- Scenario A pool -----
    private static final int TOTAL_SEGMENTS = 10_000;
    private static final double ZIPF_EXPONENT =
            Double.parseDouble(System.getProperty("segment.zipf.exponent", "1.0"));
    private static final long RANDOM_SEED = Long.getLong("segment.random.seed", 42L);
    private static final int BATCH_SIZE = Integer.getInteger("segment.batch.size", 1_000);

    // ----- Scenario B (scan then Zipfian) -----
    private static final int SCAN_LENGTH = 50_000;
    private static final int POST_SCAN_WARMUP = 20_000;
    private static final int POST_SCAN_MEASURE = 200_000;

    // ----- Scenario C (cold-start regression), scaled for ~1000-entry cache -----
    // Ratios match the original scenario: cache:scan:working-set = 1:9:3
    private static final int SCAN_C = 9_000;
    private static final int WORKING_SET_C = 3_000;
    private static final int MEASURE_C = 100_000;

    // ----- Scenario D (uniform random / cache thrash) -----
    // Pool is 25x cache capacity; uniform access means no hot data and ~95% miss rate.
    private static final int UNIFORM_POOL_D = 25_000;
    private static final int MEASURE_D = 200_000;

    private static final long DATA_SEG_LSB_MASK = 0xa000000000000000L;

    private static final SegmentCachePolicy[] POLICIES = {
        SegmentCachePolicy.CAFFEINE,
        SegmentCachePolicy.LIRS,
        SegmentCachePolicy.GUAVA
    };
    private static final String[] POLICY_NAMES = {"CAFFEINE", "LIRS", "GUAVA"};
    private static final int NUM_POLICIES = POLICIES.length;

    // ----- live Scenario A state -----
    private double[] zipfCdf;
    private Random rng;
    private SegmentCache[] liveCaches;
    private SegmentId[][] liveIds;
    private Segment[][] liveSegs;
    private LongAdder[] totalAccesses;

    @Override
    public String toString() {
        return "SegmentCachePolicyBenchmark";
    }

    /**
     * This benchmark exercises only in-memory caches; no JCR repository is used.
     */
    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        return fixture.setUpCluster(1);
    }

    /**
     * Initialises one {@link SegmentCache} per policy with pre-built
     * {@link SegmentId} and mock {@link Segment} pools for Scenario A.
     */
    @Override
    protected void beforeSuite() {
        zipfCdf = buildZipfCdf(TOTAL_SEGMENTS, ZIPF_EXPONENT);
        rng = new Random(RANDOM_SEED);
        totalAccesses = new LongAdder[NUM_POLICIES];
        liveCaches = new SegmentCache[NUM_POLICIES];
        liveIds = new SegmentId[NUM_POLICIES][TOTAL_SEGMENTS];
        liveSegs = new Segment[NUM_POLICIES][TOTAL_SEGMENTS];
        for (int p = 0; p < NUM_POLICIES; p++) {
            totalAccesses[p] = new LongAdder();
            liveCaches[p] = SegmentCache.newSegmentCache(CACHE_SIZE_MB, POLICIES[p]);
            for (int i = 0; i < TOTAL_SEGMENTS; i++) {
                UUID uuid = UUID.randomUUID();
                long msb = uuid.getMostSignificantBits();
                long lsb = (uuid.getLeastSignificantBits() & 0x0fffffffffffffffL) | DATA_SEG_LSB_MASK;
                liveIds[p][i] = new SegmentId(
                        SegmentStore.EMPTY_STORE, msb, lsb,
                        liveCaches[p]::recordHit);
                liveSegs[p][i] = Mockito.mock(Segment.class);
                Mockito.when(liveSegs[p][i].getSegmentId()).thenReturn(liveIds[p][i]);
                Mockito.when(liveSegs[p][i].estimateMemoryUsage()).thenReturn(MOCK_MEM_USAGE);
            }
        }
    }

    /**
     * Performs {@code segment.batch.size} Zipfian accesses against all three
     * caches simultaneously.  The same segment rank is presented to every
     * policy per iteration so comparisons are fair.
     */
    @Override
    protected void runTest() throws Exception {
        for (int i = 0; i < BATCH_SIZE; i++) {
            int segIdx = zipfSample(zipfCdf, rng.nextDouble());
            accessAll(segIdx);
        }
    }

    private void accessAll(int segIdx) throws ExecutionException {
        for (int p = 0; p < NUM_POLICIES; p++) {
            Segment seg = liveSegs[p][segIdx];
            liveCaches[p].getSegment(liveIds[p][segIdx], () -> seg);
            totalAccesses[p].increment();
        }
    }

    /**
     * Prints a three-scenario comparison table.  Scenario A uses the live
     * counters from the AbstractTest loop; Scenarios B and C run fresh caches.
     */
    @Override
    protected void afterSuite() {
        int cacheCapacity = (int) ((long) CACHE_SIZE_MB * 1024 * 1024 / (32 + MOCK_MEM_USAGE));
        System.out.printf(
                "%nSegmentCachePolicyBenchmark  cacheCapacity~=%d  pool=%d  zipf=%.1f%n%n",
                cacheCapacity, TOTAL_SEGMENTS, ZIPF_EXPONENT);

        System.out.println("--- Scenario A: Zipfian steady-state (AbstractTest timed run) ---");
        for (int p = 0; p < NUM_POLICIES; p++) {
            long misses = liveCaches[p].getCacheStats().getMissCount();
            long total = totalAccesses[p].sum();
            long evictions = liveCaches[p].getCacheStats().getEvictionCount();
            printResult(POLICY_NAMES[p], total - misses, misses, evictions);
        }

        System.out.printf(
                "%n--- Scenario B: scan (%,d segs) then Zipfian"
                        + " (warmup=%,d  measure=%,d ops) ---%n",
                SCAN_LENGTH, POST_SCAN_WARMUP, POST_SCAN_MEASURE);
        for (int p = 0; p < NUM_POLICIES; p++) {
            PolicySetup setup = freshSetup(p, POLICIES[p], TOTAL_SEGMENTS);
            long[] r = runScanThenZipf(setup);
            printResult(POLICY_NAMES[p], r[0], r[1], r[2]);
        }

        System.out.printf(
                "%n--- Scenario C: cold-start regression"
                        + " (scan=%,d  working-set=%,d  measure=%,d ops) ---%n",
                SCAN_C, WORKING_SET_C, MEASURE_C);
        System.out.println(
                "  scan fills TinyLFU sketch at freq=1;"
                        + " working-set entries start at freq=0");
        for (int p = 0; p < NUM_POLICIES; p++) {
            PolicySetup setup = freshSetup(p, POLICIES[p], SCAN_C + WORKING_SET_C);
            long[] r = runColdStart(setup);
            printResult(POLICY_NAMES[p], r[0], r[1], r[2]);
        }

        System.out.printf(
                "%n--- Scenario D: uniform random / cache thrash"
                        + " (pool=%,d = ~%dx cache  measure=%,d ops) ---%n",
                UNIFORM_POOL_D, UNIFORM_POOL_D / cacheCapacity, MEASURE_D);
        System.out.println(
                "  no hot data — uniform access over pool 25x cache; expected miss ~95%%");
        for (int p = 0; p < NUM_POLICIES; p++) {
            PolicySetup setup = freshSetup(p, POLICIES[p], UNIFORM_POOL_D);
            long[] r = runUniformRandom(setup);
            printResult(POLICY_NAMES[p], r[0], r[1], r[2]);
        }
    }

    /** Miss-rate column headers for the AbstractTest output row. */
    @Override
    protected String[] statsNames() {
        return new String[]{"  Caff_miss%", "  LIRS_miss%", "  Guav_miss%"};
    }

    /** Format strings for the three miss-rate columns. */
    @Override
    protected String[] statsFormats() {
        return new String[]{"  %10.1f", "  %10.1f", "  %10.1f"};
    }

    /** Current running miss-rate (%) for each policy from the live Scenario A run. */
    @Override
    protected Object[] statsValues() {
        Object[] vals = new Object[NUM_POLICIES];
        for (int p = 0; p < NUM_POLICIES; p++) {
            long misses = liveCaches[p].getCacheStats().getMissCount();
            long total = totalAccesses[p].sum();
            vals[p] = total == 0 ? 0.0 : 100.0 * misses / total;
        }
        return vals;
    }

    // -----------------------------------------------------------------------
    // PolicySetup helper
    // -----------------------------------------------------------------------

    /**
     * Groups a {@link SegmentCache} with its associated {@link SegmentId} and
     * mock {@link Segment} arrays for use in scenario runners.
     */
    private static final class PolicySetup {
        final SegmentCache cache;
        final SegmentId[] ids;
        final Segment[] segs;

        PolicySetup(SegmentCache cache, SegmentId[] ids, Segment[] segs) {
            this.cache = cache;
            this.ids = ids;
            this.segs = segs;
        }

        void access(int idx) {
            Segment s = segs[idx];
            try {
                cache.getSegment(ids[idx], () -> s);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Builds a fresh {@link PolicySetup} with {@code n} mock segments.
     *
     * @param policyIndex unique MSB so IDs don't collide across policies
     * @param policy      the cache eviction policy to use
     * @param n           number of distinct segments to create
     */
    private static PolicySetup freshSetup(int policyIndex, SegmentCachePolicy policy, int n) {
        SegmentCache cache = SegmentCache.newSegmentCache(CACHE_SIZE_MB, policy);
        SegmentId[] ids = new SegmentId[n];
        Segment[] segs = new Segment[n];
        for (int i = 0; i < n; i++) {
            UUID uuid = UUID.randomUUID();
            long msb = uuid.getMostSignificantBits();
            long lsb = (uuid.getLeastSignificantBits() & 0x0fffffffffffffffL) | DATA_SEG_LSB_MASK;
            ids[i] = new SegmentId(
                    SegmentStore.EMPTY_STORE, msb, lsb,
                    cache::recordHit);
            segs[i] = Mockito.mock(Segment.class);
            Mockito.when(segs[i].getSegmentId()).thenReturn(ids[i]);
            Mockito.when(segs[i].estimateMemoryUsage()).thenReturn(MOCK_MEM_USAGE);
        }
        return new PolicySetup(cache, ids, segs);
    }

    // -----------------------------------------------------------------------
    // Scenario runners
    // -----------------------------------------------------------------------

    /**
     * Scenario B: sequential scan then Zipfian workload.
     *
     * @return [hits, misses, evictions] measured only during the post-scan phase
     */
    private static long[] runScanThenZipf(PolicySetup setup) {
        double[] cdf = buildZipfCdf(TOTAL_SEGMENTS, ZIPF_EXPONENT);
        Random r = new Random(RANDOM_SEED);

        for (int i = 0; i < SCAN_LENGTH; i++) {
            setup.access(i % TOTAL_SEGMENTS);
        }
        for (int i = 0; i < POST_SCAN_WARMUP; i++) {
            setup.access(zipfSample(cdf, r.nextDouble()));
        }

        long missesBase = setup.cache.getCacheStats().getMissCount();
        long evictBase = setup.cache.getCacheStats().getEvictionCount();

        for (int i = 0; i < POST_SCAN_MEASURE; i++) {
            setup.access(zipfSample(cdf, r.nextDouble()));
        }

        long misses = setup.cache.getCacheStats().getMissCount() - missesBase;
        long evictions = setup.cache.getCacheStats().getEvictionCount() - evictBase;
        return new long[]{POST_SCAN_MEASURE - misses, misses, evictions};
    }

    /**
     * Scenario C: short scan below one TinyLFU decay period, then access the
     * working set with no warmup.
     *
     * @return [hits, misses, evictions]
     */
    private static long[] runColdStart(PolicySetup setup) {
        Random r = new Random(RANDOM_SEED);

        for (int i = 0; i < SCAN_C; i++) {
            setup.access(i);
        }

        long missesBase = setup.cache.getCacheStats().getMissCount();
        long evictBase = setup.cache.getCacheStats().getEvictionCount();

        for (int i = 0; i < MEASURE_C; i++) {
            setup.access(SCAN_C + r.nextInt(WORKING_SET_C));
        }

        long misses = setup.cache.getCacheStats().getMissCount() - missesBase;
        long evictions = setup.cache.getCacheStats().getEvictionCount() - evictBase;
        return new long[]{MEASURE_C - misses, misses, evictions};
    }

    /**
     * Scenario D: uniform random access over a pool far larger than the cache.
     * Warms the cache with one random pass (so each policy starts from a full
     * cache), then measures steady-state miss rate.
     *
     * @return [hits, misses, evictions]
     */
    private static long[] runUniformRandom(PolicySetup setup) {
        Random r = new Random(RANDOM_SEED);
        int n = setup.ids.length;

        // fill the cache before measuring
        for (int i = 0; i < n; i++) {
            setup.access(r.nextInt(n));
        }

        long missesBase = setup.cache.getCacheStats().getMissCount();
        long evictBase  = setup.cache.getCacheStats().getEvictionCount();

        for (int i = 0; i < MEASURE_D; i++) {
            setup.access(r.nextInt(n));
        }

        long misses    = setup.cache.getCacheStats().getMissCount()     - missesBase;
        long evictions = setup.cache.getCacheStats().getEvictionCount() - evictBase;
        return new long[]{MEASURE_D - misses, misses, evictions};
    }

    // -----------------------------------------------------------------------
    // Zipfian distribution
    // -----------------------------------------------------------------------

    /**
     * Pre-computes a cumulative Zipfian CDF over {@code n} items.
     * Item at rank 0 has weight 1/1^exponent, rank 1 has 1/2^exponent, etc.
     */
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

    /** Samples a rank from the Zipfian CDF using binary search. */
    private static int zipfSample(double[] cdf, double u) {
        int lo = 0, hi = cdf.length - 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (cdf[mid] < u) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    private static void printResult(String label, long hits, long misses, long evictions) {
        long total = hits + misses;
        double missRate = total == 0 ? 0.0 : 100.0 * misses / total;
        double evictRate = total == 0 ? 0.0 : 100.0 * evictions / total;
        System.out.printf(
                "  %-12s  miss%%=%5.1f  hits=%,8d  misses=%,8d  evictions=%,8d  evict%%=%5.1f%n",
                label, missRate, hits, misses, evictions, evictRate);
    }
}
