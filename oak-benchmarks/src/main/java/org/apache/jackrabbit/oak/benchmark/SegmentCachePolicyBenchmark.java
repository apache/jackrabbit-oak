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

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.LongAdder;

import javax.jcr.Repository;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.segment.RecordNumbers;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.SegmentCache.SegmentCachePolicy;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentReferences;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.apache.jackrabbit.oak.segment.data.SegmentData;

/**
 * Compares CAFFEINE and GUAVA eviction policies by calling {@link SegmentCache#getSegment}
 * directly on every access — no L1 memoization involved.  The metric is L2 miss rate.
 * Segments are Mockito mocks so cache misses cost nothing; this isolates eviction policy
 * behaviour from I/O noise.
 *
 * <p>Because every access hits L2, both policies see the full access stream and their
 * frequency/recency counters stay accurate.  That is not how production works — hot segments
 * are normally served from the {@link SegmentId} memoization field without touching L2 at all.
 * See {@link SegmentCacheMemoizationBenchmark} for a benchmark that reflects that reality.</p>
 *
 * <p>Run with {@code -Xmx4g}; the 11-scenario afterSuite accumulates ~50K live mocks and
 * a GC death spiral below that.</p>
 *
 * <h3>Scenario A — Zipfian steady-state (timed run)</h3>
 * A small number of segments dominate traffic (templates, nav components); access probability
 * drops off with rank.  Cache holds ~10% of the pool.  Favours frequency-aware policies.
 *
 * <h3>Scenario B — scan pollution</h3>
 * A large sequential scan (GC traversal, index rebuild) precedes a Zipfian workload.
 * The scan loads the TinyLFU sketch with equal counts, delaying re-admission of the true
 * working set until the sketch decays.
 *
 * <h3>Scenario C — sustained scan contamination</h3>
 * A fraction of every operation re-accesses random scan entries (bot / crawler traffic on
 * old content).  The sketch never decays because the contamination is continuous, so
 * Caffeine's admission freeze doesn't self-correct.  Guava is unaffected.
 *
 * <h3>Scenario D — uniform random thrash</h3>
 * Pool is 25× cache capacity with uniform access.  No policy has an advantage here;
 * establishes the ~95% miss-rate floor.
 *
 * <h3>Scenario E — burst new content</h3>
 * A warm cache is hit by a burst of new segments, each accessed several times then
 * abandoned.  Checks whether W-TinyLFU holds onto burst entries (elevated count) and
 * squeezes out the steady-state working set, vs LRU which forgets the burst by recency.
 *
 * <h3>Scenario F — periodic GC/diff scans</h3>
 * Short sequential scans interleaved with Zipfian traffic over many cycles.  Unlike the
 * single large scan in B, repeated small scans accumulate sketch pollution incrementally.
 *
 * <h3>Scenario G — write-heavy import then read-back</h3>
 * Each segment is written exactly once (import), then the most recent ones are re-read at
 * random.  Tests whether recency (Guava) or frequency (Caffeine) is a better predictor of
 * what gets re-read after an import.
 *
 * <h3>Scenario H — sliding window</h3>
 * A hot window of {@code WINDOW_SIZE_H} segments advances through a large pool; each entry
 * is hit {@code WINDOW_HITS_H} times before the window moves on.  Window is ~1.2× cache
 * capacity so every slide forces evictions.  Pure LRU is theoretically optimal here.
 *
 * <h3>Scenario I — drifting active set</h3>
 * A Zipfian window moves through a pool; the cursor advances every {@code DRIFT_I} ops so
 * older entries leave the hot set continuously.  Per-epoch miss rates show how fast each
 * policy adapts and how long Caffeine's sketch-decay freeze lasts after the window shifts.
 *
 * <h3>Scenario J — drift-rate sweep</h3>
 * Re-runs the drifting window at several cursor speeds (1, 2, 5, 10, 20, static) to produce
 * a miss-rate table indexed by churn rate.  Shows exactly where Caffeine's frequency
 * advantage flips into a disadvantage as working-set churn increases.
 *
 * <h3>Scenario K — post-compaction cold-start</h3>
 * Phase 1 warms the cache with old-generation segments (Zipfian, building sketch counts).
 * Phase 2 switches all traffic to new-generation IDs (freq=0), as happens after Oak online
 * compaction.  Caffeine's admission gate blocks new-gen entries until their count beats the
 * old-gen victims; Guava evicts by recency immediately.  Per-epoch miss rates track how long
 * the freeze lasts.
 *
 * <p>Configurable: {@code -Dsegment.batch.size} (accesses per iteration, default 1000),
 * {@code -Dsegment.zipf.exponent} (default 1.0), {@code -Dsegment.random.seed} (default 42).
 */
public class SegmentCachePolicyBenchmark extends AbstractTest {

    // ----- cache sizing: segments vary 4–256 KB; avg ~130 KB; 130 MB gives ~1000 entries -----
    private static final int CACHE_SIZE_MB = 130;
    private static final int MIN_SEG_KB = 4;
    private static final int MAX_SEG_KB = 256;

    // ----- Scenario A pool -----
    private static final int TOTAL_SEGMENTS = 10_000;
    private static final double ZIPF_EXPONENT =
            Double.parseDouble(System.getProperty("segment.zipf.exponent", "1.0"));
    private static final long RANDOM_SEED = Long.getLong("segment.random.seed", 42L);
    private static final int BATCH_SIZE = Integer.getInteger("segment.batch.size", 1_000);

    // ----- Scenario B (scan then Zipfian) -----
    private static final int SCAN_LENGTH = 50_000;
    private static final int POST_SCAN_WARMUP = 20_000;
    private static final int POST_SCAN_MEASURE = 600_000;

    // ----- Scenario C (cold-start regression) — TMG-realistic variant -----
    // SCAN_PASSES_C passes raise incumbent freq to ~10, making new entries hard to admit.
    // BG_SCAN_INTERVAL_C simulates background bot/crawler traffic that continuously
    // re-accesses old content during measurement, preventing sketch decay and sustaining
    // the freeze.  Larger WORKING_SET_C reduces per-entry revisit rate (more unique URLs).
    // Pool = SCAN_C + WORKING_SET_C.
    private static final int SCAN_C = 9_000;
    private static final int WORKING_SET_C = 5_000;
    private static final int SCAN_PASSES_C = 10;
    private static final int BG_SCAN_INTERVAL_C = 10;
    private static final int MEASURE_C = 900_000;
    private static final int EPOCH_OPS_C = 10_000;

    // ----- Scenario D: uniform random / cache thrash -----
    // Pool is 25x cache capacity; uniform access means no hot data and ~95% miss rate.
    private static final int UNIFORM_POOL_D = 25_000;
    private static final int MEASURE_D = 600_000;

    // ----- Scenario E: burst new content -----
    // Warm Zipfian cache + burst of BURST_SIZE_E new segments × BURST_ACCESSES_E hits each,
    // then measure Zipfian over original working set.  Pool = TOTAL_SEGMENTS + BURST_SIZE_E.
    private static final int BURST_SIZE_E = 500;
    private static final int BURST_ACCESSES_E = 20;
    private static final int WARMUP_E = 50_000;
    private static final int MEASURE_E = 300_000;

    // ----- Scenario F: periodic background (GC / diff) alternation -----
    private static final int CYCLES_F = 10;
    private static final int CYCLE_ZIPF_OPS_F = 10_000;
    private static final int CYCLE_SCAN_OPS_F = 2_000;
    private static final int MEASURE_F = 300_000;

    // ----- Scenario G: write-heavy import then recent read-back -----
    private static final int IMPORT_SIZE_G = 50_000;
    private static final int RECENT_WINDOW_G = 2_000;
    private static final int MEASURE_G = 300_000;

    // ----- Scenario H: sliding window / temporal locality -----
    // Window slightly > cache capacity to force eviction decisions on every slide.
    private static final int WINDOW_SIZE_H = 1_200;
    private static final int SLIDE_STEP_H = 200;
    private static final int TOTAL_POOL_H = 20_000;
    private static final int WINDOW_HITS_H = 2;
    private static final int MEASURE_H = 450_000;

    // ----- Scenario I: drifting active set with per-epoch reporting -----
    // Cursor advances 1 position every DRIFT_I ops; within the window, access follows
    // a mild Zipfian distribution (exponent 0.5, so less skewed than ZIPF_EXPONENT).
    private static final int POOL_I = 20_000;
    private static final int WIDTH_I = 1_500;
    private static final int DRIFT_I = 5;
    private static final int WARMUP_I = 50_000;
    private static final int MEASURE_I = 1_200_000;
    private static final double ZIPF_I_EXP = 0.5;
    private static final int EPOCH_OPS_I = 10_000;

    // ----- Scenario J: drift-rate sweep -----
    // Same drifting-window generator as I, swept across multiple drift speeds.
    // Large pool ensures the window does not wrap-alias across drift variants.
    private static final int POOL_J = 260_000;
    private static final int WIDTH_J = 1_500;
    private static final double ZIPF_J_EXP = 0.5;
    private static final int WARMUP_J = 50_000;
    private static final int MEASURE_J = 600_000;
    private static final int[] DRIFT_VARIANTS_J = {1, 2, 5, 10, 20, Integer.MAX_VALUE};

    // ----- Scenario K: post-compaction cold-start -----
    // 200K warmup saturates old-gen sketch to freq=15 (4-bit cap).
    // NEW_GEN_K = 15K + flat Zipf(0.5) → each new-gen entry gets ~8 hits/epoch,
    // keeping most below the freq≤5 auto-reject threshold for 3–5 epochs.
    // EPOCH_OPS_K = 2K exposes the initial spike before hot new-gen escapes the gate.
    private static final int OLD_GEN_K = 5_000;
    private static final int NEW_GEN_K = 15_000;
    private static final int WARMUP_K = 200_000;
    private static final double ZIPF_K_NEW_EXP = 0.5; // flatter than warmup — slows freq build-up
    private static final int MEASURE_K = 900_000;
    private static final int EPOCH_OPS_K = 2_000;

    private static final long DATA_SEG_LSB_MASK = 0xa000000000000000L;

    private static final SegmentCachePolicy[] POLICIES = {
        SegmentCachePolicy.CAFFEINE,
        SegmentCachePolicy.GUAVA
    };
    private static final String[] POLICY_NAMES = {"CAFFEINE", "GUAVA"};
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
     * {@link SegmentId} and {@link MinimalSegment} pools for Scenario A.
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
                int memUsage = MIN_SEG_KB * 1024 + rng.nextInt((MAX_SEG_KB - MIN_SEG_KB) * 1024);
                liveSegs[p][i] = new MinimalSegment(memUsage);
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
        int avgWeight = 32 + (MIN_SEG_KB + MAX_SEG_KB) / 2 * 1024;
        int cacheCapacity = (int) ((long) CACHE_SIZE_MB * 1024 * 1024 / avgWeight);
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
        liveCaches = null; // release live-run state — no longer needed
        liveIds = null;
        liveSegs = null;
        System.gc(); // hint GC before allocating scenario pools

        System.out.printf(
                "%n--- Scenario B: scan (%,d segs) then Zipfian"
                        + " (warmup=%,d  measure=%,d ops) ---%n",
                SCAN_LENGTH, POST_SCAN_WARMUP, POST_SCAN_MEASURE);
        for (int p = 0; p < NUM_POLICIES; p++) {
            PolicySetup setup = freshSetup(p, POLICIES[p], TOTAL_SEGMENTS, CACHE_SIZE_MB);
            long[] r = runScanThenZipf(setup);
            printResult(POLICY_NAMES[p], r[0], r[1], r[2]);
        }

        System.out.printf(
                "%n--- Scenario C: cold-start regression / TMG crawler simulation"
                        + " (scan=%,d × %d passes  working-set=%,d  bg-scan=1/%d"
                        + "  measure=%,d  epoch=%,d ops) ---%n",
                SCAN_C, SCAN_PASSES_C, WORKING_SET_C, BG_SCAN_INTERVAL_C, MEASURE_C, EPOCH_OPS_C);
        System.out.printf(
                "  incumbents at freq=%d; %.0f%% of ops re-access old content"
                        + " (bot/crawler) — prevents sketch decay%n",
                SCAN_PASSES_C, 100.0 / BG_SCAN_INTERVAL_C);
        long[][][] epochsC = new long[NUM_POLICIES][][];
        long[][] totalsC = new long[NUM_POLICIES][];
        for (int p = 0; p < NUM_POLICIES; p++) {
            List<long[]> epochs = new ArrayList<>();
            PolicySetup setup = freshSetup(p, POLICIES[p], SCAN_C + WORKING_SET_C, CACHE_SIZE_MB);
            totalsC[p] = runColdStart(setup, epochs);
            epochsC[p] = epochs.toArray(new long[0][]);
        }
        System.out.printf("  %8s", "epoch");
        for (int p = 0; p < NUM_POLICIES; p++) {
            System.out.printf("  %14s", POLICY_NAMES[p] + "_miss%");
        }
        System.out.println();
        for (int e = 0; e < epochsC[0].length; e++) {
            System.out.printf("  %8d", (long) (e + 1) * EPOCH_OPS_C);
            for (int p = 0; p < NUM_POLICIES; p++) {
                long[] ep = epochsC[p][e];
                long epTotal = ep[0] + ep[1];
                System.out.printf("  %14.1f", epTotal == 0 ? 0.0 : 100.0 * ep[1] / epTotal);
            }
            System.out.println();
        }
        for (int p = 0; p < NUM_POLICIES; p++) {
            printResult(POLICY_NAMES[p], totalsC[p][0], totalsC[p][1], totalsC[p][2]);
        }

        System.out.printf(
                "%n--- Scenario D: uniform random / cache thrash"
                        + " (pool=%,d = ~%dx cache  measure=%,d ops) ---%n",
                UNIFORM_POOL_D, UNIFORM_POOL_D / cacheCapacity, MEASURE_D);
        System.out.println(
                "  no hot data — uniform access over pool 25x cache; expected miss ~95%%");
        for (int p = 0; p < NUM_POLICIES; p++) {
            PolicySetup setup = freshSetup(p, POLICIES[p], UNIFORM_POOL_D, CACHE_SIZE_MB);
            long[] r = runUniformRandom(setup);
            printResult(POLICY_NAMES[p], r[0], r[1], r[2]);
        }

        System.out.printf(
                "%n--- Scenario E: burst new content"
                        + " (burst=%,d segs × %d hits  warmup=%,d  measure=%,d ops) ---%n",
                BURST_SIZE_E, BURST_ACCESSES_E, WARMUP_E, MEASURE_E);
        System.out.println(
                "  warm Zipfian cache hit by burst of new segments;"
                        + " measures working-set miss rate after burst subsides");
        for (int p = 0; p < NUM_POLICIES; p++) {
            PolicySetup setup = freshSetup(p, POLICIES[p], TOTAL_SEGMENTS + BURST_SIZE_E, CACHE_SIZE_MB);
            long[] r = runBurstNewContent(setup);
            printResult(POLICY_NAMES[p], r[0], r[1], r[2]);
        }

        System.out.printf(
                "%n--- Scenario F: periodic GC/diff alternation"
                        + " (cycles=%d  zipf/cycle=%,d  scan/cycle=%,d  measure=%,d ops) ---%n",
                CYCLES_F, CYCLE_ZIPF_OPS_F, CYCLE_SCAN_OPS_F, MEASURE_F);
        System.out.println(
                "  repeated small scans interleaved with Zipfian;"
                        + " cumulative sketch pollution vs LRU recency aging");
        for (int p = 0; p < NUM_POLICIES; p++) {
            PolicySetup setup = freshSetup(p, POLICIES[p], TOTAL_SEGMENTS, CACHE_SIZE_MB);
            long[] r = runPeriodicGC(setup);
            printResult(POLICY_NAMES[p], r[0], r[1], r[2]);
        }

        System.out.printf(
                "%n--- Scenario G: write-heavy import then read-back"
                        + " (import=%,d  recent-window=%,d  measure=%,d ops) ---%n",
                IMPORT_SIZE_G, RECENT_WINDOW_G, MEASURE_G);
        System.out.println(
                "  large sequential import followed by random reads of recently-imported segments;"
                        + " recency (LRU) vs frequency (Caffeine) post-import");
        for (int p = 0; p < NUM_POLICIES; p++) {
            PolicySetup setup = freshSetup(p, POLICIES[p], IMPORT_SIZE_G, CACHE_SIZE_MB);
            long[] r = runImportThenRead(setup);
            printResult(POLICY_NAMES[p], r[0], r[1], r[2]);
        }

        System.out.printf(
                "%n--- Scenario H: sliding window / temporal locality"
                        + " (window=%,d ~%.0f%% of cache  slide=%,d  pool=%,d"
                        + "  hits/item=%d  measure=%,d ops) ---%n",
                WINDOW_SIZE_H, 100.0 * WINDOW_SIZE_H / cacheCapacity,
                SLIDE_STEP_H, TOTAL_POOL_H, WINDOW_HITS_H, MEASURE_H);
        System.out.println(
                "  hot window slides forward; pure recency (LRU) is optimal;"
                        + " window > cache forces evictions on every slide");
        for (int p = 0; p < NUM_POLICIES; p++) {
            PolicySetup setup = freshSetup(p, POLICIES[p], TOTAL_POOL_H, CACHE_SIZE_MB);
            long[] r = runSlidingWindow(setup);
            printResult(POLICY_NAMES[p], r[0], r[1], r[2]);
        }

        System.out.printf(
                "%n--- Scenario I: drifting active set"
                        + " (pool=%,d  width=%,d  drift=%d  warmup=%,d"
                        + "  measure=%,d  epoch=%,d  zipf=%.1f) ---%n",
                POOL_I, WIDTH_I, DRIFT_I, WARMUP_I, MEASURE_I, EPOCH_OPS_I, ZIPF_I_EXP);
        System.out.println(
                "  window slides continuously; per-epoch miss% reveals"
                        + " W-TinyLFU sketch-decay freeze on new-entry admission");
        long[][][] epochsI = new long[NUM_POLICIES][][];
        long[][] totalsI = new long[NUM_POLICIES][];
        for (int p = 0; p < NUM_POLICIES; p++) {
            List<long[]> epochs = new ArrayList<>();
            PolicySetup setup = freshSetup(p, POLICIES[p], POOL_I, CACHE_SIZE_MB);
            totalsI[p] = runDriftingWindow(setup, epochs);
            epochsI[p] = epochs.toArray(new long[0][]);
        }
        System.out.printf("  %8s", "epoch");
        for (int p = 0; p < NUM_POLICIES; p++) {
            System.out.printf("  %14s", POLICY_NAMES[p] + "_miss%");
        }
        System.out.println();
        for (int e = 0; e < epochsI[0].length; e++) {
            System.out.printf("  %8d", (long) (e + 1) * EPOCH_OPS_I);
            for (int p = 0; p < NUM_POLICIES; p++) {
                long[] ep = epochsI[p][e];
                long epTotal = ep[0] + ep[1];
                System.out.printf("  %14.1f", epTotal == 0 ? 0.0 : 100.0 * ep[1] / epTotal);
            }
            System.out.println();
        }
        for (int p = 0; p < NUM_POLICIES; p++) {
            printResult(POLICY_NAMES[p], totalsI[p][0], totalsI[p][1], totalsI[p][2]);
        }

        System.out.printf(
                "%n--- Scenario J: drift-rate sweep"
                        + " (pool=%,d  width=%,d  warmup=%,d  measure=%,d  zipf=%.1f) ---%n",
                POOL_J, WIDTH_J, WARMUP_J, MEASURE_J, ZIPF_J_EXP);
        System.out.println(
                "  drift=1 → cursor every op; Integer.MAX_VALUE → stationary working set");
        System.out.printf("  %-12s", "drift");
        for (int p = 0; p < NUM_POLICIES; p++) {
            System.out.printf("  %14s", POLICY_NAMES[p] + "_miss%");
        }
        System.out.println();
        for (int drift : DRIFT_VARIANTS_J) {
            String label = drift == Integer.MAX_VALUE ? "static" : String.valueOf(drift);
            System.out.printf("  %-12s", label);
            for (int p = 0; p < NUM_POLICIES; p++) {
                PolicySetup setup = freshSetup(p, POLICIES[p], POOL_J, CACHE_SIZE_MB);
                long[] r = runDriftVariant(setup, drift);
                long total = r[0] + r[1];
                System.out.printf("  %14.1f", total == 0 ? 0.0 : 100.0 * r[1] / total);
            }
            System.out.println();
        }

        System.out.printf(
                "%n--- Scenario K: post-compaction cold-start"
                        + " (old-gen=%,d  new-gen=%,d  warmup=%,d  measure=%,d  epoch=%,d ops"
                        + "  zipf-new=%.1f) ---%n",
                OLD_GEN_K, NEW_GEN_K, WARMUP_K, MEASURE_K, EPOCH_OPS_K, ZIPF_K_NEW_EXP);
        System.out.println(
                "  Old-gen saturated to freq=15; new-gen (freq=0) auto-rejected by W-TinyLFU (freq≤5 gate).");
        System.out.println(
                "  Caffeine: ~40%+ miss% initially, self-corrects after ~30K ops; Guava: ~27% steady.");
        System.out.println(
                "  After convergence: Caffeine ~20% vs Guava ~24% — W-TinyLFU wins long-term.");
        long[][][] epochsK = new long[NUM_POLICIES][][];
        long[][] totalsK = new long[NUM_POLICIES][];
        for (int p = 0; p < NUM_POLICIES; p++) {
            List<long[]> epochs = new ArrayList<>();
            PolicySetup setup = freshSetup(p, POLICIES[p], OLD_GEN_K + NEW_GEN_K, CACHE_SIZE_MB);
            totalsK[p] = runCompactionColdStart(setup, epochs);
            epochsK[p] = epochs.toArray(new long[0][]);
        }
        System.out.printf("  %8s", "epoch");
        for (int p = 0; p < NUM_POLICIES; p++) {
            System.out.printf("  %14s", POLICY_NAMES[p] + "_miss%");
        }
        System.out.println();
        for (int e = 0; e < epochsK[0].length; e++) {
            System.out.printf("  %8d", (long) (e + 1) * EPOCH_OPS_K);
            for (int p = 0; p < NUM_POLICIES; p++) {
                long[] ep = epochsK[p][e];
                long epTotal = ep[0] + ep[1];
                System.out.printf("  %14.1f", epTotal == 0 ? 0.0 : 100.0 * ep[1] / epTotal);
            }
            System.out.println();
        }
        for (int p = 0; p < NUM_POLICIES; p++) {
            printResult(POLICY_NAMES[p], totalsK[p][0], totalsK[p][1], totalsK[p][2]);
        }

        runSizeSensitivity();
    }

    /**
     * Runs Scenario I (drifting active set) and Scenario K (post-compaction cold-start)
     * at half, normal, and double cache sizes to show how each policy scales with capacity.
     */
    private void runSizeSensitivity() {
        int[] sizes = {CACHE_SIZE_MB / 2, CACHE_SIZE_MB, CACHE_SIZE_MB * 2};

        System.out.printf(
                "%n--- Size sensitivity: Scenario I (drifting active set)"
                        + " (pool=%,d  width=%,d  drift=%d  measure=%,d ops) ---%n",
                POOL_I, WIDTH_I, DRIFT_I, MEASURE_I);
        System.out.printf("  %8s", "cacheMB");
        for (int p = 0; p < NUM_POLICIES; p++) {
            System.out.printf("  %14s", POLICY_NAMES[p] + "_miss%");
        }
        System.out.println();
        for (int sizeMb : sizes) {
            Segment[] poolI = createSegmentPool(POOL_I);
            System.out.printf("  %8d", sizeMb);
            for (int p = 0; p < NUM_POLICIES; p++) {
                PolicySetup setup = freshSetupWithPool(p, POLICIES[p], poolI, sizeMb);
                long[] r = runDriftingWindow(setup, new ArrayList<>());
                long total = r[0] + r[1];
                System.out.printf("  %14.1f", total == 0 ? 0.0 : 100.0 * r[1] / total);
            }
            System.out.println();
        }

        System.out.printf(
                "%n--- Size sensitivity: Scenario K (post-compaction cold-start)"
                        + " (old-gen=%,d  new-gen=%,d  measure=%,d ops) ---%n",
                OLD_GEN_K, NEW_GEN_K, MEASURE_K);
        System.out.printf("  %8s", "cacheMB");
        for (int p = 0; p < NUM_POLICIES; p++) {
            System.out.printf("  %14s", POLICY_NAMES[p] + "_miss%");
        }
        System.out.println();
        for (int sizeMb : sizes) {
            Segment[] poolK = createSegmentPool(OLD_GEN_K + NEW_GEN_K);
            System.out.printf("  %8d", sizeMb);
            for (int p = 0; p < NUM_POLICIES; p++) {
                PolicySetup setup = freshSetupWithPool(p, POLICIES[p], poolK, sizeMb);
                long[] totals = runCompactionColdStart(setup, new ArrayList<>());
                long total = totals[0] + totals[1];
                System.out.printf("  %14.1f", total == 0 ? 0.0 : 100.0 * totals[1] / total);
            }
            System.out.println();
        }
    }

    /** Miss-rate column headers for the AbstractTest output row. */
    @Override
    protected String[] statsNames() {
        return new String[]{"  Caff_miss%", "  Guav_miss%"};
    }

    /** Format strings for the five miss-rate columns. */
    @Override
    protected String[] statsFormats() {
        return new String[]{"  %10.1f", "  %10.1f"};
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
     * Creates {@code n} reusable mock segments with {@code estimateMemoryUsage()} stubs.
     * The pool can be shared across multiple {@link #freshSetupWithPool} calls (one per policy)
     * so that mock objects are not recreated per policy in the size-sensitivity sweep.
     *
     * @param n number of distinct segments to create
     * @return array of mock segments with size stubs applied
     */
    private static Segment[] createSegmentPool(int n) {
        Segment[] segs = new Segment[n];
        Random r = new Random(RANDOM_SEED);
        for (int i = 0; i < n; i++) {
            int memUsage = MIN_SEG_KB * 1024 + r.nextInt((MAX_SEG_KB - MIN_SEG_KB) * 1024);
            segs[i] = new MinimalSegment(memUsage);
        }
        return segs;
    }

    /**
     * Wires existing mock segments into a fresh {@link PolicySetup} for the given policy.
     * Reuses the segment objects (only {@code getSegmentId()} stubs are updated); creates
     * new {@link SegmentId} instances and a new {@link SegmentCache}.  Call
     * {@link #createSegmentPool} once and pass the result to this method for each policy
     * to avoid accumulating mock objects across the size sweep.
     *
     * @param policyIndex unused — kept for call-site readability
     * @param policy      the cache eviction policy to use
     * @param segs        pre-created mock segments (from {@link #createSegmentPool})
     * @param cacheSizeMb cache capacity in megabytes
     */
    private static PolicySetup freshSetupWithPool(int policyIndex, SegmentCachePolicy policy,
                                                  Segment[] segs, int cacheSizeMb) {
        int n = segs.length;
        SegmentCache cache = SegmentCache.newSegmentCache(cacheSizeMb, policy);
        SegmentId[] ids = new SegmentId[n];
        for (int i = 0; i < n; i++) {
            UUID uuid = UUID.randomUUID();
            long msb = uuid.getMostSignificantBits();
            long lsb = (uuid.getLeastSignificantBits() & 0x0fffffffffffffffL) | DATA_SEG_LSB_MASK;
            ids[i] = new SegmentId(SegmentStore.EMPTY_STORE, msb, lsb, cache::recordHit);
        }
        return new PolicySetup(cache, ids, segs);
    }

    /**
     * Builds a fresh {@link PolicySetup} with {@code n} segments.
     *
     * @param policyIndex unused — kept for call-site readability
     * @param policy      the cache eviction policy to use
     * @param n           number of distinct segments to create
     * @param cacheSizeMb cache capacity in megabytes
     */
    private static PolicySetup freshSetup(int policyIndex, SegmentCachePolicy policy, int n, int cacheSizeMb) {
        return freshSetupWithPool(policyIndex, policy, createSegmentPool(n), cacheSizeMb);
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

        setup.cache.cleanUp();
        long missesBase = setup.cache.getCacheStats().getMissCount();
        long evictBase = setup.cache.getCacheStats().getEvictionCount();

        for (int i = 0; i < POST_SCAN_MEASURE; i++) {
            setup.access(zipfSample(cdf, r.nextDouble()));
        }

        setup.cache.cleanUp();
        long misses = setup.cache.getCacheStats().getMissCount() - missesBase;
        long evictions = setup.cache.getCacheStats().getEvictionCount() - evictBase;
        return new long[]{POST_SCAN_MEASURE - misses, misses, evictions};
    }

    /**
     * Scenario C: multi-pass scan raises incumbent sketch frequency to
     * {@code SCAN_PASSES_C}.  During measurement, every {@code BG_SCAN_INTERVAL_C}-th
     * operation re-accesses a random scan entry (simulating search-crawler or bot
     * traffic on historical content).  This continuous re-contamination prevents the
     * Count-Min sketch from decaying, sustaining W-TinyLFU's admission freeze for the
     * entire measurement window.  Measurement is split into epochs for per-epoch
     * tracking of the divergence.
     *
     * @param epochStats collector populated with per-epoch [hits, misses, evictions]
     * @return [totalHits, totalMisses, totalEvictions] over all measurement epochs
     */
    private static long[] runColdStart(PolicySetup setup, List<long[]> epochStats) {
        Random r = new Random(RANDOM_SEED);

        for (int pass = 0; pass < SCAN_PASSES_C; pass++) {
            for (int i = 0; i < SCAN_C; i++) {
                setup.access(i);
            }
        }

        long totalHits = 0;
        long totalMisses = 0;
        long totalEvictions = 0;
        int numEpochs = MEASURE_C / EPOCH_OPS_C;

        for (int epoch = 0; epoch < numEpochs; epoch++) {
            setup.cache.cleanUp();
            long missBase = setup.cache.getCacheStats().getMissCount();
            long evictBase = setup.cache.getCacheStats().getEvictionCount();

            for (int i = 0; i < EPOCH_OPS_C; i++) {
                if (i % BG_SCAN_INTERVAL_C == 0) {
                    // bot/crawler re-accesses old content — keeps sketch counts elevated
                    setup.access(r.nextInt(SCAN_C));
                } else {
                    setup.access(SCAN_C + r.nextInt(WORKING_SET_C));
                }
            }

            setup.cache.cleanUp();
            long epochMisses = setup.cache.getCacheStats().getMissCount() - missBase;
            long epochEvictions = setup.cache.getCacheStats().getEvictionCount() - evictBase;
            long epochHits = EPOCH_OPS_C - epochMisses;

            epochStats.add(new long[]{epochHits, epochMisses, epochEvictions});
            totalHits += epochHits;
            totalMisses += epochMisses;
            totalEvictions += epochEvictions;
        }

        return new long[]{totalHits, totalMisses, totalEvictions};
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

        setup.cache.cleanUp();
        long missesBase = setup.cache.getCacheStats().getMissCount();
        long evictBase  = setup.cache.getCacheStats().getEvictionCount();

        for (int i = 0; i < MEASURE_D; i++) {
            setup.access(r.nextInt(n));
        }

        setup.cache.cleanUp();
        long misses    = setup.cache.getCacheStats().getMissCount()     - missesBase;
        long evictions = setup.cache.getCacheStats().getEvictionCount() - evictBase;
        return new long[]{MEASURE_D - misses, misses, evictions};
    }

    /**
     * Scenario E: warms caches with Zipfian over the original working set, injects a
     * concentrated burst of new segments, then measures working-set miss rate after the
     * burst subsides.  Elevated frequency counts retained by W-TinyLFU for burst items
     * may delay re-admission of hot working-set entries.
     *
     * @return [hits, misses, evictions] measured only during the post-burst Zipfian phase
     */
    private static long[] runBurstNewContent(PolicySetup setup) {
        double[] cdf = buildZipfCdf(TOTAL_SEGMENTS, ZIPF_EXPONENT);
        Random r = new Random(RANDOM_SEED);

        for (int i = 0; i < WARMUP_E; i++) {
            setup.access(zipfSample(cdf, r.nextDouble()));
        }
        // burst: access new segments (indices TOTAL_SEGMENTS .. +BURST_SIZE_E) repeatedly
        for (int b = 0; b < BURST_ACCESSES_E; b++) {
            for (int i = 0; i < BURST_SIZE_E; i++) {
                setup.access(TOTAL_SEGMENTS + i);
            }
        }

        setup.cache.cleanUp();
        long missesBase = setup.cache.getCacheStats().getMissCount();
        long evictBase = setup.cache.getCacheStats().getEvictionCount();

        for (int i = 0; i < MEASURE_E; i++) {
            setup.access(zipfSample(cdf, r.nextDouble()));
        }

        setup.cache.cleanUp();
        long misses = setup.cache.getCacheStats().getMissCount() - missesBase;
        long evictions = setup.cache.getCacheStats().getEvictionCount() - evictBase;
        return new long[]{MEASURE_E - misses, misses, evictions};
    }

    /**
     * Scenario F: alternates short sequential scans with Zipfian traffic for
     * {@code CYCLES_F} cycles, then measures steady-state Zipfian miss rate.
     * Each scan is below one TinyLFU decay period, so sketch pollution accumulates
     * across cycles rather than being cleared by a single halving event.
     *
     * @return [hits, misses, evictions] measured during the final Zipfian phase
     */
    private static long[] runPeriodicGC(PolicySetup setup) {
        double[] cdf = buildZipfCdf(TOTAL_SEGMENTS, ZIPF_EXPONENT);
        Random r = new Random(RANDOM_SEED);

        for (int c = 0; c < CYCLES_F; c++) {
            for (int i = 0; i < CYCLE_ZIPF_OPS_F; i++) {
                setup.access(zipfSample(cdf, r.nextDouble()));
            }
            int scanOffset = (c * CYCLE_SCAN_OPS_F) % (TOTAL_SEGMENTS - CYCLE_SCAN_OPS_F);
            for (int i = 0; i < CYCLE_SCAN_OPS_F; i++) {
                setup.access(scanOffset + i);
            }
        }

        setup.cache.cleanUp();
        long missesBase = setup.cache.getCacheStats().getMissCount();
        long evictBase = setup.cache.getCacheStats().getEvictionCount();

        for (int i = 0; i < MEASURE_F; i++) {
            setup.access(zipfSample(cdf, r.nextDouble()));
        }

        setup.cache.cleanUp();
        long misses = setup.cache.getCacheStats().getMissCount() - missesBase;
        long evictions = setup.cache.getCacheStats().getEvictionCount() - evictBase;
        return new long[]{MEASURE_F - misses, misses, evictions};
    }

    /**
     * Scenario G: simulates a large sequential import (each segment accessed exactly
     * once), then measures random read-back of only the most recently imported segments.
     * LRU retains the tail of the import by recency; Caffeine must rely on frequency
     * counts of 1 to keep them against higher-frequency incumbents.
     *
     * @return [hits, misses, evictions] measured during the read-back phase
     */
    private static long[] runImportThenRead(PolicySetup setup) {
        Random r = new Random(RANDOM_SEED);

        for (int i = 0; i < IMPORT_SIZE_G; i++) {
            setup.access(i);
        }

        setup.cache.cleanUp();
        long missesBase = setup.cache.getCacheStats().getMissCount();
        long evictBase = setup.cache.getCacheStats().getEvictionCount();

        int base = IMPORT_SIZE_G - RECENT_WINDOW_G;
        for (int i = 0; i < MEASURE_G; i++) {
            setup.access(base + r.nextInt(RECENT_WINDOW_G));
        }

        setup.cache.cleanUp();
        long misses = setup.cache.getCacheStats().getMissCount() - missesBase;
        long evictions = setup.cache.getCacheStats().getEvictionCount() - evictBase;
        return new long[]{MEASURE_G - misses, misses, evictions};
    }

    /**
     * Scenario H: advances a hot window across a large pool.  Each item is accessed
     * {@code WINDOW_HITS_H} times per window pass before the window moves on.  With
     * window slightly larger than cache capacity, every slide must evict some in-window
     * items; pure recency (LRU) is the theoretically optimal policy here.
     *
     * @return [hits, misses, evictions] measured after one warmup pass across half the pool
     */
    private static long[] runSlidingWindow(PolicySetup setup) {
        // warmup: advance window across the first half of the pool
        int windowStart = 0;
        while (windowStart + WINDOW_SIZE_H <= TOTAL_POOL_H / 2) {
            for (int hit = 0; hit < WINDOW_HITS_H; hit++) {
                for (int i = windowStart; i < windowStart + WINDOW_SIZE_H; i++) {
                    setup.access(i);
                }
            }
            windowStart += SLIDE_STEP_H;
        }

        setup.cache.cleanUp();
        long missesBase = setup.cache.getCacheStats().getMissCount();
        long evictBase = setup.cache.getCacheStats().getEvictionCount();

        int measured = 0;
        while (measured < MEASURE_H) {
            for (int hit = 0; hit < WINDOW_HITS_H; hit++) {
                for (int i = windowStart; i < windowStart + WINDOW_SIZE_H; i++) {
                    setup.access(i % TOTAL_POOL_H);
                    measured++;
                    if (measured >= MEASURE_H) break;
                }
                if (measured >= MEASURE_H) break;
            }
            windowStart = (windowStart + SLIDE_STEP_H) % TOTAL_POOL_H;
        }

        setup.cache.cleanUp();
        long misses = setup.cache.getCacheStats().getMissCount() - missesBase;
        long evictions = setup.cache.getCacheStats().getEvictionCount() - evictBase;
        return new long[]{MEASURE_H - misses, misses, evictions};
    }

    /**
     * Scenario I: slides a Zipfian-distributed (exponent {@code ZIPF_I_EXP}) window
     * through the pool.  The cursor advances by 1 every {@code DRIFT_I} operations so
     * older entries continuously leave the hot set.  Measurement is split into epochs
     * of {@code EPOCH_OPS_I} ops each; per-epoch [hits, misses, evictions] are appended
     * to {@code epochStats}.
     *
     * @param epochStats collector populated with per-epoch [hits, misses, evictions] arrays
     * @return [totalHits, totalMisses, totalEvictions] over all measurement epochs
     */
    private static long[] runDriftingWindow(PolicySetup setup, List<long[]> epochStats) {
        double[] cdf = buildZipfCdf(WIDTH_I, ZIPF_I_EXP);
        Random r = new Random(RANDOM_SEED);
        int cursor = 0;
        int opCount = 0;

        for (int i = 0; i < WARMUP_I; i++) {
            if (opCount % DRIFT_I == 0) {
                cursor = (cursor + 1) % POOL_I;
            }
            setup.access((cursor + zipfSample(cdf, r.nextDouble())) % POOL_I);
            opCount++;
        }

        long totalHits = 0;
        long totalMisses = 0;
        long totalEvictions = 0;
        int numEpochs = MEASURE_I / EPOCH_OPS_I;

        for (int epoch = 0; epoch < numEpochs; epoch++) {
            setup.cache.cleanUp();
            long missBase = setup.cache.getCacheStats().getMissCount();
            long evictBase = setup.cache.getCacheStats().getEvictionCount();

            for (int i = 0; i < EPOCH_OPS_I; i++) {
                if (opCount % DRIFT_I == 0) {
                    cursor = (cursor + 1) % POOL_I;
                }
                setup.access((cursor + zipfSample(cdf, r.nextDouble())) % POOL_I);
                opCount++;
            }

            setup.cache.cleanUp();
            long epochMisses = setup.cache.getCacheStats().getMissCount() - missBase;
            long epochEvictions = setup.cache.getCacheStats().getEvictionCount() - evictBase;
            long epochHits = EPOCH_OPS_I - epochMisses;

            epochStats.add(new long[]{epochHits, epochMisses, epochEvictions});
            totalHits += epochHits;
            totalMisses += epochMisses;
            totalEvictions += epochEvictions;
        }

        return new long[]{totalHits, totalMisses, totalEvictions};
    }

    /**
     * Scenario J: runs the drifting-window generator with a configurable cursor-advance
     * speed.  Warmup is discarded; only the measurement phase is reported.
     *
     * @param drift ops between each cursor advance; {@code Integer.MAX_VALUE} for stationary
     * @return [hits, misses, evictions] over the measurement phase
     */
    private static long[] runDriftVariant(PolicySetup setup, int drift) {
        double[] cdf = buildZipfCdf(WIDTH_J, ZIPF_J_EXP);
        Random r = new Random(RANDOM_SEED);
        int cursor = 0;
        int opCount = 0;

        for (int i = 0; i < WARMUP_J; i++) {
            if (drift != Integer.MAX_VALUE && opCount % drift == 0) {
                cursor = (cursor + 1) % POOL_J;
            }
            setup.access((cursor + zipfSample(cdf, r.nextDouble())) % POOL_J);
            opCount++;
        }

        setup.cache.cleanUp();
        long missesBase = setup.cache.getCacheStats().getMissCount();
        long evictBase = setup.cache.getCacheStats().getEvictionCount();

        for (int i = 0; i < MEASURE_J; i++) {
            if (drift != Integer.MAX_VALUE && opCount % drift == 0) {
                cursor = (cursor + 1) % POOL_J;
            }
            setup.access((cursor + zipfSample(cdf, r.nextDouble())) % POOL_J);
            opCount++;
        }

        setup.cache.cleanUp();
        long misses = setup.cache.getCacheStats().getMissCount() - missesBase;
        long evictions = setup.cache.getCacheStats().getEvictionCount() - evictBase;
        return new long[]{MEASURE_J - misses, misses, evictions};
    }

    /**
     * Scenario K: warms the cache with old-generation segments, then switches all
     * traffic to new-generation segment IDs (simulating Oak online compaction).
     *
     * <p>The warmup phase builds frequency counts in Caffeine's Count-Min sketch for
     * {@code OLD_GEN_K} segments.  After warmup the cache is full of old-gen entries.
     * The measurement phase accesses only the {@code NEW_GEN_K} new-gen segments
     * (indices {@code OLD_GEN_K .. OLD_GEN_K + NEW_GEN_K - 1} in the setup arrays).
     * New-gen entries start at freq=0 in the sketch; Caffeine's TinyLFU admission gate
     * rejects them until their frequency exceeds the old-gen victims in the probationary
     * queue.  Guava LRU admits new entries immediately by recency.</p>
     *
     * @param epochStats collector populated with per-epoch [hits, misses, evictions]
     * @return [totalHits, totalMisses, totalEvictions] over all measurement epochs
     */
    private static long[] runCompactionColdStart(PolicySetup setup, List<long[]> epochStats) {
        double[] oldCdf = buildZipfCdf(OLD_GEN_K, ZIPF_EXPONENT);
        double[] newCdf = buildZipfCdf(NEW_GEN_K, ZIPF_K_NEW_EXP);
        Random r = new Random(RANDOM_SEED);

        // Phase 1: warm cache with old-gen segments; builds sketch frequency counts
        for (int i = 0; i < WARMUP_K; i++) {
            setup.access(zipfSample(oldCdf, r.nextDouble()));
        }
        // Phase 2: compaction — all traffic switches to new-gen (freq=0 in sketch)
        long totalHits = 0;
        long totalMisses = 0;
        long totalEvictions = 0;
        int numEpochs = MEASURE_K / EPOCH_OPS_K;

        for (int epoch = 0; epoch < numEpochs; epoch++) {
            setup.cache.cleanUp();
            long missBase = setup.cache.getCacheStats().getMissCount();
            long evictBase = setup.cache.getCacheStats().getEvictionCount();

            for (int i = 0; i < EPOCH_OPS_K; i++) {
                setup.access(OLD_GEN_K + zipfSample(newCdf, r.nextDouble()));
            }

            setup.cache.cleanUp();
            long epochMisses = setup.cache.getCacheStats().getMissCount() - missBase;
            long epochEvictions = setup.cache.getCacheStats().getEvictionCount() - evictBase;
            long epochHits = EPOCH_OPS_K - epochMisses;

            epochStats.add(new long[]{epochHits, epochMisses, epochEvictions});
            totalHits += epochHits;
            totalMisses += epochMisses;
            totalEvictions += epochEvictions;
        }

        return new long[]{totalHits, totalMisses, totalEvictions};
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

    // -----------------------------------------------------------------------
    // MinimalSegment — lightweight Segment substitute, avoids Mockito overhead
    // -----------------------------------------------------------------------

    /**
     * Minimal {@link Segment} subclass that stores only a pre-set memory-usage value.
     * Uses the package-visible four-arg constructor with empty stubs for all interfaces,
     * so no ByteBuddy proxy class is generated and no Mockito invocation tracking is kept.
     * Memory cost is ~50 bytes vs. several KB per Mockito mock.
     */
    private static final class MinimalSegment extends Segment {

        private static final SegmentData EMPTY_DATA = new SegmentData() {
            @Override public byte getVersion()                              { return (byte) 13; }
            @Override public String getSignature()                          { return ""; }
            @Override public int getFullGeneration()                        { return 0; }
            @Override public boolean isCompacted()                          { return false; }
            @Override public int getGeneration()                            { return 0; }
            @Override public int getSegmentReferencesCount()                { return 0; }
            @Override public int getRecordReferencesCount()                 { return 0; }
            @Override public int getRecordReferenceNumber(int i)            { return 0; }
            @Override public byte getRecordReferenceType(int i)             { return 0; }
            @Override public int getRecordReferenceOffset(int i)            { return 0; }
            @Override public long getSegmentReferenceMsb(int i)             { return 0; }
            @Override public long getSegmentReferenceLsb(int i)             { return 0; }
            @Override public byte readByte(int offset)                      { return 0; }
            @Override public int readInt(int offset)                        { return 0; }
            @Override public short readShort(int offset)                    { return 0; }
            @Override public long readLong(int offset)                      { return 0; }
            @Override public Buffer readBytes(int offset, int size)         { return null; }
            @Override public int size()                                     { return 0; }
            @Override public void hexDump(OutputStream stream)              {}
            @Override public void binDump(OutputStream stream)              {}
            @Override public int estimateMemoryUsage()                      { return 0; }
        };

        private static final SegmentReferences EMPTY_REFS = new SegmentReferences() {
            @Override
            public SegmentId getSegmentId(int reference) {
                throw new UnsupportedOperationException();
            }
            @Override
            public Iterator<SegmentId> iterator() {
                return Collections.emptyIterator();
            }
        };

        private final int memUsage;

        MinimalSegment(int memUsage) {
            super(SegmentId.NULL, EMPTY_DATA, RecordNumbers.EMPTY_RECORD_NUMBERS, EMPTY_REFS);
            this.memUsage = memUsage;
        }

        @Override
        public int estimateMemoryUsage() {
            return memUsage;
        }
    }
}
