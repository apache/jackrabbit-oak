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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.benchmark.DocumentCacheBenchmark.Configuration;
import org.apache.jackrabbit.oak.benchmark.DocumentCacheBenchmark.Context;
import org.apache.jackrabbit.oak.benchmark.DocumentCacheBenchmark.Policy;
import org.apache.jackrabbit.oak.benchmark.DocumentCacheBenchmark.Result;
import org.apache.jackrabbit.oak.benchmark.DocumentCacheBenchmark.Scenario;
import org.apache.jackrabbit.oak.benchmark.DocumentCacheBenchmark.State;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Tests configuration and operation accounting in {@link DocumentCacheBenchmark}. */
public class DocumentCacheBenchmarkTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private final Map<String, String> previous = new HashMap<>();
    private static final String[] PROPERTIES = {"document.cache.entries", "document.cache.operations",
            "document.cache.threads", "document.cache.warmup", "document.cache.policies", "document.cache.scenarios",
            "document.cache.states", "document.cache.persistent.enabled", "document.cache.caffeine.maximumWeightMultiplier"};

    @Before
    public void captureProperties() {
        for (String name : PROPERTIES) { previous.put(name, System.getProperty(name)); System.clearProperty(name); }
    }

    @After
    public void restoreProperties() {
        previous.forEach((name, value) -> {
            if (value == null) { System.clearProperty(name); } else { System.setProperty(name, value); }
        });
    }

    /** Small smoke runs exercise every available production policy. */
    @Test
    public void reportRunsAllProductionPoliciesWithFixedSmokeParameters() {
        System.setProperty("document.cache.entries", "32");
        System.setProperty("document.cache.operations", "128");
        System.setProperty("document.cache.warmup", "64");
        System.setProperty("document.cache.threads", "3");
        boolean caffeine = Boolean.getBoolean("oak.documentMK.caffeineCache");
        boolean async = Boolean.getBoolean("oak.documentMK.asyncCacheMaintenance");
        String policies = caffeine ? (async ? "CAFFEINE_SYNC,CAFFEINE_ASYNC" : "CAFFEINE_SYNC") : "CACHE_LIRS";
        System.setProperty("document.cache.policies", policies);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream previousOutput = System.out;
        try (PrintStream capture = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            System.setOut(capture);
            new DocumentCacheBenchmark().run(Collections.emptyList());
        } finally {
            System.setOut(previousOutput);
        }
        String report = output.toString(StandardCharsets.UTF_8);
        Assert.assertTrue(report.contains("sampledP95ns"));
        Assert.assertTrue(report.contains("persistentWrites=ASYNC"));
        long rows = report.lines().filter(line -> line.matches("(STEADY_STATE|CHURN|INVALIDATION|CONCURRENT) .*" )).count();
        Assert.assertEquals(caffeine && async ? 80 : 40, rows);
    }

    /** Reject unknown labels and incompatible policy selections. */
    @Test
    public void selectorsRejectUnknownAndIncompatiblePolicies() {
        Configuration defaults = DocumentCacheBenchmark.configuration();
        Assert.assertEquals(10_000, defaults.entries());
        Assert.assertFalse(defaults.policies().isEmpty());
        System.setProperty("document.cache.scenarios", " churn ");
        System.setProperty("document.cache.states", "cold");
        Assert.assertEquals(Set.of(Scenario.CHURN), DocumentCacheBenchmark.configuration().scenarios());
        Assert.assertEquals(Set.of(State.COLD), DocumentCacheBenchmark.configuration().states());
        System.setProperty("document.cache.scenarios", "unknown");
        Assert.assertThrows(IllegalArgumentException.class, DocumentCacheBenchmark::configuration);
        System.clearProperty("document.cache.scenarios");
        boolean caffeine = Boolean.getBoolean("oak.documentMK.caffeineCache");
        System.setProperty("document.cache.policies", caffeine ? "CACHE_LIRS" : "CAFFEINE_ASYNC");
        Assert.assertThrows(IllegalArgumentException.class, DocumentCacheBenchmark::configuration);
        System.setProperty("document.cache.policies", "CACHE_LIRS,CAFFEINE_ASYNC");
        Assert.assertThrows(IllegalArgumentException.class, DocumentCacheBenchmark::configuration);
    }

    /** Reject weight scaling that underflows or exceeds the supported range. */
    @Test
    public void scaledWeightsOutsideSupportedRangeAreRejected() {
        for (double multiplier : new double[] {.00001, Double.MAX_VALUE}) {
            Configuration config = config(1, 1, 1, 0, multiplier);
            IllegalArgumentException failure = Assert.assertThrows(IllegalArgumentException.class,
                    () -> DocumentCacheBenchmark.runScenario(config, Policy.CAFFEINE_SYNC, Scenario.STEADY_STATE, State.COLD, 1));
            Assert.assertTrue(failure.getMessage().contains("CAFFEINE_SYNC/STEADY_STATE/COLD"));
            Assert.assertTrue(failure.getCause() instanceof IllegalArgumentException);
        }
    }

    /** Count every operation across warm and cold workloads. */
    @Test
    public void productionCachesAccountForAllWorkloadsAndBothCacheStates() {
        Set<Policy> policies = DocumentCacheBenchmark.configuration().policies();
        Configuration config = new Configuration(64, 137, 3, 100, 1, true, policies,
                Set.of(Scenario.values()), Set.of(State.values()));
        for (Policy policy : policies) {
            for (Scenario scenario : Scenario.values()) {
                for (State state : State.values()) {
                    Result result = DocumentCacheBenchmark.runScenario(config, policy, scenario, state, 128);
                    Assert.assertEquals(137, result.operations());
                    Assert.assertTrue(result.elapsedNanos() > 0);
                    Assert.assertTrue(result.p95Nanos() > 0);
                    Assert.assertTrue(result.stats().missCount() >= result.backendLoads());
                    Assert.assertEquals(result.backendLoads(), result.stats().loadSuccessCount());
                }
            }
        }
    }

    /** Disabling persistence still uses the production cache builder. */
    @Test
    public void cacheWithoutPersistenceAlsoUsesProductionConstruction() {
        Policy policy = Boolean.getBoolean("oak.documentMK.caffeineCache")
                ? Policy.CAFFEINE_SYNC : Policy.CACHE_LIRS;
        Configuration config = new Configuration(64, 100, 1, 0, 1, false, Set.of(policy),
                Set.of(Scenario.STEADY_STATE), Set.of(State.COLD));
        Result result = DocumentCacheBenchmark.runScenario(config, policy, Scenario.STEADY_STATE, State.COLD, 10);
        Assert.assertEquals(100, result.operations());
        Assert.assertTrue(result.backendLoads() > 0);
    }

    /** Reject invalid benchmark sizes before creating caches. */
    @Test
    public void invalidSizesAndMultipliersAreRejected() {
        for (int entries : new int[] {0, -1, Integer.MAX_VALUE}) {
            Assert.assertThrows(IllegalArgumentException.class, () -> config(entries, 1, 1, 0, 1));
        }
        Assert.assertThrows(IllegalArgumentException.class, () -> config(1, 0, 1, 0, 1));
        Assert.assertThrows(IllegalArgumentException.class, () -> config(1, 1, 0, 0, 1));
        Assert.assertThrows(IllegalArgumentException.class, () -> config(1, 1, 1, -1, 1));
        for (double multiplier : new double[] {0, -1, Double.NaN, Double.POSITIVE_INFINITY}) {
            Assert.assertThrows(IllegalArgumentException.class, () -> config(1, 1, 1, 0, multiplier));
        }
        Assert.assertThrows(IllegalArgumentException.class, () -> new Configuration(1, 1, 1, 0, 1, false,
                Set.of(), Set.of(Scenario.STEADY_STATE), Set.of(State.WARM)));
    }

    /** Calculate p95 consistently for empty and unsorted samples. */
    @Test
    public void percentileIsCalculatedFromSortedSamples() {
        Assert.assertEquals(0, DocumentCacheBenchmark.percentile95(Collections.emptyList()));
        Assert.assertEquals(5, DocumentCacheBenchmark.percentile95(Arrays.asList(2L, 5L, 1L, 4L, 3L)));
        Assert.assertEquals(3, DocumentCacheBenchmark.percentile95(Collections.singletonList(3L)));
    }

    /** Warmup is excluded, cold clears the cache, and invalidation forces new loads. */
    @Test
    public void singleKeyWorkloadsHaveDistinctMeasuredLoads() {
        for (boolean persistent : new boolean[] {false, true}) {
            Policy policy = currentPolicy();
            Configuration config = new Configuration(64, 33, 1, 100, 1, persistent, Set.of(policy),
                    Set.of(Scenario.STEADY_STATE, Scenario.INVALIDATION), Set.of(State.values()));
            assertLoads(0, DocumentCacheBenchmark.runScenario(config, policy, Scenario.STEADY_STATE, State.WARM, 1));
            assertLoads(1, DocumentCacheBenchmark.runScenario(config, policy, Scenario.STEADY_STATE, State.COLD, 1));
            assertLoads(3, DocumentCacheBenchmark.runScenario(config, policy, Scenario.INVALIDATION, State.WARM, 1));
        }
    }

    /** The first churn transition introduces the second key without capacity evictions. */
    @Test
    public void churnMovesItsWindowAtOperation1000() {
        Policy policy = currentPolicy();
        Configuration config = new Configuration(64, 1001, 1, 0, 1, false, Set.of(policy),
                Set.of(Scenario.CHURN), Set.of(State.COLD));
        assertLoads(2, DocumentCacheBenchmark.runScenario(config, policy, Scenario.CHURN, State.COLD, 2));
    }

    /** Sampling includes ordinary reads as well as all periodic invalidation phases. */
    @Test
    public void samplingCoversEveryInvalidationPhaseAndPartialBlocks() {
        SplittableRandom random = new SplittableRandom(42);
        BitSet phases = new BitSet(16);
        for (int block = 0; block < 1024; block++) {
            int start = block * 64;
            int sample = DocumentCacheBenchmark.samplePosition(start, 65536, random);
            Assert.assertTrue(sample >= start && sample < start + 64);
            phases.set(sample % 16);
        }
        Assert.assertEquals(16, phases.cardinality());
        Assert.assertEquals(64, DocumentCacheBenchmark.samplePosition(64, 65, random));
    }

    /** Startup-selected builder flags cannot silently change labels or persistence settings. */
    @Test
    public void inheritedStartupSettingsAreCheckedInFreshProcesses() throws Exception {
        File inherited = folder.newFolder();
        File output = folder.newFile();
        runIsolated(output, "no-persistence", "-Doak.documentMK.persCache=" + inherited.getAbsolutePath(),
                "-Doak.documentMK.caffeineCache=true");
        Assert.assertArrayEquals(new String[0], inherited.list());
        runIsolated(output, "reject-guava", "-Doak.documentMK.caffeineCache=false",
                "-Doak.documentMK.guavaCache=true");
        Assert.assertFalse(Files.readString(output.toPath()).lines().anyMatch(line -> line.startsWith("STEADY_STATE ")));
    }

    /** An ASYNC label must match the effective policy, including startup-only toggle state. */
    @Test
    public void asyncPolicyRequiresAnEnabledFeatureInFreshProcesses() throws Exception {
        File output = folder.newFile();
        runIsolated(output, "reject-async", "-Doak.documentMK.caffeineCache=true",
                "-Doak.documentMK.asyncCacheMaintenance=false");
        runIsolated(output, "sync-with-async-feature", "-Doak.documentMK.caffeineCache=true",
                "-Doak.documentMK.asyncCacheMaintenance=true");
        runIsolated(output, "accept-async", "-Doak.documentMK.caffeineCache=true",
                "-Doak.documentMK.asyncCacheMaintenance=true");
    }

    /** An interrupted worker leaves its interruption flag set and stops before another read. */
    @Test
    public void interruptedWorkerStopsBeforeReadingTheCache() throws Exception {
        boolean incoming = Thread.interrupted();
        Policy policy = currentPolicy();
        Configuration config = config(64, 100, 1, 0, 1);
        try (Context context = DocumentCacheBenchmark.createContext(config, policy, Scenario.STEADY_STATE)) {
            PathRev key = new PathRev(Path.ROOT, new RevisionVector(new Revision(1, 0, 1)));
            Thread.currentThread().interrupt();
            Assert.assertThrows(IllegalStateException.class, () -> DocumentCacheBenchmark.runTask(context,
                    Collections.singletonList(key), Scenario.STEADY_STATE, 100, 42));
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            if (incoming) { Thread.currentThread().interrupt(); }
        }
    }

    /** Store disposal waits for cancelled workers and preserves the closing thread's flag. */
    @Test
    public void cleanupWaitsForCancelledWorkersBeforeDisposal() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean stopped = new AtomicBoolean();
        AtomicBoolean disposed = new AtomicBoolean();
        MemoryDocumentStore documents = new MemoryDocumentStore() {
            @Override
            public void dispose() {
                Assert.assertTrue("Store disposed while a worker was running", stopped.get());
                disposed.set(true);
                super.dispose();
            }
        };
        DocumentNodeStore store = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .setDocumentStore(documents).setPersistentCache(null).setAsyncDelay(0).build();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Context context = new Context(store, store.getNodeCache(), null, executor, 1);
        executor.submit(() -> {
            entered.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException expected) {
                cancelled.countDown();
            }
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            } finally {
                stopped.set(true);
            }
        });
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread closing = new Thread(() -> {
            Thread.currentThread().interrupt();
            try { context.close(); }
            catch (Exception e) { failure.set(e); }
        });
        try {
            Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
            closing.start();
            Assert.assertTrue(cancelled.await(5, TimeUnit.SECONDS));
            Assert.assertFalse(disposed.get());
            release.countDown();
            closing.join(5000);
            Assert.assertFalse(closing.isAlive());
            Assert.assertNull(failure.get());
            Assert.assertTrue(disposed.get());
            Assert.assertTrue(closing.isInterrupted());
        } finally {
            release.countDown();
            if (closing.getState() == Thread.State.NEW) { closing.start(); }
            closing.join(5000);
        }
    }

    /** A failed directory cleanup must not replace the store-disposal failure. */
    @Test
    public void cleanupPreservesDisposalFailureAndSuppressesDeletionFailure() throws Exception {
        DocumentNodeStore store = Mockito.mock(DocumentNodeStore.class);
        IllegalStateException disposal = new IllegalStateException("disposal failed");
        IOException deletion = new IOException("deletion failed");
        Mockito.doThrow(disposal).when(store).dispose();
        File directory = folder.newFolder();
        Context context = new Context(store, null, directory, null, 1);
        try (MockedStatic<FileUtils> files = Mockito.mockStatic(FileUtils.class)) {
            files.when(() -> FileUtils.deleteDirectory(directory)).thenThrow(deletion);
            Assert.assertSame(disposal, Assert.assertThrows(IllegalStateException.class, context::close));
            Assert.assertArrayEquals(new Throwable[] {deletion}, disposal.getSuppressed());
            files.verify(() -> FileUtils.deleteDirectory(directory));
        }
    }

    /** A direct-cache cleanup error includes the policy, workload and cache state. */
    @Test
    public void failedScenarioIncludesItsPolicyAndState() throws Exception {
        IOException deletion = new IOException("deletion failed");
        Configuration config = new Configuration(8, 1, 1, 0, 1, true, Set.of(currentPolicy()),
                Set.of(Scenario.STEADY_STATE), Set.of(State.COLD));
        try (MockedStatic<FileUtils> files = Mockito.mockStatic(FileUtils.class, Mockito.CALLS_REAL_METHODS)) {
            files.when(() -> FileUtils.deleteDirectory(Mockito.any(File.class))).thenAnswer(invocation -> {
                invocation.callRealMethod();
                throw deletion;
            });
            IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                    () -> DocumentCacheBenchmark.runScenario(config, currentPolicy(), Scenario.STEADY_STATE, State.COLD, 1));
            Assert.assertTrue(failure.getMessage().contains(currentPolicy() + "/STEADY_STATE/COLD"));
            Assert.assertSame(deletion, failure.getCause());
        }
    }

    /** Checks startup-only settings before any builder class is initialized in this JVM. */
    public static void main(String[] args) throws Exception {
        if (args.length != 1) { throw new IllegalArgumentException("Expected one startup-check name"); }
        if (args[0].equals("no-persistence")) {
            Configuration config = config(64, 33, 1, 1, 1);
            try (Context context = DocumentCacheBenchmark.createContext(config, Policy.CAFFEINE_SYNC, Scenario.STEADY_STATE)) {
                Assert.assertNull(PersistentCache.getPersistentCacheStats(context.cache()));
            }
        } else if (args[0].equals("reject-guava")) {
            IllegalArgumentException failure = Assert.assertThrows(IllegalArgumentException.class,
                    () -> new DocumentCacheBenchmark().run(Collections.emptyList()));
            Assert.assertTrue(failure.getMessage().contains("oak.documentMK.guavaCache"));
        } else if (args[0].equals("reject-async")) {
            IllegalArgumentException failure = Assert.assertThrows(IllegalArgumentException.class,
                    () -> DocumentCacheBenchmark.createContext(config(64, 33, 1, 1, 1),
                            Policy.CAFFEINE_ASYNC, Scenario.STEADY_STATE));
            Assert.assertTrue(failure.getMessage().contains("oak.documentMK.asyncCacheMaintenance"));
        } else if (args[0].equals("sync-with-async-feature")) {
            try (Context context = DocumentCacheBenchmark.createContext(
                    new Configuration(64, 33, 1, 1, 1, true, Set.of(Policy.CAFFEINE_SYNC),
                            Set.of(Scenario.STEADY_STATE), Set.of(State.WARM)),
                    Policy.CAFFEINE_SYNC, Scenario.STEADY_STATE)) {
                Assert.assertEquals("NodeCache", context.cache().getClass().getSimpleName());
            }
        } else if (args[0].equals("accept-async")) {
            try (Context context = DocumentCacheBenchmark.createContext(
                    new Configuration(64, 33, 1, 1, 1, true, Set.of(Policy.CAFFEINE_SYNC),
                            Set.of(Scenario.STEADY_STATE), Set.of(State.WARM)),
                    Policy.CAFFEINE_ASYNC, Scenario.STEADY_STATE)) {
                Assert.assertSame(context.store().getNodeCache(), context.cache());
                Assert.assertEquals("AsyncNodeCache", context.cache().getClass().getSimpleName());
            }
        } else { throw new IllegalArgumentException("Unknown startup check: " + args[0]); }
    }

    private void runIsolated(File output, String check, String... properties) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(new File(System.getProperty("java.home"), "bin/java").getAbsolutePath());
        Collections.addAll(command, properties);
        Collections.addAll(command, "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                DocumentCacheBenchmarkTest.class.getName(), check);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output).start();
        try {
            Assert.assertTrue("Startup check did not finish", process.waitFor(30, TimeUnit.SECONDS));
            Assert.assertEquals(Files.readString(output.toPath()), 0, process.exitValue());
        } finally { process.destroyForcibly(); }
    }

    private static Policy currentPolicy() {
        return Boolean.getBoolean("oak.documentMK.caffeineCache")
                ? Policy.CAFFEINE_SYNC : Policy.CACHE_LIRS;
    }

    private static void assertLoads(long expected, Result result) {
        Assert.assertEquals(expected, result.backendLoads());
        Assert.assertEquals(expected, result.stats().loadSuccessCount());
    }

    private static Configuration config(int entries, int operations, int threads, int warmup, double multiplier) {
        return new Configuration(entries, operations, threads, warmup, multiplier, false, Set.of(Policy.CAFFEINE_SYNC),
                Set.of(Scenario.STEADY_STATE), Set.of(State.WARM));
    }
}
