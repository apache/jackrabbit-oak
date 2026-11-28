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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.benchmark.DocumentCacheBenchmark.Policy;
import org.apache.jackrabbit.oak.benchmark.DocumentCacheRepositoryBenchmark.Result;
import org.apache.jackrabbit.oak.benchmark.DocumentCacheRepositoryBenchmark.Scenario;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Tests real repository workload accounting and data checks in {@link DocumentCacheRepositoryBenchmark}. */
public class DocumentCacheRepositoryBenchmarkTest {
    /** Reads and traversal retain the populated repository across memory and disk configurations. */
    @Test
    public void realReadAndTraversalWorkloadsRetainAllNodes() {
        for (boolean persistent : new boolean[] {false, true}) {
            for (Scenario scenario : new Scenario[] {Scenario.POINT_READ, Scenario.CHILDREN_SCAN, Scenario.REOPEN_READ}) {
                Result result = run(scenario, 1003, 21, 3, persistent);
                Assert.assertEquals(21, result.operations());
                Assert.assertEquals(21, result.reads());
                Assert.assertEquals(0, result.writes());
                Assert.assertEquals(0, result.mergeNanos() + result.diffNanos() + result.readbackNanos());
                Assert.assertTrue(result.elapsedNanos() > 0);
                if (scenario == Scenario.REOPEN_READ) {
                    Assert.assertTrue("Reopen must load from disk or the document backend",
                            result.documentFinds() > 0 || persistent && result.persistentHits() > 0);
                }
            }
        }
    }

    /** Scanning the partial final group checks its exact size and detects a missing child. */
    @Test
    public void childrenScanChecksThePartialFinalGroup() throws Exception {
        int width = Integer.getInteger("document.repository.childrenPerGroup", 100);
        Assume.assumeTrue("A partial group requires width greater than one", width > 1);
        int partial = Math.min(3, width - 1);
        int nodes = width + partial;
        long seed = DocumentCacheBenchmark.RANDOM_SEED;
        while (new SplittableRandom(seed).nextInt(nodes) < width) { seed++; }
        long partialGroupSeed = seed;
        DocumentNodeStore store = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .setPersistentCache(null).setAsyncDelay(0).build();
        try {
            DocumentCacheRepositoryBenchmark.populate(store, nodes);
            Assert.assertEquals(1, DocumentCacheRepositoryBenchmark.runTask(
                    store, Scenario.CHILDREN_SCAN, nodes, 1, partialGroupSeed).reads());
            NodeBuilder root = store.getRoot().builder();
            root.child("benchmark").child("bucket-0").child("group-1").child("node-" + width).remove();
            store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                    () -> DocumentCacheRepositoryBenchmark.runTask(store, Scenario.CHILDREN_SCAN, nodes, 1, partialGroupSeed));
            Assert.assertEquals("Traversal lost children: " + (partial - 1), failure.getMessage());
        } finally {
            store.dispose();
        }
    }

    /** Population spreads groups across buckets instead of creating an unbounded sibling list. */
    @Test
    public void populationBoundsFolderWidthAndPreservesPartialLastGroup() throws Exception {
        int width = Integer.getInteger("document.repository.childrenPerGroup", 100);
        int firstInSecondBucket = Math.multiplyExact(width, 100);
        DocumentNodeStore store = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .setPersistentCache(null).setAsyncDelay(0).build();
        try {
            DocumentCacheRepositoryBenchmark.populate(store, firstInSecondBucket + 3);
            Assert.assertEquals(2, store.getRoot().getChildNode("benchmark").getChildNodeCount(3));
            Assert.assertEquals(100, store.getRoot().getChildNode("benchmark")
                    .getChildNode("bucket-0").getChildNodeCount(101));
            Assert.assertEquals(width, store.getRoot().getChildNode("benchmark")
                    .getChildNode("bucket-0").getChildNode("group-0").getChildNodeCount(width + 1));
            Assert.assertTrue(store.getRoot().getChildNode("benchmark").getChildNode("bucket-1")
                    .getChildNode("group-" + (firstInSecondBucket + 2) / width)
                    .getChildNode("node-" + (firstInSecondBucket + 2)).getProperty("value") != null);
        } finally {
            store.dispose();
        }
    }

    /** Commit and diff workloads assert the property written by each actual merge. */
    @Test
    public void writesCommitAndCompareRealNodeStates() {
        Result result = run(Scenario.COMMIT_DIFF, 10, 7, 1, true);
        Assert.assertTrue(result.mergeNanos() > 0);
        Assert.assertTrue(result.diffNanos() > 0);
        Assert.assertTrue(result.readbackNanos() > 0);
        Assert.assertTrue(result.mergeNanos() + result.diffNanos() + result.readbackNanos() <= result.elapsedNanos());
        Assert.assertTrue(result.localDiffHits() + result.localDiffMisses() > 0);
        Assert.assertEquals(0, result.reads());
        Assert.assertEquals(7, result.writes());
        Assert.assertTrue(result.documentFinds() > 0);
    }

    /** Normal merges verify the saved value without an additional group-diff request. */
    @Test
    public void commitsReadBackEveryValueWithoutForcedDiff() {
        Result result = run(Scenario.COMMIT_ONLY, 10, 7, 1, true);
        Assert.assertTrue(result.mergeNanos() > 0);
        Assert.assertEquals(0, result.diffNanos());
        Assert.assertTrue(result.readbackNanos() > 0);
        Assert.assertTrue(result.mergeNanos() + result.readbackNanos() <= result.elapsedNanos());
        Assert.assertEquals(7, result.operations());
        Assert.assertEquals(7, result.writes());
        Assert.assertEquals(0, result.reads());
        Assert.assertTrue(result.documentFinds() > 0);
    }

    /** Uneven worker batches preserve total work while real reads overlap commits. */
    @Test
    public void concurrentMixedWorkloadAccountsForEveryReadAndWrite() {
        Result result = run(Scenario.CONCURRENT_MIXED, 32, 67, 3, true);
        Assert.assertEquals(67, result.reads() + result.writes());
        Assert.assertEquals(23, result.writes());
        Assert.assertTrue(result.documentFinds() > 0);
    }

    /** Concurrent writers use distinct nodes so benchmarked merges have no application conflicts. */
    @Test
    public void concurrentWritersCommitEveryDisjointUpdate() {
        Result result = run(Scenario.CONCURRENT_WRITERS, 32, 67, 3, true);
        Assert.assertEquals(67, result.writes());
        Assert.assertEquals(0, result.reads());
    }

    /** A reopened cache can satisfy real repository reads from data written before disposal. */
    @Test
    public void reopenReadsActuallyHitPersistentCache() {
        Result result = run(Scenario.REOPEN_READ, 1000, 1000, 1, true);
        Assert.assertTrue("Reopen must exercise disk hits, not just another backend load", result.persistentHits() > 0);
    }

    /** Invalid sizes are rejected before storage or workers are created. */
    @Test
    public void nonPositiveSizesAreRejected() {
        Assert.assertThrows(IllegalArgumentException.class, () -> run(Scenario.POINT_READ, 0, 1, 1, false));
        Assert.assertThrows(IllegalArgumentException.class, () -> run(Scenario.POINT_READ, 1, 0, 1, false));
        Assert.assertThrows(IllegalArgumentException.class, () -> run(Scenario.POINT_READ, 1, 1, 0, false));
        Assert.assertThrows(IllegalArgumentException.class, () -> DocumentCacheRepositoryBenchmark.runScenario(
                policy(), Scenario.POINT_READ, 1, 1, 0, 1, false));
        Assert.assertThrows(IllegalArgumentException.class, () -> DocumentCacheRepositoryBenchmark.runScenario(
                policy(), Scenario.POINT_READ, 1, 1, 1, 1, false, -1));
    }

    /** Duration-based runs keep doing real work after their minimum operation count is reached. */
    @Test
    public void timedWorkloadsMeasureCompletedOperationsForTheWholeInterval() {
        long duration = TimeUnit.MILLISECONDS.toNanos(20);
        for (Scenario scenario : new Scenario[] {Scenario.POINT_READ, Scenario.COMMIT_ONLY,
                Scenario.COMMIT_DIFF, Scenario.CONCURRENT_MIXED, Scenario.CONCURRENT_WRITERS, Scenario.REOPEN_READ}) {
            Result result = DocumentCacheRepositoryBenchmark.runScenario(policy(), scenario, 32, 3, 1, 3, true, duration);
            Assert.assertTrue("measurement must cover its minimum duration", result.elapsedNanos() >= duration);
            Assert.assertTrue("duration-based run must continue beyond the minimum work", result.operations() > 3);
            Assert.assertEquals(result.operations(), result.reads() + result.writes());
            if (scenario == Scenario.COMMIT_ONLY) { Assert.assertEquals(0, result.diffNanos()); }
            if (scenario == Scenario.CONCURRENT_MIXED) {
                Assert.assertTrue(result.reads() > 0);
                Assert.assertTrue(result.writes() > 0);
            }
        }
    }

    /** Reports only completed real repository operations and validates configurable sizes. */
    @Test
    public void reportRunsAllRealScenariosAndRejectsInvalidProperties() {
        Map<String, String> previous = new HashMap<>();
        String[] names = {"document.repository.nodes", "document.repository.operations", "document.repository.cacheMB",
                "document.cache.threads", "document.repository.backgroundMillis", "document.repository.durationSeconds",
                "document.repository.warmupSeconds", "document.repository.scenarios"};
        for (String name : names) { previous.put(name, System.getProperty(name)); }
        PrintStream output = System.out;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (PrintStream capture = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            System.setProperty(names[0], "32");
            System.setProperty(names[1], "21");
            System.setProperty(names[2], "1");
            System.setProperty(names[3], "3");
            System.setProperty(names[4], "0");
            System.setProperty(names[5], "0");
            System.setProperty(names[6], "0");
            System.clearProperty(names[7]);
            System.setOut(capture);
            new DocumentCacheRepositoryBenchmark().run(Collections.emptyList());
            long rows = bytes.toString(StandardCharsets.UTF_8).lines()
                    .filter(line -> line.matches("(POINT_READ|CHILDREN_SCAN|COMMIT_ONLY|COMMIT_DIFF|CONCURRENT_MIXED|CONCURRENT_WRITERS|REOPEN_READ) .*"))
                    .count();
            Assert.assertEquals(7L * DocumentCacheBenchmark.configuration().policies().size(), rows);
            bytes.reset();
            System.setProperty(names[6], "1");
            System.setProperty(names[7], "POINT_READ");
            new DocumentCacheRepositoryBenchmark().run(Collections.emptyList());
            String warmedReport = bytes.toString(StandardCharsets.UTF_8);
            Assert.assertEquals(DocumentCacheBenchmark.configuration().policies().size(), warmedReport.lines()
                    .filter(line -> line.startsWith("POINT_READ ")).count());
            warmedReport.lines().filter(line -> line.startsWith("POINT_READ "))
                    .forEach(line -> Assert.assertEquals("warm-up work must not appear in the report", "21", line.split(" ")[3]));
            System.setProperty(names[6], "0");
            System.setProperty(names[5], "-1");
            Assert.assertThrows(IllegalArgumentException.class,
                    () -> new DocumentCacheRepositoryBenchmark().run(Collections.emptyList()));
            System.setProperty(names[5], "0");
            System.setProperty(names[6], "-1");
            Assert.assertThrows(IllegalArgumentException.class,
                    () -> new DocumentCacheRepositoryBenchmark().run(Collections.emptyList()));
            System.setProperty(names[6], "0");
            System.setProperty(names[0], "0");
            Assert.assertThrows(IllegalArgumentException.class,
                    () -> new DocumentCacheRepositoryBenchmark().run(Collections.emptyList()));
            System.setProperty(names[0], "32");
            System.setProperty(names[4], "-1");
            Assert.assertThrows(IllegalArgumentException.class,
                    () -> new DocumentCacheRepositoryBenchmark().run(Collections.emptyList()));
        } finally {
            System.setOut(output);
            previous.forEach((name, value) -> {
                if (value == null) { System.clearProperty(name); } else { System.setProperty(name, value); }
            });
        }
    }

    /** Selection accepts named workloads and rejects empty or unknown scenario lists. */
    @Test
    public void scenarioSelectionRejectsInvalidWorkloads() {
        String name = "document.repository.scenarios";
        String previous = System.getProperty(name);
        try {
            System.setProperty(name, " point_read , COMMIT_ONLY ");
            Assert.assertEquals(2, DocumentCacheRepositoryBenchmark.selectedScenarios().size());
            Assert.assertTrue(DocumentCacheRepositoryBenchmark.selectedScenarios().contains(Scenario.COMMIT_ONLY));
            System.setProperty(name, ",");
            Assert.assertThrows(IllegalArgumentException.class, DocumentCacheRepositoryBenchmark::selectedScenarios);
            System.setProperty(name, "unknown");
            Assert.assertThrows(IllegalArgumentException.class, DocumentCacheRepositoryBenchmark::selectedScenarios);
        } finally {
            if (previous == null) { System.clearProperty(name); } else { System.setProperty(name, previous); }
        }
    }

    /** Interrupted workers stop before attempting another repository read. */
    @Test
    public void interruptedTaskStopsWithoutClearingItsFlag() {
        boolean incoming = Thread.interrupted();
        DocumentNodeStore store = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .setPersistentCache(null).setAsyncDelay(0).build();
        try {
            Thread.currentThread().interrupt();
            Assert.assertThrows(IllegalStateException.class, () -> DocumentCacheRepositoryBenchmark.runTask(
                    store, Scenario.POINT_READ, 1, 1, DocumentCacheBenchmark.RANDOM_SEED));
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            store.dispose();
            if (incoming) { Thread.currentThread().interrupt(); }
        }
    }

    /** A requested ASYNC workload fails before setup unless both startup opt-ins are enabled. */
    @Test
    public void disabledFeatureCannotProduceAnAsyncRepositoryLabel() {
        Assume.assumeFalse(Boolean.getBoolean("oak.documentMK.caffeineCache")
                && Boolean.getBoolean("oak.documentMK.asyncCacheMaintenance"));
        IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                () -> DocumentCacheRepositoryBenchmark.runScenario(Policy.CAFFEINE_ASYNC,
                        Scenario.POINT_READ, 1, 1, 1, 1, true));
        Assert.assertTrue(failure.getCause() instanceof IllegalArgumentException);
        Assert.assertTrue(failure.getCause().getMessage().contains("both startup opt-ins"));
    }

    /** Missing nodes, missing values and empty values must never count as completed reads. */
    @Test
    public void invalidRepositoryValuesAreRejected() throws Exception {
        DocumentNodeStore store = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .setPersistentCache(null).setAsyncDelay(0).build();
        try {
            Assert.assertThrows(IllegalStateException.class, () -> DocumentCacheRepositoryBenchmark.runTask(
                    store, Scenario.POINT_READ, 1, 1, DocumentCacheBenchmark.RANDOM_SEED));
            NodeBuilder root = store.getRoot().builder();
            root.child("benchmark").child("bucket-0").child("group-0").child("node-0");
            store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            Assert.assertThrows(IllegalStateException.class, () -> DocumentCacheRepositoryBenchmark.runTask(
                    store, Scenario.POINT_READ, 1, 1, DocumentCacheBenchmark.RANDOM_SEED));
            root = store.getRoot().builder();
            root.child("benchmark").child("bucket-0").child("group-0").child("node-0").setProperty("value", "");
            store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            Assert.assertThrows(IllegalStateException.class, () -> DocumentCacheRepositoryBenchmark.runTask(
                    store, Scenario.POINT_READ, 1, 1, DocumentCacheBenchmark.RANDOM_SEED));
        } finally {
            store.dispose();
        }
    }

    /** Interrupted cleanup waits for a cancelled task before disposing its repository. */
    @Test
    public void interruptedCleanupRetainsRepositoryUntilWorkersStop() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean stopped = new AtomicBoolean();
        AtomicBoolean disposed = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        MemoryDocumentStore documents = new MemoryDocumentStore() {
            @Override
            public void dispose() {
                Assert.assertTrue("Repository disposed while its worker was running", stopped.get());
                disposed.set(true);
                super.dispose();
            }
        };
        DocumentNodeStore store = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .setDocumentStore(documents).setPersistentCache(null).setAsyncDelay(0).build();
        ExecutorService workers = Executors.newSingleThreadExecutor();
        workers.submit(() -> {
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
        Thread closing = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                DocumentCacheRepositoryBenchmark.stopWorkers(workers);
                Assert.assertTrue(Thread.currentThread().isInterrupted());
                store.dispose();
            } catch (Throwable e) {
                failure.set(e);
            }
        });
        try {
            Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
            closing.start();
            Assert.assertTrue(cancelled.await(5, TimeUnit.SECONDS));
            closing.interrupt();
            Assert.assertFalse(disposed.get());
            Assert.assertTrue(closing.isAlive());
            release.countDown();
            closing.join(5000);
            Assert.assertFalse(closing.isAlive());
            Assert.assertNull(failure.get());
            Assert.assertTrue(disposed.get());
        } finally {
            release.countDown();
            if (closing.getState() == Thread.State.NEW) { closing.start(); }
            closing.join(5000);
        }
    }

    /** Directory failures preserve a workload error or report cleanup as the primary error. */
    @Test
    public void cleanupPreservesThePrimaryRepositoryFailure() throws Exception {
        String name = "document.repository.backgroundMillis";
        String previous = System.getProperty(name);
        IOException deletion = new IOException("deletion failed");
        try (MockedStatic<FileUtils> files = Mockito.mockStatic(FileUtils.class, Mockito.CALLS_REAL_METHODS)) {
            files.when(() -> FileUtils.deleteDirectory(Mockito.any(File.class))).thenAnswer(invocation -> {
                invocation.callRealMethod();
                throw deletion;
            });
            for (boolean failWorkload : new boolean[] {false, true}) {
                System.setProperty(name, failWorkload ? "-1" : "0");
                IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                        () -> run(Scenario.POINT_READ, 1, 1, 1, true));
                if (failWorkload) {
                    Assert.assertTrue(failure.getCause() instanceof IllegalArgumentException);
                    Assert.assertTrue(failure.getCause().getMessage().contains(name));
                    Assert.assertArrayEquals(new Throwable[] {deletion}, failure.getSuppressed());
                } else {
                    Assert.assertTrue(failure.getMessage().contains(policy() + "/POINT_READ"));
                    Assert.assertSame(deletion, failure.getCause());
                }
            }
        } finally {
            if (previous == null) { System.clearProperty(name); } else { System.setProperty(name, previous); }
        }
    }

    private static Result run(Scenario scenario, int nodes, int operations, int threads, boolean persistent) {
        return DocumentCacheRepositoryBenchmark.runScenario(policy(), scenario, nodes, operations, 1, threads, persistent);
    }

    private static Policy policy() {
        return Boolean.getBoolean("oak.documentMK.caffeineCache")
                ? (Boolean.getBoolean("oak.documentMK.asyncCacheMaintenance")
                        ? Policy.CAFFEINE_ASYNC : Policy.CAFFEINE_SYNC) : Policy.CACHE_LIRS;
    }
}
