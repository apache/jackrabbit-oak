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
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.benchmark.DocumentCacheBenchmark.Policy;
import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.CacheStatsSnapshot;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheType;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCacheStats;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/** Measures real DocumentNodeStore reads, traversal, commits and cache reopen on a memory backend. */
public final class DocumentCacheRepositoryBenchmark extends Benchmark {
    private static final int CHILDREN_PER_GROUP = positiveProperty("document.repository.childrenPerGroup", 100);
    private static final int GROUPS_PER_BUCKET = 100;
    enum Scenario { POINT_READ, CHILDREN_SCAN, COMMIT_ONLY, COMMIT_DIFF, CONCURRENT_MIXED, CONCURRENT_WRITERS, REOPEN_READ }

    record Result(long operations, long reads, long writes, long documentFinds, long documentQueries, long elapsedNanos,
                  CacheStatsSnapshot nodeStats, long persistentHits, long localDiffHits, long localDiffMisses,
                  long mergeNanos, long diffNanos, long readbackNanos) { }

    /**
     * Runs repository workloads separately from the direct-cache microbenchmark.
     * @param fixtures runner selection; these workloads always use MemoryDocumentStore
     */
    @Override
    public void run(Iterable<RepositoryFixture> fixtures) {
        Objects.requireNonNull(fixtures);
        DocumentCacheBenchmark.Configuration cacheConfig = DocumentCacheBenchmark.configuration();
        int nodes = positiveProperty("document.repository.nodes", 10_000);
        int operations = positiveProperty("document.repository.operations", 20_000);
        int memoryMB = positiveProperty("document.repository.cacheMB", 256);
        int durationSeconds = nonNegativeProperty("document.repository.durationSeconds", 60);
        int warmupSeconds = nonNegativeProperty("document.repository.warmupSeconds", 20);
        Set<Scenario> scenarios = selectedScenarios();
        System.out.printf(Locale.ROOT, "%nDocumentCacheRepositoryBenchmark nodes=%d operations=%d cacheMB=%d"
                + " childrenPerGroup=%d backgroundMillis=%d threads=%d persistent=%s durationSeconds=%d warmupSeconds=%d backend=MemoryDocumentStore%n",
                nodes, operations, memoryMB, CHILDREN_PER_GROUP,
                nonNegativeProperty("document.repository.backgroundMillis", 1000),
                cacheConfig.threads(), cacheConfig.persistent(), durationSeconds, warmupSeconds);
        System.out.println("Every eligible Document cache, including LOCAL_DIFF, uses the named policy.");
        System.out.println("repositoryScenario policy ops/s reads writes documentFinds documentQueries nodeHit% persistentHits localDiffHits localDiffMisses mergeMs diffMs readbackMs elapsedMs");
        for (Policy policy : Policy.values()) {
            if (!cacheConfig.policies().contains(policy)) { continue; }
            for (Scenario scenario : Scenario.values()) {
                if (!scenarios.contains(scenario)) { continue; }
                if (warmupSeconds > 0) {
                    runScenario(policy, scenario, nodes, operations, memoryMB,
                            cacheConfig.threads(), cacheConfig.persistent(), TimeUnit.SECONDS.toNanos(warmupSeconds));
                }
                Result result = runScenario(policy, scenario, nodes, operations, memoryMB,
                        cacheConfig.threads(), cacheConfig.persistent(), TimeUnit.SECONDS.toNanos(durationSeconds));
                System.out.printf(Locale.ROOT, "%s %s %.0f %d %d %d %d %.2f %d %d %d %.3f %.3f %.3f %.3f%n", scenario, policy,
                        result.operations() * 1e9 / result.elapsedNanos(), result.reads(), result.writes(),
                        result.documentFinds(), result.documentQueries(), result.nodeStats().hitRate() * 100, result.persistentHits(),
                        result.localDiffHits(), result.localDiffMisses(), result.mergeNanos() / 1e6,
                        result.diffNanos() / 1e6, result.readbackNanos() / 1e6, result.elapsedNanos() / 1e6);
            }
        }
    }

    static Result runScenario(Policy policy, Scenario scenario, int nodes, int operations, int memoryMB,
                              int threads, boolean persistent) {
        return runScenario(policy, scenario, nodes, operations, memoryMB, threads, persistent, 0);
    }

    static Result runScenario(Policy policy, Scenario scenario, int nodes, int operations, int memoryMB,
                              int threads, boolean persistent, long minimumDurationNanos) {
        if (nodes <= 0 || operations <= 0 || memoryMB <= 0 || threads <= 0 || minimumDurationNanos < 0) {
            throw new IllegalArgumentException("sizes must be positive and minimum duration must be non-negative");
        }
        File directory = null;
        DocumentNodeStore store = null;
        ScheduledExecutorService statisticsExecutor = Executors.newSingleThreadScheduledExecutor();
        StatisticsProvider statistics = new DefaultStatisticsProvider(statisticsExecutor);
        IllegalStateException failure = null;
        try {
            directory = persistent ? Files.createTempDirectory("oak-document-repository-benchmark-").toFile() : null;
            CountingDocumentStore documents = new CountingDocumentStore();
            store = createStore(policy, memoryMB, directory, documents, statistics);
            populate(store, nodes);
            // Read every real value before timing; eviction and persistent writes can occur here.
            for (int id = 0; id < nodes; id++) { read(store.getRoot(), id); }
            if (scenario == Scenario.REOPEN_READ) {
                store.dispose();
                store = null;
                store = createStore(policy, memoryMB, directory, documents, statistics);
            }
            CacheStatsSnapshot before = store.getNodeCache().stats();
            long finds = documents.finds.get();
            long queries = documents.queries.get();
            long persistentHits = persistentHits(store);
            AbstractCacheStats localDiff = localDiffStats(store);
            long localHits = localDiff.getHitCount();
            long localMisses = localDiff.getMissCount();
            long start = System.nanoTime();
            Counts counts = scenario == Scenario.CONCURRENT_MIXED || scenario == Scenario.CONCURRENT_WRITERS
                    ? runConcurrent(store, scenario, nodes, operations, Math.min(threads, nodes), minimumDurationNanos)
                    : runTask(store, scenario, nodes, operations, DocumentCacheBenchmark.RANDOM_SEED, minimumDurationNanos);
            store.getNodeCache().cleanUp();
            long elapsedNanos = System.nanoTime() - start;
            return new Result(counts.reads() + counts.writes(), counts.reads(), counts.writes(), documents.finds.get() - finds,
                    documents.queries.get() - queries, elapsedNanos, store.getNodeCache().stats().minus(before),
                    persistentHits(store) - persistentHits, localDiff.getHitCount() - localHits,
                    localDiff.getMissCount() - localMisses, counts.mergeNanos(), counts.diffNanos(), counts.readbackNanos());
        } catch (Exception e) {
            failure = new IllegalStateException("Repository cache workload failed: " + policy + "/" + scenario, e);
            throw failure;
        } finally {
            try {
                DocumentCacheBenchmark.disposeStore(store, directory);
            } catch (IOException | RuntimeException e) {
                if (failure != null) { failure.addSuppressed(e); }
                else { throw new IllegalStateException("Repository cache cleanup failed: " + policy + "/" + scenario, e); }
            } finally {
                statisticsExecutor.shutdownNow();
            }
        }
    }

    private static DocumentNodeStore createStore(Policy policy, int memoryMB, File directory,
                                                  MemoryDocumentStore documents, StatisticsProvider statistics) {
        DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .setDocumentStore(documents).setStatisticsProvider(statistics).setClusterId(1)
                .setAsyncDelay(nonNegativeProperty("document.repository.backgroundMillis", 1000))
                .memoryCacheSize(Math.multiplyExact((long) memoryMB, 1024 * 1024))
                .setPersistentCache(directory == null ? null : directory.getAbsolutePath() + ",+async,+asyncDiff");
        CacheBuilder.MaintenanceMode requested = policy == Policy.CAFFEINE_ASYNC
                ? CacheBuilder.MaintenanceMode.ASYNC : CacheBuilder.MaintenanceMode.SYNC;
        for (CacheType type : Set.of(CacheType.NODE, CacheType.CHILDREN, CacheType.DIFF,
                CacheType.LOCAL_DIFF, CacheType.DOCUMENT, CacheType.PREV_DOCUMENT)) {
            builder.setCacheMaintenanceMode(type, requested);
            if (builder.getCacheMaintenanceMode(type) != requested) {
                throw new IllegalArgumentException("ASYNC repository workload requires both startup opt-ins");
            }
        }
        return builder.build();
    }

    static void populate(DocumentNodeStore store, int nodes) throws Exception {
        for (int first = 0; first < nodes; first += 500) {
            NodeBuilder root = store.getRoot().builder();
            for (int id = first; id < Math.min(nodes, first + 500); id++) {
                group(root, id).child("node-" + id)
                        .setProperty("value", "x".repeat(64 + id % 8 * 128));
            }
            store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
    }

    record Counts(long reads, long writes, long mergeNanos, long diffNanos, long readbackNanos) { }

    static Counts runTask(DocumentNodeStore store, Scenario scenario, int nodes,
                                  int operations, long seed) throws Exception {
        return runTask(store, scenario, nodes, operations, seed, 0);
    }

    private static Counts runTask(DocumentNodeStore store, Scenario scenario, int nodes,
                                  int operations, long seed, long minimumDurationNanos) throws Exception {
        SplittableRandom random = new SplittableRandom(seed);
        long reads = 0;
        long writes = 0;
        long mergeNanos = 0;
        long diffNanos = 0;
        long readbackNanos = 0;
        long start = System.nanoTime();
        for (long operation = 0; operation < operations || minimumDurationNanos > 0
                && (operation % 256 != 0 || System.nanoTime() - start < minimumDurationNanos); operation++) {
            if (Thread.currentThread().isInterrupted()) {
                throw new IllegalStateException("Repository cache workload interrupted");
            }
            int id = scenario == Scenario.CONCURRENT_WRITERS ? (int) (seed - DocumentCacheBenchmark.RANDOM_SEED)
                    : scenario == Scenario.REOPEN_READ ? (int) (operation % nodes) : random.nextInt(nodes);
            if (scenario == Scenario.COMMIT_ONLY || scenario == Scenario.COMMIT_DIFF || scenario == Scenario.CONCURRENT_WRITERS) {
                NodeState before = store.getRoot();
                NodeBuilder root = before.builder();
                group(root, id).child("node-" + id)
                        .setProperty("value", "updated-" + seed + "-" + operation);
                long phaseStart = System.nanoTime();
                NodeState after = store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                mergeNanos += System.nanoTime() - phaseStart;
                phaseStart = System.nanoTime();
                NodeState afterGroup = group(after, id);
                if (scenario != Scenario.COMMIT_ONLY) {
                    NodeState beforeGroup = group(before, id);
                    afterGroup.compareAgainstBaseState(beforeGroup, new DefaultNodeStateDiff());
                    diffNanos += System.nanoTime() - phaseStart;
                    phaseStart = System.nanoTime();
                }
                String committed = afterGroup.getChildNode("node-" + id).getProperty("value").getValue(Type.STRING);
                if (!committed.equals("updated-" + seed + "-" + operation)) {
                    throw new IllegalStateException("Commit lost the updated value");
                }
                readbackNanos += System.nanoTime() - phaseStart;
                writes++;
            } else if (scenario == Scenario.CHILDREN_SCAN) {
                NodeState group = group(store.getRoot(), id);
                long visited = 0;
                for (ChildNodeEntry child : group.getChildNodeEntries()) {
                    requireValue(child.getNodeState());
                    visited++;
                }
                if (visited != Math.min(CHILDREN_PER_GROUP, nodes - id / CHILDREN_PER_GROUP * CHILDREN_PER_GROUP)) {
                    throw new IllegalStateException("Traversal lost children: " + visited);
                }
                reads++;
            } else {
                read(store.getRoot(), id);
                reads++;
            }
        }
        return new Counts(reads, writes, mergeNanos, diffNanos, readbackNanos);
    }

    private static Counts runConcurrent(DocumentNodeStore store, Scenario scenario, int nodes, int operations,
                                         int threads, long minimumDurationNanos) throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Counts>> tasks = new ArrayList<>();
            for (int worker = 0; worker < threads; worker++) {
                int count = operations / threads + (worker < operations % threads ? 1 : 0);
                long seed = DocumentCacheBenchmark.RANDOM_SEED + worker;
                Scenario task = scenario == Scenario.CONCURRENT_WRITERS ? scenario
                        : worker == 0 ? Scenario.COMMIT_DIFF : Scenario.POINT_READ;
                tasks.add(workers.submit(() -> runTask(store, task, nodes, count, seed, minimumDurationNanos)));
            }
            long reads = 0;
            long writes = 0;
            long mergeNanos = 0;
            long diffNanos = 0;
            long readbackNanos = 0;
            for (Future<Counts> task : tasks) {
                Counts counts = task.get();
                reads += counts.reads();
                writes += counts.writes();
                mergeNanos += counts.mergeNanos();
                diffNanos += counts.diffNanos();
                readbackNanos += counts.readbackNanos();
            }
            return new Counts(reads, writes, mergeNanos, diffNanos, readbackNanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            stopWorkers(workers);
        }
    }

    static void stopWorkers(ExecutorService workers) {
        boolean interrupted = Thread.interrupted();
        try {
            workers.shutdownNow();
            // Keep the store owned until every cancelled task stops using it.
            for (;;) {
                try {
                    if (workers.awaitTermination(30, TimeUnit.SECONDS)) { break; }
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) { Thread.currentThread().interrupt(); }
        }
    }

    private static void read(NodeState root, int id) {
        requireValue(group(root, id).getChildNode("node-" + id));
    }

    private static NodeState group(NodeState root, int id) {
        int group = id / CHILDREN_PER_GROUP;
        return root.getChildNode("benchmark").getChildNode("bucket-" + group / GROUPS_PER_BUCKET)
                .getChildNode("group-" + group);
    }

    private static NodeBuilder group(NodeBuilder root, int id) {
        int group = id / CHILDREN_PER_GROUP;
        return root.child("benchmark").child("bucket-" + group / GROUPS_PER_BUCKET).child("group-" + group);
    }

    private static void requireValue(NodeState node) {
        if (!node.exists() || node.getProperty("value") == null
                || node.getProperty("value").getValue(Type.STRING).isEmpty()) {
            throw new IllegalStateException("Repository read lost a populated node or its value");
        }
    }

    private static AbstractCacheStats localDiffStats(DocumentNodeStore store) {
        for (AbstractCacheStats stats : store.getDiffCacheStats()) {
            if ("Document-LocalDiff".equals(stats.getName())) { return stats; }
        }
        throw new IllegalStateException("Repository workload has no local-diff cache statistics");
    }

    private static long persistentHits(DocumentNodeStore store) {
        PersistentCacheStats stats = PersistentCache.getPersistentCacheStats(store.getNodeCache());
        return stats == null ? 0 : stats.getHitCount();
    }

    private static int positiveProperty(String name, int fallback) {
        int value = Integer.parseInt(System.getProperty(name, Integer.toString(fallback)));
        if (value <= 0) { throw new IllegalArgumentException(name + " must be positive"); }
        return value;
    }

    private static int nonNegativeProperty(String name, int fallback) {
        int value = Integer.parseInt(System.getProperty(name, Integer.toString(fallback)));
        if (value < 0) { throw new IllegalArgumentException(name + " must be non-negative"); }
        return value;
    }

    static Set<Scenario> selectedScenarios() {
        String configured = System.getProperty("document.repository.scenarios");
        if (configured == null) { return EnumSet.allOf(Scenario.class); }
        Set<Scenario> scenarios = EnumSet.noneOf(Scenario.class);
        for (String name : configured.split(",")) {
            scenarios.add(Scenario.valueOf(name.trim().toUpperCase(Locale.ROOT)));
        }
        if (scenarios.isEmpty()) { throw new IllegalArgumentException("Select at least one repository scenario"); }
        return scenarios;
    }

    private static final class CountingDocumentStore extends MemoryDocumentStore {
        private final AtomicLong finds = new AtomicLong();
        private final AtomicLong queries = new AtomicLong();

        /** Counts backend queries separately from individual document reads. */
        @Override
        public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey,
                                                 String indexedProperty, long startValue, int limit) {
            if (collection == Collection.NODES) { queries.incrementAndGet(); }
            return super.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
        }

        /** Counts node reads at the MemoryDocumentStore boundary. */
        @Override
        public <T extends Document> T find(Collection<T> collection, String key) {
            if (collection == Collection.NODES) { finds.incrementAndGet(); }
            return super.find(collection, key);
        }
    }
}
