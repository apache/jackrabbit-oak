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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.cache.EmpiricalWeigher;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.CacheStatsSnapshot;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheType;

/**
 * Measures the production node cache with a fixed-weight synthetic loader and persistence.
 * Database latency is excluded; see the README for isolated-JVM commands.
 */
public final class DocumentCacheBenchmark extends Benchmark {
    private static final int VALUE_MEMORY = 512;
    static final long RANDOM_SEED = 42;
    private static final double[] RATIOS = {0.5, 1, 2, 5, 10};
    private static final RevisionVector REVISION = new RevisionVector(new Revision(1, 0, 1));

    enum Policy { CACHE_LIRS, CAFFEINE_SYNC, CAFFEINE_ASYNC }
    enum Scenario { STEADY_STATE, CHURN, INVALIDATION, CONCURRENT }
    enum State { WARM, COLD }

    record Configuration(int entries, int operations, int threads, int warmup, double multiplier,
                         boolean persistent, Set<Policy> policies, Set<Scenario> scenarios, Set<State> states) {
        Configuration {
            policies = Set.copyOf(policies);
            scenarios = Set.copyOf(scenarios);
            states = Set.copyOf(states);
            if (policies.isEmpty() || scenarios.isEmpty() || states.isEmpty()) {
                throw new IllegalArgumentException("Select at least one policy, scenario and cache state");
            }
            if (entries <= 0 || entries > Integer.MAX_VALUE / 10) {
                throw new IllegalArgumentException("document.cache.entries must be between 1 and " + Integer.MAX_VALUE / 10);
            }
            if (operations <= 0 || threads <= 0 || warmup < 0) {
                throw new IllegalArgumentException("operations and threads must be positive; warmup must be non-negative");
            }
            if (!Double.isFinite(multiplier) || multiplier <= 0) {
                throw new IllegalArgumentException("document.cache.caffeine.maximumWeightMultiplier must be finite and positive");
            }
        }
    }

    record Result(long elapsedNanos, long operations, long backendLoads, long p95Nanos,
                  CacheStatsSnapshot stats) { }

    @Override
    public void run(Iterable<RepositoryFixture> fixtures) {
        Configuration config = configuration();
        System.out.printf(Locale.ROOT, "%nDocumentCacheBenchmark entries=%d operations=%d threads=%d"
                        + " persistent=%s persistentWrites=ASYNC seed=%d caffeineWeightMultiplier=%.2f%n",
                config.entries, config.operations, config.threads, config.persistent, RANDOM_SEED, config.multiplier);
        System.out.println("scenario state ratio policy ops/s ns/op sampledP95ns hit% misses evictions loaderCalls");
        for (Scenario scenario : Scenario.values()) {
            if (!config.scenarios.contains(scenario)) { continue; }
            for (State state : State.values()) {
                if (!config.states.contains(state)) { continue; }
                for (double ratio : RATIOS) {
                    for (Policy policy : Policy.values()) {
                        if (!config.policies.contains(policy)) { continue; }
                        Result result = runScenario(config, policy, scenario, state,
                                Math.max(1, (int) (config.entries * ratio)));
                        System.out.printf(Locale.ROOT, "%s %s %.1fx %s %.0f %.1f %d %.2f %d %d %d%n",
                                scenario, state, ratio, policy, result.operations * 1e9 / result.elapsedNanos,
                                (double) result.elapsedNanos / result.operations, result.p95Nanos,
                                result.stats.hitRate() * 100, result.stats.missCount(), result.stats.evictionCount(),
                                result.backendLoads);
                    }
                }
            }
        }
    }

    static Configuration configuration() {
        int operations = integer("document.cache.operations", 2_000_000);
        boolean caffeine = Boolean.getBoolean("oak.documentMK.caffeineCache");
        Set<Policy> defaults = caffeine
                ? (Boolean.getBoolean("oak.documentMK.asyncCacheMaintenance")
                        ? Set.of(Policy.CAFFEINE_SYNC, Policy.CAFFEINE_ASYNC) : Set.of(Policy.CAFFEINE_SYNC))
                : Set.of(Policy.CACHE_LIRS);
        Set<Policy> policies = selected("document.cache.policies", Policy.class, defaults);
        if (policies.contains(Policy.CACHE_LIRS) && Boolean.getBoolean("oak.documentMK.guavaCache")) {
            throw new IllegalArgumentException("CACHE_LIRS requires -Doak.documentMK.guavaCache=false");
        }
        if (policies.contains(Policy.CACHE_LIRS) == caffeine
                || (!caffeine && policies.stream().anyMatch(p -> p != Policy.CACHE_LIRS))) {
            throw new IllegalArgumentException("Run CACHE_LIRS in a separate JVM with -Doak.documentMK.caffeineCache=false;"
                    + " Caffeine modes require -Doak.documentMK.caffeineCache=true");
        }
        return new Configuration(integer("document.cache.entries", 10_000), operations,
                integer("document.cache.threads", Math.max(2, Runtime.getRuntime().availableProcessors())),
                integer("document.cache.warmup", Math.max(10_000, operations / 10)),
                Double.parseDouble(System.getProperty("document.cache.caffeine.maximumWeightMultiplier", "1.0")),
                Boolean.parseBoolean(System.getProperty("document.cache.persistent.enabled", "true")), policies,
                selected("document.cache.scenarios", Scenario.class, Set.of(Scenario.values())),
                selected("document.cache.states", State.class, Set.of(State.values())));
    }

    static Result runScenario(Configuration config, Policy policy, Scenario scenario, State state, int workingSet) {
        try (Context context = createContext(config, policy, scenario)) {
            List<PathRev> keys = new ArrayList<>(workingSet);
            for (int i = 0; i < workingSet; i++) {
                keys.add(new PathRev(Path.fromString(
                        String.format(Locale.ROOT, "/benchmark-%08d", i)), REVISION));
            }
            runOperations(context, keys, scenario, config.warmup);
            context.cache.cleanUp();
            if (state == State.COLD) {
                context.cache.invalidateAll();
                context.cache.cleanUp();
            }
            CacheStatsSnapshot initialStats = context.cache.stats();
            long start = System.nanoTime();
            List<TaskResult> tasks = runOperations(context, keys, scenario, config.operations);
            context.cache.cleanUp();
            long elapsed = System.nanoTime() - start;
            long completed = 0;
            long loads = 0;
            List<Long> samples = new ArrayList<>();
            for (TaskResult task : tasks) {
                completed += task.operations;
                loads += task.loads;
                for (long sample : task.samples) { samples.add(sample); }
            }
            return new Result(elapsed, completed, loads, percentile95(samples),
                    context.cache.stats().minus(initialStats));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Document cache workload failed: " + policy + "/" + scenario + "/" + state, e);
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException("Document cache workload failed: " + policy + "/" + scenario + "/" + state, e);
        }
    }

    static Context createContext(Configuration config, Policy policy, Scenario scenario) throws IOException {
        PathRev sampleKey = new PathRev(Path.fromString("/benchmark-00000000"), REVISION);
        long weight = new EmpiricalWeigher().weigh(sampleKey, (CacheValue) () -> VALUE_MEMORY);
        long maximum = Math.multiplyExact(config.entries, weight);
        if (policy != Policy.CACHE_LIRS) {
            double scaled = maximum * config.multiplier;
            if (scaled > Long.MAX_VALUE / 2.0 || scaled < 1) {
                throw new IllegalArgumentException("Caffeine maximum weight is outside the supported range");
            }
            maximum = (long) scaled;
        }
        File directory = config.persistent ? Files.createTempDirectory("oak-document-cache-benchmark-").toFile() : null;
        DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .memoryCacheSize(Math.multiplyExact(maximum, 2)).memoryCacheDistribution(50, 0, 0, 0, 0)
                .setAsyncDelay(0).setCacheMaintenanceMode(CacheType.NODE,
                        policy == Policy.CAFFEINE_ASYNC ? CacheBuilder.MaintenanceMode.ASYNC : CacheBuilder.MaintenanceMode.SYNC);
        builder.setPersistentCache(directory == null ? null : directory.getAbsolutePath() + ",+async");
        try {
            CacheBuilder.MaintenanceMode expected = policy == Policy.CAFFEINE_ASYNC
                    ? CacheBuilder.MaintenanceMode.ASYNC : CacheBuilder.MaintenanceMode.SYNC;
            if (builder.getCacheMaintenanceMode(CacheType.NODE) != expected) {
                throw new IllegalArgumentException("CAFFEINE_ASYNC requires enabled FT_OAK-12437; use "
                        + "-Doak.documentMK.asyncCacheMaintenance=true at JVM startup");
            }
            DocumentNodeStore store = builder.build();
            ExecutorService executor = scenario == Scenario.CONCURRENT ? Executors.newFixedThreadPool(config.threads) : null;
            return new Context(store, store.getNodeCache(), directory, executor, config.threads);
        } catch (RuntimeException e) {
            if (builder.getPersistentCache() != null) { builder.getPersistentCache().close(); }
            if (directory != null) { FileUtils.deleteDirectory(directory); }
            throw e;
        }
    }

    record Context(DocumentNodeStore store, Cache<PathRev, DocumentNodeState> cache,
                           File directory, ExecutorService executor, int threads) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            boolean interrupted = Thread.interrupted();
            try {
                if (executor != null) {
                    executor.shutdownNow();
                    for (;;) {
                        try {
                            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("Document cache workers did not terminate");
                            }
                            break;
                        } catch (InterruptedException e) {
                            interrupted = true;
                        }
                    }
                }
                disposeStore(store, directory);
            } finally {
                if (interrupted) { Thread.currentThread().interrupt(); }
            }
        }
    }

    static void disposeStore(DocumentNodeStore store, File directory) throws IOException {
        try (Closeable cleanup = () -> {
            if (directory != null) { FileUtils.deleteDirectory(directory); }
        }) {
            if (store != null) { store.dispose(); }
        }
    }

    private record TaskResult(long operations, long loads, long[] samples) { }

    private static List<TaskResult> runOperations(Context context, List<PathRev> keys, Scenario scenario, int operations) {
        if (scenario != Scenario.CONCURRENT) {
            return Collections.singletonList(runTask(context, keys, scenario, operations, RANDOM_SEED));
        }
        List<Callable<TaskResult>> tasks = new ArrayList<>();
        for (int thread = 0; thread < context.threads; thread++) {
            int count = operations / context.threads + (thread < operations % context.threads ? 1 : 0);
            long seed = RANDOM_SEED + thread;
            tasks.add(() -> runTask(context, keys, scenario, count, seed));
        }
        try {
            List<TaskResult> completed = new ArrayList<>();
            for (Future<TaskResult> result : context.executor.invokeAll(tasks)) { completed.add(result.get()); }
            return completed;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Document cache benchmark interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Document cache benchmark task failed", e.getCause());
        }
    }

    static TaskResult runTask(Context context, List<PathRev> keys, Scenario scenario, int operations, long seed) {
        SplittableRandom random = new SplittableRandom(seed);
        SplittableRandom sampling = new SplittableRandom(seed ^ 0x5DEECE66DL);
        long[] loads = {0};
        long[] samples = new long[(int) ((operations + 63L) / 64)];
        int sampleCount = 0;
        int sampleOperation = operations == 0 ? -1 : samplePosition(0, operations, sampling);
        int offset = 0;
        for (int operation = 0; operation < operations; operation++) {
            if (operation % 1024 == 0 && Thread.currentThread().isInterrupted()) {
                throw new IllegalStateException("Document cache benchmark worker interrupted");
            }
            int index;
            if (scenario == Scenario.CHURN) {
                if (operation > 0 && operation % 1000 == 0) { offset = (offset + Math.max(1, keys.size() / 100)) % keys.size(); }
                index = (offset + random.nextInt(Math.max(1, keys.size() / 2))) % keys.size();
            } else { index = hotColdIndex(random, keys.size()); }
            PathRev key = keys.get(index);
            long sampleStart = operation == sampleOperation ? System.nanoTime() : 0;
            if (scenario == Scenario.INVALIDATION && operation % 16 == 0) { context.cache.invalidate(key); }
            context.cache.get(key, missing -> {
                loads[0]++;
                return new DocumentNodeState(context.store, missing.getPath(), REVISION, Collections.emptyMap(),
                        false, VALUE_MEMORY, REVISION, false);
            });
            if (sampleStart != 0) { samples[sampleCount++] = System.nanoTime() - sampleStart; }
            if (operation % 64 == 63 && operation + 1 < operations) {
                sampleOperation = samplePosition(operation + 1, operations, sampling);
            }
        }
        return new TaskResult(operations, loads[0], samples);
    }

    // Independent sampling keeps periodic invalidation from dominating the latency distribution.
    static int samplePosition(int blockStart, int operations, SplittableRandom sampling) {
        return blockStart + sampling.nextInt(Math.min(64, operations - blockStart));
    }

    static long percentile95(Iterable<Long> samples) {
        List<Long> sorted = new ArrayList<>();
        samples.forEach(sorted::add);
        Collections.sort(sorted);
        return sorted.isEmpty() ? 0 : sorted.get((int) Math.ceil(sorted.size() * .95) - 1);
    }

    private static int hotColdIndex(SplittableRandom random, int size) {
        int hot = Math.max(1, size / 5);
        return hot == size ? 0 : random.nextInt(10) < 8 ? random.nextInt(hot) : hot + random.nextInt(size - hot);
    }

    private static int integer(String name, int defaultValue) {
        return Integer.parseInt(System.getProperty(name, Integer.toString(defaultValue)));
    }

    private static <E extends Enum<E>> Set<E> selected(String name, Class<E> type, Set<E> defaults) {
        String configured = System.getProperty(name);
        if (configured == null) { return defaults; }
        return Arrays.stream(configured.split(",")).map(String::trim).map(s -> Enum.valueOf(type,
                s.toUpperCase(Locale.ROOT))).collect(Collectors.toSet());
    }
}
