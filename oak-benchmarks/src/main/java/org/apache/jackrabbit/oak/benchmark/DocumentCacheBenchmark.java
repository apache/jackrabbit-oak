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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.EmpiricalWeigher;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.CacheStatsSnapshot;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;

/**
 * Compares CacheLIRS and Caffeine under representative DocumentNodeStore cache workloads.
 *
 * <p>The benchmark uses the Oak cache API, {@link EmpiricalWeigher}, fixed-size
 * {@link StringValue} keys and values, and identical maximum weights. It covers steady-state
 * reads, a drifting active set, explicit invalidation followed by reload, and concurrent reads.
 * Each scenario runs with working sets at 0.5, 1, 2, 5, and 10 times the configured cache
 * capacity.</p>
 *
 * <p>Run with:
 * {@code java -Xmx2g -jar target/oak-benchmarks-*.jar
 * benchmark DocumentCacheBenchmark Oak-MemoryNS}. Workload size can be adjusted with
 * {@code -Ddocument.cache.entries}, {@code -Ddocument.cache.operations}, and
 * {@code -Ddocument.cache.threads}. Individual policies/scenarios can be selected with
 * {@code -Ddocument.cache.policies} and {@code -Ddocument.cache.scenarios}. Use
 * {@code -Ddocument.cache.caffeine.maximumWeightMultiplier} to evaluate a different memory
 * allocation for Caffeine.</p>
 */
public final class DocumentCacheBenchmark extends Benchmark {

    private static final int CACHE_ENTRIES = Integer.getInteger("document.cache.entries", 10_000);
    private static final int OPERATIONS = Integer.getInteger("document.cache.operations", 2_000_000);
    private static final int THREADS = Integer.getInteger("document.cache.threads",
            Math.max(2, Runtime.getRuntime().availableProcessors()));
    private static final int WARMUP_OPERATIONS = Math.max(10_000, OPERATIONS / 10);
    private static final int VALUE_LENGTH = 256;
    // Match DocumentNodeStoreBuilder's production CacheLIRS configuration.
    private static final int LIRS_AVERAGE_WEIGHT = 2_000;
    private static final long RANDOM_SEED = 42L;
    private static final double[] WORKING_SET_RATIOS = {0.5, 1.0, 2.0, 5.0, 10.0};
    private static final Set<String> SELECTED_SCENARIOS = selectedValues(
            "document.cache.scenarios", Scenario.values());
    private static final Set<String> SELECTED_POLICIES = selectedValues(
            "document.cache.policies", Policy.values());
    private static final EmpiricalWeigher WEIGHER = new EmpiricalWeigher();
    private static final int ENTRY_WEIGHT = WEIGHER.weigh(value(0), value(0));
    private static final long MAXIMUM_WEIGHT = (long) CACHE_ENTRIES * ENTRY_WEIGHT;

    private enum Policy {
        CACHE_LIRS {
            @Override
            Cache<StringValue, StringValue> createCache(double maximumWeightMultiplier) {
                return CacheLIRS.<StringValue, StringValue>newBuilder()
                        .maximumWeight(MAXIMUM_WEIGHT)
                        .averageWeight(LIRS_AVERAGE_WEIGHT)
                        .weigher(WEIGHER::weigh)
                        .segmentCount(16)
                        .recordStats()
                        .build()
                        .asOakCache();
            }
        },
        CAFFEINE {
            @Override
            Cache<StringValue, StringValue> createCache(double maximumWeightMultiplier) {
                return CacheBuilder.<StringValue, StringValue>newBuilder()
                        .maximumWeight((long) (MAXIMUM_WEIGHT
                                * maximumWeightMultiplier))
                        .weigher(WEIGHER::weigh)
                        .recordStats()
                        .build();
            }
        };

        abstract Cache<StringValue, StringValue> createCache(double maximumWeightMultiplier);
    }

    private enum Scenario {
        STEADY_STATE,
        CHURN,
        INVALIDATION,
        CONCURRENT
    }

    private record Result(long elapsedNanos, long operations, long backendLoads,
                          CacheStatsSnapshot stats) {
    }

    @Override
    public void run(Iterable<RepositoryFixture> fixtures) {
        double caffeineMaximumWeightMultiplier = caffeineMaximumWeightMultiplier();
        validateConfiguration(caffeineMaximumWeightMultiplier);
        System.out.printf(Locale.ROOT,
                "%nDocumentCacheBenchmark cacheEntries=%,d maxWeight=%,d operations=%,d threads=%d"
                        + " caffeineMaximumWeightMultiplier=%.2f%n",
                CACHE_ENTRIES, MAXIMUM_WEIGHT, OPERATIONS, THREADS,
                caffeineMaximumWeightMultiplier);
        System.out.println("scenario       ratio policy          ops/s      ns/op    hit%      misses"
                + "   evictions backendLoads");

        for (Scenario scenario : Scenario.values()) {
            if (!SELECTED_SCENARIOS.contains(scenario.name())) {
                continue;
            }
            for (double ratio : WORKING_SET_RATIOS) {
                int workingSetSize = Math.max(1, (int) (CACHE_ENTRIES * ratio));
                for (Policy policy : Policy.values()) {
                    if (!SELECTED_POLICIES.contains(policy.name())) {
                        continue;
                    }
                    Result result = runScenario(policy, scenario, workingSetSize, OPERATIONS,
                            caffeineMaximumWeightMultiplier);
                    printResult(scenario, ratio, policy, result);
                }
            }
        }
    }

    private static Result runScenario(Policy policy, Scenario scenario, int workingSetSize,
                                      int operations, double caffeineMaximumWeightMultiplier) {
        Cache<StringValue, StringValue> cache =
                policy.createCache(caffeineMaximumWeightMultiplier);
        List<StringValue> keys = createValues(workingSetSize);
        AtomicLong backendLoads = new AtomicLong();
        runOperations(cache, keys, scenario, WARMUP_OPERATIONS, backendLoads);
        cache.cleanUp();
        CacheStatsSnapshot initialStats = cache.stats();
        long initialBackendLoads = backendLoads.get();
        long start = System.nanoTime();
        long completed = runOperations(cache, keys, scenario, operations, backendLoads);
        cache.cleanUp();
        long elapsed = System.nanoTime() - start;
        Result result = new Result(elapsed, completed, backendLoads.get() - initialBackendLoads,
                cache.stats().minus(initialStats));
        cache.invalidateAll();
        cache.cleanUp();
        return result;
    }

    private static long runOperations(Cache<StringValue, StringValue> cache,
                                      List<StringValue> keys,
                                      Scenario scenario,
                                      int operations,
                                      AtomicLong backendLoads) {
        if (scenario == Scenario.CONCURRENT) {
            return runConcurrent(cache, keys, operations, backendLoads);
        }
        return runSingleThreaded(cache, keys, scenario, operations, backendLoads);
    }

    private static long runSingleThreaded(Cache<StringValue, StringValue> cache,
                                          List<StringValue> keys,
                                          Scenario scenario,
                                          int operations,
                                          AtomicLong backendLoads) {
        SplittableRandom random = new SplittableRandom(RANDOM_SEED);
        int churnOffset = 0;
        for (int operation = 0; operation < operations; operation++) {
            int index;
            if (scenario == Scenario.CHURN) {
                if (operation > 0 && operation % 1_000 == 0) {
                    churnOffset = (churnOffset + Math.max(1, keys.size() / 100)) % keys.size();
                }
                index = (churnOffset + random.nextInt(Math.max(1, keys.size() / 2))) % keys.size();
            } else {
                index = hotColdIndex(random, keys.size());
            }
            StringValue key = keys.get(index);
            if (scenario == Scenario.INVALIDATION && operation % 16 == 0) {
                cache.invalidate(key);
            }
            cache.get(key, ignored -> {
                backendLoads.incrementAndGet();
                return key;
            });
        }
        return operations;
    }

    private static long runConcurrent(Cache<StringValue, StringValue> cache,
                                      List<StringValue> keys,
                                      int operations,
                                      AtomicLong backendLoads) {
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        try {
            List<Callable<Long>> tasks = new ArrayList<>();
            int operationsPerThread = operations / THREADS;
            int remainder = operations % THREADS;
            for (int thread = 0; thread < THREADS; thread++) {
                int taskOperations = operationsPerThread + (thread < remainder ? 1 : 0);
                long seed = RANDOM_SEED + thread;
                tasks.add(() -> runConcurrentTask(cache, keys, taskOperations, seed, backendLoads));
            }
            long completed = 0;
            for (Future<Long> future : executor.invokeAll(tasks)) {
                completed += future.get();
            }
            return completed;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Concurrent document cache benchmark was interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Concurrent document cache benchmark task failed", e.getCause());
        } finally {
            executor.shutdownNow();
        }
    }

    private static long runConcurrentTask(Cache<StringValue, StringValue> cache,
                                          List<StringValue> keys,
                                          int operations,
                                          long seed,
                                          AtomicLong backendLoads) {
        SplittableRandom random = new SplittableRandom(seed);
        for (int operation = 0; operation < operations; operation++) {
            StringValue key = keys.get(hotColdIndex(random, keys.size()));
            cache.get(key, ignored -> {
                backendLoads.incrementAndGet();
                return key;
            });
        }
        return operations;
    }

    private static int hotColdIndex(SplittableRandom random, int workingSetSize) {
        int hotSetSize = Math.max(1, workingSetSize / 5);
        if (hotSetSize == workingSetSize) {
            return 0;
        }
        if (random.nextInt(10) < 8) {
            return random.nextInt(hotSetSize);
        }
        return hotSetSize + random.nextInt(workingSetSize - hotSetSize);
    }

    private static List<StringValue> createValues(int count) {
        List<StringValue> values = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            values.add(value(index));
        }
        return values;
    }

    private static StringValue value(int index) {
        String prefix = String.format(Locale.ROOT, "document-%08d-", index);
        return StringValue.fromString(prefix + "x".repeat(VALUE_LENGTH - prefix.length()));
    }

    private static void printResult(Scenario scenario, double ratio, Policy policy, Result result) {
        double operationsPerSecond = result.operations * 1_000_000_000.0 / result.elapsedNanos;
        double nanosPerOperation = (double) result.elapsedNanos / result.operations;
        System.out.printf(Locale.ROOT, "%-14s %4.1fx %-11s %,12.0f %,10.1f %7.2f %,11d"
                        + " %,11d %,12d%n",
                scenario, ratio, policy, operationsPerSecond, nanosPerOperation,
                result.stats.hitRate() * 100, result.stats.missCount(),
                result.stats.evictionCount(), result.backendLoads);
    }

    private static double caffeineMaximumWeightMultiplier() {
        String value = System.getProperty(
                "document.cache.caffeine.maximumWeightMultiplier", "1.0");
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "document.cache.caffeine.maximumWeightMultiplier must be a number: " + value, e);
        }
    }

    private static void validateConfiguration(double caffeineMaximumWeightMultiplier) {
        if (CACHE_ENTRIES <= 0) {
            throw new IllegalArgumentException("document.cache.entries must be greater than 0");
        }
        if (OPERATIONS <= 0) {
            throw new IllegalArgumentException("document.cache.operations must be greater than 0");
        }
        if (THREADS <= 0) {
            throw new IllegalArgumentException("document.cache.threads must be greater than 0");
        }
        if (!Double.isFinite(caffeineMaximumWeightMultiplier)
                || caffeineMaximumWeightMultiplier <= 0) {
            throw new IllegalArgumentException(
                    "document.cache.caffeine.maximumWeightMultiplier must be greater than 0");
        }
        if (VALUE_LENGTH < 32) {
            throw new IllegalStateException("VALUE_LENGTH must leave room for fixed-width keys");
        }
    }

    private static Set<String> selectedValues(String propertyName, Enum<?>[] values) {
        String configured = System.getProperty(propertyName);
        if (configured == null || configured.isBlank()) {
            return Arrays.stream(values).map(Enum::name).collect(java.util.stream.Collectors.toSet());
        }
        return Set.of(configured.toUpperCase(Locale.ROOT).split(","));
    }
}
