/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.commons.internal.concurrent;

import org.assertj.core.api.Condition;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class ForkJoinUtilsTest {

    @Test
    public void isInCommonPool() {
        Map<Integer, Boolean> results = getParallelTestStream()
                .boxed()
                .map(i -> Map.entry(i, isInCommonPool(Thread.currentThread())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertThat(results)
                .hasSizeBetween(9, 10) // account for the main thread executing an item
                .allSatisfy((key, value) -> {
                    assertThat(key).isBetween(0, 9);
                    assertThat(value).isTrue();
                });
    }

    @Test
    public void submitCallableInCustomPool() {
        Callable<Map<Integer, Boolean>> callable = () -> getParallelTestStream()
                .boxed()
                .collect(Collectors.toMap(i -> i, i -> isInCommonPool(Thread.currentThread())));
        Map<Integer, Boolean> results = ForkJoinUtils
                .submitInCustomPool("custom-pool", 4, callable)
                .join();
        assertThat(results)
                .hasSize(10)
                .allSatisfy((key, value) -> {
                    assertThat(key).isBetween(0, 9);
                    assertThat(value).isFalse();
                });
    }

    @Test
    public void invokeCallableInCustomPool() {
        Callable<Map<Integer, Boolean>> callable = () -> getParallelTestStream()
                .boxed()
                .collect(Collectors.toMap(i -> i, i -> isInCommonPool(Thread.currentThread())));
        Map<Integer, Boolean> results = ForkJoinUtils
                .invokeInCustomPool("custom-pool", 4, callable);
        assertThat(results)
                .hasSize(10)
                .allSatisfy((key, value) -> {
                    assertThat(key).isBetween(0, 9);
                    assertThat(value).isFalse();
                });
    }

    @Test
    public void submitRunnableInCustomPool() {
        Map<Integer, Boolean> results = new ConcurrentHashMap<>();
        Runnable runnable = () -> getParallelTestStream()
                .boxed()
                .forEach(i -> results.put(i, isInCommonPool(Thread.currentThread())));
        ForkJoinUtils
                .submitInCustomPool("custom-pool", 4, runnable)
                .join();
        assertThat(results)
                .hasSize(10)
                .allSatisfy((key, value) -> {
                    assertThat(key).isBetween(0, 9);
                    assertThat(value).isFalse();
                });
    }

    @Test
    public void executeRunnableInCustomPool() {
        Runnable runnable = () -> getParallelTestStream()
                .forEach(i -> assertThat(Thread.currentThread().getName()).startsWith("custom-pool-"));
        ForkJoinUtils
                .executeInCustomPool("custom-pool", 4, runnable);
    }

    @Test
    public void reentryInCustomPool() {
        Map<Integer, String> results = ForkJoinUtils
                .invokeInCustomPool("outer-pool", 4, getReentryTestCallable());

        assertThat(results)
                .hasSize(16)
                .containsOnlyKeys(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
                .values()
                .haveExactly(16, new Condition<>(
                        value -> value.startsWith("outer-pool-"),
                        "should start with 'outer-pool-'"));
    }


    @Test
    public void noReentryInCommonPool() throws Exception {
        Map<Integer, String> results = getReentryTestCallable().call();

        assertThat(results)
                .hasSize(16)
                .containsOnlyKeys(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
                .values()
                .haveExactly(16, new Condition<>(
                        threadName -> threadName.startsWith("inner-pool-"),
                        "should start with 'inner-pool-'"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void submitInCustomPoolWithInvalidParallelism() {
        ForkJoinUtils.submitInCustomPool("custom-pool", 0, () -> {});
    }

    private static boolean isInCommonPool(Thread thread) {
        return ForkJoinUtils.getWorkerPool(thread) == ForkJoinPool.commonPool();
    }

    private static @NotNull Callable<Map<Integer, String>> getReentryTestCallable() {
        return () -> IntStream.range(0, 4)
                .parallel()
                .mapToObj(i -> ForkJoinUtils.submitInCustomPool("inner-pool-" + i, 4, () -> IntStream.range(0, 4)
                                .parallel()
                                .map(j -> i * 4 + j)
                                .mapToObj(k -> Map.entry(k, Thread.currentThread().getName()))
                                .collect(Collectors.toUnmodifiableList()))
                        .join())
                .flatMap(List::stream)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static @NotNull IntStream getParallelTestStream() {
        CountDownLatch latch = new CountDownLatch(9);
        return IntStream.range(0, 10)
                .parallel()
                // the "main" thread is used in conjunction with the common pool, but is not itself in the common pool
                .filter(i -> {
                    Thread thread = Thread.currentThread();
                    boolean isMainThread = Objects.equals(thread.getName(), "main");
                    if (isMainThread) {
                        try {
                            // make sure "main" thread processes at most one item
                            latch.await(100, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    latch.countDown();
                    return !isMainThread;
                });
    }
}
