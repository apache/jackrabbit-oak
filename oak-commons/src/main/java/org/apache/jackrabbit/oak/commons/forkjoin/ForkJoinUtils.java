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
package org.apache.jackrabbit.oak.commons.forkjoin;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;

/**
 * Utility class to support the use of {@link ForkJoinPool|ForkJoinPools} and related APIs.
 */
public final class ForkJoinUtils {

    /**
     * Runs a task in a custom {@link ForkJoinPool}. This is useful when a custom pool is needed for the
     * execution of a task that forks {@link java.util.concurrent.ForkJoinTask|ForkJoinTasks} internally.
     * <p>
     * Using a custom {@code ForkJoinPool} can be useful if I/O heavy loads are being processed, as it is
     * ill-advised to use the common pool for such tasks. The common pool is optimized for CPU-bound tasks
     * and running I/O heavy loads in it can lead to thread starvation.
     * <p>
     * The terminal operation of a parallel stream is such an example:
     * <pre>
     *     ForkJoinUtils.executeInCustomPool("myPool", 4, () -> Stream.of(files)
     *         .parallel()
     *         .forEach(file -> file.delete());
     *
     * </pre>
     *
     * @param task the task to run
     * @return the result of the task execution
     * @see #executeInCustomPool(String, int, Callable)
     */
    public static ForkJoinTask<?> executeInCustomPool(String poolName, int parallelism, Runnable task) {
        return executeInCustomPool(poolName, parallelism, ForkJoinTask.adapt(task));
    }

    /**
     * Runs a task in a custom {@link ForkJoinPool}. This is useful when a custom pool is needed for the
     * execution of a task that forks {@link java.util.concurrent.ForkJoinTask|ForkJoinTasks} internally.
     * <p>
     * Using a custom {@code ForkJoinPool} can be useful if I/O heavy loads are being processed, as it is
     * ill-advised to use the common pool for such tasks. The common pool is optimized for CPU-bound tasks
     * and running I/O heavy loads in it can lead to thread starvation.
     * <p>
     * The terminal operation of a parallel stream is such an example:
     * <pre>
     *     List<byte[]> bytes = ForkJoinUtils.executeInCustomPool("myPool", 4, () -> Stream.of(files)
     *         .parallel()
     *         .map(file -> file.readBytes())
     *         .collect(Collectors.toList()));
     * </pre>
     *
     * @param task the task to run
     * @return the result of the task execution
     * @see #executeInCustomPool(String, int, Runnable)
     */
    public static <T> ForkJoinTask<T> executeInCustomPool(String poolName, int parallelism, Callable<T> task) {
        return executeInCustomPool(poolName, parallelism, ForkJoinTask.adapt(task));
    }

    private static <T> ForkJoinTask<T> executeInCustomPool(String poolName, int parallelism, ForkJoinTask<T> task) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("Parallelism must be greater than 0");
        }
        ForkJoinPool workerPool = getWorkerPool(Thread.currentThread());
        ForkJoinPool poolToShutdown = null;
        if (workerPool == null || workerPool == ForkJoinPool.commonPool()) {
            poolToShutdown = workerPool = new ForkJoinPool(parallelism, p -> new ForkJoinWorkerThread(p) {
                @Override
                protected void onStart() {
                    super.onStart();
                    setName(poolName + "-" + getPoolIndex());
                }
            }, null, false);
        }

        try {
            return workerPool.submit(task);
        } finally {
            Optional.ofNullable(poolToShutdown)
                    .ifPresent(ForkJoinPool::shutdown);
        }
    }

    static ForkJoinPool getWorkerPool(Thread thread) {
        if (thread instanceof ForkJoinWorkerThread) {
            ForkJoinWorkerThread workerThread = (ForkJoinWorkerThread) thread;
            return workerThread.getPool();
        }
        return null;
    }

    private ForkJoinUtils() {
        // Utility class, no instantiation
    }
}
