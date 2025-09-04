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
     * In contrast to {@link #executeInCustomPool(String, int, Runnable)} this method returns a
     * {@code ForkJoinTask}, and there is no guarantee that the execution has completed until
     * {@link ForkJoinTask#join()} (or a similar method) is called.
     * <p>
     * Using a custom {@code ForkJoinPool} can be useful if I/O heavy loads are being processed, as it is
     * ill-advised to use the common pool for such tasks. The common pool is optimized for CPU-bound tasks
     * and running I/O heavy loads in it can lead to thread starvation.
     * <p>
     * The terminal operation of a parallel stream is such an example:
     * <pre>
     *     ForkJoinTask<Void> task = ForkJoinUtils.submitInCustomPool("myPool", 4, () -> Stream.of(files)
     *         .parallel()
     *         .forEach(file -> file.delete());
     *     task.join();
     * </pre>
     *
     * @param poolName the name of the custom thread pool; thread names are prefixed with this name
     * @param parallelism the number of threads in the pool
     * @param task the task to run
     * @return the result of the task execution
     * @see #submitInCustomPool(String, int, Callable)
     * @see #executeInCustomPool(String, int, Runnable)
     */
    @SuppressWarnings("unchecked")
    public static ForkJoinTask<Void> submitInCustomPool(String poolName, int parallelism, Runnable task) {
        return submitInCustomPool(poolName, parallelism, (ForkJoinTask<Void>) ForkJoinTask.adapt(task));
    }

    /**
     * Runs a task in a custom {@link ForkJoinPool}. This is useful when a custom pool is needed for the
     * execution of a task that forks {@link java.util.concurrent.ForkJoinTask|ForkJoinTasks} internally.
     * <p>
     * In contrast to {@link #submitInCustomPool(String, int, Runnable)} this method blocks until all
     * threads have completed their execution.
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
     * </pre>
     *
     * @param poolName the name of the custom thread pool; thread names are prefixed with this name
     * @param parallelism the number of threads in the pool
     * @param task the task to run
     * @see #submitInCustomPool(String, int, Runnable)
     */
    public static void executeInCustomPool(String poolName, int parallelism, Runnable task) {
        submitInCustomPool(poolName, parallelism, task).join();
    }

    /**
     * Runs a task in a custom {@link ForkJoinPool}. This is useful when a custom pool is needed for the
     * execution of a task that forks {@link java.util.concurrent.ForkJoinTask|ForkJoinTasks} internally.
     * <p>
     * In contrast to {@link #invokeInCustomPool(String, int, Callable)} this method returns a
     * {@code ForkJoinTask}, and there is no guarantee that the execution has completed until
     * {@link ForkJoinTask#join()} (or a similar method) is called.
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
     * @param poolName the name of the custom thread pool; thread names are prefixed with this name
     * @param parallelism the number of threads in the pool
     * @param task the task to run
     * @return the result of the task execution
     * @see #invokeInCustomPool(String, int, Callable)
     */
    public static <T> ForkJoinTask<T> submitInCustomPool(String poolName, int parallelism, Callable<T> task) {
        return submitInCustomPool(poolName, parallelism, ForkJoinTask.adapt(task));
    }

    /**
     * Runs a task in a custom {@link ForkJoinPool}. This is useful when a custom pool is needed for the
     * execution of a task that forks {@link java.util.concurrent.ForkJoinTask|ForkJoinTasks} internally.
     * <p>
     * In contrast to {@link #submitInCustomPool(String, int, Callable)} this method blocks until all
     * threads have completed their execution and returns the result of the task.
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
     * @param poolName the name of the custom thread pool; thread names are prefixed with this name
     * @param parallelism the number of threads in the pool
     * @param task the task to run
     * @return the result of the task execution
     * @see #invokeInCustomPool(String, int, Callable)
     */
    public static <T> T invokeInCustomPool(String poolName, int parallelism, Callable<T> task) {
        return submitInCustomPool(poolName, parallelism, task).join();
    }

    private static <T> ForkJoinTask<T> submitInCustomPool(String poolName, int parallelism, ForkJoinTask<T> task) {
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
