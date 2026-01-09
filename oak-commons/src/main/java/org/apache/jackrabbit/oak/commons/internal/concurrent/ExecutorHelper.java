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

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Helper class complementing JDK's @see java.util.concurrent.Executors class with additional ExecutionService creation
 */
public class ExecutorHelper {

    /**
     * Create a ThreadPoolExecutor with an unbounded LinkedBlockingQueue as work queue.
     * <br>
     * NOTE: The maximum number of threads in each executor service, when using a LinkedBlockingQueue(), is corePoolSize.
     * all other tasks are kept in the LinkedBlockingQueue, which is unbounded.
     * <br>
     * @see <a href="https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/ThreadPoolExecutor.html">ThreadPoolExecutor javadoc</a>
     * <br>
     * (Using a bounded queue, such as SynchronousQueue would result in RejectedExecutionHandler if the queue would reach its capacity).
     * We want to keep things simple and don't want to use back pressure or other mechanisms. So in summary, corePoolSize threads are used, per service.
     * @param poolSize The size of the thread pool
     * @param namePattern The name pattern for the threads containing %d replaced by the thread number (calculated by NamedThreadFactory).
     * @param handler  An optional handler for uncaught exceptions
     * @return A ThreadPoolExecutor
     */
    public static ThreadPoolExecutor linkedQueueExecutor(int poolSize, @NotNull String namePattern, Thread.UncaughtExceptionHandler handler) {
        return linkedQueueExecutor(poolSize, poolSize, namePattern, handler);
    }

    /**
     * Create a ThreadPoolExecutor with an unbounded LinkedBlockingQueue as work queue.
     * <br>
     * Helper method delegating to {@link #linkedQueueExecutor(int, String, Thread.UncaughtExceptionHandler)} with null handler.
     * @param poolSize The size of the thread pool
     * @param namePattern The name pattern for the threads containing %d replaced by the thread number (calculated by NamedThreadFactory).
     * @return A ThreadPoolExecutor
     */
    public static ExecutorService linkedQueueExecutor(int poolSize, @NotNull String namePattern) {
        return linkedQueueExecutor(poolSize, namePattern, null);
    }


    /**
     * Create a ThreadPoolExecutor with an unbounded LinkedBlockingQueue as work queue which will start a single thread
     * in presence of work and will shut down the thread after 60 seconds of inactivity.
     * <br>
     * NOTE: ThreadPoolExecutor with an (unbounded) LinkedBlockingQueue does not allow setting both corePoolSize and maximumPoolSize,
     * and thus does not allow starting more than a single thread in combination with reduction to 0.
     * @param namePattern The name pattern for the threads containing %d replaced by the thread number (calculated by NamedThreadFactory).
     * @param handler  An optional handler for uncaught exceptions
     * @return A ThreadPoolExecutor which will activate a single thread on demand and shut it down after 60 seconds of inactivity.
     */
    public static ThreadPoolExecutor onDemandSingleThreadLinkedQueueExecutor(@NotNull String namePattern, Thread.UncaughtExceptionHandler handler) {
        return linkedQueueExecutor(0, 1, namePattern, handler);
    }

    private static ThreadPoolExecutor linkedQueueExecutor(int minSize, int maxSize, @NotNull String namePattern, Thread.UncaughtExceptionHandler handler) {
        // Either minSize=0 and maxSize=1 (maxSize > 1 behaves as if maxSize were 1)
        //     or minSize=maxSize         (maxSize <> minSize behaves as if minSize were maxSize)
        var factory = BasicThreadFactory.builder()
                .daemon(true)
                .namingPattern(namePattern);
        if(handler != null) {
            factory.uncaughtExceptionHandler(handler);
        }
        return new ThreadPoolExecutor(minSize, maxSize, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), factory.build());
    }

}
