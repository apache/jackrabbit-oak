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

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.apache.jackrabbit.guava.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Util methods for {@link java.util.concurrent.Executor}
 */
public class ExecutorUtils {
    private ExecutorUtils() {
        // no instances for you
    }

    public static Executor directExecutor() {
        return DirectExecutor.INSTANCE;
    }

    public static ExecutorService newDirectExecutorService() {
        return new DirectExecutorService();
    }

    public static ExecutorService getExitingExecutorService(ThreadPoolExecutor executor) {
        setDeamonThreadFactory(executor);
        final ExecutorService service = Executors.unconfigurableExecutorService(executor);
        // JVM shutdown hook for graceful executor shutdown
        addRuntimeShutdownHook(executor);
        return service;

    }

    private static void addRuntimeShutdownHook(final ExecutorService executor) {
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> new ExecutorCloser(executor, 120, TimeUnit.SECONDS).close(),
                "RuntimeShutdownHook-for-" + executor));
    }

    private static void setDeamonThreadFactory(final ThreadPoolExecutor executor) {
        executor.setThreadFactory(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setThreadFactory(executor.getThreadFactory())
                        .build());
    }
}
