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

package org.apache.jackrabbit.oak.commons.concurrent;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Utility class to properly close any ExecutorService.
 * </p>
 * 
 * <p>
 * It will attempt a graceful close within the provided timeout. If after such any of the contained
 * tasks are not terminated yet, it will force a shutdown and track a warning in the logs.
 * </p>
 * 
 */
public final class ExecutorCloser implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorCloser.class);
    private final ExecutorService executorService;
    private final int timeout;
    private final TimeUnit timeUnit;

    /**
     * will attempt a graceful close in 5 seconds
     * 
     * @param executorService
     */
    public ExecutorCloser(@Nullable ExecutorService executorService) {
        this(executorService, 5, TimeUnit.SECONDS);
    }

    /**
     * will attempt a graceful close by the provided time.
     * 
     * @param executorService the executor to close
     * @param timeout the time to wait for
     * @param unit the unit of time
     */
    public ExecutorCloser(@Nullable ExecutorService executorService, int timeout, TimeUnit unit) {
        this.executorService = executorService;
        this.timeout = timeout;
        this.timeUnit = unit;
    }

    @Override
    public void close() {
        if (executorService == null) {
            return;
        }
        try {
            executorService.shutdown();
            executorService.awaitTermination(timeout, timeUnit);
        } catch (InterruptedException e) {
            LOG.error("Error while shutting down the ExecutorService", e);
            Thread.currentThread().interrupt();
        } finally {
            if (!executorService.isShutdown()) {
                LOG.warn("ExecutorService `{}` didn't shutdown property. Will be forced now.",
                    executorService);
            }
            executorService.shutdownNow();
        }

    }
}