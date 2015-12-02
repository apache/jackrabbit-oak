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
 * Utility class to properly close any ExecutorService. It ensures
 * that executor is properly closed once the call from close is returned
 */
public final class ExecutorCloser implements Closeable {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ExecutorService executorService;
    private final int timeout;
    private final TimeUnit timeUnit;

    public ExecutorCloser(@Nullable ExecutorService executorService) {
        this(executorService, 5, TimeUnit.SECONDS);
    }

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
            log.error("Error while shutting down the executorService", e);
            Thread.currentThread().interrupt();
        } finally {
            if (!executorService.isTerminated()) {
                log.warn("ExecutorService didn't shutdown properly. Will be forced now.");
            }
            executorService.shutdownNow();
        }
    }
}