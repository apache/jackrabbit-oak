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
package org.apache.jackrabbit.oak.commons.concurrent;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorUtils {
    private final static Logger LOG = LoggerFactory.getLogger(ExecutorUtils.class);
    
    /**
     * same as {@link #shutdown(ExecutorService, TimeUnit, long)} waiting for 10 seconds.
     * 
     * @param e
     */
    public static void shutdownIn10s(@Nonnull ExecutorService e) {
        shutdown(e, TimeUnit.SECONDS, 10);
    }
    
    /**
     * Convenience method for gracefully shutdown an {@link ExecutorService}.
     * 
     * @param executor The executor to be shut down. Cannot be null.
     * @param unit The time unit for the timeout. Cannot be null.
     * @param timeout how long to wait for. Cannot be negative
     */
    public static void shutdown(@Nonnull ExecutorService executor, 
                                @Nonnull TimeUnit unit, 
                                long timeout) {
        checkNotNull(executor);
        checkNotNull(unit);
        checkArgument(timeout >= 0, "timeout cannot be negative");
        
        try {
            executor.shutdown();
            executor.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            LOG.error("Error while shutting down the ExecutorService", e);
            Thread.currentThread().interrupt();
        } finally {
            if (!executor.isShutdown()) {
                LOG.warn("ExecutorService `{}` didn't shutdown property. Will be forced now.",
                    executor);
            }
            executor.shutdownNow();
        }
    }
}
