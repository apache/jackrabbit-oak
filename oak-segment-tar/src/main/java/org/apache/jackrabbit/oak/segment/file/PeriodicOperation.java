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

package org.apache.jackrabbit.oak.segment.file;

import static java.util.concurrent.Executors.defaultThreadFactory;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A background operation that run continuously. Executions of this operation
 * are interleaved by a user-specified period of time.
 */
class PeriodicOperation {

    private final ScheduledExecutorService scheduler;

    private final long period;

    private final TimeUnit timeUnit;

    private final Runnable runnable;

    /**
     * Create a new background operation that runs at a specific time rate.
     *
     * @param name     the name of the background operation.
     * @param period   the period between successive executions.
     * @param timeUnit the time unit of the period.
     * @param runnable the {@code Runnable} to execute.
     */
    public PeriodicOperation(String name, long period, TimeUnit timeUnit, Runnable runnable) {
        this.scheduler = newSingleThreadScheduledExecutor(new OperationThreadFactory(defaultThreadFactory(), name));
        this.period = period;
        this.timeUnit = timeUnit;
        this.runnable = runnable;
    }

    /**
     * Start the operation.
     */
    public void start() {
        scheduler.scheduleAtFixedRate(runnable, period, period, timeUnit);
    }

    /**
     * Stop the operation and wait for its termination.
     *
     * @param timeout  the amount of time to wait for the termination.
     * @param timeUnit the time unit of the wait time.
     * @return {@code true} if the operation was stopped in time, {@code false}
     * if stopping the operation timed out.
     * @throws InterruptedException if interrupted while waiting.
     */
    public boolean stop(long timeout, TimeUnit timeUnit) throws InterruptedException {
        scheduler.shutdown();
        return scheduler.awaitTermination(timeout, timeUnit);
    }

}
