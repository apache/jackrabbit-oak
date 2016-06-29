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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A background operation running on a different thread that is executed upon an
 * external trigger. Only one instance of the same operation is guaranteed to
 * run at any time.
 */
class TriggeredOperation {

    private final ExecutorService scheduler;

    private final Runnable runnable;

    private Future<?> future;

    /**
     * Create a new triggered operation.
     *
     * @param name     the name of the operation.
     * @param runnable the code to run when the operation is triggered.
     */
    public TriggeredOperation(String name, Runnable runnable) {
        this.scheduler = newSingleThreadScheduledExecutor(new OperationThreadFactory(defaultThreadFactory(), name));
        this.runnable = runnable;
    }

    /**
     * Trigger the execution of the operation. If the operation is still
     * running, calling this method doesn't have any effect. Otherwise, a new
     * execution of the operation is started.
     */
    public void trigger() {
        synchronized (this) {
            if (future == null || future.isDone()) {
                future = scheduler.submit(runnable);
            }
        }
    }

    /**
     * Stop the operation and wait for its termination.
     *
     * @param timeout  the amount of time to wait.
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
