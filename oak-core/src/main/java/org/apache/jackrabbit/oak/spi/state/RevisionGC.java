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

package org.apache.jackrabbit.oak.spi.state;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link RevisionGCMBean} based on a {@code Runnable}.
 */
public class RevisionGC implements RevisionGCMBean {
    private static final Logger log = LoggerFactory.getLogger(RevisionGC.class);

    private final Runnable gc;
    private final ExecutorService executorService;

    private Future<Long> gcOp;

    /**
     * @param gc               Revision garbage collector
     * @param executorService  executor service for running the garbage collection task
     *                         in the background.
     */
    public RevisionGC(
            @Nonnull Runnable gc,
            @Nonnull ExecutorService executorService) {
        this.gc = checkNotNull(gc);
        this.executorService = checkNotNull(executorService);
    }


    @Nonnull
    @Override
    public String startRevisionGC() {
        if (gcOp != null && !gcOp.isDone()) {
            return "Garbage collection already running";
        } else {
            gcOp = executorService.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    long t0 = System.nanoTime();
                    gc.run();
                    return System.nanoTime() - t0;
                }
            });
            return getRevisionGCStatus();
        }
    }

    @Nonnull
    @Override
    public String getRevisionGCStatus() {
        if (gcOp == null) {
            return "Garbage collection not started";
        } else if (gcOp.isCancelled()) {
            return "Garbage collection cancelled";
        } else if (gcOp.isDone()) {
            try {
                return "Garbage collection completed in " + formatTime(gcOp.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return "Garbage Collection status unknown: " + e.getMessage();
            } catch (ExecutionException e) {
                log.error("Garbage collection failed", e.getCause());
                return "Garbage collection failed: " + e.getCause().getMessage();
            }
        } else {
            return "Garbage collection running";
        }
    }

    private static String formatTime(long nanos) {
        return TimeUnit.MINUTES.convert(nanos, TimeUnit.NANOSECONDS) + " minutes";
    }
}
