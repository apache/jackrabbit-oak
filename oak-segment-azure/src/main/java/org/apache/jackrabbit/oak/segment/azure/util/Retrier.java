/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.segment.azure.util;

import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Retrier {
    private static final Logger log = LoggerFactory.getLogger(Retrier.class);

    private final int maxAttempts;
    private final int intervalMs;

    Retrier(int maxAttempts, int intervalMs) {
        this.maxAttempts = maxAttempts;
        this.intervalMs = intervalMs;
    }

    public static Retrier withParams(int maxAttempts, int intervalMs) {
        return new Retrier(maxAttempts, intervalMs);
    }

    public <T> T execute(ThrowingSupplier<T> supplier) throws IOException {
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return supplier.get();
            } catch (Exception e) {
                // if last attempt fails or if unexpected exception, exit by throwing the last exception
                if (attempt == maxAttempts) {
                    log.error("Can't execute the operation (attempt {}/{}). Reason: {}", attempt, maxAttempts, e.getMessage());
                    throw e;
                } else if (!(e instanceof IOException || e instanceof RepositoryNotReachableException)) {
                    throw new RuntimeException("Unexpected exception while executing the operation", e);
                }
                log.error("Can't execute the operation (attempt {}/{}). Retrying in {} ms...", attempt, maxAttempts, intervalMs, e);
            }
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore the interrupted status
                throw new RuntimeException("Retries interrupted");
            }
        }
        throw new AssertionError("Should never reach here");
    }

    public void execute(ThrowingRunnable runnable) throws IOException {
        execute(() -> {
            runnable.run();
            return null;
        });
    }

    @FunctionalInterface
    public interface ThrowingSupplier<T> {
        T get() throws IOException;
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws IOException;
    }
}
