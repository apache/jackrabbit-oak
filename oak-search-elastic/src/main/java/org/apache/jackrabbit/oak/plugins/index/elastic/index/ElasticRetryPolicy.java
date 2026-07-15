/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import org.apache.jackrabbit.oak.plugins.index.ConfigHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ElasticRetryPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticRetryPolicy.class);

    // 0 - disabled, > 0 - retry for this number of seconds to reconnect to Elastic
    public static final String OAK_INDEXER_ELASTIC_CONNECTION_RETRY_SECONDS = "oak.indexer.elastic.connectionRetrySeconds";
    public static final int DEFAULT_OAK_INDEXER_ELASTIC_CONNECTION_RETRY_SECONDS = 30;

    public interface IOOperation {
        void execute() throws IOException;
    }

    public static final ElasticRetryPolicy NO_RETRY = new ElasticRetryPolicy(1, 0, 0, 0) {
        @Override
        public void withRetries(IOOperation callable) throws IOException {
            // No retries, just execute the operation
            callable.execute();
        }
    };

    public static ElasticRetryPolicy createRetryPolicyFromSystemProperties() {
        long connectionRetrySeconds = ConfigHelper.getSystemPropertyAsInt(
                OAK_INDEXER_ELASTIC_CONNECTION_RETRY_SECONDS,
                DEFAULT_OAK_INDEXER_ELASTIC_CONNECTION_RETRY_SECONDS);
        if (connectionRetrySeconds <= 0) {
            return NO_RETRY;
        }
        return new ElasticRetryPolicy(100, connectionRetrySeconds * 1000, 50, 5000);
    }

    private final int maxAttempts;
    private final long maxRetryTimeMs;
    private final long initialIntervalMs;
    private final long maxIntervalMs;

    public ElasticRetryPolicy(int maxAttempts, long maxRetryTimeMs, long initialIntervalMs, long maxIntervalMs) {
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("Invalid value for maxAttempts: " + maxAttempts + ". Must be greater than 0");
        }
        if (maxRetryTimeMs < 0) {
            throw new IllegalArgumentException("Invalid value for maxRetryTimeMs: " + maxRetryTimeMs + ". Must be non-negative");
        }
        if (initialIntervalMs < 0) {
            throw new IllegalArgumentException("Invalid value for initialIntervalMs: " + initialIntervalMs + ". Must be non-negative");
        }
        if (maxIntervalMs < 0) {
            throw new IllegalArgumentException("Invalid value for maxIntervalMs: " + maxIntervalMs + ". Must be non-negative");
        }
        this.maxAttempts = maxAttempts;
        this.maxRetryTimeMs = maxRetryTimeMs;
        this.initialIntervalMs = initialIntervalMs;
        this.maxIntervalMs = maxIntervalMs;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public long getMaxRetryTimeMs() {
        return maxRetryTimeMs;
    }

    public long getInitialIntervalMs() {
        return initialIntervalMs;
    }

    public long getMaxIntervalMs() {
        return maxIntervalMs;
    }

    public void withRetries(IOOperation callable) throws IOException {
        int numberOfAttempts = 0;
        long retryUntil = 0;
        long waitTime = initialIntervalMs;
        while (true) {
            numberOfAttempts++;
            if (numberOfAttempts > 1) {
                // Log the retry attempt only if it's not the first attempt
                LOG.info("Retrying operation (attempt {}/{})", numberOfAttempts, maxAttempts);
            }
            try {
                callable.execute();
                return; // Success, exit the loop
            } catch (IOException e) {
                if (numberOfAttempts >= maxAttempts) {
                    LOG.warn("Maximum retries exceeded, giving up. Operation failed {} times. Exception: {}", numberOfAttempts, e.toString());
                    throw e;
                }
                long now = System.nanoTime();
                if (retryUntil == 0) {
                    retryUntil = now + TimeUnit.MILLISECONDS.toNanos(maxRetryTimeMs);
                }
                if (now > retryUntil) {
                    LOG.warn("Max retry time exceeded. Operation failed after {} ms and {} attempts", maxRetryTimeMs, numberOfAttempts, e);
                    throw e;
                }
                LOG.warn("Operation failed in attempt {}/{}. Retrying after {} ms", numberOfAttempts, maxAttempts, waitTime, e);
                try {
                    Thread.sleep(waitTime);
                    // Exponential backoff with a cap at maxIntervalMs
                    waitTime = Math.min(waitTime * 2, maxIntervalMs);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "ElasticRetryPolicy{" +
                "maxRetries=" + maxAttempts +
                ", maxRetryTimeMs=" + maxRetryTimeMs +
                ", initialIntervalMs=" + initialIntervalMs +
                ", maxIntervalMs=" + maxIntervalMs +
                '}';
    }
}