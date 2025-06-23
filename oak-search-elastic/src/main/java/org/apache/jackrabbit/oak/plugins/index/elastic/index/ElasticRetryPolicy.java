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

    public static final ElasticRetryPolicy NO_RETRY = new ElasticRetryPolicy(0, 0, 0, 0) {
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

    private final int maxRetries;
    private final long maxRetryTimeMs;
    private final long initialIntervalMs;
    private final long maxIntervalMs;

    public ElasticRetryPolicy(int maxRetries, long maxRetryTimeMs, long initialIntervalMs, long maxIntervalMs) {
        this.maxRetries = maxRetries;
        this.maxRetryTimeMs = maxRetryTimeMs;
        this.initialIntervalMs = initialIntervalMs;
        this.maxIntervalMs = maxIntervalMs;
    }

    public int getMaxRetries() {
        return maxRetries;
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
        int timesRetried = 0;
        long retryUntil = 0;
        long waitTime = initialIntervalMs;
        while (true) {
            if (timesRetried > 0) {
                // Log the retry attempt only if it's not the first attempt
                LOG.info("Retrying operation (attempt {}/{})", timesRetried + 1, maxRetries + 1);
            }
            try {
                callable.execute();
                return; // Success, exit the loop
            } catch (IOException e) {
                timesRetried++;
                if (timesRetried > maxRetries) {
                    LOG.warn("Maximum retries exceeded, giving up. Operation failed {} times. Exception: {}", timesRetried, e.toString());
                    throw e;
                }
                long now = System.nanoTime();
                if (retryUntil == 0) {
                    retryUntil = now + TimeUnit.MILLISECONDS.toNanos(maxRetryTimeMs);
                }
                if (now > retryUntil) {
                    LOG.warn("Max retry time exceeded. Operation failed after {} ms and {} attempts", maxRetryTimeMs, timesRetried, e);
                    throw e;
                }
                LOG.warn("Operation failed. Retrying after {} ms (attempt {}/{})", waitTime, timesRetried, maxRetries, e);
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
}