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

package org.apache.jackrabbit.oak.segment.azure.util;

import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;

public class AzureRequestOptions {

    static final String RETRY_ATTEMPTS_PROP = "segment.azure.retry.attempts";
    static final int DEFAULT_RETRY_ATTEMPTS = 5;

    static final String RETRY_BACKOFF_PROP = "segment.azure.retry.backoff";
    static final int DEFAULT_RETRY_BACKOFF_SECONDS = 5;

    static final String TIMEOUT_INTERVAL_PROP = "segment.timeout.interval";
    static final int DEFAULT_TIMEOUT_INTERVAL = 1;

    static final String WRITE_TIMEOUT_INTERVAL_PROP = "segment.write.timeout.interval";

    private AzureRequestOptions() {
    }


    public static RequestRetryOptions getRetryOptionsDefault() {
        return getRetryOptionsDefault(null);
    }

    public static RequestRetryOptions getRetryOptionsDefault(String secondaryHost) {
        int maxTries = Integer.getInteger(RETRY_ATTEMPTS_PROP, DEFAULT_RETRY_ATTEMPTS);
        int tryTimeoutInSeconds = getReadTryTimeoutInSeconds();
        long retryDelayInMs = Integer.getInteger(RETRY_BACKOFF_PROP, DEFAULT_RETRY_BACKOFF_SECONDS) * 1_000L;
        long maxRetryDelayInMs = retryDelayInMs;

        return new RequestRetryOptions(RetryPolicyType.FIXED,
                maxTries,
                tryTimeoutInSeconds,
                retryDelayInMs,
                maxRetryDelayInMs,
                secondaryHost);
    }

    /**
     * secondaryHost is null because there is no writer in secondary
     * @return
     */
    public static RequestRetryOptions getRetryOperationsOptimiseForWriteOperations() {
        int maxTries = Integer.getInteger(RETRY_ATTEMPTS_PROP, DEFAULT_RETRY_ATTEMPTS);
        // if the value for write is not set use the read value
        int tryTimeoutInSeconds = Integer.getInteger(WRITE_TIMEOUT_INTERVAL_PROP, getReadTryTimeoutInSeconds());
        long retryDelayInMs = Integer.getInteger(WRITE_TIMEOUT_INTERVAL_PROP, DEFAULT_RETRY_BACKOFF_SECONDS) * 1_000L;
        long maxRetryDelayInMs = retryDelayInMs;

        return new RequestRetryOptions(RetryPolicyType.FIXED,
                maxTries,
                tryTimeoutInSeconds,
                retryDelayInMs,
                maxRetryDelayInMs,
                null);
    }

    private static int getReadTryTimeoutInSeconds() {
        return Integer.getInteger(TIMEOUT_INTERVAL_PROP, DEFAULT_TIMEOUT_INTERVAL);
    }

}