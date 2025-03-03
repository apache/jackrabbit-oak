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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureRequestOptions {
    private static final Logger log = LoggerFactory.getLogger(AzureRequestOptions.class);

    static final String RETRY_POLICY_TYPE_PROP = "segment.retry.policy.type";
    static final String RETRY_POLICY_TYPE_DEFAULT = "fixed";

    static final String RETRY_ATTEMPTS_PROP = "segment.azure.retry.attempts";
    static final int DEFAULT_RETRY_ATTEMPTS = 5;

    static final String TIMEOUT_EXECUTION_PROP = "segment.timeout.execution";
    static final int DEFAULT_TIMEOUT_EXECUTION = 30;

    static final String RETRY_DELAY_MIN_PROP = "segment.retry.delay.min";
    static final int DEFAULT_RETRY_DELAY_MIN = 100;

    static final String RETRY_DELAY_MAX_PROP = "segment.retry.delay.max";
    static final int DEFAULT_RETRY_DELAY_MAX = 5000;

    static final String WRITE_TIMEOUT_EXECUTION_PROP = "segment.write.timeout.execution";

    static final String WRITE_RETRY_DELAY_MIN_PROP = "segment.write.retry.delay.min";

    static final String WRITE_RETRY_DELAY_MAX_PROP = "segment.write.retry.delay.max";

    private AzureRequestOptions() {
    }


    public static RequestRetryOptions getRetryOptionsDefault() {
        return getRetryOptionsDefault(null);
    }

    public static RequestRetryOptions getRetryOptionsDefault(String secondaryHost) {
        RetryPolicyType retryPolicyType = getRetryPolicyType();
        int maxTries = Integer.getInteger(RETRY_ATTEMPTS_PROP, DEFAULT_RETRY_ATTEMPTS);
        int tryTimeoutInSeconds = getReadTryTimeoutInSeconds();
        long retryDelayInMs = getRetryDelayInMs();
        long maxRetryDelayInMs = getMaxRetryDelayInMs();

        log.info("Azure retry policy type set to: {}", retryPolicyType);

        return new RequestRetryOptions(retryPolicyType,
                maxTries,
                tryTimeoutInSeconds,
                retryDelayInMs,
                maxRetryDelayInMs,
                secondaryHost);
    }

    /**
     * secondaryHost is null because there is no writer in secondary
     *
     * @return
     */
    public static RequestRetryOptions getRetryOperationsOptimiseForWriteOperations() {
        RetryPolicyType retryPolicyType = getRetryPolicyType();
        int maxTries = Integer.getInteger(RETRY_ATTEMPTS_PROP, DEFAULT_RETRY_ATTEMPTS);
        // if the value for write are not set use the read value
        int tryTimeoutInSeconds = Integer.getInteger(WRITE_TIMEOUT_EXECUTION_PROP, getReadTryTimeoutInSeconds());
        long retryDelayInMs = Integer.getInteger(WRITE_RETRY_DELAY_MIN_PROP, getRetryDelayInMs());
        long maxRetryDelayInMs = Integer.getInteger(WRITE_RETRY_DELAY_MAX_PROP, getMaxRetryDelayInMs());

        log.info("Azure write retry policy type set to: {}", retryPolicyType);

        return new RequestRetryOptions(retryPolicyType,
                maxTries,
                tryTimeoutInSeconds,
                retryDelayInMs,
                maxRetryDelayInMs,
                null);
    }

    private static int getReadTryTimeoutInSeconds() {
        return Integer.getInteger(TIMEOUT_EXECUTION_PROP, DEFAULT_TIMEOUT_EXECUTION);
    }

    private static int getRetryDelayInMs() {
        return Integer.getInteger(RETRY_DELAY_MIN_PROP, DEFAULT_RETRY_DELAY_MIN);
    }

    private static int getMaxRetryDelayInMs() {
        return Integer.getInteger(RETRY_DELAY_MAX_PROP, DEFAULT_RETRY_DELAY_MAX);
    }

    private static RetryPolicyType getRetryPolicyType() {
        String envRetryPolicyType = System.getProperty(RETRY_POLICY_TYPE_PROP, RETRY_POLICY_TYPE_DEFAULT).toUpperCase();

        try {
            return RetryPolicyType.valueOf(envRetryPolicyType);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("Retry policy '%s' not supported. Please use FIXED or EXPONENTIAL", envRetryPolicyType), e);
        }
    }

}