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

import com.microsoft.azure.storage.RetryLinearRetry;
import com.microsoft.azure.storage.blob.BlobRequestOptions;

import java.util.concurrent.TimeUnit;

public class AzureRequestOptionsV8 {

    static final String RETRY_ATTEMPTS_PROP = "segment.azure.retry.attempts";
    static final int DEFAULT_RETRY_ATTEMPTS = 5;

    static final String RETRY_BACKOFF_PROP = "segment.azure.retry.backoff";
    static final int DEFAULT_RETRY_BACKOFF_SECONDS = 5;

    static final String TIMEOUT_EXECUTION_PROP = "segment.timeout.execution";
    static final int DEFAULT_TIMEOUT_EXECUTION = 30;

    static final String TIMEOUT_INTERVAL_PROP = "segment.timeout.interval";
    static final int DEFAULT_TIMEOUT_INTERVAL = 1;

    static final String WRITE_TIMEOUT_EXECUTION_PROP = "segment.write.timeout.execution";

    static final String WRITE_TIMEOUT_INTERVAL_PROP = "segment.write.timeout.interval";

    private AzureRequestOptionsV8() {
    }

    /**
     * Apply default request options to the blobRequestOptions if they are not already set.
     * @param blobRequestOptions
     */
    public static void applyDefaultRequestOptions(BlobRequestOptions blobRequestOptions) {
        if (blobRequestOptions.getRetryPolicyFactory() == null) {
            int retryAttempts = Integer.getInteger(RETRY_ATTEMPTS_PROP, DEFAULT_RETRY_ATTEMPTS);
            if (retryAttempts > 0) {
                Integer retryBackoffSeconds = Integer.getInteger(RETRY_BACKOFF_PROP, DEFAULT_RETRY_BACKOFF_SECONDS);
                blobRequestOptions.setRetryPolicyFactory(new RetryLinearRetry((int) TimeUnit.SECONDS.toMillis(retryBackoffSeconds), retryAttempts));
            }
        }
        if (blobRequestOptions.getMaximumExecutionTimeInMs() == null) {
            int timeoutExecution = Integer.getInteger(TIMEOUT_EXECUTION_PROP, DEFAULT_TIMEOUT_EXECUTION);
            if (timeoutExecution > 0) {
                blobRequestOptions.setMaximumExecutionTimeInMs((int) TimeUnit.SECONDS.toMillis(timeoutExecution));
            }
        }
        if (blobRequestOptions.getTimeoutIntervalInMs() == null) {
            int timeoutInterval = Integer.getInteger(TIMEOUT_INTERVAL_PROP, DEFAULT_TIMEOUT_INTERVAL);
            if (timeoutInterval > 0) {
                blobRequestOptions.setTimeoutIntervalInMs((int) TimeUnit.SECONDS.toMillis(timeoutInterval));
            }
        }
    }

    /**
     * Optimise the blob request options for write operations. This method does not change the original blobRequestOptions.
     * This method also applies the default request options if they are not already set, by calling {@link #applyDefaultRequestOptions(BlobRequestOptions)}
     * @param blobRequestOptions
     * @return write optimised blobRequestOptions
     */
    public static BlobRequestOptions optimiseForWriteOperations(BlobRequestOptions blobRequestOptions) {
        BlobRequestOptions writeOptimisedBlobRequestOptions = new BlobRequestOptions(blobRequestOptions);
        applyDefaultRequestOptions(writeOptimisedBlobRequestOptions);

        Integer writeTimeoutExecution = Integer.getInteger(WRITE_TIMEOUT_EXECUTION_PROP);
        if (writeTimeoutExecution != null) {
            writeOptimisedBlobRequestOptions.setMaximumExecutionTimeInMs((int) TimeUnit.SECONDS.toMillis(writeTimeoutExecution));
        }

        Integer writeTimeoutInterval = Integer.getInteger(WRITE_TIMEOUT_INTERVAL_PROP);
        if (writeTimeoutInterval != null) {
            writeOptimisedBlobRequestOptions.setTimeoutIntervalInMs((int) TimeUnit.SECONDS.toMillis(writeTimeoutInterval));
        }

        return writeOptimisedBlobRequestOptions;
    }
}
