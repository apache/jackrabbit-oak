package org.apache.jackrabbit.oak.segment.azure.util;

import com.microsoft.azure.storage.RetryLinearRetry;
import com.microsoft.azure.storage.blob.BlobRequestOptions;

import java.util.concurrent.TimeUnit;

public class AzureRequestOptions {

    private static final String RETRY_ATTEMPTS_PROP = "segment.azure.retry.attempts";
    private static final int DEFAULT_RETRY_ATTEMPTS = 5;

    private static final String RETRY_BACKOFF_PROP = "segment.azure.retry.backoff";
    private static final int DEFAULT_RETRY_BACKOFF_SECONDS = 5;

    private static final String TIMEOUT_EXECUTION_PROP = "segment.timeout.execution";
    private static final int DEFAULT_TIMEOUT_EXECUTION = 30;

    private static final String TIMEOUT_INTERVAL_PROP = "segment.timeout.interval";
    private static final int DEFAULT_TIMEOUT_INTERVAL = 1;

    private static final String WRITE_TIMEOUT_EXECUTION_PROP = "segment.write.timeout.execution";

    private static final String WRITE_TIMEOUT_INTERVAL_PROP = "segment.write.timeout.interval";

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

    public static BlobRequestOptions optimiseForWriteOperations(BlobRequestOptions blobRequestOptions) {
        BlobRequestOptions writeOptimisedBlobRequestOptions = new BlobRequestOptions(blobRequestOptions);

        Integer writeTimeoutExecution = Integer.getInteger(WRITE_TIMEOUT_EXECUTION_PROP);
        if (writeTimeoutExecution != null) {
            writeOptimisedBlobRequestOptions.setMaximumExecutionTimeInMs(writeTimeoutExecution);
        }

        Integer writeTimeoutInterval = Integer.getInteger(WRITE_TIMEOUT_INTERVAL_PROP);
        if (writeTimeoutInterval != null) {
            writeOptimisedBlobRequestOptions.setTimeoutIntervalInMs(writeTimeoutInterval);
        }

        return writeOptimisedBlobRequestOptions;
    }
}
