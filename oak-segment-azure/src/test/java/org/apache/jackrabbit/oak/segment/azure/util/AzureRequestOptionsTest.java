package org.apache.jackrabbit.oak.segment.azure.util;

import com.microsoft.azure.storage.blob.BlobRequestOptions;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class AzureRequestOptionsTest {

    private BlobRequestOptions blobRequestOptions;

    @Before
    public void setUp() {
        blobRequestOptions = new BlobRequestOptions();
    }

    @Test
    public void testApplyDefaultRequestOptions() {
        AzureRequestOptions.applyDefaultRequestOptions(blobRequestOptions);
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(AzureRequestOptions.DEFAULT_TIMEOUT_EXECUTION)), Long.valueOf(blobRequestOptions.getMaximumExecutionTimeInMs()));
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(AzureRequestOptions.DEFAULT_TIMEOUT_INTERVAL)), Long.valueOf(blobRequestOptions.getTimeoutIntervalInMs()));
    }

    @Test
    public void testApplyDefaultRequestOptionsWithCustomTimeouts() {
        System.setProperty(AzureRequestOptions.TIMEOUT_EXECUTION_PROP, "10");
        System.setProperty(AzureRequestOptions.TIMEOUT_INTERVAL_PROP, "5");

        AzureRequestOptions.applyDefaultRequestOptions(blobRequestOptions);
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(10)), Long.valueOf(blobRequestOptions.getMaximumExecutionTimeInMs()));
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(5)), Long.valueOf(blobRequestOptions.getTimeoutIntervalInMs()));

        System.clearProperty(AzureRequestOptions.TIMEOUT_EXECUTION_PROP);
        System.clearProperty(AzureRequestOptions.TIMEOUT_INTERVAL_PROP);
    }

    @Test
    public void testOptimiseForWriteOperations() {
        BlobRequestOptions writeBlobRequestoptions = AzureRequestOptions.optimiseForWriteOperations(blobRequestOptions);
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(AzureRequestOptions.DEFAULT_TIMEOUT_EXECUTION)), Long.valueOf(writeBlobRequestoptions.getMaximumExecutionTimeInMs()));
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(AzureRequestOptions.DEFAULT_TIMEOUT_INTERVAL)), Long.valueOf(writeBlobRequestoptions.getTimeoutIntervalInMs()));
    }

    @Test
    public void testOptimiseForWriteOperationsWithCustomTimeouts() {
        System.setProperty(AzureRequestOptions.WRITE_TIMEOUT_EXECUTION_PROP, "10");
        System.setProperty(AzureRequestOptions.WRITE_TIMEOUT_INTERVAL_PROP, "5");

        BlobRequestOptions writeBlobRequestoptions = AzureRequestOptions.optimiseForWriteOperations(blobRequestOptions);
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(10)), Long.valueOf(writeBlobRequestoptions.getMaximumExecutionTimeInMs()));
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(5)), Long.valueOf(writeBlobRequestoptions.getTimeoutIntervalInMs()));

        System.clearProperty(AzureRequestOptions.WRITE_TIMEOUT_EXECUTION_PROP);
        System.clearProperty(AzureRequestOptions.WRITE_TIMEOUT_INTERVAL_PROP);
    }
}
