package org.apache.jackrabbit.oak.segment.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AzureStorageMonitorPolicyTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private SimpleRemoteStoreMonitor simpleRemoteStoreMonitor;
    private BlobContainerClient container;


    @Before
    public void setup() throws BlobStorageException {
        simpleRemoteStoreMonitor = new SimpleRemoteStoreMonitor();

        container = azurite.getContainer("oak-test",
                new AzureStorageMonitorPolicy().setMonitor(simpleRemoteStoreMonitor))
                .getContainerClient();
    }

    @Test
    public void testSuccess() {
        // there were api calls for the setup
        int successBefore = simpleRemoteStoreMonitor.success;

        // run 2 successful requests:
        container.getProperties();
        container.exists();

        assertEquals(successBefore + 2, simpleRemoteStoreMonitor.success);
        assertEquals(0, simpleRemoteStoreMonitor.error);
    }


    @Test
    public void testBusinessError() {
        // there were api calls for the setup
        int successBefore = simpleRemoteStoreMonitor.success;

        // run error:
        try {
            container.getBlobClient("not-existing-blob").openInputStream();
        } catch (Exception ignored) {
        }
        assertEquals(1, simpleRemoteStoreMonitor.error);
        assertEquals(successBefore, simpleRemoteStoreMonitor.success);
    }

    @Test
    public void testDuration() {
        container.getProperties();
        container.exists();

        assertTrue(simpleRemoteStoreMonitor.totalDuration > 0);
    }

    private static class SimpleRemoteStoreMonitor implements RemoteStoreMonitor {
        int success;
        int error;
        int totalDuration;

        @Override
        public void requestCount() {
            success++;
        }

        @Override
        public void requestError() {
            error++;
        }

        @Override
        public void requestDuration(long duration, TimeUnit timeUnit) {
            totalDuration += duration;
        }
    }
}