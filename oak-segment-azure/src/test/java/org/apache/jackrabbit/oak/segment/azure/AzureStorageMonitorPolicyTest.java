package org.apache.jackrabbit.oak.segment.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AzureStorageMonitorPolicyTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private SimpleRemoteStoreMonitor simpleRemoteStoreMonitor;


    @Before
    public void setup() throws BlobStorageException, InvalidKeyException, URISyntaxException {
        simpleRemoteStoreMonitor = new SimpleRemoteStoreMonitor();
    }

    public BlobContainerClient setupClient(String connectionString) {
        return new BlobServiceClientBuilder()
                .addPolicy(new AzureStorageMonitorPolicy()
                        .setMonitor(simpleRemoteStoreMonitor))
                .connectionString(connectionString)
                .buildClient()
                .getBlobContainerClient("oak-test");
    }


    @Test
    public void testSuccess() {
        BlobContainerClient container = setupClient(azurite.getConnectionString());

        // run 2 successful requests:
        container.getProperties();
        container.exists();

        assertEquals(2, simpleRemoteStoreMonitor.success);
        assertEquals(0, simpleRemoteStoreMonitor.error);
    }


    @Test
    public void testBusinessError() {
        BlobContainerClient container = setupClient(azurite.getConnectionString());

        // run error:
        try {
            container.getBlobClient("not-existing-blob").openInputStream();
        } catch (Exception ignored) {
        }
        assertEquals(1, simpleRemoteStoreMonitor.error);
        assertEquals(0, simpleRemoteStoreMonitor.success);
    }

    @Test
    public void testDuration() {
        BlobContainerClient container = setupClient(azurite.getConnectionString());

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