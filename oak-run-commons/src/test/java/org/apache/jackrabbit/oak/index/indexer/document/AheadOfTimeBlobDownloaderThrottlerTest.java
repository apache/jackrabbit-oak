package org.apache.jackrabbit.oak.index.indexer.document;

import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class AheadOfTimeBlobDownloaderThrottlerTest {

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    @Test
    public void testReserveSpaceForBlob() throws ExecutionException, InterruptedException, TimeoutException {
        AheadOfTimeBlobDownloaderThrottler aotThrottler = new AheadOfTimeBlobDownloaderThrottler(8192);
        aotThrottler.reserveSpaceForBlob(1, 1024);
        aotThrottler.reserveSpaceForBlob(2, 4096);
        Future<?> f = executorService.submit((() -> {
            long available = 0;
            try {
                available = aotThrottler.reserveSpaceForBlob(3, 4096);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println("Available: " + available);
            assertEquals(4096, available);
        }));
        aotThrottler.advanceIndexer(2);
        f.get(1, TimeUnit.SECONDS);
    }
}
