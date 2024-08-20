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
package org.apache.jackrabbit.oak.index.indexer.document;

import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class DefaultAheadOfTimeBlobDownloaderThrottlerTest {

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    @Test
    public void testReserveSpaceForBlob() throws ExecutionException, InterruptedException, TimeoutException {
        AheadOfTimeBlobDownloaderThrottler aotThrottler = new AheadOfTimeBlobDownloaderThrottler(8192);
        aotThrottler.reserveSpaceForBlob(1, 1024);
        aotThrottler.reserveSpaceForBlob(2, 4096);
        Future<?> f = executorService.submit((() -> {
            long available;
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
