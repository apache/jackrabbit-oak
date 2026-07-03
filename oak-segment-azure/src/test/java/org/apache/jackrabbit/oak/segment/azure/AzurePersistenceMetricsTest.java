/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.storage.blob.BlobContainerClient;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.segment.file.MetricsRemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;

public class AzurePersistenceMetricsTest {

    private ScheduledExecutorService executor;
    private DefaultStatisticsProvider statisticsProvider;
    private CounterStats lockLostCounter;

    @Before
    public void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();
        statisticsProvider = new DefaultStatisticsProvider(executor);
        lockLostCounter = statisticsProvider.getCounterStats(
                MetricsRemoteStoreMonitor.REPOSITORY_LOCK_LOST, StatsOptions.DEFAULT);
    }

    @After
    public void tearDown() {
        new ExecutorCloser(executor).close();
    }

    @Test
    public void onRepositoryLockLostIncrementsMetricWhenMonitorAttached() {
        MetricsRemoteStoreMonitor monitor = new MetricsRemoteStoreMonitor(statisticsProvider);
        BlobContainerClient containerClient = Mockito.mock(BlobContainerClient.class);
        AzurePersistence persistence = new AzurePersistence(containerClient, "oak");

        persistence.createArchiveManager(false, false,
                Mockito.mock(IOMonitor.class), Mockito.mock(FileStoreMonitor.class), monitor);

        assertEquals(0, lockLostCounter.getCount());

        persistence.onRepositoryLockLost();

        assertEquals(1, lockLostCounter.getCount());
    }

    @Test
    public void onRepositoryLockLostIsNoOpWhenMonitorNotAttached() {
        BlobContainerClient containerClient = Mockito.mock(BlobContainerClient.class);
        AzurePersistence persistence = new AzurePersistence(containerClient, "oak");

        persistence.onRepositoryLockLost();

        assertEquals(0, lockLostCounter.getCount());
    }
}
