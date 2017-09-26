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

package org.apache.jackrabbit.oak.plugins.document;

import java.lang.management.ManagementFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.codahale.metrics.Meter;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DocumentNodeStoreStatsTest {
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private MetricStatisticsProvider statsProvider =
            new MetricStatisticsProvider(ManagementFactory.getPlatformMBeanServer(),executor);
    private DocumentNodeStoreStats stats = new DocumentNodeStoreStats(statsProvider);

    @After
    public void shutDown(){
        statsProvider.close();
        new ExecutorCloser(executor).close();
    }

    @Test
    public void backgroundRead() throws Exception{
        BackgroundReadStats readStats = new BackgroundReadStats();
        readStats.numExternalChanges = 5;
        stats.doneBackgroundRead(readStats);
        assertEquals(5, getMeter(DocumentNodeStoreStats.BGR_NUM_CHANGES_RATE).getCount());
    }

    @Test
    public void backgroundWrite() throws Exception{
        BackgroundWriteStats writeStats = new BackgroundWriteStats();
        writeStats.num = 7;
        stats.doneBackgroundUpdate(writeStats);
        assertEquals(7, getMeter(DocumentNodeStoreStats.BGW_NUM_WRITES_RATE).getCount());

    }

    private Meter getMeter(String name) {
        return statsProvider.getRegistry().getMeters().get(name);
    }
}
