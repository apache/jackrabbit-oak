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

package org.apache.jackrabbit.oak.plugins.blob;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BlobStoreStatsTest {
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private StatisticsProvider statsProvider = new DefaultStatisticsProvider(executor);
    private BlobStoreStats stats = new BlobStoreStats(statsProvider);

    @After
    public void shutDown(){
        new ExecutorCloser(executor).close();
    }

    @Test
    public void upload() throws Exception{
        stats.uploaded(103, TimeUnit.SECONDS, 1079);
        assertEquals(103, stats.getUploadTotalSeconds());
        assertEquals(1079, stats.getUploadTotalSize());
        assertEquals(0, stats.getUploadCount());

        stats.uploadCompleted("foo");
        assertEquals(1, stats.getUploadCount());

        stats.uploaded(53, TimeUnit.SECONDS, 47);
        assertEquals(103 + 53, stats.getUploadTotalSeconds());
        assertEquals(1079 + 47, stats.getUploadTotalSize());

        stats.uploadCompleted("foo");
        assertEquals(2, stats.getUploadCount());
    }

    @Test
    public void download() throws Exception{
        stats.downloaded("foo", 103, TimeUnit.SECONDS, 1079);
        assertEquals(103, stats.getDownloadTotalSeconds());
        assertEquals(1079, stats.getDownloadTotalSize());
        assertEquals(0, stats.getDownloadCount());

        stats.downloadCompleted("foo");
        assertEquals(1, stats.getDownloadCount());

        stats.downloaded("foo", 53, TimeUnit.SECONDS, 47);
        assertEquals(103 + 53, stats.getDownloadTotalSeconds());
        assertEquals(1079 + 47, stats.getDownloadTotalSize());

        stats.downloadCompleted("foo");
        assertEquals(2, stats.getDownloadCount());
    }
}