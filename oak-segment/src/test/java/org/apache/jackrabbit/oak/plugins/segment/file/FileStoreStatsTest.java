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

package org.apache.jackrabbit.oak.plugins.segment.file;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class FileStoreStatsTest {
    @Rule
    public final TemporaryFolder segmentFolder = new TemporaryFolder(new File("target"));

    private FileStore fileStore;
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private StatisticsProvider statsProvider = new DefaultStatisticsProvider(executor);

    @Before
    public void createFileStore() throws Exception {
        BlobStore blobStore = mock(BlobStore.class);
        fileStore = FileStore.builder(segmentFolder.newFolder())
                .withBlobStore(blobStore)
                .withStatisticsProvider(statsProvider).build();
    }

    @After
    public void shutDown(){
        fileStore.close();
        new ExecutorCloser(executor).close();
    }

    @Test
    public void initCall() throws Exception{
        FileStoreStats stats = new FileStoreStats(statsProvider, fileStore, 1000);
        assertEquals(1000, stats.getApproximateSize());

        stats.written(500);
        assertEquals(1500, stats.getApproximateSize());

        stats.reclaimed(250);
        assertEquals(1250, stats.getApproximateSize());

        assertEquals(1, stats.getTarFileCount());
    }

    @Test
    public void tarWriterIntegration() throws Exception{
        FileStoreStats stats = new FileStoreStats(statsProvider, fileStore, 0);
        UUID id = UUID.randomUUID();
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits() & (-1 >>> 4); // OAK-1672
        byte[] data = "Hello, World!".getBytes(UTF_8);

        File file = segmentFolder.newFile();
        TarWriter writer = new TarWriter(file, stats);
        writer.writeEntry(msb, lsb, data, 0, data.length);
        writer.close();

        assertEquals(stats.getApproximateSize(), file.length());
    }

}
