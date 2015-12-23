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

package org.apache.jackrabbit.oak.spi.blob;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStatsCollector;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StatsCollectorTest {
    private BlobStore store;
    private TestCollector collector = new TestCollector();

    @Before
    public void setupStore(){
        store = createBlobStore(collector);
    }

    protected BlobStore createBlobStore(BlobStatsCollector collector) {
        MemoryBlobStore store = new MemoryBlobStore();
        store.setStatsCollector(collector);
        return store;
    }

    @Test
    public void uploadCallback() throws Exception {
        store.writeBlob(testStream(1042));
        assertEquals(1042, collector.size);
    }

    @Test
    public void downloadCallback() throws Exception {
        String id = store.writeBlob(testStream(1042));

        collector.size = 0;
        InputStream is = store.getInputStream(id);
        CountingOutputStream cos = new CountingOutputStream(new NullOutputStream());
        IOUtils.copy(is, cos);
        is.close();

        assertEquals(1042, cos.getCount());
        assertEquals(1042, collector.size);
    }

    private InputStream testStream(int size) {
        //Cannot use NullInputStream as it throws exception upon EOF
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    private static class TestCollector implements BlobStatsCollector {
        long size;

        @Override
        public void uploaded(long timeTaken, TimeUnit unit, long size) {
            this.size = size;
        }

        @Override
        public void downloaded(String blobId, long timeTaken, TimeUnit unit, long size) {
            this.size = size;
        }
    }
}
