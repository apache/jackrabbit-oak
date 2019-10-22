/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import javax.jcr.RepositoryException;

import com.microsoft.azure.storage.StorageException;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.RandomInputStream;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreStats;
import org.apache.jackrabbit.oak.plugins.blob.datastore.BlobStoreStatsTestableFileDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStatsCollector;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AzureDataStoreBlobStatsTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private BlobStoreStats stats;
    private DataStoreBlobStore dsbs;
    private AzureDataStore ds;
    private String container;

    @BeforeClass
    public static void assumptions() {
        assumeTrue(AzureDataStoreUtils.isAzureConfigured());
    }

    @Before
    public void setup() throws IOException, RepositoryException, URISyntaxException, StorageException {
        Random randomGen = new Random();
        Properties props = AzureDataStoreUtils.getAzureConfig();
        container = String.valueOf(randomGen.nextInt(9999)) + "-" + String.valueOf(randomGen.nextInt(9999))
                + "-test";
        props.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, container);

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        StatisticsProvider statsProvider = new DefaultStatisticsProvider(executor);
        stats = new BlobStoreStats(statsProvider);

        ds = new GetRecDelayedAzureDataStore(stats);
        ds.setProperties(props);
        ds.init(folder.newFolder().getAbsolutePath());

        dsbs = new DataStoreBlobStore(ds);
        dsbs.setBlobStatsCollector(stats);
    }

    @After
    public void teardown() {
        ds = null;
        try {
            AzureDataStoreUtils.deleteContainer(container);
        } catch (Exception ignore) {}
    }

    @Test
    public void testGetRecordStats() throws IOException, DataStoreException {
        DataRecord addedRecord = dsbs.addRecord(new RandomInputStream(System.currentTimeMillis(), 20*1024));

        long getRecordTimeLastMinuteBefore = sum((long[]) stats.getDownloadRateHistory().get("per second"));

        DataRecord readRecord = dsbs.getRecord(addedRecord.getIdentifier());
        try (InputStream in = readRecord.getStream()) {
            while (in.available() > 0) { in.read(); }
        }

        long getRecordTimeLastMinuteAfter = waitForNonzeroMetric(
                input -> sum((long[]) input.getDownloadRateHistory().get("per second")), stats);

        assertTrue(getRecordTimeLastMinuteBefore < getRecordTimeLastMinuteAfter);
    }


    private long sum(long[] a) {
        long rv = 0L;
        for (long l : a) {
            rv += l;
        }
        return rv;
    }

    private <T> Long waitForNonzeroMetric(Function<T, Long> f, T input) {
        int intervalMilliseconds = 100;
        int waitMilliseconds = 1000;
        long end = System.currentTimeMillis() + waitMilliseconds;
        Long output = f.apply(input);
        if (null != output && output > 0L) {
            return output;
        }
        do {
            try {
                Thread.sleep(intervalMilliseconds);
            }
            catch (InterruptedException e) { }
            output = f.apply(input);
            if (null != output && output > 0L) {
                return output;
            }
        }
        while (System.currentTimeMillis() < end);
        return 0L;
    }

    private static class GetRecDelayedAzureDataStore extends AzureDataStore {
        private BlobStatsCollector stats;
        GetRecDelayedAzureDataStore(BlobStatsCollector stats) {
            this.stats = stats;
        }

        @Override
        public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
            long start = System.nanoTime();
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) { }
            return BlobStoreStatsTestableFileDataStore.ReadDelayedDataRecord.wrap(super.getRecord(identifier), stats, start);
        }
    }
}
