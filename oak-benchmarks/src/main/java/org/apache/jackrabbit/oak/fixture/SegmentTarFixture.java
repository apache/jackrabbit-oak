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

package org.apache.jackrabbit.oak.fixture;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

class SegmentTarFixture extends OakFixture {

    private FileStore[] stores;

    private BlobStoreFixture[] blobStoreFixtures = new BlobStoreFixture[0];

    private final File base;

    private final int maxFileSizeMB;

    private final int cacheSizeMB;

    private final boolean memoryMapping;

    private final boolean useBlobStore;

    private final int dsCacheSizeInMB;

    public SegmentTarFixture(String name, File base, int maxFileSizeMB, int cacheSizeMB,
        boolean memoryMapping, boolean useBlobStore, int dsCacheSizeInMB) {
        super(name);
        this.base = base;
        this.maxFileSizeMB = maxFileSizeMB;
        this.cacheSizeMB = cacheSizeMB;
        this.memoryMapping = memoryMapping;
        this.useBlobStore = useBlobStore;
        this.dsCacheSizeInMB = dsCacheSizeInMB;
    }

    @Override
    public Oak getOak(int clusterId) throws Exception {
        FileStore fs = fileStoreBuilder(base)
                .withMaxFileSize(maxFileSizeMB)
                .withSegmentCacheSize(cacheSizeMB)
                .withMemoryMapping(memoryMapping)
                .build();
        return newOak(SegmentNodeStoreBuilders.builder(fs).build());
    }

    @Override
    public Oak[] setUpCluster(int n, StatisticsProvider statsProvider) throws Exception {
        Oak[] cluster = new Oak[n];
        stores = new FileStore[cluster.length];
        if (useBlobStore) {
            blobStoreFixtures = new BlobStoreFixture[cluster.length];
        }

        for (int i = 0; i < cluster.length; i++) {
            BlobStore blobStore = null;
            if (useBlobStore) {
                blobStoreFixtures[i] = BlobStoreFixture.create(base, true, dsCacheSizeInMB, statsProvider);
                blobStore = blobStoreFixtures[i].setUp();
            }

            FileStoreBuilder builder = fileStoreBuilder(new File(base, unique));
            if (blobStore != null) {
                builder.withBlobStore(blobStore);
            }
            stores[i] = builder
                    .withMaxFileSize(maxFileSizeMB)
                    .withStatisticsProvider(statsProvider)
                    .withSegmentCacheSize(cacheSizeMB)
                    .withMemoryMapping(memoryMapping)
                    .build();
            cluster[i] = newOak(SegmentNodeStoreBuilders.builder(stores[i]).build());
        }
        return cluster;
    }

    @Override
    public void tearDownCluster() {
        for (FileStore store : stores) {
            store.close();
        }
        for (BlobStoreFixture blobStore : blobStoreFixtures) {
            blobStore.tearDown();
        }
        FileUtils.deleteQuietly(new File(base, unique));
    }

    public BlobStoreFixture[] getBlobStoreFixtures() {
        return blobStoreFixtures;
    }

    public FileStore[] getStores() {
        return stores;
    }

}
