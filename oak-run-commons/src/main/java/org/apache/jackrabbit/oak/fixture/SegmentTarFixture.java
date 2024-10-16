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
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundExceptionListener;
import org.apache.jackrabbit.oak.segment.aws.AwsContext;
import org.apache.jackrabbit.oak.segment.aws.AwsPersistence;
import org.apache.jackrabbit.oak.segment.aws.Configuration;
import org.apache.jackrabbit.oak.segment.azure.v8.AzurePersistenceV8;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServerSync;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentTarFixture extends OakFixture {

    private static final Logger log = LoggerFactory.getLogger(SegmentTarFixture.class);

    /**
     * Listener instance doing nothing on a {@code SegmentNotFoundException}
     */
    SegmentNotFoundExceptionListener IGNORE_SNFE = new SegmentNotFoundExceptionListener() {
        @Override
        public void notify(@NotNull SegmentId id, @NotNull SegmentNotFoundException snfe) { }
    };

    private static final int MB = 1024 * 1024;
    private static final int DEFAULT_TIMEOUT = 60_000;

    static class SegmentTarFixtureBuilder {
        private final String name;
        private final File base;

        private int maxFileSize;
        private int segmentCacheSize;
        private boolean memoryMapping;
        public int binariesInlineThreshold;
        private boolean useBlobStore;
        private int dsCacheSize;

        private String awsBucketName;
        private String awsRootPath;
        private String awsJournalTableName;
        private String awsLockTableName;

        private String azureConnectionString;
        private String azureContainerName;
        private String azureRootPath;
        
        public static SegmentTarFixtureBuilder segmentTarFixtureBuilder(String name, File directory) {
            return new SegmentTarFixtureBuilder(name, directory);
        }
        
        private SegmentTarFixtureBuilder(String name, File base) {
            this.name = name;
            this.base = base;
        }
        
        public SegmentTarFixtureBuilder withMaxFileSize(int maxFileSize) {
            this.maxFileSize = maxFileSize;
            return this;
        }
        
        public SegmentTarFixtureBuilder withSegmentCacheSize(int segmentCacheSize) {
            this.segmentCacheSize = segmentCacheSize;
            return this;
        }
        
        public SegmentTarFixtureBuilder withMemoryMapping(boolean memoryMapping) {
            this.memoryMapping = memoryMapping;
            return this;
        }

        public SegmentTarFixtureBuilder withBinariesInlineThreshold(int binariesInlineThreshold) {
            this.binariesInlineThreshold = binariesInlineThreshold;
            return this;
        }

        public SegmentTarFixtureBuilder withBlobStore(boolean useBlobStore) {
            this.useBlobStore = useBlobStore;
            return this;
        }

        public SegmentTarFixtureBuilder withDSCacheSize(int dsCacheSize) {
            this.dsCacheSize = dsCacheSize;
            return this;
        }

        public SegmentTarFixtureBuilder withAws(String awsBucketName, String awsRootPath, String awsJournalTableName, String awsLockTableName) {
            this.awsBucketName = awsBucketName;
            this.awsRootPath = awsRootPath;
            this.awsJournalTableName = awsJournalTableName;
            this.awsLockTableName = awsLockTableName;
            return this;
        }

        public SegmentTarFixtureBuilder withAzure(String azureConnectionString, String azureContainerName, String azureRootPath) {
            this.azureConnectionString = azureConnectionString;
            this.azureContainerName = azureContainerName;
            this.azureRootPath = azureRootPath;
            return this;
        }

        public SegmentTarFixture build() {
            return new SegmentTarFixture(this);
        }
    }

    private final File base;
    private final int maxFileSize;
    private final int segmentCacheSize;
    private final boolean memoryMapping;
    private final int binariesInlineThreshold;
    private final boolean useBlobStore;
    private final int dsCacheSize;

    private final boolean withColdStandby;
    private final int syncInterval;
    private final boolean shareBlobStore;
    private final boolean oneShotRun;
    private final boolean secure;

    private final String awsBucketName;
    private final String awsRootPath;
    private final String awsJournalTableName;
    private final String awsLockTableName;

    private final String azureConnectionString;
    private final String azureContainerName;
    private final String azureRootPath;

    private final File parentPath;

    private FileStore[] stores;
    private BlobStoreFixture[] blobStoreFixtures; 

    private StandbyServerSync[] serverSyncs;
    private StandbyClientSync[] clientSyncs;
    private ScheduledExecutorService[] executors;

    private CloudBlobContainer[] containers;

    public SegmentTarFixture(SegmentTarFixtureBuilder builder) {
        this(builder, false, -1);
    }

    public SegmentTarFixture(SegmentTarFixtureBuilder builder, boolean withColdStandby, int syncInterval) {
        this(builder, withColdStandby, syncInterval, false, false, false);
    }

    public SegmentTarFixture(SegmentTarFixtureBuilder builder, boolean withColdStandby, int syncInterval,
            boolean shareBlobStore, boolean oneShotRun, boolean secure) {
        super(builder.name);
        this.base = builder.base;
        this.parentPath = new File(base, unique);

        this.maxFileSize = builder.maxFileSize;
        this.segmentCacheSize = builder.segmentCacheSize;
        this.memoryMapping = builder.memoryMapping;
        this.binariesInlineThreshold = builder.binariesInlineThreshold;
        this.useBlobStore = builder.useBlobStore;
        this.dsCacheSize = builder.dsCacheSize;

        this.awsBucketName = builder.awsBucketName;
        this.awsRootPath = builder.awsRootPath;
        this.awsJournalTableName = builder.awsJournalTableName;
        this.awsLockTableName = builder.awsLockTableName;

        this.azureConnectionString = builder.azureConnectionString;
        this.azureContainerName = builder.azureContainerName;
        this.azureRootPath = builder.azureRootPath;

        this.withColdStandby = withColdStandby;
        this.syncInterval = syncInterval;
        this.shareBlobStore = shareBlobStore;
        this.oneShotRun = oneShotRun;
        this.secure = secure;
    }

    private static Configuration getAwsConfig(String awsBucketName, String awsRootPath, String awsJournalTableName, String awsLockTableName) {
        return new Configuration() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return null;
            }

            @Override
            public String sessionToken() {
                return null;
            }

            @Override
            public String secretKey() {
                return null;
            }

            @Override
            public String rootDirectory() {
                return awsRootPath;
            }

            @Override
            public String region() {
                return null;
            }

            @Override
            public String lockTableName() {
                return awsLockTableName;
            }

            @Override
            public String journalTableName() {
                return awsJournalTableName;
            }

            @Override
            public String bucketName() {
                return awsBucketName;
            }

            @Override
            public String accessKey() {
                return null;
            }
        };
    }

    @Override
    public Oak getOak(int clusterId) throws Exception {
        FileStoreBuilder fileStoreBuilder = fileStoreBuilder(parentPath)
                .withMaxFileSize(maxFileSize)
                .withSegmentCacheSize(segmentCacheSize)
                .withMemoryMapping(memoryMapping);

        if (awsBucketName != null) {
            Configuration config = getAwsConfig(awsBucketName, awsRootPath, awsJournalTableName, awsLockTableName);
            AwsContext awsContext = AwsContext.create(config);
            fileStoreBuilder.withCustomPersistence(new AwsPersistence(awsContext));
        }

        if (azureConnectionString != null) {
            CloudStorageAccount cloud = CloudStorageAccount.parse(azureConnectionString);
            CloudBlobContainer container = cloud.createCloudBlobClient().getContainerReference(azureContainerName);
            container.createIfNotExists();
            CloudBlobDirectory directory = container.getDirectoryReference(azureRootPath);
            fileStoreBuilder.withCustomPersistence(new AzurePersistenceV8(directory));
        }

        BlobStore blobStore = null;
        if (useBlobStore) {
            FileDataStore fds = new FileDataStore();
            fds.setMinRecordLength(4092);
            fds.init(parentPath.getAbsolutePath());
            blobStore = new DataStoreBlobStore(fds);

            fileStoreBuilder.withBlobStore(blobStore);
        }

        FileStore fs = fileStoreBuilder
            .withBinariesInlineThreshold(binariesInlineThreshold)
            .build();
        Oak oak = newOak(SegmentNodeStoreBuilders.builder(fs).build());
        if (blobStore != null) {
            oak.getWhiteboard()
                .register(BlobAccessProvider.class, (BlobAccessProvider) blobStore, Collections.EMPTY_MAP);
        }

        return oak;
    }

    @Override
    public Oak[] setUpCluster(int n, StatisticsProvider statsProvider) throws Exception {
        init(n);

        Oak[] cluster = new Oak[n];

        for (int i = 0; i < cluster.length; i++) {
            BlobStore blobStore = null;
            if (useBlobStore) {
                blobStoreFixtures[i] = BlobStoreFixture.create(parentPath, true, dsCacheSize, statsProvider);
                blobStore = blobStoreFixtures[i].setUp();
            }

            FileStoreBuilder builder = fileStoreBuilder(new File(parentPath, "primary-" + i));

            if (awsBucketName != null) {
                Configuration config = getAwsConfig(awsBucketName + "-" + i, awsRootPath, awsJournalTableName + "-" + i, awsLockTableName + "-" + i);
                AwsContext awsContext = AwsContext.create(config);
                builder.withCustomPersistence(new AwsPersistence(awsContext));
            }

            if (azureConnectionString != null) {
                CloudStorageAccount cloud = CloudStorageAccount.parse(azureConnectionString);
                CloudBlobContainer container = cloud.createCloudBlobClient().getContainerReference(azureContainerName);
                container.createIfNotExists();
                containers[i] = container;
                CloudBlobDirectory directory = container.getDirectoryReference(azureRootPath + "/primary-" + i);
                builder.withCustomPersistence(new AzurePersistenceV8(directory));
            }

            if (blobStore != null) {
                builder.withBlobStore(blobStore);
            }

            stores[i] = builder
                    .withMaxFileSize(maxFileSize)
                    .withStatisticsProvider(statsProvider)
                    .withSegmentCacheSize(segmentCacheSize)
                    .withMemoryMapping(memoryMapping)
                    .withStrictVersionCheck(true)
                    .withBinariesInlineThreshold(binariesInlineThreshold)
                    .build();

            if (withColdStandby) {
                attachStandby(i, n, statsProvider, blobStore);
            }

            cluster[i] = newOak(SegmentNodeStoreBuilders.builder(stores[i]).build());
            if (blobStore != null) {
                cluster[i].getWhiteboard()
                    .register(BlobAccessProvider.class, (BlobAccessProvider) blobStore, Collections.EMPTY_MAP);
            }
        }
        return cluster;
    }

    /**
     * Attaches a standby instance located at index (n+i) in the cluster to the
     * primary located at index i
     * 
     * @param i
     *            the primary index
     * @param n
     *            the number of primary instances in the cluster
     * @param statsProvider
     *            statistics provider for the file/blob store(s)
     * @param blobStore
     *            the blob store used by the primary (can be <code>null</code>)
     * @throws InvalidFileStoreVersionException
     *             if an incorrect oak-segment-tar version is used
     * @throws IOException
     *             if the file store manifest cannot be saved
     */
    private void attachStandby(int i, int n, StatisticsProvider statsProvider, BlobStore blobStore)
            throws InvalidFileStoreVersionException, IOException {
        FileStoreBuilder builder = fileStoreBuilder(new File(parentPath, "standby-" + i));

        if (useBlobStore) {
            if (shareBlobStore) {
                builder.withBlobStore(blobStore);
            } else {
                blobStoreFixtures[n + i] = BlobStoreFixture.create(parentPath, true, dsCacheSize, statsProvider);
                builder.withBlobStore(blobStoreFixtures[n + i].setUp());
            }
        }

        SegmentGCOptions gcOptions = SegmentGCOptions.defaultGCOptions()
            .setRetainedGenerations(1);

        stores[n + i] = builder
            .withGCOptions(gcOptions)
            .withMaxFileSize(maxFileSize)
            .withStatisticsProvider(statsProvider)
            .withSegmentCacheSize(segmentCacheSize)
            .withMemoryMapping(memoryMapping)
            .withSnfeListener(IGNORE_SNFE)
            .build();

        int port = 0;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }

        serverSyncs[i] = StandbyServerSync.builder()
            .withPort(port)
            .withFileStore(stores[i])
            .withBlobChunkSize(1 * MB)
            .withSecureConnection(secure)
            .build();
        clientSyncs[i] = StandbyClientSync.builder()
            .withHost("127.0.0.1")
            .withPort(port)
            .withFileStore(stores[n + 1])
            .withSecureConnection(secure)
            .withReadTimeoutMs(DEFAULT_TIMEOUT)
            .withAutoClean(false)
            .withSpoolFolder(new File(System.getProperty("java.io.tmpdir")))
            .build();

        if (!oneShotRun) {
            serverSyncs[i].start();
            clientSyncs[i].start();

            executors[i] = Executors.newScheduledThreadPool(1);
            executors[i].scheduleAtFixedRate(clientSyncs[i], 0, syncInterval, TimeUnit.SECONDS);
        }
    }

    /**
     * Initializes various arrays holding internal references based on the
     * settings provided (e.g. use data store or not, attach cold standby
     * instance, etc.)
     * 
     * @param n
     *            number of primary instances in the cluster
     */
    private void init(int n) {
        int fileStoresLength = n;
        int blobStoresLength = 0;

        if (withColdStandby) {
            fileStoresLength = 2 * n;

            if (useBlobStore) {
                if (shareBlobStore) {
                    blobStoresLength = n;
                } else {
                    blobStoresLength = 2 * n;
                }
            } 

            serverSyncs = new StandbyServerSync[n];
            clientSyncs = new StandbyClientSync[n];

            if (!oneShotRun) {
                executors = new ScheduledExecutorService[n];
            }
        } else {
            if (useBlobStore) {
                blobStoresLength = n;
            }
        }

        stores = new FileStore[fileStoresLength];
        blobStoreFixtures = new BlobStoreFixture[blobStoresLength];

        if (azureConnectionString != null) {
            containers = new CloudBlobContainer[n];
        }
    }

    @Override
    public void tearDownCluster() {
        if (withColdStandby) {
            for (StandbyClientSync clientSync : clientSyncs) {
                clientSync.close();
            }

            for (StandbyServerSync serverSync : serverSyncs) {
                serverSync.close();
            }

            if (!oneShotRun) {
                for (ExecutorService executor : executors) {
                    executor.shutdownNow();
                }
            }
        }

        for (FileStore store : stores) {
            store.close();
        }

        if (blobStoreFixtures != null) {
            for (BlobStoreFixture bsf : blobStoreFixtures) {
                bsf.tearDown();
            }
        }

        if (containers != null) {
            for (CloudBlobContainer container : containers) {
                if (container != null) {
                    try {
                        container.deleteIfExists();
                    } catch (StorageException e) {
                        log.error("Can't remove container", e);
                    }
                }
            }
        }

        FileUtils.deleteQuietly(parentPath);
    }

    public BlobStoreFixture[] getBlobStoreFixtures() {
        return blobStoreFixtures;
    }

    public FileStore[] getStores() {
        return stores;
    }

    public StandbyServerSync[] getServerSyncs() {
        return serverSyncs;
    }

    public StandbyClientSync[] getClientSyncs() {
        return clientSyncs;
    }

}
