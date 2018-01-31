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

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.SegmentTarFixture.SegmentTarFixtureBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentNodeStoreBuilder.newRDBDocumentNodeStoreBuilder;

public abstract class OakFixture {

    public static final String OAK_MEMORY = "Oak-Memory";
    public static final String OAK_MEMORY_NS = "Oak-MemoryNS";

    public static final String OAK_MONGO = "Oak-Mongo";
    public static final String OAK_MONGO_DS = "Oak-Mongo-DS";
    public static final String OAK_MONGO_NS = "Oak-MongoNS";

    public static final String OAK_RDB = "Oak-RDB";
    public static final String OAK_RDB_DS = "Oak-RDB-DS";

    public static final String OAK_SEGMENT_TAR = "Oak-Segment-Tar";
    public static final String OAK_SEGMENT_TAR_DS = "Oak-Segment-Tar-DS";
    public static final String OAK_SEGMENT_TAR_COLD = "Oak-Segment-Tar-Cold";

    public static final String OAK_COMPOSITE_STORE = "Oak-Composite-Store";
    public static final String OAK_COMPOSITE_MEMORY_STORE = "Oak-Composite-Memory-Store";


    private final String name;
    protected final String unique;

    protected OakFixture(String name) {
        this.name = name;
        this.unique = getUniqueDatabaseName(name);
    }

    public static String getUniqueDatabaseName(String name) {
        return String.format("%s-%d", name, System.currentTimeMillis());
    }

    public abstract Oak getOak(int clusterId) throws Exception;

    public abstract Oak[] setUpCluster(int n, StatisticsProvider statsProvider) throws Exception;

    public abstract void tearDownCluster();

    @Override
    public String toString() {
        return name;
    }

    public static OakFixture getMemory(long cacheSize) {
        return getMemory(OAK_MEMORY, cacheSize);
    }

    public static OakFixture getMemoryNS(long cacheSize) {
        return getMemory(OAK_MEMORY_NS, cacheSize);
    }

    public static OakFixture getMemory(String name, final long cacheSize) {
        return new OakFixture(name) {

            @Override
            public Oak getOak(int clusterId) throws Exception {
                Oak oak;
                oak = newOak(new MemoryNodeStore());
                return oak;
            }

            @Override
            public Oak[] setUpCluster(int n, StatisticsProvider statsProvider) throws Exception {
                Oak[] cluster = new Oak[n];
                for (int i = 0; i < cluster.length; i++) {
                    Oak oak;
                    oak = newOak(new MemoryNodeStore());
                    cluster[i] = oak;
                }
                return cluster;
            }

            @Override
            public void tearDownCluster() {
                // nothing to do
            }
        };
    }

    public static OakFixture getMongo(String uri,
                                      boolean dropDBAfterTest, long cacheSize) {
        return getMongo(OAK_MONGO, uri,
                dropDBAfterTest, cacheSize, false, null, 0);
    }

    public static OakFixture getMongo(String host, int port, String database,
                                      boolean dropDBAfterTest, long cacheSize) {
        return getMongo(OAK_MONGO, host, port, database,
                dropDBAfterTest, cacheSize, false, null, 0);
    }

    public static OakFixture getMongoNS(String uri,
                                      boolean dropDBAfterTest, long cacheSize) {
        return getMongo(OAK_MONGO_NS, uri,
                dropDBAfterTest, cacheSize, false, null, 0);
    }

    public static OakFixture getMongoNS(String host, int port, String database,
                                        boolean dropDBAfterTest, long cacheSize) {
        return getMongo(OAK_MONGO_NS, host, port, database,
                dropDBAfterTest, cacheSize, false, null, 0);
    }

    public static OakFixture getMongo(String name, final String host,
                                      final int port, String database,
                                      final boolean dropDBAfterTest, final long cacheSize,
                                      final boolean useFileDataStore,
                                      final File base,
                                      final int fdsCacheInMB) {
        if (database == null) {
            database = getUniqueDatabaseName(name);
        }
        String uri = "mongodb://" + host + ":" + port + "/" + database;
        return getMongo(name, uri, dropDBAfterTest, cacheSize, useFileDataStore, base, fdsCacheInMB);
    }

    public static OakFixture getMongo(final String name, final String uri,
                                      final boolean dropDBAfterTest, final long cacheSize,
                                      final boolean useDataStore,
                                      final File base, final int dsCacheInMB) {
        return new MongoFixture(name, uri, dropDBAfterTest, cacheSize, useDataStore, base, dsCacheInMB);
    }

    public static OakFixture getRDB(final String name, final String jdbcuri, final String jdbcuser, final String jdbcpasswd,
        final String tablePrefix, final boolean dropDBAfterTest, final long cacheSize, final int vgcMaxAge) {
        return getRDB(name, jdbcuri, jdbcuser, jdbcpasswd, tablePrefix, dropDBAfterTest, cacheSize, false, null, 0, vgcMaxAge);
    }

    public static OakFixture getRDB(final String name, final String jdbcuri, final String jdbcuser, final String jdbcpasswd,
                                    final String tablePrefix, final boolean dropDBAfterTest, final long cacheSize,
                                    final boolean useDataStore, final File base, final int dsCacheInMB, final int vgcMaxAge) {
        return new OakFixture(name) {
            private DocumentNodeStore[] nodeStores;
            private VersionGarbageCollectionJob versionGarbageCollectionJob = null;
            private BlobStoreFixture blobStoreFixture;

            private RDBOptions getOptions(boolean dropDBAFterTest, String tablePrefix) {
                return new RDBOptions().dropTablesOnClose(dropDBAfterTest).tablePrefix(tablePrefix);
            }

            private BlobStore getBlobStore(StatisticsProvider statsProvider) {
                try {
                    if (useDataStore) {
                        initializeBlobStoreFixture(statsProvider);
                        return blobStoreFixture.setUp();
                    } else {
                        DataSource ds = RDBDataSourceFactory.forJdbcUrl(jdbcuri, jdbcuser, jdbcpasswd);
                        return new RDBBlobStore(ds, getOptions(dropDBAfterTest, tablePrefix));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Oak getOak(int clusterId) throws Exception {
                DataSource ds = RDBDataSourceFactory.forJdbcUrl(jdbcuri, jdbcuser, jdbcpasswd);
                DocumentNodeStoreBuilder<?> builder = newRDBDocumentNodeStoreBuilder()
                        .setRDBConnection(ds, getOptions(dropDBAfterTest, tablePrefix)).memoryCacheSize(cacheSize)
                        .setClusterId(clusterId).setLogging(false);
                BlobStore blobStore = getBlobStore(StatisticsProvider.NOOP);
                if (blobStore != null) {
                    builder.setBlobStore(blobStore);
                }
                return newOak(builder.build());
            }

            @Override
            public Oak[] setUpCluster(int n, StatisticsProvider statsProvider) throws Exception {
                Oak[] cluster = new Oak[n];
                nodeStores = new DocumentNodeStore[cluster.length];
                for (int i = 0; i < cluster.length; i++) {
                    BlobStore blobStore = getBlobStore(statsProvider);
                    DataSource ds = RDBDataSourceFactory.forJdbcUrl(jdbcuri, jdbcuser, jdbcpasswd);
                    DocumentNodeStoreBuilder<?> builder = newRDBDocumentNodeStoreBuilder()
                            .setRDBConnection(ds, getOptions(dropDBAfterTest, tablePrefix)).memoryCacheSize(cacheSize)
                            .setStatisticsProvider(statsProvider)
                            // FIXME: OAK-3389
                            .setLeaseCheck(false)
                            .setClusterId(i + 1).setLogging(false);
                    if (blobStore != null) {
                        builder.setBlobStore(blobStore);
                    }
                    nodeStores[i] = builder.build();
                    cluster[i] = newOak(nodeStores[i]);
                }
                if (vgcMaxAge > 0 && nodeStores.length >= 1) {
                    versionGarbageCollectionJob = new VersionGarbageCollectionJob(nodeStores[0], vgcMaxAge);
                    Thread t = new Thread(versionGarbageCollectionJob);
                    t.setDaemon(true);
                    t.start();
                }
                return cluster;
            }

            @Override
            public void tearDownCluster() {
                String dropped = "";
                if (versionGarbageCollectionJob != null) {
                    versionGarbageCollectionJob.stop();
                }
                for (DocumentNodeStore ns : nodeStores) {
                    ns.dispose();
                    if (ns.getDocumentStore() instanceof RDBDocumentStore) {
                        dropped += ((RDBDocumentStore)ns.getDocumentStore()).getDroppedTables();
                    }
                }
                if (dropDBAfterTest) {
                    if (blobStoreFixture != null) {
                        blobStoreFixture.tearDown();
                    }

                    if (dropped.isEmpty()) {
                        throw new RuntimeException("dropdb was set, but tables have not been dropped");
                    }
                }
            }

            private void initializeBlobStoreFixture(StatisticsProvider statsProvider) {
                if (useDataStore && blobStoreFixture == null) {
                    blobStoreFixture = BlobStoreFixture.create(base, true, dsCacheInMB, statsProvider);
                }
            }
        };
    }

    private static class VersionGarbageCollectionJob implements Runnable {

        private static final Logger LOG = LoggerFactory.getLogger(OakFixture.class);
        private boolean stopped = false;
        final VersionGarbageCollector vgc;
        final long maxAge;

        public VersionGarbageCollectionJob(DocumentNodeStore dns, long maxAge) {
            this.vgc = dns.getVersionGarbageCollector();
            this.maxAge = maxAge;
        }

        @Override
        public void run() {
            while(!stopped) {
                try {
                    VersionGCStats stats = this.vgc.gc(maxAge, TimeUnit.SECONDS);
                    LOG.debug("vgc: " + stats);
                    // org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS_RESOLUTION
                    Thread.sleep(5 * 1000);
                }
                catch (Throwable ex) {
                    LOG.warn("While running GC", ex);
                }
            }
        }

        public void stop() {
            this.vgc.cancel();
            this.stopped = true;
        }
    }

    public static OakFixture getSegmentTar(final String name, final File base, final int maxFileSizeMB,
            final int cacheSizeMB, final boolean memoryMapping, final boolean useBlobStore, final int dsCacheInMB,
            final boolean withColdStandby, final int syncInterval, final boolean shareBlobStore, final boolean secure,
            final boolean oneShotRun) {

        SegmentTarFixtureBuilder builder = SegmentTarFixtureBuilder.segmentTarFixtureBuilder(name, base);
        builder.withMaxFileSize(maxFileSizeMB).withSegmentCacheSize(cacheSizeMB).withMemoryMapping(memoryMapping)
                .withBlobStore(useBlobStore).withDSCacheSize(dsCacheInMB);

        return new SegmentTarFixture(builder, withColdStandby, syncInterval, shareBlobStore, secure, oneShotRun);
    }

    public static OakFixture getVanillaSegmentTar(final File base, final int maxFileSizeMB,
            final int cacheSizeMB, final boolean memoryMapping) {

        return getSegmentTar(OakFixture.OAK_SEGMENT_TAR, base, maxFileSizeMB, cacheSizeMB, memoryMapping, false, 0,
                false, -1, false, false, false);
    }

    public static OakFixture getSegmentTarWithDataStore(final File base,
        final int maxFileSizeMB, final int cacheSizeMB, final boolean memoryMapping, final int dsCacheInMB) {
        
        return getSegmentTar(OakFixture.OAK_SEGMENT_TAR_DS, base, maxFileSizeMB, cacheSizeMB, memoryMapping, true, dsCacheInMB,
                false, -1, false, false, false);
    }
    
    public static OakFixture getSegmentTarWithColdStandby(final File base, final int maxFileSizeMB,
            final int cacheSizeMB, final boolean memoryMapping, final boolean useBlobStore, final int dsCacheInMB,
            final int syncInterval, final boolean shareBlobStore, final boolean secure, final boolean oneShotRun) {
        
        return getSegmentTar(OakFixture.OAK_SEGMENT_TAR_COLD, base, maxFileSizeMB, cacheSizeMB, memoryMapping, useBlobStore,
                dsCacheInMB, true, syncInterval, shareBlobStore, secure, oneShotRun);
    }

    public static OakFixture getCompositeStore(final String name, final File base,
                                               final int maxFileSizeMB, final int cacheSizeMB, final boolean memoryMapping,
                                               final int mounts, final int pathsPerMount) {
        return new CompositeStoreFixture(name, base, maxFileSizeMB, cacheSizeMB, memoryMapping, mounts, pathsPerMount);
    }

    public static OakFixture getCompositeMemoryStore(final String name, final int mounts, final int pathsPerMount) {
        return new CompositeStoreFixture(name, mounts, pathsPerMount);
    }

    public static class MongoFixture extends OakFixture {

        private final String uri;

        private final boolean dropDBAfterTest;

        private final long cacheSize;

        private final boolean useDataStore;

        private final File base;

        private final int dsCacheInMB;

        private List<DocumentNodeStore> nodeStores = new ArrayList<>();
        private BlobStoreFixture blobStoreFixture;

        public MongoFixture(final String name, final String uri,
                            final boolean dropDBAfterTest, final long cacheSize,
                            final boolean useDataStore,
                            final File base, final int dsCacheInMB) {
            super(name);
            this.uri = uri;
            this.dropDBAfterTest = dropDBAfterTest;
            this.cacheSize = cacheSize;
            this.useDataStore = useDataStore;
            this.base = base;
            this.dsCacheInMB = dsCacheInMB;
        }

        public DocumentNodeStoreBuilder<?> getBuilder(int clusterId) throws UnknownHostException {
            MongoConnection mongo = new MongoConnection(uri);
            DocumentNodeStoreBuilder<?> builder = new MongoDocumentNodeStoreBuilder() {
                @Override
                public DocumentNodeStore build() {
                    DocumentNodeStore ns = super.build();
                    nodeStores.add(ns);
                    return ns;
                }
            }.setMongoDB(mongo.getDB()).
                    memoryCacheSize(cacheSize).
                    setClusterId(clusterId).
                    setLogging(false);

            configurePersistentCache(builder);
            setupBlobStore(builder, StatisticsProvider.NOOP);
            return builder;
        }

        @Override
        public Oak getOak(int clusterId) throws Exception {
            return newOak(getBuilder(clusterId).build());
        }

        public Oak[] setUpCluster(DocumentNodeStoreBuilder<?>[] builders, StatisticsProvider statsProvider) throws Exception {
            Oak[] cluster = new Oak[builders.length];
            for (int i = 0; i < cluster.length; i++) {
                cluster[i] = newOak(builders[i].build());
            }
            return cluster;
        }

        @Override
        public Oak[] setUpCluster(int n, StatisticsProvider statsProvider) throws Exception {
            DocumentNodeStoreBuilder<?>[] builders = new DocumentNodeStoreBuilder[n];
            for (int i = 0; i < n; i++) {
                builders[i] = getBuilder(i + 1);
            }
            return setUpCluster(builders, statsProvider);
        }

        @Override
        public void tearDownCluster() {
            for (DocumentNodeStore ns : nodeStores) {
                ns.dispose();
            }
            nodeStores.clear();
            if (dropDBAfterTest) {
                try {
                    MongoConnection mongo =
                            new MongoConnection(uri);
                    mongo.getDB().dropDatabase();
                    mongo.close();
                    if(blobStoreFixture != null){
                        blobStoreFixture.tearDown();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private void setupBlobStore(DocumentNodeStoreBuilder<?> builder, StatisticsProvider statsProvider) {
            initializeBlobStoreFixture(statsProvider);
            if (blobStoreFixture != null) {
                builder.setBlobStore(blobStoreFixture.setUp());
            }
        }

        private void initializeBlobStoreFixture(StatisticsProvider statsProvider) {
            if (blobStoreFixture != null){
                return;
            }

            if (useDataStore) {
                blobStoreFixture =
                        BlobStoreFixture.create(base, true, dsCacheInMB, statsProvider);
            }
        }

        private void configurePersistentCache(DocumentNodeStoreBuilder<?> builder) {
            //TODO Persistent cache should be removed in teardown
            builder.setPersistentCache("target/persistentCache,time");

            String persistentCacheIncludes = System.getProperty("persistentCacheIncludes");

            Set<String> paths = new HashSet<>();
            if (persistentCacheIncludes != null) {
                for (String p : Splitter.on(',').split(persistentCacheIncludes)) {
                    p = p != null ? Strings.emptyToNull(p.trim()) : null;
                    if (p != null) {
                        paths.add(p);
                    }
                }

                PathFilter pf = new PathFilter(paths, emptyList());
                System.out.println("Configuring persistent cache to only cache nodes under paths " + paths);
                Predicate<String> cachePredicate = path -> path != null && pf.filter(path) == PathFilter.Result.INCLUDE;
                builder.setNodeCachePredicate(cachePredicate);
            }
        }

    }

    static Oak newOak(NodeStore nodeStore) {
        return new Oak(nodeStore).with(ManagementFactory.getPlatformMBeanServer());
    }

}