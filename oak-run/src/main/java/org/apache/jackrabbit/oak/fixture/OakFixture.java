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
import java.net.UnknownHostException;

import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.kernel.NodeStoreKernel;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

public abstract class OakFixture {

    public static final String OAK_MEMORY = "Oak-Memory";
    public static final String OAK_MEMORY_NS = "Oak-MemoryNS";

    public static final String OAK_MONGO = "Oak-Mongo";
    public static final String OAK_MONGO_FDS = "Oak-Mongo-FDS";
    public static final String OAK_MONGO_NS = "Oak-MongoNS";
    public static final String OAK_MONGO_MK = "Oak-MongoMK";

    public static final String OAK_RDB = "Oak-RDB";

    public static final String OAK_TAR = "Oak-Tar";
    public static final String OAK_TAR_FDS = "Oak-Tar-FDS";


    private final String name;
    protected final String unique;

    protected OakFixture(String name) {
        this.name = name;
        this.unique = getUniqueDatabaseName(name);
    }

    private static String getUniqueDatabaseName(String name) {
        return String.format("%s-%d", name, System.currentTimeMillis());
    }

    public abstract MicroKernel getMicroKernel() throws Exception;

    public abstract Oak getOak(int clusterId) throws Exception;

    public abstract Oak[] setUpCluster(int n) throws Exception;

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
            public MicroKernel getMicroKernel() {
                return new NodeStoreKernel(new MemoryNodeStore());
            }

            @Override
            public Oak getOak(int clusterId) throws Exception {
                Oak oak;
                oak = new Oak(new MemoryNodeStore());
                return oak;
            }

            @Override
            public Oak[] setUpCluster(int n) throws Exception {
                Oak[] cluster = new Oak[n];
                for (int i = 0; i < cluster.length; i++) {
                    Oak oak;
                    oak = new Oak(new MemoryNodeStore());
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

    public static OakFixture getMongo(String host, int port, String database,
                                      boolean dropDBAfterTest, long cacheSize) {
        return getMongo(OAK_MONGO, false, host, port, database,
                dropDBAfterTest, cacheSize, false, null, 0);
    }

    public static OakFixture getMongoMK(String host, int port, String database,
                                        boolean dropDBAfterTest, long cacheSize) {
        return getMongo(OAK_MONGO_MK, true, host, port, database,
                dropDBAfterTest, cacheSize, false, null, 0);
    }

    public static OakFixture getMongoNS(String host, int port, String database,
                                        boolean dropDBAfterTest, long cacheSize) {
        return getMongo(OAK_MONGO_NS, false, host, port, database,
                dropDBAfterTest, cacheSize, false, null, 0);
    }

    public static OakFixture getMongo(String name, final boolean useMk, final String host,
                                      final int port, String database,
                                      final boolean dropDBAfterTest, final long cacheSize,
                                      final boolean useFileDataStore,
                                      final File base,
                                      final int fdsCacheInMB) {
        if (database == null) {
            database = getUniqueDatabaseName(name);
        }
        String uri = "mongodb://" + host + ":" + port + "/" + database;
        return getMongo(name, uri, useMk, dropDBAfterTest, cacheSize, useFileDataStore, base, fdsCacheInMB);
    }

    public static OakFixture getMongo(final String name, final String uri,
                                      final boolean useMk,
                                      final boolean dropDBAfterTest, final long cacheSize,
                                      final boolean useFileDataStore,
                                      final File base, final int fdsCacheInMB) {
        return new OakFixture(name) {
            private DocumentMK[] kernels;
            private BlobStoreFixture blobStoreFixture;

            {
                if (useFileDataStore) {
                    blobStoreFixture = BlobStoreFixture.getFileDataStore(base, fdsCacheInMB);
                } else {
                    blobStoreFixture = BlobStoreFixture.create(base, false);
                }
            }

            @Override
            public MicroKernel getMicroKernel() throws UnknownHostException {
                MongoConnection mongo = new MongoConnection(uri);
                DocumentMK.Builder mkBuilder = new DocumentMK.Builder().
                        setMongoDB(mongo.getDB()).
                        memoryCacheSize(cacheSize).
                        setLogging(false);
                setupBlobStore(mkBuilder);
                return mkBuilder.open();
            }

            @Override
            public Oak getOak(int clusterId) throws Exception {
                MongoConnection mongo = new MongoConnection(uri);
                DocumentMK.Builder mkBuilder = new DocumentMK.Builder().
                        setMongoDB(mongo.getDB()).
                        memoryCacheSize(cacheSize).
                        setClusterId(clusterId).setLogging(false);
                setupBlobStore(mkBuilder);
                DocumentMK dmk = mkBuilder.open();
                Oak oak;
                if (useMk) {
                    oak = new Oak(new KernelNodeStore(dmk, cacheSize));
                } else {
                    oak = new Oak(dmk.getNodeStore());
                }
                return oak;
            }

            @Override
            public Oak[] setUpCluster(int n) throws Exception {
                Oak[] cluster = new Oak[n];
                kernels = new DocumentMK[cluster.length];
                for (int i = 0; i < cluster.length; i++) {
                    MongoConnection mongo = new MongoConnection(uri);
                    DocumentMK.Builder mkBuilder = new DocumentMK.Builder().
                            setMongoDB(mongo.getDB()).
                            memoryCacheSize(cacheSize).
                            setClusterId(i).setLogging(false);
                    setupBlobStore(mkBuilder);
                    kernels[i] = mkBuilder.open();
                    Oak oak;
                    if (useMk) {
                        oak = new Oak(new KernelNodeStore(kernels[i], cacheSize));
                    } else {
                        oak = new Oak(kernels[i].getNodeStore());
                    }
                    cluster[i] = oak;
                }
                return cluster;
            }

            @Override
            public void tearDownCluster() {
                for (DocumentMK kernel : kernels) {
                    kernel.dispose();
                }
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

            private void setupBlobStore(DocumentMK.Builder mkBuilder) {
                if (blobStoreFixture != null) {
                    mkBuilder.setBlobStore(blobStoreFixture.setUp());
                }
            }
        };
    }

    public static OakFixture getRDB(final String name, final String jdbcuri, final String jdbcuser, final String jdbcpasswd,
                                    final boolean useMk, final boolean dropDBAfterTest, final long cacheSize) {
        return new OakFixture(name) {
            private DocumentMK[] kernels;
            private BlobStore blobStore;

            private BlobStore getBlobStore() {
                try {
                    DataSource ds = RDBDataSourceFactory.forJdbcUrl(jdbcuri, jdbcuser, jdbcpasswd);
                    blobStore = new RDBBlobStore(ds);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                return blobStore;
            }

            @Override
            public MicroKernel getMicroKernel() {
                DataSource ds = RDBDataSourceFactory.forJdbcUrl(jdbcuri, jdbcuser, jdbcpasswd);
                DocumentMK.Builder mkBuilder = new DocumentMK.Builder().setRDBConnection(ds).memoryCacheSize(cacheSize)
                        .setLogging(false);
                BlobStore blobStore = getBlobStore();
                if (blobStore != null) {
                    mkBuilder.setBlobStore(blobStore);
                }
                return mkBuilder.open();
            }

            @Override
            public Oak getOak(int clusterId) throws Exception {
                DataSource ds = RDBDataSourceFactory.forJdbcUrl(jdbcuri, jdbcuser, jdbcpasswd);
                DocumentMK.Builder mkBuilder = new DocumentMK.Builder().setRDBConnection(ds).memoryCacheSize(cacheSize)
                        .setClusterId(clusterId).setLogging(false);
                BlobStore blobStore = getBlobStore();
                if (blobStore != null) {
                    mkBuilder.setBlobStore(blobStore);
                }
                DocumentMK dmk = mkBuilder.open();
                Oak oak;
                if (useMk) {
                    oak = new Oak(new KernelNodeStore(dmk, cacheSize));
                } else {
                    oak = new Oak(dmk.getNodeStore());
                }
                return oak;
            }

            @Override
            public Oak[] setUpCluster(int n) throws Exception {
                Oak[] cluster = new Oak[n];
                kernels = new DocumentMK[cluster.length];
                for (int i = 0; i < cluster.length; i++) {
                    BlobStore blobStore = getBlobStore();
                    DataSource ds = RDBDataSourceFactory.forJdbcUrl(jdbcuri, jdbcuser, jdbcpasswd);
                    DocumentMK.Builder mkBuilder = new DocumentMK.Builder().setRDBConnection(ds).memoryCacheSize(cacheSize)
                            .setClusterId(i).setLogging(false);
                    if (blobStore != null) {
                        mkBuilder.setBlobStore(blobStore);
                    }
                    kernels[i] = mkBuilder.open();
                    Oak oak;
                    if (useMk) {
                        oak = new Oak(new KernelNodeStore(kernels[i], cacheSize));
                    } else {
                        oak = new Oak(kernels[i].getNodeStore());
                    }
                    cluster[i] = oak;
                }
                return cluster;
            }

            @Override
            public void tearDownCluster() {
                for (DocumentMK kernel : kernels) {
                    kernel.dispose();
                }
                if (dropDBAfterTest) {
                    throw new RuntimeException("dropdb not supported for RDB persistence");
                }
            }
        };
    }

    public static OakFixture getTar(
            final String name, final File base, final int maxFileSizeMB, final int cacheSizeMB,
            final boolean memoryMapping, final boolean useBlobStore) {
        return new SegmentFixture(name, base, maxFileSizeMB, cacheSizeMB, memoryMapping, useBlobStore);
    }

    public static class SegmentFixture extends OakFixture {
        private FileStore[] stores;
        private BlobStoreFixture[] blobStoreFixtures = new BlobStoreFixture[0];
        private final File base;
        private final int maxFileSizeMB;
        private final int cacheSizeMB;
        private final boolean memoryMapping;
        private final boolean useBlobStore;

        public SegmentFixture(String name, File base, int maxFileSizeMB, int cacheSizeMB,
                              boolean memoryMapping, boolean useBlobStore) {
            super(name);
            this.base = base;
            this.maxFileSizeMB = maxFileSizeMB;
            this.cacheSizeMB = cacheSizeMB;
            this.memoryMapping = memoryMapping;
            this.useBlobStore = useBlobStore;
        }

        @Override
        public MicroKernel getMicroKernel() throws Exception {
            FileStore fs = new FileStore(base, maxFileSizeMB, cacheSizeMB, memoryMapping);
            return new NodeStoreKernel(new SegmentNodeStore(fs));
        }

        @Override
        public Oak getOak(int clusterId) throws Exception {
            FileStore fs = new FileStore(base, maxFileSizeMB, cacheSizeMB, memoryMapping);
            return new Oak(new SegmentNodeStore(fs));
        }

        @Override
        public Oak[] setUpCluster(int n) throws Exception {
            Oak[] cluster = new Oak[n];
            stores = new FileStore[cluster.length];
            if (useBlobStore) {
                blobStoreFixtures = new BlobStoreFixture[cluster.length];
            }

            for (int i = 0; i < cluster.length; i++) {
                BlobStore blobStore = null;
                if (useBlobStore) {
                    blobStoreFixtures[i] = BlobStoreFixture.create(base, true);
                    blobStore = blobStoreFixtures[i].setUp();
                }

                stores[i] = new FileStore(blobStore,
                        new File(base, unique),
                        EmptyNodeState.EMPTY_NODE,
                        maxFileSizeMB, cacheSizeMB, memoryMapping);
                cluster[i] = new Oak(new SegmentNodeStore(stores[i]));
            }
            return cluster;
        }

        @Override
        public void tearDownCluster() {
            for (SegmentStore store : stores) {
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

}