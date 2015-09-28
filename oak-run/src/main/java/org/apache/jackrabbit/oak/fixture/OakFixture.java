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
import java.util.Map;

import javax.sql.DataSource;

import com.google.common.collect.Maps;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.blob.cloud.CloudBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

public abstract class OakFixture {

    public static final String OAK_MEMORY = "Oak-Memory";
    public static final String OAK_MEMORY_NS = "Oak-MemoryNS";
    public static final String OAK_MEMORY_MK = "Oak-MemoryMK";

    public static final String OAK_MONGO = "Oak-Mongo";
    public static final String OAK_MONGO_NS = "Oak-MongoNS";
    public static final String OAK_MONGO_MK = "Oak-MongoMK";

    public static final String OAK_RDB = "Oak-RDB";

    public static final String OAK_H2 = "Oak-H2";
    public static final String OAK_TAR = "Oak-Tar";


    private final String name;
    protected final String unique;

    protected OakFixture(String name) {
        this.name = name;
        this.unique = String.format("%s-%d", name, System.currentTimeMillis());
    }

    public abstract Oak getOak(int clusterId) throws Exception;
    public abstract Oak[] setUpCluster(int n) throws Exception;
    public abstract void tearDownCluster();

    @Override
    public String toString() {
        return name;
    }

    public static OakFixture getMemory(long cacheSize) {
        return getMemory(OAK_MEMORY, false, cacheSize);
    }

    public static OakFixture getMemoryNS(long cacheSize) {
        return getMemory(OAK_MEMORY_NS, false, cacheSize);
    }

    public static OakFixture getMemoryMK(long cacheSize) {
        return getMemory(OAK_MEMORY_MK, true, cacheSize);
    }

    public static OakFixture getMemory(String name, final boolean useMk, final long cacheSize) {
        return new OakFixture(name) {
            @Override
            public Oak getOak(int clusterId) throws Exception {
                Oak oak;
                if (useMk) {
                    MicroKernel kernel = new MicroKernelImpl();
                    oak = new Oak(new KernelNodeStore(kernel, cacheSize));
                } else {
                    oak = new Oak(new MemoryNodeStore());
                }
                return oak;
            }

            @Override
            public Oak[] setUpCluster(int n) throws Exception {
                Oak[] cluster = new Oak[n];
                for (int i = 0; i < cluster.length; i++) {
                    Oak oak;
                    if (useMk) {
                        MicroKernel kernel = new MicroKernelImpl();
                        oak = new Oak(new KernelNodeStore(kernel, cacheSize));
                    } else {
                        oak = new Oak(new MemoryNodeStore());
                    }
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
                dropDBAfterTest, cacheSize);
    }

    public static OakFixture getMongoMK(String host, int port, String database,
                                        boolean dropDBAfterTest, long cacheSize) {
        return getMongo(OAK_MONGO_MK, true, host, port, database,
                dropDBAfterTest, cacheSize);
    }

    public static OakFixture getMongoNS(String host, int port, String database,
                                        boolean dropDBAfterTest, long cacheSize) {
        return getMongo(OAK_MONGO_NS, false, host, port, database,
                dropDBAfterTest, cacheSize);
    }

    public static OakFixture getMongo(String name, final boolean useMk, final String host,
                                      final int port, final String database,
                                      final boolean dropDBAfterTest, final long cacheSize) {
        return new OakFixture(name) {
            private String dbName = database != null ? database : unique;
            private DocumentMK[] kernels;
            private BlobStore blobStore;

            private BlobStore getBlobStore() {

                try {
                    String className = System.getProperty("dataStore");
                    if(className != null){
                        DataStore ds = Class.forName(className).asSubclass(DataStore.class).newInstance();
                        PropertiesUtil.populate(ds, getConfig(), false);
                        ds.init(null);
                        blobStore = new DataStoreBlobStore(ds);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                return blobStore;
            }

            /**
             * Taken from org.apache.jackrabbit.oak.plugins.document.blob.ds.DataStoreUtils
             */
            private Map<String,?> getConfig(){
                Map<String,Object> result = Maps.newHashMap();
                for(Map.Entry<String,?> e : Maps.fromProperties(System.getProperties()).entrySet()){
                    String key = e.getKey();
                    if(key.startsWith("ds.") || key.startsWith("bs.")){
                        key = key.substring(3); //length of bs.
                        result.put(key, e.getValue());
                    }
                }
                return result;
            }

            @Override
            public Oak getOak(int clusterId) throws Exception {
                MongoConnection mongo = new MongoConnection(host, port, dbName);
                BlobStore blobStore = getBlobStore();
                DocumentMK.Builder mkBuilder = new DocumentMK.Builder().
                        setMongoDB(mongo.getDB()).
                        memoryCacheSize(cacheSize).
                        setClusterId(clusterId).setLogging(false);
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
                    MongoConnection mongo = new MongoConnection(host, port, dbName);
                    BlobStore blobStore = getBlobStore();
                    DocumentMK.Builder mkBuilder = new DocumentMK.Builder().
                            setMongoDB(mongo.getDB()).
                            memoryCacheSize(cacheSize).
                            setClusterId(i + 1).setLogging(false);
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
                    try {
                        MongoConnection mongo =
                                new MongoConnection(host, port, dbName);
                        mongo.getDB().dropDatabase();
                        mongo.close();
                        if (blobStore instanceof CloudBlobStore) {
                            ((CloudBlobStore) blobStore).deleteBucket();
                        } else if (blobStore instanceof DataStoreBlobStore) {
                            ((DataStoreBlobStore) blobStore).clearInUse();
                            ((DataStoreBlobStore) blobStore).deleteAllOlderThan(
                                    System.currentTimeMillis() + 10000000);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
    }

    public static OakFixture getRDB(final String name, final String jdbcuri, final String jdbcuser, final String jdbcpasswd,
            final boolean useMk, final String tablePrefix, final boolean dropDBAfterTest, final long cacheSize) {
        return new OakFixture(name) {
            private DocumentMK[] kernels;
            private BlobStore blobStore;

            private RDBOptions getOptions(boolean dropDBAFterTest, String tablePrefix) {
                return new RDBOptions().dropTablesOnClose(dropDBAfterTest).tablePrefix(tablePrefix);
            }

            private BlobStore getBlobStore() {
                try {
                    DataSource ds = RDBDataSourceFactory.forJdbcUrl(jdbcuri, jdbcuser, jdbcpasswd);
                    blobStore = new RDBBlobStore(ds, getOptions(dropDBAfterTest, tablePrefix));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                return blobStore;
            }

            @Override
            public Oak getOak(int clusterId) throws Exception {
                DataSource ds = RDBDataSourceFactory.forJdbcUrl(jdbcuri, jdbcuser, jdbcpasswd);
                DocumentMK.Builder mkBuilder = new DocumentMK.Builder()
                        .setRDBConnection(ds, getOptions(dropDBAfterTest, tablePrefix)).memoryCacheSize(cacheSize)
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
                    DocumentMK.Builder mkBuilder = new DocumentMK.Builder()
                            .setRDBConnection(ds, getOptions(dropDBAfterTest, tablePrefix)).memoryCacheSize(cacheSize)
                            .setClusterId(i + 1).setLogging(false);
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
                String dropped = "";
                for (DocumentMK kernel : kernels) {
                    kernel.dispose();
                    if (kernel.getDocumentStore() instanceof RDBDocumentStore) {
                        dropped += ((RDBDocumentStore)kernel.getDocumentStore()).getDroppedTables();
                    }
                }
                if (dropDBAfterTest) {
                    if (dropped.isEmpty()) {
                        throw new RuntimeException("dropdb was set, but tables have not been dropped");
                    }
                }
            }
        };
    }

    public static OakFixture getTar(
            final File base, final int maxFileSizeMB, final int cacheSizeMB,
            final boolean memoryMapping) {
        return new OakFixture(OAK_TAR) {
            private SegmentStore[] stores;

            @Override
            public Oak getOak(int clusterId) throws Exception {
                FileStore fs = new FileStore(base, maxFileSizeMB, cacheSizeMB, memoryMapping);
                return new Oak(new SegmentNodeStore(fs));
            }

            @Override
            public Oak[] setUpCluster(int n) throws Exception {
                Oak[] cluster = new Oak[n];
                stores = new FileStore[cluster.length];
                for (int i = 0; i < cluster.length; i++) {
                    stores[i] = new FileStore(
                            new File(base, unique),
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
                FileUtils.deleteQuietly(new File(base, unique));
            }
        };
    }

    public static OakFixture getH2MK(final File base, final long cacheSize) {
        return new OakFixture(OAK_H2) {
            private MicroKernelImpl[] kernels;

            @Override
            public Oak getOak(int clusterId) throws Exception {
                return new Oak(new KernelNodeStore(new MicroKernelImpl(base.getPath()), cacheSize));
            }

            @Override
            public Oak[] setUpCluster(int n) throws Exception {
                Oak[] cluster = new Oak[n];
                kernels = new MicroKernelImpl[cluster.length];
                for (int i = 0; i < cluster.length; i++) {
                    kernels[i] = new MicroKernelImpl(new File(base, unique).getPath());
                    cluster[i] = new Oak(new KernelNodeStore(kernels[i], cacheSize));
                }
                return cluster;
            }
            @Override
            public void tearDownCluster() {
                for (MicroKernelImpl kernel : kernels) {
                    kernel.dispose();
                }
                FileUtils.deleteQuietly(new File(base, unique));
            }
        };
    }
}