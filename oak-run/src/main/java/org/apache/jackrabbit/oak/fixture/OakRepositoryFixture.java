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

import javax.jcr.Repository;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreConfiguration;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreHelper;
import org.apache.jackrabbit.oak.plugins.blob.cloud.CloudBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;

public abstract class OakRepositoryFixture implements RepositoryFixture {

    public static RepositoryFixture getMemory(final long cacheSize) {
        return getMemory("Oak-Memory", false, cacheSize);
    }

    public static RepositoryFixture getMemoryNS(final long cacheSize) {
        return getMemory("Oak-MemoryNS", false, cacheSize);
    }

    public static RepositoryFixture getMemoryMK(final long cacheSize) {
        return getMemory("Oak-MemoryMK", true, cacheSize);
    }

    private static RepositoryFixture getMemory(String name, final boolean useMK, final long cacheSize) {
        return new OakRepositoryFixture(name) {
            @Override
            protected Repository[] internalSetUpCluster(int n) throws Exception {
                Repository[] cluster = new Repository[n];
                for (int i = 0; i < cluster.length; i++) {
                    Oak oak;
                    if (useMK) {
                        MicroKernel kernel = new MicroKernelImpl();
                        oak = new Oak(new KernelNodeStore(kernel, cacheSize));
                    } else {
                        oak = new Oak(new MemoryNodeStore());
                    }
                    cluster[i] = new Jcr(oak).createRepository();
                }
                return cluster;
            }
        };
    }

    public static RepositoryFixture getH2MK(
            final File base, final long cacheSize) {
        return new OakRepositoryFixture("Oak-H2") {
            private MicroKernelImpl[] kernels;
            @Override
            protected Repository[] internalSetUpCluster(int n) throws Exception {
                Repository[] cluster = new Repository[n];
                kernels = new MicroKernelImpl[cluster.length];
                for (int i = 0; i < cluster.length; i++) {
                    kernels[i] = new MicroKernelImpl(
                            new File(base, unique).getPath());
                    Oak oak = new Oak(new KernelNodeStore(kernels[i], cacheSize));
                    cluster[i] = new Jcr(oak).createRepository();
                }
                return cluster;
            }
            @Override
            public void tearDownCluster() {
                super.tearDownCluster();
                for (MicroKernelImpl kernel : kernels) {
                    kernel.dispose();
                }
                FileUtils.deleteQuietly(new File(base, unique));
            }
        };
    }

    public static RepositoryFixture getMongo(
            final String host, final int port, final String database,
            final boolean dropDBAfterTest, final long cacheSize) {
        return getMongo("Oak-Mongo", false, host, port, database,
                dropDBAfterTest, cacheSize);
    }

    public static RepositoryFixture getMongoMK(
            final String host, final int port, final String database,
            final boolean dropDBAfterTest, final long cacheSize) {
        return getMongo("Oak-MongoMK", true, host, port, database,
                dropDBAfterTest, cacheSize);
    }

    public static RepositoryFixture getMongoNS(
            final String host, final int port, final String database,
            final boolean dropDBAfterTest, final long cacheSize) {
        return getMongo("Oak-MongoNS", false, host, port, database,
                dropDBAfterTest, cacheSize);
    }

    private static RepositoryFixture getMongo(String name, final boolean useMK,
            final String host, final int port, final String database,
            final boolean dropDBAfterTest, final long cacheSize) {

        return new OakRepositoryFixture(name) {
            private String dbName = database != null ? database : unique;
            private DocumentMK[] kernels;
            private BlobStore blobStore;

            private BlobStore getBlobStore() {
                BlobStoreConfiguration config =
                        BlobStoreConfiguration.newInstance().loadFromSystemProps();
                try {
                    blobStore =
                            BlobStoreHelper.create(config).orNull();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                return blobStore;
            }

            @Override
            protected Repository[] internalSetUpCluster(int n) throws Exception {
                Repository[] cluster = new Repository[n];
                kernels = new DocumentMK[cluster.length];
                for (int i = 0; i < cluster.length; i++) {
                    MongoConnection mongo =
                            new MongoConnection(host, port, dbName);
                    BlobStore blobStore = getBlobStore();
                    DocumentMK.Builder mkBuilder = new DocumentMK.Builder().
                            setMongoDB(mongo.getDB()).
                            memoryCacheSize(cacheSize).
                            setClusterId(i).setLogging(false);
                    if (blobStore != null) {
                            mkBuilder.setBlobStore(blobStore);
                    }
                    kernels[i] = mkBuilder.open();
                    Oak oak;
                    if (useMK) {
                        oak = new Oak(new KernelNodeStore(kernels[i], cacheSize));
                    } else {
                        oak = new Oak(kernels[i].getNodeStore());
                    }
                    cluster[i] = new Jcr(oak).createRepository();
                }
                return cluster;
            }

            @Override
            public void tearDownCluster() {
                super.tearDownCluster();
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

    public static RepositoryFixture getTar(
            final File base, final int maxFileSizeMB, final int cacheSizeMB,
            final boolean memoryMapping) {
        return new OakRepositoryFixture("Oak-Tar") {
            private SegmentStore[] stores;
            @Override
            protected Repository[] internalSetUpCluster(int n) throws Exception {
                Repository[] cluster = new Repository[n];
                stores = new FileStore[cluster.length];
                for (int i = 0; i < cluster.length; i++) {
                    stores[i] = new FileStore(
                            new File(base, unique),
                            maxFileSizeMB, cacheSizeMB, memoryMapping);
                    Oak oak = new Oak(new SegmentNodeStore(stores[i]));
                    cluster[i] = new Jcr(oak).createRepository();
                }
                return cluster;
            }
            @Override
            public void tearDownCluster() {
                super.tearDownCluster();
                for (SegmentStore store : stores) {
                    store.close();
                }
                FileUtils.deleteQuietly(new File(base, unique));
            }
        };
    }

    private final String name;

    protected final String unique;

    private Repository[] cluster;

    protected OakRepositoryFixture(String name) {
        this.name = name;
        this.unique = String.format("%s-%d", name, System.currentTimeMillis());
    }

    @Override
    public boolean isAvailable(int n) {
        return true;
    }

    @Override
    public final Repository[] setUpCluster(int n) throws Exception {
        cluster = internalSetUpCluster(n);
        return cluster;
    }

    @Override
    public void syncRepositoryCluster(Repository... nodes) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void tearDownCluster() {
        if (cluster != null) {
            for (Repository repo : cluster) {
                if (repo instanceof JackrabbitRepository) {
                    ((JackrabbitRepository) repo).shutdown();
                }
            }
        }
    }

    @Override
    public String toString() {
        return name;
    }

    protected abstract Repository[] internalSetUpCluster(int n) throws Exception;
}
