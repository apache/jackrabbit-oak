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
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.mongo.MongoStore;

import com.mongodb.MongoClient;

public abstract class OakRepositoryFixture implements RepositoryFixture {

    public static RepositoryFixture getMemory(final long cacheSize) {
        return new OakRepositoryFixture("Oak-Memory") {
            @Override
            protected Repository[] internalSetUpCluster(int n) throws Exception {
                Repository[] cluster = new Repository[n];
                MicroKernel kernel = new MicroKernelImpl();
                for (int i = 0; i < cluster.length; i++) {
                    Oak oak = new Oak(new KernelNodeStore(kernel, cacheSize));
                    cluster[i] = new Jcr(oak).createRepository();
                }
                return cluster;
            }
        };
    }

    public static RepositoryFixture getDefault(
            final File base, final long cacheSize) {
        return new OakRepositoryFixture("Oak-Default") {
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
        return new OakRepositoryFixture("Oak-Mongo") {
            private String dbName = database != null ? database : unique;
            private DocumentMK[] kernels;
            @Override
            protected Repository[] internalSetUpCluster(int n) throws Exception {
                Repository[] cluster = new Repository[n];
                kernels = new DocumentMK[cluster.length];
                for (int i = 0; i < cluster.length; i++) {
                    MongoConnection mongo =
                            new MongoConnection(host, port, dbName);
                    kernels[i] = new DocumentMK.Builder().
                            setMongoDB(mongo.getDB()).
                            setClusterId(i).setLogging(false).open();
                    Oak oak = new Oak(new KernelNodeStore(kernels[i], cacheSize));
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
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
    }

    public static RepositoryFixture getMongoNS(
            final String host, final int port, final String database,
            final boolean dropDBAfterTest, final long cacheSize) {
        return new OakRepositoryFixture("Oak-MongoNS") {
            private String dbName = database != null ? database : unique;
            private DocumentNodeStore[] stores;
            @Override
            protected Repository[] internalSetUpCluster(int n) throws Exception {
                Repository[] cluster = new Repository[n];
                stores = new DocumentNodeStore[cluster.length];
                for (int i = 0; i < cluster.length; i++) {
                    MongoConnection mongo =
                            new MongoConnection(host, port, dbName);
                    stores[i] = new DocumentMK.Builder().
                            setMongoDB(mongo.getDB()).
                            memoryCacheSize(cacheSize).
                            setClusterId(i).setLogging(false).getNodeStore();
                    Oak oak = new Oak(stores[i]);
                    cluster[i] = new Jcr(oak).createRepository();
                }
                return cluster;
            }
            @Override
            public void tearDownCluster() {
                super.tearDownCluster();
                for (DocumentNodeStore store : stores) {
                    store.dispose();
                }
                if (dropDBAfterTest) {
                    try {
                        MongoConnection mongo =
                                new MongoConnection(host, port, dbName);
                        mongo.getDB().dropDatabase();
                        mongo.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
    }

    public static RepositoryFixture getSegment(
            final String host, final int port, final int cacheSizeMB) {
        return new OakRepositoryFixture("Oak-Segment") {
            private SegmentStore[] stores;
            private MongoClient mongo;
            @Override
            protected Repository[] internalSetUpCluster(int n) throws Exception {
                Repository[] cluster = new Repository[n];
                stores = new SegmentStore[cluster.length];
                mongo = new MongoClient(host, port);
                for (int i = 0; i < cluster.length; i++) {
                    stores[i] = new MongoStore(mongo.getDB(unique), cacheSizeMB);
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
                mongo.getDB(unique).dropDatabase();
                mongo.close();
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
