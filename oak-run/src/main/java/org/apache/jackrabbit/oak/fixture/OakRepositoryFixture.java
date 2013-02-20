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
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.blob.MongoBlobStore;
import org.apache.jackrabbit.mongomk.prototype.MongoMK;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.segment.MongoStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;

import com.mongodb.Mongo;

public abstract class OakRepositoryFixture implements RepositoryFixture {

    public static RepositoryFixture getMemory() {
        return new OakRepositoryFixture() {
            @Override
            public void setUpCluster(Repository[] cluster) throws Exception {
                MicroKernel kernel = new MicroKernelImpl();
                for (int i = 0; i < cluster.length; i++) {
                    Oak oak = new Oak(kernel);
                    cluster[i] = new Jcr(oak).createRepository();
                }
            }
        };
    }

    public static RepositoryFixture getDefault() {
        return new OakRepositoryFixture() {
            private File file = new File("oak-benchmark-microkernel");
            private MicroKernelImpl[] kernels;
            @Override
            public void setUpCluster(Repository[] cluster) throws Exception {
                kernels = new MicroKernelImpl[cluster.length];
                for (int i = 0; i < cluster.length; i++) {
                    kernels[i] = new MicroKernelImpl(file.getName());
                    cluster[i] = new Jcr(kernels[i]).createRepository();
                }
            }
            @Override
            public void tearDownCluster(Repository[] cluster) {
                for (MicroKernelImpl kernel : kernels) {
                    kernel.dispose();
                }
                FileUtils.deleteQuietly(file);
            }
        };
    }

    public static RepositoryFixture getMongo() {
        return new OakRepositoryFixture() {
            private MongoMicroKernel[] kernels;
            @Override
            public void setUpCluster(Repository[] cluster) throws Exception {
                kernels = new MongoMicroKernel[cluster.length];
                for (int i = 0; i < cluster.length; i++) {
                    MongoConnection mongo = new MongoConnection(
                            "127.0.0.1", 27017, "oak-benchmark-mongo");
                    kernels[i] = new MongoMicroKernel(
                            mongo,
                            new MongoNodeStore(mongo.getDB()),
                            new MongoBlobStore(mongo.getDB()));
                    cluster[i] = new Jcr(kernels[i]).createRepository();
                }
            }
            @Override
            public void tearDownCluster(Repository[] cluster) {
                for (MongoMicroKernel kernel : kernels) {
                    kernel.dispose();
                }
                try {
                    MongoConnection mongo = new MongoConnection(
                            "127.0.0.1", 27017, "oak-benchmark-mongo");
                    mongo.getDB().dropDatabase();
                    mongo.close();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        };
    }

    public static RepositoryFixture getNewMongo() {
        return new OakRepositoryFixture() {
            private MongoConnection mongo;
            private MongoMK[] kernels;
            @Override
            public void setUpCluster(Repository[] cluster) throws Exception {
                mongo = new MongoConnection(
                        "127.0.0.1", 27017, "oak-benchmark-newmongo");
                kernels = new MongoMK[cluster.length];
                for (int i = 0; i < cluster.length; i++) {
                    kernels[i] = new MongoMK(mongo.getDB(), i);
                    cluster[i] = new Jcr(kernels[i]).createRepository();
                }
            }
            @Override
            public void tearDownCluster(Repository[] cluster) {
                for (MongoMK kernel : kernels) {
                    kernel.dispose();
                }
                mongo.getDB().dropDatabase();
                mongo.close();
            }
        };
    }

    public static RepositoryFixture getSegment() {
        return new OakRepositoryFixture() {
            private Mongo mongo;
            @Override
            public void setUpCluster(Repository[] cluster) throws Exception {
                mongo = new Mongo();
                for (int i = 0; i < cluster.length; i++) {
                    SegmentStore store = new MongoStore(
                            mongo.getDB("oak-benchmark-segment"));
                    Oak oak = new Oak(new SegmentNodeStore(store));
                    cluster[i] = new Jcr(oak).createRepository();
                }
            }
            @Override
            public void tearDownCluster(Repository[] cluster) {
                mongo.getDB("oak-benchmark-segment").dropDatabase();
                mongo.close();
            }
        };
    }

    @Override
    public boolean isAvailable() {
        return true;
    }


    @Override
    public void syncRepositoryCluster(Repository... nodes) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void tearDownCluster(Repository[] cluster) {
    }

}
