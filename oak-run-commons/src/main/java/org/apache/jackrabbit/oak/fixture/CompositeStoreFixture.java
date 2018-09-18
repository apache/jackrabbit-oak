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

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.composite.CompositeNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import java.io.File;
import java.io.IOException;

import com.mongodb.MongoClientURI;

import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder.newMongoDocumentNodeStoreBuilder;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

abstract class CompositeStoreFixture extends OakFixture {

    private final int mounts;

    private final int pathsPerMount;

    private CompositeStoreFixture(String name, int mounts, int pathsPerMount) {
        super(name);
        this.mounts = mounts;
        this.pathsPerMount = pathsPerMount;
    }

    static OakFixture newCompositeMemoryFixture(String name, int mounts, int pathsPerMount) {
        return new CompositeStoreFixture(name, mounts, pathsPerMount) {
            @Override
            protected NodeStore getNodeStore() {
                return new MemoryNodeStore();
            }

            @Override
            public void tearDownCluster() {
                // nothing to do
            }
        };
    }

    static OakFixture newCompositeSegmentFixture(String name, File base, int maxFileSizeMB, int cacheSizeMB,
                                                 boolean memoryMapping, int mounts, int pathsPerMount) {
        return new CompositeStoreFixture(name, mounts, pathsPerMount) {

            private FileStore fileStore;

            @Override
            protected NodeStore getNodeStore()
                    throws IOException, InvalidFileStoreVersionException {
                FileStoreBuilder fsBuilder = fileStoreBuilder(new File(base, unique))
                        .withMaxFileSize(maxFileSizeMB)
                        .withSegmentCacheSize(cacheSizeMB)
                        .withMemoryMapping(memoryMapping);
                fileStore = fsBuilder.build();
                return SegmentNodeStoreBuilders.builder(fileStore).build();
            }

            @Override
            public void tearDownCluster() {
                if (fileStore != null) {
                    fileStore.close();
                }
            }
        };
    }

    static OakFixture newCompositeMongoFixture(String name,
                                               String uri,
                                               boolean dropDBAfterTest,
                                               long cacheSize,
                                               int mounts,
                                               int pathsPerMount) {
        return new CompositeStoreFixture(name, mounts, pathsPerMount) {

            private String database = new MongoClientURI(uri).getDatabase();
            private DocumentNodeStore ns;

            @Override
            protected NodeStore getNodeStore() {
                ns = newMongoDocumentNodeStoreBuilder()
                        .memoryCacheSize(cacheSize)
                        .setMongoDB(uri, database, 0)
                        .build();
                return ns;
            }

            @Override
            public void tearDownCluster() {
                if (ns != null) {
                    ns.dispose();
                }
                if (dropDBAfterTest) {
                    MongoConnection c = new MongoConnection(uri);
                    try {
                        c.getDatabase(database).drop();
                    } finally {
                        c.close();
                    }
                }
            }
        };
    }

    @Override
    public Oak getOak(int clusterId) throws Exception {
        NodeStore nodeStore = getNodeStore();
        Mounts.Builder mip = Mounts.newBuilder();
        for (int i = 0; i < mounts; i++) {
            String[] paths = new String[pathsPerMount];
            for (int j = 0; j < pathsPerMount; j++) {
                paths[j] = String.format("/mount-%d-path-%d", i, j);
            }
            mip.readOnlyMount("custom-mount-" + i, paths);
        }
        CompositeNodeStore.Builder builder = new CompositeNodeStore.Builder(mip.build(), nodeStore);
        for (int i = 0; i < mounts; i++) {
            builder.addMount("custom-mount-" + i, nodeStore);
        }
        return new Oak(builder.build());
    }

    protected abstract NodeStore getNodeStore() throws IOException, InvalidFileStoreVersionException;

    @Override
    public Oak[] setUpCluster(int n, StatisticsProvider statsProvider) throws Exception {
        if (n != 1) {
            throw new IllegalArgumentException();
        }
        return new Oak[] { getOak(1) };
    }
}
