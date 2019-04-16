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
import org.apache.jackrabbit.oak.composite.InitialContentMigrator;
import org.apache.jackrabbit.oak.composite.checks.MountedNodeStoreChecker;
import org.apache.jackrabbit.oak.composite.checks.NamespacePrefixNodestoreChecker;
import org.apache.jackrabbit.oak.composite.checks.NodeStoreChecksService;
import org.apache.jackrabbit.oak.composite.checks.NodeTypeDefinitionNodeStoreChecker;
import org.apache.jackrabbit.oak.composite.checks.NodeTypeMountedNodeStoreChecker;
import org.apache.jackrabbit.oak.composite.checks.UniqueIndexNodeStoreChecker;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.MongoClientURI;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder.newMongoDocumentNodeStoreBuilder;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

abstract class CompositeStoreFixture extends OakFixture {

    private static final MountInfoProvider MOUNT_INFO_PROVIDER = Mounts.newBuilder()
            .mount("libs", true, asList(
                    "/oak:index/*$" // pathsSupportingFragments
            ), asList(
                    "/libs",        // mountedPaths
                    "/apps",
                    "/jcr:system/rep:permissionStore/oak:mount-libs-crx.default"))
            .build();

    private CompositeStoreFixture(String name) {
        super(name);
    }

    static OakFixture newCompositeMemoryFixture(String name) {
        return new CompositeStoreFixture(name) {
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
                                                 boolean memoryMapping) {
        return new CompositeStoreFixture(name) {

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
                                               long cacheSize) {
        return new CompositeStoreFixture(name) {

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

    protected abstract NodeStore getNodeStore() throws IOException, InvalidFileStoreVersionException;

    @Override
    public Oak getOak(int clusterId) throws Exception {
        MemoryNodeStore seed = new MemoryNodeStore();
        Oak oakSeed = new Oak(seed);
        populateSeed(oakSeed);

        NodeStore global = getNodeStore();
        new InitialContentMigrator(global, seed, MOUNT_INFO_PROVIDER.getMountByName("libs")).migrate();

        List<MountedNodeStoreChecker<?>> checkerList = new ArrayList<>();
        checkerList.add(new NamespacePrefixNodestoreChecker());
        checkerList.add(new NodeTypeDefinitionNodeStoreChecker());
        checkerList.add(new NodeTypeMountedNodeStoreChecker());
        checkerList.add(new UniqueIndexNodeStoreChecker());

        NodeStore composite = new CompositeNodeStore.Builder(MOUNT_INFO_PROVIDER, global)
                .addMount("libs", seed)
                .with(new NodeStoreChecksService(MOUNT_INFO_PROVIDER, checkerList))
                .build();
        return new Oak(composite);
    }

    // this method allows to populate the /apps and /libs subtrees, which becomes
    // immutable after the composite node store is created
    private void populateSeed(Oak oakSeed) {
    }

    @Override
    public Oak[] setUpCluster(int n, StatisticsProvider statsProvider) throws Exception {
        if (n != 1) {
            throw new IllegalArgumentException();
        }
        return new Oak[] { getOak(1) };
    }
}
