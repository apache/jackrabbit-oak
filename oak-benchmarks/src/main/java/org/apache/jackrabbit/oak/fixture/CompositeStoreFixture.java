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

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

class CompositeStoreFixture extends OakFixture {

    private final File base;

    private final int maxFileSizeMB;

    private final int cacheSizeMB;

    private final boolean memoryMapping;

    private final int mounts;

    private final int pathsPerMount;

    private final boolean inMemory;

    private FileStore fileStore;

    public CompositeStoreFixture(String name, int mounts, int pathsPerMount) {
        super(name);
        this.inMemory = true;
        this.mounts = mounts;
        this.pathsPerMount = pathsPerMount;

        this.base = null;
        this.maxFileSizeMB = -1;
        this.cacheSizeMB = -1;
        this.memoryMapping = false;
    }

    public CompositeStoreFixture(String name, File base, int maxFileSizeMB, int cacheSizeMB,
                                 boolean memoryMapping, int mounts, int pathsPerMount) {
        super(name);
        this.base = base;
        this.maxFileSizeMB = maxFileSizeMB;
        this.cacheSizeMB = cacheSizeMB;
        this.memoryMapping = memoryMapping;
        this.mounts = mounts;
        this.pathsPerMount = pathsPerMount;
        this.inMemory = false;
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

    private NodeStore getNodeStore() throws IOException, InvalidFileStoreVersionException {
        if (inMemory) {
            return new MemoryNodeStore();
        }
        FileStoreBuilder fsBuilder = fileStoreBuilder(new File(base, unique))
                .withMaxFileSize(maxFileSizeMB)
                .withSegmentCacheSize(cacheSizeMB)
                .withMemoryMapping(memoryMapping);
        fileStore = fsBuilder.build();
        return SegmentNodeStoreBuilders.builder(fileStore).build();
    }

    @Override
    public Oak[] setUpCluster(int n, StatisticsProvider statsProvider) throws Exception {
        if (n != 1) {
            throw new IllegalArgumentException();
        }
        return new Oak[] { getOak(1) };
    }

    @Override
    public void tearDownCluster() {
        if (fileStore != null) {
            fileStore.close();
        }
    }
}
