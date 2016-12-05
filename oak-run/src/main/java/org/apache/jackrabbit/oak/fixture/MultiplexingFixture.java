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
import org.apache.jackrabbit.oak.plugins.multiplex.MultiplexingNodeStore;
import org.apache.jackrabbit.oak.plugins.multiplex.SimpleMountInfoProvider;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import java.io.File;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

class MultiplexingFixture extends OakFixture {

    private final File base;

    private final int maxFileSizeMB;

    private final int cacheSizeMB;

    private final boolean memoryMapping;

    private final int mounts;

    private FileStore fileStore;

    public MultiplexingFixture(String name, File base, int maxFileSizeMB, int cacheSizeMB,
                               boolean memoryMapping, int mounts) {
        super(name);
        this.base = base;
        this.maxFileSizeMB = maxFileSizeMB;
        this.cacheSizeMB = cacheSizeMB;
        this.memoryMapping = memoryMapping;
        this.mounts = mounts;
    }

    @Override
    public Oak getOak(int clusterId) throws Exception {
        FileStoreBuilder fsBuilder = fileStoreBuilder(new File(base, unique))
                .withMaxFileSize(maxFileSizeMB)
                .withSegmentCacheSize(cacheSizeMB)
                .withMemoryMapping(memoryMapping);
        fileStore = fsBuilder.build();
        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        SimpleMountInfoProvider.Builder mip = new SimpleMountInfoProvider.Builder();
        for (int i = 0; i < mounts; i++) {
            mip.readOnlyMount("mount" + i, "/path/" + i);
        }
        MultiplexingNodeStore.Builder builder = new MultiplexingNodeStore.Builder(mip.build(), nodeStore);
        for (int i = 0; i < mounts; i++) {
            builder.addMount("mount" + i, nodeStore);
        }
        return new Oak(builder.build());
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
