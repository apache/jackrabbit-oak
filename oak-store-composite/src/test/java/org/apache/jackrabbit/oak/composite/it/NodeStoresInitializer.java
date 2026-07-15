/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.composite.it;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * Initializes the NodeStores used by the Integration Tests. Executed by Maven in the pre-integration-test phase.
 */
public class NodeStoresInitializer {

    public static final String GLOBAL_STORE = "segmentstore-composite-global";
    public static final String LIBS_STORE = "segmentstore-composite-mount-libs";
    public static final String APPS_STORE = "segmentstore-composite-mount-apps";
    
    private static boolean alreadyInitialized;

    public static void initTestStores() {
        if (alreadyInitialized) {
            return;
        }
        try {
            Path targetPath = Paths.get("target", "it", "nodestores").toAbsolutePath();
            initRepo(targetPath, GLOBAL_STORE, builder -> {
                builder.child("content").child("globalMount");
                builder.child("libs").child("globalMount");
                builder.child("apps").child("globalMount");
            });
            initRepo(targetPath, LIBS_STORE, builder -> {
                builder.child("libs").child("libsMount");
                builder.child("apps").child("libsMount");
            });
            initRepo(targetPath, APPS_STORE, builder -> {
                builder.child("apps").child("appsMount");
                builder.child("libs").child("appsMount");
            });
            alreadyInitialized = true;
        } catch (IOException | CommitFailedException e) {
            throw new RuntimeException("Error while initializing test node stores", e);
        }
    }

    private static void initRepo(Path targetPath, String store, Consumer<NodeBuilder> consumer) throws IOException, CommitFailedException {
        Path path = targetPath.resolve(store);
        FileUtils.deleteDirectory(path.toFile());
        FileStore filestore = getFileStore(path);
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(filestore).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        consumer.accept(builder);
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, new CommitInfo("1", "user"));
        filestore.flush();
        filestore.close();
    }

    private static FileStore getFileStore(Path path) {
        try {
            return FileStoreBuilder.fileStoreBuilder(path.toFile()).build();
        } catch (InvalidFileStoreVersionException | IOException e) {
            throw new RuntimeException("Failed to build FileStore at path " + path, e);
        }
    }
}
