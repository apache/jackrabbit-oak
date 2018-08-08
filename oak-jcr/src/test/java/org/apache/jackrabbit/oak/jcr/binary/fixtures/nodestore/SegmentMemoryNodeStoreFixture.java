/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.jcr.binary.fixtures.nodestore;

import java.io.File;
import java.io.IOException;
import javax.jcr.RepositoryException;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.datastore.DataStoreFixture;
import org.apache.jackrabbit.oak.jcr.util.ComponentHolder;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

/**
 * Creates a repository with
 * - SegmentNodeStore, storing data in-memory
 * - an optional DataStore provided by DataStoreFixture
 */
public class SegmentMemoryNodeStoreFixture extends NodeStoreFixture implements ComponentHolder {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DataStoreFixture dataStoreFixture;

    private final Table<NodeStore, String, Object> components = HashBasedTable.create();

    public SegmentMemoryNodeStoreFixture(@Nullable DataStoreFixture dataStoreFixture) {
        this.dataStoreFixture = dataStoreFixture;
    }

    @Override
    public boolean isAvailable() {
        // if a DataStore is configured, it must be available for our NodeStore to be available
        return dataStoreFixture == null || dataStoreFixture.isAvailable();
    }

    @Override
    public NodeStore createNodeStore() {
        try {
            log.info("Creating NodeStore using " + toString());

            File fileStoreRoot = FixtureUtils.createTempFolder();
            FileStoreBuilder fileStoreBuilder = FileStoreBuilder.fileStoreBuilder(fileStoreRoot)
                .withNodeDeduplicationCacheSize(16384)
                .withMaxFileSize(256)
                .withMemoryMapping(false);

            File dataStoreFolder = null;
            BlobStore blobStore = null;
            DataStore dataStore = null;
            if (dataStoreFixture != null) {
                dataStore = dataStoreFixture.createDataStore();

                // init with a new folder inside a temporary one
                dataStoreFolder = FixtureUtils.createTempFolder();
                dataStore.init(dataStoreFolder.getAbsolutePath());

                blobStore = new DataStoreBlobStore(dataStore);
                fileStoreBuilder.withBlobStore(blobStore);
            }

            FileStore fileStore = fileStoreBuilder.build();
            NodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

            // track all main components
            if (dataStore != null) {
                components.put(nodeStore, DataStore.class.getName(), dataStore);
                components.put(nodeStore, DataStore.class.getName() + ":folder", dataStoreFolder);
            }
            if (blobStore != null) {
                components.put(nodeStore, BlobStore.class.getName(), blobStore);
            }
            components.put(nodeStore, FileStore.class.getName(), fileStore);
            components.put(nodeStore, FileStore.class.getName() + ":root", fileStoreRoot);

            return nodeStore;

        } catch (IOException | InvalidFileStoreVersionException | RepositoryException e) {
            throw new AssertionError("Cannot create test repo fixture " + toString(), e);
        }
    }

    @Override
    public void dispose(NodeStore nodeStore) {
        try {
            File fileStoreRoot = (File) components.get(nodeStore, FileStore.class.getName() + ":root");
            FileUtils.deleteQuietly(fileStoreRoot);

            DataStore dataStore = (DataStore) components.get(nodeStore, DataStore.class.getName());
            if (dataStore != null && dataStoreFixture != null) {
                dataStoreFixture.dispose(dataStore);

                File dataStoreFolder = (File) components.get(nodeStore, DataStore.class.getName() + ":folder");
                FileUtils.deleteQuietly(dataStoreFolder);
            }
        } finally {
            components.row(nodeStore).clear();
        }
    }

    @Override
    public String toString() {
        // for nice Junit parameterized test labels
        return FixtureUtils.getFixtureLabel(this, dataStoreFixture);
    }

    @Override
    public <T> T get(NodeStore nodeStore, String componentName) {
        return (T) components.get(nodeStore, componentName);
    }
}
