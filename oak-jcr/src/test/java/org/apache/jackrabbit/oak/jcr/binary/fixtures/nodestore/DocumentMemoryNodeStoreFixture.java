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
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

/**
 * Creates a repository with
 * - DocumentNodeStore, storing data in-memory
 * - an optional DataStore provided by DataStoreFixture
 */
public class DocumentMemoryNodeStoreFixture extends NodeStoreFixture implements ComponentHolder {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DataStoreFixture dataStoreFixture;

    private final Table<NodeStore, String, Object> components = HashBasedTable.create();

    public DocumentMemoryNodeStoreFixture(@Nullable DataStoreFixture dataStoreFixture) {
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

            DocumentNodeStoreBuilder<?> documentNodeStoreBuilder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder();

            File dataStoreFolder = null;
            BlobStore blobStore = null;
            DataStore dataStore = null;
            if (dataStoreFixture != null) {
                dataStore = dataStoreFixture.createDataStore();

                // init with a new folder inside a temporary one
                dataStoreFolder = FixtureUtils.createTempFolder();
                dataStore.init(dataStoreFolder.getAbsolutePath());

                blobStore = new DataStoreBlobStore(dataStore);
                documentNodeStoreBuilder.setBlobStore(blobStore);
            }

            NodeStore nodeStore = documentNodeStoreBuilder.build();

            // track all main components
            if (dataStore != null) {
                components.put(nodeStore, DataStore.class.getName(), dataStore);
                components.put(nodeStore, DataStore.class.getName() + ":folder", dataStoreFolder);
            }
            if (blobStore != null) {
                components.put(nodeStore, BlobStore.class.getName(), blobStore);
            }

            return nodeStore;
        } catch (IOException | RepositoryException e) {
            throw new AssertionError("Cannot create test repo fixture " + toString(), e);
        }
    }

    @Override
    public void dispose(NodeStore nodeStore) {
        try {
            if (nodeStore instanceof DocumentNodeStore) {
                ((DocumentNodeStore)nodeStore).dispose();
            }

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
