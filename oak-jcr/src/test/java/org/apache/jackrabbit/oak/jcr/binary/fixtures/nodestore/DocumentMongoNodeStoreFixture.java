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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.jcr.RepositoryException;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.datastore.DataStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessDSGCFixture;
import org.apache.jackrabbit.oak.jcr.util.ComponentHolder;
import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a repository with
 * - DocumentNodeStore, storing data in mongo (available locally or deloying in docker if available)
 * - an optional DataStore provided by DataStoreFixture
 */
public class DocumentMongoNodeStoreFixture extends NodeStoreFixture implements ComponentHolder,
    BinaryAccessDSGCFixture {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DataStoreFixture dataStoreFixture;
    private final Table<NodeStore, String, Object> components = HashBasedTable.create();
    private MongoConnection connection;
    private final Clock clock;
    public MongoConnectionFactory connFactory;
    private String db;
    public DocumentMongoNodeStoreFixture(@Nullable DataStoreFixture dataStoreFixture) {
        this.dataStoreFixture = dataStoreFixture;
        this.clock = new Clock.Virtual();
    }

    /**
     * Mandatory to be called to initialize the connectionFactory.
     * Lazy initializes it to limit docker container init only if relevant datastores available.
     *
     * @return
     */
    @Override
    public boolean isAvailable() {
        db = UUID.randomUUID().toString();

        // if a DataStore is configured, it must be available for our NodeStore to be available
        if ((dataStoreFixture == null || dataStoreFixture.isAvailable())) {
            try {
                this.connFactory = new MongoConnectionFactory();
                this.connection = connFactory.getConnection(db);

                return (connection != null);
            } catch (Exception e) {}
        }
        return false;
    }

    @Override
    public NodeStore createNodeStore() {

        try {
            log.info("Creating NodeStore using " + toString());
            clock.waitUntil(Revision.getCurrentTimestamp());

            MongoDocumentNodeStoreBuilder documentNodeStoreBuilder =
                MongoDocumentNodeStoreBuilder.newMongoDocumentNodeStoreBuilder()
                    .setMongoDB(connection.getMongoClient(), connection.getDBName());
            documentNodeStoreBuilder.clock(clock);

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
        } catch (IOException | RepositoryException | InterruptedException e) {
            throw new AssertionError("Cannot create test repo fixture " + toString(), e);
        }
    }

    @Override
    public void dispose(NodeStore nodeStore) {
        try {
            if (nodeStore != null && nodeStore instanceof DocumentNodeStore) {
                ((DocumentNodeStore)nodeStore).dispose();
            }

            DataStore dataStore = (DataStore) components.get(nodeStore, DataStore.class.getName());
            if (dataStore != null && dataStoreFixture != null) {
                dataStoreFixture.dispose(dataStore);

                File dataStoreFolder = (File) components.get(nodeStore, DataStore.class.getName() + ":folder");
                FileUtils.deleteQuietly(dataStoreFolder);
            }
            MongoUtils.dropDatabase(db);
            if (connection != null) {
                connection.close();
            }
        } finally {
            components.row(nodeStore).clear();
        }
    }

    @Override
    public void compactStore(NodeStore nodeStore) throws IOException, InterruptedException {
        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(10));
        VersionGarbageCollector vGC = ((DocumentNodeStore) nodeStore).getVersionGarbageCollector();
        VersionGarbageCollector.VersionGCStats stats = vGC.gc(0, TimeUnit.MILLISECONDS);
    }

    @Override
    public BlobReferenceRetriever getBlobReferenceRetriever(NodeStore nodeStore) {
        return new DocumentBlobReferenceRetriever((DocumentNodeStore) nodeStore);
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
