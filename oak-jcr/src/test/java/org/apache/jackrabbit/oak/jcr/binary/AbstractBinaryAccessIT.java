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
package org.apache.jackrabbit.oak.jcr.binary;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import javax.jcr.Repository;

import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.datastore.AzureDataStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.datastore.S3DataStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.nodestore.DocumentMemoryNodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.nodestore.SegmentMemoryNodeStoreFixture;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordAccessProvider;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.runners.Parameterized;

/** Base test class for testing direct HTTP access to binaries. */
public abstract class AbstractBinaryAccessIT extends AbstractRepositoryTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<?> dataStoreFixtures() {
        Collection<NodeStoreFixture> fixtures = new ArrayList<>();

        S3DataStoreFixture s3 = new S3DataStoreFixture();
        fixtures.add(new SegmentMemoryNodeStoreFixture(s3));
        fixtures.add(new DocumentMemoryNodeStoreFixture(s3));

        AzureDataStoreFixture azure = new AzureDataStoreFixture();
        fixtures.add(new SegmentMemoryNodeStoreFixture(azure));
        fixtures.add(new DocumentMemoryNodeStoreFixture(azure));

        return fixtures;
    }

    protected AbstractBinaryAccessIT(NodeStoreFixture fixture, boolean reuseNodeStore) {
        super(fixture, reuseNodeStore);
    }

    /**
     * Adjust JCR repository creation to register BlobAccessProvider (BlobStore) in Whiteboard
     * so it can be picked up by oak-jcr.
     */
    @Override
    protected Repository createRepository(NodeStore nodeStore) {
        Whiteboard wb = new DefaultWhiteboard();

        BlobStore blobStore = getNodeStoreComponent(BlobStore.class);
        if (blobStore != null && blobStore instanceof BlobAccessProvider) {
            wb.register(BlobAccessProvider.class, (BlobAccessProvider) blobStore,
                    Collections.emptyMap());

        }

        return initJcr(new Jcr(nodeStore).with(wb)).createRepository();
    }

    /** Return underlying DataStore configuration extension */
    protected ConfigurableDataRecordAccessProvider getConfigurableHttpDataRecordProvider() {
        DataStore dataStore = getNodeStoreComponent(DataStore.class);
        if (dataStore != null && dataStore instanceof ConfigurableDataRecordAccessProvider) {
            return (ConfigurableDataRecordAccessProvider) dataStore;
        }
        throw new AssertionError("issue with test setup, cannot retrieve underlying DataStore / ConfigurableDataRecordAccessProvider");
    }
}
