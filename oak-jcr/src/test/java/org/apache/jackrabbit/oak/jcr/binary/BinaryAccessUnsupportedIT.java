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

import static org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils.storeBinaryAndRetrieve;
import static org.junit.Assert.assertNull;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import javax.jcr.Binary;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.JackrabbitValueFactory;
import org.apache.jackrabbit.api.binary.BinaryDownload;
import org.apache.jackrabbit.api.binary.BinaryDownloadOptions;
import org.apache.jackrabbit.api.binary.BinaryUpload;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.datastore.FileDataStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.nodestore.DocumentMemoryNodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.nodestore.SegmentMemoryNodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.util.Content;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

/**
 * Test binary upload / download capabilities of the {@link JackrabbitValueFactory} interface when the underlying
 * implementation does not support these features.  If the underlying doesn't support these features the implementation
 * will return null when the methods are called and clients are expected to check for null to determine if the
 * features are supported.
 */
public class BinaryAccessUnsupportedIT extends AbstractRepositoryTest {
    private JackrabbitValueFactory uploadProvider;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<?> dataStoreFixtures() {
        Collection<NodeStoreFixture> fixtures = new ArrayList<>();

        // Create a fixture using FileDataStore.  FileDataStore doesn't support the direct access features so
        // it should be a valid real-world example of how the API should behave when the implementation doesn't
        // have the feature support.
        FileDataStoreFixture fds = new FileDataStoreFixture();
        fixtures.add(new SegmentMemoryNodeStoreFixture(fds));
        fixtures.add(new DocumentMemoryNodeStoreFixture(fds));

        return fixtures;
    }

    public BinaryAccessUnsupportedIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        uploadProvider = (JackrabbitValueFactory) getAdminSession().getValueFactory();
    }

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

    @Test
    public void testInitiateUploadUnsupportedReturnsNull() throws Exception {
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(1024*20, 10);
        assertNull(upload);
    }

    @Test
    public void testCompleteUploadUnsupportedReturnsNull() throws Exception {
        Binary binary = uploadProvider.completeBinaryUpload("fake_token");
        assertNull(binary);
    }

    @Test
    public void testGetDownloadURIUnsupportedReturnsNull() throws Exception {
        Content content = Content.createRandom(1024*20);
        Binary binary = storeBinaryAndRetrieve(getAdminSession(), "/my_path", content);

        // the returned binary could not be implementing BinaryDownload...
        if (binary instanceof BinaryDownload) {
            // ...or implement it but return null on getURI()
            URI downloadURI = ((BinaryDownload) binary).getURI(BinaryDownloadOptions.DEFAULT);
            assertNull(downloadURI);
        }
    }
}
