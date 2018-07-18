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

import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.SECONDS;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.createFileWithBinary;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.getRandomString;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;

import javax.jcr.Binary;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.JackrabbitValueFactory;
import org.apache.jackrabbit.api.binary.BinaryDownload;
import org.apache.jackrabbit.api.binary.BinaryDownloadOptions;
import org.apache.jackrabbit.api.binary.BinaryUpload;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
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
    private Session session;
    private JackrabbitValueFactory uploadProvider;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<?> dataStoreFixtures() {
        Collection<NodeStoreFixture> fixtures = Lists.newArrayList();

        // Create a fixture using FileDataStore.  FileDataStore doesn't support the direct access features so
        // it should be a valid real-world example of how the API should behave when the implementation doesn't
        // have the feature support.
        FileDataStoreFixture fds = new FileDataStoreFixture();
        fixtures.add(new AbstractHttpBinaryIT.SegmentMemoryNodeStoreFixture(fds));
        fixtures.add(new AbstractHttpBinaryIT.DocumentMemoryNodeStoreFixture(fds));

        return fixtures;
    }

    public BinaryAccessUnsupportedIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        session = getAdminSession();
        uploadProvider = (JackrabbitValueFactory) session.getValueFactory();
    }

    protected static class FileDataStoreFixture implements AbstractHttpBinaryIT.DataStoreFixture {
        @Override
        public DataStore createDataStore() {
            return new FileDataStore();
        }
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
        String content = getRandomString(1024*20);
        InputStream stream = new ByteArrayInputStream(content.getBytes());
        Binary writeBinary = createFileWithBinary(session, "/my_path", stream);

        // Wait for binary to save
        Thread.sleep(5 * SECONDS);
        stream.close();

        assertTrue(writeBinary instanceof BinaryDownload);

        URI downloadURI = ((BinaryDownload) writeBinary).getURI(BinaryDownloadOptions.DEFAULT);
        assertNull(downloadURI);
    }
}
