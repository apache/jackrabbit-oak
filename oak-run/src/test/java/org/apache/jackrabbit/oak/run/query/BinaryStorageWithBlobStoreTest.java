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
package org.apache.jackrabbit.oak.run.query;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test to verify that binary properties throw IllegalStateException with a clear error message
 * when read as STRING - this happens when they're stored externally in BlobStore.
 * Verifies the exception message includes the property name and helpful guidance (OAK-12133).
 */
public class BinaryStorageWithBlobStoreTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private Repository repository;
    private FileStore fileStore;
    private Session session;
    private BlobStore blobStore;

    @Before
    public void setup() throws Exception {
        FileDataStore fds = new FileDataStore();
        // store all binaries externally, even tiny ones
        fds.setMinRecordLength(0);
        fds.init(folder.newFolder("blobstore").getAbsolutePath());
        blobStore = new DataStoreBlobStore(fds);

        fileStore = createFileStore();
        repository = createRepository(fileStore);
        session = repository.login(new SimpleCredentials(UserConstants.DEFAULT_ADMIN_ID,
                UserConstants.DEFAULT_ADMIN_ID.toCharArray()));
    }

    @After
    public void tearDown() {
        if (session != null) {
            session.logout();
        }
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
        if (fileStore != null) {
            fileStore.close();
        }
    }

    private FileStore createFileStore() throws IOException, InvalidFileStoreVersionException {
        // force binaries > 10 bytes to be external
        return FileStoreBuilder.fileStoreBuilder(folder.getRoot())
                .withBlobStore(blobStore)
                .withBinariesInlineThreshold(10)
                .build();
    }

    private Repository createRepository(FileStore fileStore) {
        return new Jcr(new Oak(SegmentNodeStoreBuilders.builder(fileStore).build()))
                .createRepository();
    }

    @Test
    public void testSmallBinaryAsStringWithExternalBlobStore() throws Exception {
        Node test = session.getRootNode().addNode("test", "nt:unstructured");

        byte[] data = new byte[45];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        test.setProperty("binary1", session.getValueFactory().createBinary(
                new ByteArrayInputStream(data)));

        session.save();

        Property prop = test.getProperty("binary1");

        try {
            prop.getString();
            fail("Expected IllegalStateException when reading binary property as string");
        } catch (IllegalStateException e) {
            String message = e.getMessage();
            assertTrue("Exception message should mention the property name 'binary1', but was: " + message,
                    message.contains("binary1"));
            assertTrue("Exception message should mention 'Attempting to read binary property', but was: " + message,
                    message.contains("Attempting to read binary property"));
            assertTrue("Exception message should mention 'can fail if the binary is stored externally', but was: " + message,
                    message.contains("can fail if the binary is stored externally"));
        }
    }

    @Test
    public void testBinaryAsStringWithExternalBlobStore() throws Exception {
        Node test = session.getRootNode().addNode("test2", "nt:unstructured");

        byte[] data = new byte[17 * 1024];

        test.setProperty("binary2", session.getValueFactory().createBinary(
                new ByteArrayInputStream(data)));

        session.save();

        Property prop = test.getProperty("binary2");

        try {
            prop.getString();
            fail("Expected IllegalStateException when reading binary property as string");
        } catch (IllegalStateException e) {
            String message = e.getMessage();
            assertTrue("Exception message should mention the property name 'binary2', but was: " + message,
                    message.contains("binary2"));
            assertTrue("Exception message should mention 'Attempting to read binary property', but was: " + message,
                    message.contains("Attempting to read binary property"));
            assertTrue("Exception message should mention 'can fail if the binary is stored externally', but was: " + message,
                    message.contains("can fail if the binary is stored externally"));
        }
    }
}
