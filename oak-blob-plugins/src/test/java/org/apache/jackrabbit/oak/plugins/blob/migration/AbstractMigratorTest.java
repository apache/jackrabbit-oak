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

package org.apache.jackrabbit.oak.plugins.blob.migration;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.split.DefaultSplitBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public abstract class AbstractMigratorTest {

    private static final int LENGTH = 1024 * 16;

    private static final Random RANDOM = new Random();

    private File repository;

    private NodeStore nodeStore;

    private BlobStore newBlobStore;

    private BlobMigrator migrator;

    @Before
    public void setup() throws CommitFailedException, IllegalArgumentException, IOException {
        Path target = FileSystems.getDefault().getPath("target");
        repository = java.nio.file.Files.createTempDirectory(target, "migrate-").toFile();
        BlobStore oldBlobStore = createOldBlobStore(repository);
        NodeStore originalNodeStore = createNodeStore(oldBlobStore, repository);
        createContent(originalNodeStore);
        closeNodeStore();

        newBlobStore = createNewBlobStore(repository);
        DefaultSplitBlobStore splitBlobStore = new DefaultSplitBlobStore(repository.getPath(), oldBlobStore, newBlobStore);
        nodeStore = createNodeStore(splitBlobStore, repository);
        migrator = new BlobMigrator(splitBlobStore, nodeStore);

        // see OAK-6066
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

    }

    protected abstract NodeStore createNodeStore(BlobStore blobStore, File repository) throws IOException;

    protected abstract void closeNodeStore();

    protected abstract BlobStore createOldBlobStore(File repository);

    protected abstract BlobStore createNewBlobStore(File repository);

    @After
    public void teardown() throws IOException {
        closeNodeStore();
        FileUtils.deleteQuietly(repository);
    }

    @Test
    public void blobsExistsOnTheNewBlobStore() throws IOException, CommitFailedException {
        migrator.migrate();
        NodeState root = nodeStore.getRoot();
        for (int i = 1; i <= 3; i++) {
            assertPropertyOnTheNewStore(root.getChildNode("node" + i).getProperty("prop"));
        }
    }

    @Test
    public void blobsCanBeReadAfterSwitchingBlobStore() throws IOException, CommitFailedException {
        migrator.migrate();
        closeNodeStore();

        nodeStore = createNodeStore(newBlobStore, repository);
        NodeState root = nodeStore.getRoot();
        for (int i = 1; i <= 3; i++) {
            assertPropertyExists(root.getChildNode("node" + i).getProperty("prop"));
        }
    }

    private void assertPropertyExists(PropertyState property) {
        if (property.isArray()) {
            for (Blob blob : property.getValue(Type.BINARIES)) {
                assertEquals(LENGTH, blob.length());
            }
        } else {
            assertEquals(LENGTH, property.getValue(Type.BINARY).length());
        }
    }

    private void assertPropertyOnTheNewStore(PropertyState property) throws IOException {
        if (property.isArray()) {
            for (Blob blob : property.getValue(Type.BINARIES)) {
                assertPropertyOnTheNewStore(blob);
            }
        } else {
            assertPropertyOnTheNewStore(property.getValue(Type.BINARY));
        }
    }

    private void assertPropertyOnTheNewStore(Blob blob) throws IOException {
        String blobId = blob.getContentIdentity();
        assertStreamEquals(blob.getNewStream(), newBlobStore.getInputStream(blobId));
    }

    private static void createContent(NodeStore nodeStore) throws IOException, CommitFailedException {
        NodeBuilder rootBuilder = nodeStore.getRoot().builder();
        rootBuilder.child("node1").setProperty("prop", createBlob(nodeStore));
        rootBuilder.child("node2").setProperty("prop", createBlob(nodeStore));
        PropertyBuilder<Blob> builder = PropertyBuilder.array(Type.BINARY, "prop");
        builder.addValue(createBlob(nodeStore));
        builder.addValue(createBlob(nodeStore));
        builder.addValue(createBlob(nodeStore));
        rootBuilder.child("node3").setProperty(builder.getPropertyState());
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static Blob createBlob(NodeStore nodeStore) throws IOException {
        byte[] buffer = new byte[LENGTH];
        RANDOM.nextBytes(buffer);
        return new ArrayBasedBlob(buffer);
    }

    private static void assertStreamEquals(InputStream expected, InputStream actual) throws IOException {
        while (true) {
            int expectedByte = expected.read();
            int actualByte = actual.read();
            assertEquals(expectedByte, actualByte);
            if (expectedByte == -1) {
                break;
            }
        }
    }
}
