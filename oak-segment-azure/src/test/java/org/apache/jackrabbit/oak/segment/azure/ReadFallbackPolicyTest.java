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
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link ReadFallbackPolicy} using two Azurite instances to simulate
 * a disaster recovery scenario where the primary storage is missing blobs
 * that exist in the secondary (backup) storage.
 */
public class ReadFallbackPolicyTest {

    @ClassRule
    public static AzuriteDockerRule primaryAzurite = new AzuriteDockerRule();

    @ClassRule
    public static AzuriteDockerRule secondaryAzurite = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private static final String CONTAINER_NAME = "oak-test";
    private static final String ROOT_PREFIX = "oak";

    private BlobContainerClient primaryReadClient;
    private BlobContainerClient primaryWriteClient;
    private BlobContainerClient primaryNoRetryClient;

    private BlobContainerClient secondaryReadClient;
    private BlobContainerClient secondaryWriteClient;
    private BlobContainerClient secondaryNoRetryClient;

    @Before
    public void setup() throws BlobStorageException {
        secondaryReadClient = secondaryAzurite.getReadBlobContainerClient(CONTAINER_NAME);
        secondaryWriteClient = secondaryAzurite.getWriteBlobContainerClient(CONTAINER_NAME);
        secondaryNoRetryClient = secondaryAzurite.getNoRetryBlobContainerClient(CONTAINER_NAME);

        primaryReadClient = primaryAzurite.getReadBlobContainerClient(CONTAINER_NAME);
        primaryWriteClient = primaryAzurite.getWriteBlobContainerClient(CONTAINER_NAME);
        primaryNoRetryClient = primaryAzurite.getNoRetryBlobContainerClient(CONTAINER_NAME);
    }

    /**
     * Verifies the policy at the blob level: a blob only on secondary is readable
     * via a primary client with the fallback policy.
     */
    @Test
    public void testFallbackAtBlobLevel() throws BlobStorageException {
        secondaryWriteClient.getBlobClient("test-blob.txt").getBlockBlobClient()
                .upload(new ByteArrayInputStream("hello".getBytes()), 5, true);

        assertFalse(primaryReadClient.getBlobClient("test-blob.txt").exists());

        ReadFallbackPolicy fallbackPolicy = new ReadFallbackPolicy(secondaryAzurite.getBlobEndpoint());
        BlobContainerClient fallbackClient = primaryAzurite.getContainerClientWithPolicies(
                CONTAINER_NAME, null, fallbackPolicy);

        String content = fallbackClient.getBlobClient("test-blob.txt").downloadContent().toString();
        assertEquals("hello", content);
    }

    /**
     * Verifies the policy does not redirect write operations.
     * A blob written via the primary client should NOT appear on the secondary.
     */
    @Test
    public void testWriteNotRedirected() throws BlobStorageException {
        ReadFallbackPolicy fallbackPolicy = new ReadFallbackPolicy(secondaryAzurite.getBlobEndpoint());
        BlobContainerClient fallbackClient = primaryAzurite.getContainerClientWithPolicies(
                CONTAINER_NAME, null, fallbackPolicy);

        fallbackClient.getBlobClient("write-test.txt").getBlockBlobClient()
                .upload(new ByteArrayInputStream("data".getBytes()), 4, true);

        assertEquals("data", primaryReadClient.getBlobClient("write-test.txt").downloadContent().toString());
        assertFalse(secondaryReadClient.getBlobClient("write-test.txt").exists());
    }

    /**
     * End-to-end FileStore test: write same content to both primary and secondary,
     * then open a FileStore on primary with fallback policy. Reads work. Then write new
     * content — it goes to primary only.
     */
    @Test
    public void testFileStoreWithFallbackPolicy() throws IOException, InvalidFileStoreVersionException, CommitFailedException, BlobStorageException {
        // 1. Write content to secondary (the "backup")
        AzurePersistence secondaryPersistence = new AzurePersistence(secondaryReadClient, secondaryWriteClient, secondaryNoRetryClient, ROOT_PREFIX);
        try (FileStore fs = FileStoreTestUtil.createFileStore(folder.newFolder(), secondaryPersistence)) {
            SegmentNodeStore ns = SegmentNodeStoreBuilders.builder(fs).build();
            NodeBuilder builder = ns.getRoot().builder();
            builder.setProperty("foo", "bar");
            builder.child("content").setProperty("title", "hello");
            ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fs.flush();
        }

        // 2. Write same content to primary
        AzurePersistence primaryPersistence = new AzurePersistence(primaryReadClient, primaryWriteClient, primaryNoRetryClient, ROOT_PREFIX);
        try (FileStore fs = FileStoreTestUtil.createFileStore(folder.newFolder(), primaryPersistence)) {
            SegmentNodeStore ns = SegmentNodeStoreBuilders.builder(fs).build();
            NodeBuilder builder = ns.getRoot().builder();
            builder.setProperty("foo", "bar");
            builder.child("content").setProperty("title", "hello");
            ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fs.flush();
        }

        // 3. Open FileStore on primary with fallback policy and verify reads
        ReadFallbackPolicy fallbackPolicy = new ReadFallbackPolicy(secondaryAzurite.getBlobEndpoint());
        BlobContainerClient fallbackReadClient = primaryAzurite.getContainerClientWithPolicies(
                CONTAINER_NAME, null, fallbackPolicy);

        AzurePersistence drPersistence = new AzurePersistence(fallbackReadClient, primaryWriteClient, primaryNoRetryClient, ROOT_PREFIX);
        try (FileStore fs = FileStoreTestUtil.createFileStore(folder.newFolder(), drPersistence)) {
            SegmentNodeStore ns = SegmentNodeStoreBuilders.builder(fs).build();
            assertEquals("bar", ns.getRoot().getString("foo"));
            assertEquals("hello", ns.getRoot().getChildNode("content").getString("title"));

            // 4. Write new content — goes to primary
            NodeBuilder builder = ns.getRoot().builder();
            builder.setProperty("newProp", "newValue");
            ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fs.flush();
        }

        // 5. Verify new content is in primary
        AzurePersistence primaryOnly = new AzurePersistence(primaryReadClient, primaryWriteClient, primaryNoRetryClient, ROOT_PREFIX);
        try (FileStore fs = FileStoreTestUtil.createFileStore(folder.newFolder(), primaryOnly)) {
            SegmentNodeStore ns = SegmentNodeStoreBuilders.builder(fs).build();
            assertEquals("newValue", ns.getRoot().getString("newProp"));
        }

        // 6. Verify new content is NOT in secondary
        AzurePersistence secondaryOnly = new AzurePersistence(secondaryReadClient, secondaryWriteClient, secondaryNoRetryClient, ROOT_PREFIX);
        try (FileStore fs = FileStoreTestUtil.createFileStore(folder.newFolder(), secondaryOnly)) {
            SegmentNodeStore ns = SegmentNodeStoreBuilders.builder(fs).build();
            assertEquals("bar", ns.getRoot().getString("foo"));
            assertNull(ns.getRoot().getString("newProp"));
        }
    }
}
