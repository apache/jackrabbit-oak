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
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.remote.RemoteUtilities;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the DR (Disaster Recovery) mode where segment blobs are split
 * across primary and secondary Azure storage accounts. Verifies that a
 * FileStore with the fallback policy can read from both locations, and
 * new writes only go to the primary.
 */
public class AzureDRModeTest {

    @ClassRule
    public static AzuriteDockerRule primaryAzurite = new AzuriteDockerRule();

    @ClassRule
    public static AzuriteDockerRule secondaryAzurite = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private static final String CONTAINER_NAME = "oak-dr-test";
    private static final String ROOT_PREFIX = "oak";
    private static final Pattern SEGMENT_PATTERN = Pattern.compile(RemoteUtilities.SEGMENT_FILE_NAME_PATTERN);

    private BlobContainerClient primaryReadClient;
    private BlobContainerClient primaryWriteClient;
    private BlobContainerClient primaryNoRetryClient;

    private BlobContainerClient secondaryReadClient;
    private BlobContainerClient secondaryWriteClient;
    private BlobContainerClient secondaryNoRetryClient;

    @Before
    public void setup() throws BlobStorageException {
        primaryReadClient = primaryAzurite.getReadBlobContainerClient(CONTAINER_NAME);
        primaryWriteClient = primaryAzurite.getWriteBlobContainerClient(CONTAINER_NAME);
        primaryNoRetryClient = primaryAzurite.getNoRetryBlobContainerClient(CONTAINER_NAME);

        secondaryReadClient = secondaryAzurite.getReadBlobContainerClient(CONTAINER_NAME);
        secondaryWriteClient = secondaryAzurite.getWriteBlobContainerClient(CONTAINER_NAME);
        secondaryNoRetryClient = secondaryAzurite.getNoRetryBlobContainerClient(CONTAINER_NAME);
    }

    /**
     * Write content to primary producing 10+ segments, close the FileStore,
     * then copy a subset of segment blobs (2nd, 5th, 7th, 8th) to secondary
     * and delete them from primary. Open a new FileStore in DR mode and verify
     * all content is readable. Then write new content and verify it only goes
     * to primary.
     */
    @Test
    public void testDRModeWithPartialSegmentsOnSecondary() throws Exception {
        // 1. Write enough content to primary to generate 10+ segment blobs
        AzurePersistence primaryPersistence = new AzurePersistence(primaryReadClient, primaryWriteClient, primaryNoRetryClient, ROOT_PREFIX);
        try (FileStore fs = FileStoreTestUtil.createFileStore(folder.newFolder(), primaryPersistence)) {
            SegmentNodeStore ns = SegmentNodeStoreBuilders.builder(fs).build();
            for (int i = 0; i < 20; i++) {
                NodeBuilder builder = ns.getRoot().builder();
                builder.child("node-" + i).setProperty("data", createLargeString(i));
                builder.child("node-" + i).setProperty("index", i);
                ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                fs.flush();
            }
        }

        // 2. List all segment blobs from primary
        List<BlobItem> segmentBlobs = listSegmentBlobs(primaryReadClient);
        assertTrue("Expected at least 10 segment blobs, got " + segmentBlobs.size(), segmentBlobs.size() >= 10);

        // 3. Copy 2nd, 5th, 7th, 8th segment blobs to secondary (0-indexed: 1, 4, 6, 7)
        int[] indicesToMove = {1, 4, 6, 7};
        List<BlobItem> movedBlobs = new ArrayList<>();
        for (int idx : indicesToMove) {
            if (idx < segmentBlobs.size()) {
                BlobItem blob = segmentBlobs.get(idx);
                movedBlobs.add(blob);
                copyBlobBetweenAzuriteInstances(primaryReadClient, secondaryWriteClient, blob.getName());
            }
        }

        // Also copy all non-segment blobs (closed markers, graph, bindex, etc.) to secondary
        List<BlobItem> allBlobs = listAllBlobs(primaryReadClient, ROOT_PREFIX);
        for (BlobItem blob : allBlobs) {
            String name = AzureUtilities.getName(blob);
            if (!SEGMENT_PATTERN.matcher(name).matches()) {
                copyBlobBetweenAzuriteInstances(primaryReadClient, secondaryWriteClient, blob.getName());
            }
        }

        // 4. Delete those segment blobs from primary
        for (BlobItem blob : movedBlobs) {
            primaryWriteClient.getBlobClient(blob.getName()).delete();
        }

        // Verify they are gone from primary
        Set<String> remainingPrimaryBlobs = listSegmentBlobs(primaryReadClient).stream()
                .map(BlobItem::getName).collect(Collectors.toSet());
        for (BlobItem moved : movedBlobs) {
            assertTrue("Blob should have been deleted from primary: " + moved.getName(),
                    !remainingPrimaryBlobs.contains(moved.getName()));
        }

        // Verify they exist on secondary
        Set<String> secondaryBlobNames = listSegmentBlobs(secondaryReadClient).stream()
                .map(BlobItem::getName).collect(Collectors.toSet());
        for (BlobItem moved : movedBlobs) {
            assertTrue("Blob should exist on secondary: " + moved.getName(),
                    secondaryBlobNames.contains(moved.getName()));
        }

        // 5. Capture secondary blob count before DR mode writes
        int secondaryBlobCountBefore = listAllBlobs(secondaryReadClient, ROOT_PREFIX).size();

        // 6. Open FileStore in DR mode with fallback to secondary
        ReadFallbackPolicy fallbackPolicy = new ReadFallbackPolicy(secondaryAzurite.getBlobEndpoint());
        BlobContainerClient fallbackReadClient = primaryAzurite.getContainerClientWithPolicies(
                CONTAINER_NAME, null, fallbackPolicy);

        AzurePersistence drPersistence = new AzurePersistence(
                fallbackReadClient, primaryWriteClient, primaryNoRetryClient,
                secondaryReadClient, ROOT_PREFIX, null, null);

        try (FileStore fs = FileStoreTestUtil.createFileStore(folder.newFolder(), drPersistence)) {
            SegmentNodeStore ns = SegmentNodeStoreBuilders.builder(fs).build();
            NodeState root = ns.getRoot();

            // 6. Verify ALL original content is readable
            for (int i = 0; i < 20; i++) {
                NodeState child = root.getChildNode("node-" + i);
                assertTrue("node-" + i + " should exist", child.exists());
                assertEquals(i, (long) child.getLong("index"));
                assertNotNull("data property should exist on node-" + i, child.getString("data"));
            }

            // 7. Write new content - should go to primary only
            NodeBuilder builder = ns.getRoot().builder();
            builder.child("dr-new-node").setProperty("drProp", "drValue");
            ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fs.flush();
        }

        // 8. Verify no new blobs appeared on secondary
        int secondaryBlobCountAfter = listAllBlobs(secondaryReadClient, ROOT_PREFIX).size();
        assertEquals("No new blobs should appear on secondary after DR writes",
                secondaryBlobCountBefore, secondaryBlobCountAfter);

        // 9. Verify new content is on primary
        AzurePersistence primaryOnly = new AzurePersistence(primaryReadClient, primaryWriteClient, primaryNoRetryClient, ROOT_PREFIX);
        try (FileStore fs = FileStoreTestUtil.createFileStore(folder.newFolder(), primaryOnly)) {
            SegmentNodeStore ns = SegmentNodeStoreBuilders.builder(fs).build();
            assertEquals("drValue", ns.getRoot().getChildNode("dr-new-node").getString("drProp"));
        }

        // 10. Verify new content is NOT on secondary
        AzurePersistence secondaryOnly = new AzurePersistence(secondaryReadClient, secondaryWriteClient, secondaryNoRetryClient, ROOT_PREFIX);
        try (FileStore fs = FileStoreTestUtil.createFileStore(folder.newFolder(), secondaryOnly)) {
            SegmentNodeStore ns = SegmentNodeStoreBuilders.builder(fs).build();
            assertNull("New content should not be on secondary",
                    ns.getRoot().getChildNode("dr-new-node").getString("drProp"));
        }
    }

    private String createLargeString(int seed) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("data-").append(seed).append("-line-").append(i).append('\n');
        }
        return sb.toString();
    }

    private List<BlobItem> listSegmentBlobs(BlobContainerClient client) {
        return listAllBlobs(client, ROOT_PREFIX).stream()
                .filter(blob -> SEGMENT_PATTERN.matcher(AzureUtilities.getName(blob)).matches())
                .toList();
    }

    private List<BlobItem> listAllBlobs(BlobContainerClient client, String prefix) {
        ListBlobsOptions options = new ListBlobsOptions();
        options.setPrefix(AzureUtilities.asAzurePrefix(prefix));
        return AzureUtilities.getBlobs(client, options);
    }

    private void copyBlobBetweenAzuriteInstances(BlobContainerClient source, BlobContainerClient destination, String blobName) {
        byte[] data = source.getBlobClient(blobName).downloadContent().toBytes();
        java.util.Map<String, String> metadata = source.getBlobClient(blobName).getProperties().getMetadata();
        com.azure.storage.blob.options.BlockBlobSimpleUploadOptions uploadOptions =
                new com.azure.storage.blob.options.BlockBlobSimpleUploadOptions(
                        new java.io.ByteArrayInputStream(data), data.length);
        if (metadata != null && !metadata.isEmpty()) {
            uploadOptions.setMetadata(metadata);
        }
        destination.getBlobClient(blobName).getBlockBlobClient().uploadWithResponse(
                uploadOptions, null, null);
    }
}
